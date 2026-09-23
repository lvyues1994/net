#pragma once

#include <atomic>
#include <chrono>
#include <exception>
#include <type_traits>
#include <utility>

#include "co2/contract.hpp"
#include "co2/detail/awaitable.hpp"

#include "net/coroutine.hpp"
#include "net/detail/combinator.hpp"
#include "net/detail/completion_frame.hpp"
#include "net/detail/storage.hpp"
#include "net/detail/timeout_state.hpp"
#include "net/error.hpp"
#include "net/io_awaitable_promise_base.hpp"
#include "net/io_context.hpp"
#include "net/io_env.hpp"
#include "net/io_result.hpp"
#include "net/timer.hpp"

// timeout / delay（Corosio 的 timeout.hpp / delay.hpp 同形）：
//
//   CO2_AWAIT_SET(r, net::timeout(sock.read_some(buf), std::chrono::seconds{5}));
//   if (r.ec == net::cond::timeout) ...          // 到期：操作已被取消，r 是默认载荷 + error::timed_out
//
//   CO2_AWAIT(net::delay(std::chrono::milliseconds{100}));   // 睁着眼睛睡一会；stop_token 可打断
//
// timeout 让一个返回 io_result 的 IoAwaitable 与一个截止时刻赛跑：给操作一个插入的 stop_token，武装一个
// 定时器；操作先完成就原样返回它的结果（成功、错误或异常），定时器先到就请求停止、等操作以
// operation_aborted 结束，然后给出 ec == error::timed_out（等价 cond::timeout、std::errc::timed_out）。
// 操作到期后仍以别的结果结束（数据恰好到了、或真的出错了）时返回那个结果——不丢数据、不掩盖错误。
// 时长从挂起时刻起算；给 time_point 则是绝对截止。
//
// 与 when_any(op, timer.wait()) 相比：没有 runner 协程帧；定时器与挂起期间的其它状态按 io_context 复用
//（detail/timeout_state.hpp），每次挂起只新建一个 stop_source；操作在 await_ready 里就完成时什么都不建。
// 被限时的可以是 task。前置条件：等待协程的执行器属于某个 io_context（定时器从它那里来）。

namespace net {
namespace detail {

struct deadline_spec {
    std::chrono::steady_clock::time_point at;
    std::chrono::nanoseconds after;
    bool absolute;

    std::chrono::steady_clock::time_point resolve() const noexcept {
        return absolute ? at : std::chrono::steady_clock::now() + after;
    }

    // resolve() 的结果可能已经不在未来（绝对时刻，或零时长）：武装前要查一次是否已到期。
    bool may_have_passed() const noexcept { return absolute || after <= std::chrono::nanoseconds::zero(); }
};

template <class Rep, class Period>
std::chrono::nanoseconds clamp_to_nanoseconds(std::chrono::duration<Rep, Period> const d) noexcept {
    // 窄类型的 duration 转 nanoseconds 会溢出：先用 double 夹到范围内。
    using dsec = std::chrono::duration<double>;
    if (dsec(d) >= dsec(std::chrono::nanoseconds::max())) return std::chrono::nanoseconds::max();
    if (dsec(d) <= dsec(std::chrono::nanoseconds::zero())) return std::chrono::nanoseconds::zero();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(d);
}

template <class R> R timed_out_result() {
    R r{};
    r.ec = make_error_code(error::timed_out);
    return r;
}

template <class A> struct timeout_awaitable {
    using result_type = awaitable_result_t<A>;
    static_assert(is_io_result<result_type>::value, "net::timeout requires an awaitable that completes with io_result");
    static_assert(std::is_default_constructible<result_type>::value,
                  "net::timeout needs a default-constructible io_result for the timed-out case");

    timeout_awaitable(A awaitable_, deadline_spec const deadline_) : holder(std::move(awaitable_)), deadline{deadline_} {}

    // 只在启动前移动（进父帧的 awaiter 槽）。
    timeout_awaitable(timeout_awaitable&& other) noexcept(std::is_nothrow_move_constructible<A>::value)
        : holder(std::move(other.holder)), deadline{other.deadline} {
        CO2_CONTRACT_CHECK(other.state.state == nullptr);
    }

    timeout_awaitable(timeout_awaitable const&) = delete;
    timeout_awaitable& operator=(timeout_awaitable const&) = delete;
    timeout_awaitable& operator=(timeout_awaitable&&) = delete;

    // 操作不用等就完成：不建定时器。带环境的版本把父环境转给操作（停止已请求时它以 operation_aborted 结束）。
    bool await_ready() { return start(nullptr); }
    bool await_ready(io_env const* const env) { return start(env); }

    coroutine_handle<> await_suspend(coroutine_handle<> const h, io_env const* const env) {
        auto* const context = io_context_of(env->executor.context());
        CO2_CONTRACT_CHECK(context != nullptr && "net::timeout: the awaiting coroutine's executor must belong to an io_context");
        auto& s = state.acquire(*context, true);
        s.begin(h, env);
        inner_frame.set(&on_inner_done, this);

        // 先发起操作。同步完成（例如已被取消）时不必武装定时器。
        auto const started = holder.get().await_suspend(inner_frame.handle(), &s.child_env);
        if (started == inner_frame.handle()) {
            capture_inner();
            return h;
        }
        s.arm(deadline.resolve(), deadline.may_have_passed());
        // 放下启动方那一份；两个都已完成（定时器同步到期且操作被同步取消）就不挂起。操作要求转移过去的
        //（task 的帧）从这里开始跑。
        if (s.finish_start()) return h;
        return started;
    }

    result_type await_resume() {
        if (exception) std::rethrow_exception(exception);
        auto result = storage.take();
        // 定时器赢了、操作因此被取消 → 超时；操作到期后仍以别的结果结束（数据恰好到了、真的出错了）
        // → 那个结果。
        if (state.state != nullptr && state.state->timer_won_race() && result.ec == error::operation_aborted)
            return timed_out_result<result_type>();
        return result;
    }

  private:
    bool start(io_env const* const env) {
        holder.start();
        if (not await_ready_with(holder.get(), env)) return false;
        capture_inner();
        return true;
    }

    void capture_inner() noexcept {
        try {
            storage.emplace(holder.get().await_resume());
        } catch (...) {
            exception = std::current_exception();
        }
        holder.finish();
    }

    static coroutine_handle<> on_inner_done(void* const user) {
        auto* const self = static_cast<timeout_awaitable*>(user);
        self->capture_inner();
        return self->state.state->inner_done();
    }

    timeout_state_ref state; // 最先声明：最后析构（见 timeout_state_ref）
    awaiter_holder<A> holder;
    deadline_spec deadline;
    completion_frame inner_frame;
    result_storage<result_type> storage;
    std::exception_ptr exception;
};

struct delay_awaitable {
    explicit delay_awaitable(deadline_spec const deadline_) noexcept : deadline{deadline_} {}

    delay_awaitable(delay_awaitable&& other) noexcept : deadline{other.deadline} {
        CO2_CONTRACT_CHECK(other.state.state == nullptr);
    }

    delay_awaitable(delay_awaitable const&) = delete;
    delay_awaitable& operator=(delay_awaitable const&) = delete;
    delay_awaitable& operator=(delay_awaitable&&) = delete;

    bool await_ready() const noexcept { return false; }

    coroutine_handle<> await_suspend(coroutine_handle<> const h, io_env const* const env) {
        auto* const context = io_context_of(env->executor.context());
        CO2_CONTRACT_CHECK(context != nullptr && "net::delay: the awaiting coroutine's executor must belong to an io_context");
        auto& s = state.acquire(*context, false);
        s.timer.expires_at(deadline.resolve());
        s.wait = s.timer.wait();
        if (await_ready_with(s.wait, env)) return h; // 停止已请求：不等，operation_aborted
        return s.wait.await_suspend(h, env);          // 定时器直接恢复父协程；stop_token 由它处理
    }

    io_result<> await_resume() noexcept { return state.state->wait.await_resume(); }

  private:
    timeout_state_ref state;
    deadline_spec deadline;
};

} // namespace detail

// 让 awaitable 与时限赛跑（时长从挂起时刻起算）。
template <class A, class Rep, class Period>
detail::timeout_awaitable<A> timeout(A awaitable, std::chrono::duration<Rep, Period> const after) {
    static_assert(is_io_awaitable<A>::value, "net::timeout requires an IoAwaitable");
    return detail::timeout_awaitable<A>{std::move(awaitable),
                                        detail::deadline_spec{{}, detail::clamp_to_nanoseconds(after), false}};
}

// 让 awaitable 与绝对截止时刻赛跑。
template <class A>
detail::timeout_awaitable<A> timeout(A awaitable, std::chrono::steady_clock::time_point const at) {
    static_assert(is_io_awaitable<A>::value, "net::timeout requires an IoAwaitable");
    return detail::timeout_awaitable<A>{std::move(awaitable), detail::deadline_spec{at, {}, true}};
}

// 等一段时间。返回 io_result<>：被 stop_token 打断时 ec == error::operation_aborted。
template <class Rep, class Period>
detail::delay_awaitable delay(std::chrono::duration<Rep, Period> const after) noexcept {
    return detail::delay_awaitable{detail::deadline_spec{{}, detail::clamp_to_nanoseconds(after), false}};
}

inline detail::delay_awaitable delay(std::chrono::steady_clock::time_point const at) noexcept {
    return detail::delay_awaitable{detail::deadline_spec{at, {}, true}};
}

} // namespace net
