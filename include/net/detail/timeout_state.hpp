#pragma once

#include <atomic>
#include <chrono>

#include "net/continuation.hpp"
#include "net/coroutine.hpp"
#include "net/detail/combinator.hpp"
#include "net/detail/completion_frame.hpp"
#include "net/detail/storage.hpp"
#include "net/io_env.hpp"
#include "net/timer.hpp"

// timeout() / delay() 挂起期间的状态里与被限时操作类型无关的那部分，按 io_context 复用：定时器连同后端的
// timer_impl 留给下一次，awaiter 只剩操作、结果与一个完成帧，放得进协程帧的 awaiter 槽（CO2_AWAIT_STORAGE_SIZE），
// 不必每次装箱、建定时器。
//
// stop_source 每次挂起新建：被限时的操作可能把 stop_token 交给比它活得久的东西（例如它启动的后台链），
// 复用会让那边看到下一个使用者的停止请求。

namespace net {

struct io_context;

namespace detail {

// 从任意线程请求取消 wait（与 stop_token 回调同一路径）：已排队的以 operation_aborted 完成；尚未排队的
// 记标记、排队时同步中止；已经结束的留下一个过期标记，由之后的 await_resume（finish）清掉。所以可能与它
// 并发时不能 await_resume：先用 timer_completion_error 看结果，等不再并发（归还时）再 await_resume。
void request_timer_cancel(timer_wait_awaitable const& wait) noexcept;
std::error_code timer_completion_error(timer_wait_awaitable const& wait) noexcept;

struct timeout_state {
    static constexpr unsigned none = 0U;
    static constexpr unsigned inner_won = 1U;
    static constexpr unsigned timer_won = 2U;

    explicit timeout_state(io_context& context) : timer{context} {}

    timeout_state(timeout_state const&) = delete;
    timeout_state& operator=(timeout_state const&) = delete;

    // 挂起开始：记下父协程；子环境 = 父执行器 + 本状态的 stop_token + 父帧分配器；父 stop_token 转发过来。
    // 定时器的环境没有 stop_token：被限时的操作先完成时直接 request_timer_cancel，常见路径上不必 request_stop。
    // 计数：被限时的操作 + 定时器 + 启动方自己那一份。
    void begin(coroutine_handle<> const h, io_env const* const env) {
        parent.h = h;
        parent_env = env;
        child_env.executor = env->executor;
        child_env.stop_token = source.get_token();
        child_env.frame_allocator = env->frame_allocator;
        timer_env.executor = env->executor;
        timer_env.frame_allocator = env->frame_allocator;
        wait = timer.wait(); // 操作一发起就可能在别的线程上完成并撤定时器：wait 要先就位
        remaining.store(3U, std::memory_order_relaxed);
        winner.store(none, std::memory_order_relaxed);
        if (env->stop_token.stop_possible()) forwarding.emplace(env->stop_token, forward_stop{&source});
    }

    // 被限时的操作已发起、没有同步完成：武装定时器。已到期、或操作已经完成并撤了定时器时同步走完成回调。
    // may_have_passed 为假时 at 必在未来，省掉一次 now()。
    void arm(std::chrono::steady_clock::time_point const at, bool const may_have_passed) {
        timer.expires_at(at);
        timer_frame.set(&on_timer_done, this);
        timer_armed = true;
        if (may_have_passed && wait.await_ready()) {
            on_timer_done(this);
        } else if (wait.await_suspend(timer_frame.handle(), &timer_env) == timer_frame.handle()) {
            on_timer_done(this);
        }
    }

    // 启动方放下自己那一份：返回 true 表示两个参与者都已完成，不必挂起。
    bool finish_start() noexcept { return remaining.fetch_sub(1U, std::memory_order_acq_rel) == 1U; }

    // 被限时的操作完成（结果已取走）：先到则撤定时器——尚未武装的，武装时同步中止；已经到期的，留下的
    // 过期标记由归还时的 await_resume 清掉（release_timeout_state）。
    coroutine_handle<> inner_done() noexcept {
        if (claim(inner_won)) request_timer_cancel(wait);
        return participant_done();
    }

    bool timer_won_race() const noexcept { return winner.load(std::memory_order_acquire) == timer_won; }

    steady_timer timer;
    timer_wait_awaitable wait{nullptr};
    completion_frame timer_frame;
    io_env child_env;
    io_env timer_env;
    io_env const* parent_env = nullptr;
    continuation parent;
    std::atomic<unsigned> remaining{0U};
    std::atomic<unsigned> winner{none};
    bool timer_armed = false; // 定时器的 wait 还没 await_resume（归还时补上）
    stop_source source{nostopstate};
    late_init<stop_callback<forward_stop>> forwarding; // 晚于 source 构造、先于它析构
    timeout_state* next_free = nullptr;                // io_context 的空闲链

  private:
    bool claim(unsigned const who) noexcept {
        auto expected = none;
        return winner.compare_exchange_strong(expected, who, std::memory_order_acq_rel);
    }

    // 一个参与者完成：最后一个恢复父协程（经父执行器 dispatch；已在其线程上则对称转移）。
    coroutine_handle<> participant_done() noexcept {
        if (remaining.fetch_sub(1U, std::memory_order_acq_rel) != 1U) return nullptr;
        return parent_env->executor.dispatch(parent);
    }

    // 被限时的操作可能正在别的线程上 request_timer_cancel：这里只看结果，不 await_resume。
    static coroutine_handle<> on_timer_done(void* const user) noexcept {
        auto* const self = static_cast<timeout_state*>(user);
        if (not timer_completion_error(self->wait) && self->claim(timer_won)) self->source.request_stop(); // 到期：撤操作
        return self->participant_done();
    }
};

// 从 context 的空闲链取一份（没有就新建）；with_stop_source 为真时给它一个新的 stop_source。
timeout_state* acquire_timeout_state(io_context& context, bool with_stop_source);
// 还给它的 io_context：撤掉父 stop_token 的转发、放掉 stop_source。定时器不能还有未完成的 wait。
void release_timeout_state(timeout_state* state) noexcept;

// awaiter 里持有一份状态；析构时归还。声明在 awaiter 的其它成员之前——最后析构，被限时的操作
//（可能是一个 task，帧里还拿着子 stop_token）先于它销毁。
struct timeout_state_ref {
    timeout_state_ref() noexcept = default;
    timeout_state_ref(timeout_state_ref&& other) noexcept : state{other.state} { other.state = nullptr; }
    timeout_state_ref(timeout_state_ref const&) = delete;
    timeout_state_ref& operator=(timeout_state_ref const&) = delete;
    timeout_state_ref& operator=(timeout_state_ref&&) = delete;

    ~timeout_state_ref() {
        if (state != nullptr) release_timeout_state(state);
    }

    timeout_state& acquire(io_context& context, bool const with_stop_source) {
        state = acquire_timeout_state(context, with_stop_source);
        return *state;
    }

    timeout_state* state = nullptr;
};

} // namespace detail
} // namespace net
