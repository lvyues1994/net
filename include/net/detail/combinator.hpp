#pragma once

#include <atomic>
#include <cstddef>
#include <exception>
#include <system_error>
#include <tuple>
#include <type_traits>
#include <utility>

#include "co2/contract.hpp"
#include "co2/detail/awaitable.hpp"

#include "net/continuation.hpp"
#include "net/coroutine.hpp"
#include "net/detail/completion_frame.hpp"
#include "net/detail/storage.hpp"
#include "net/io_awaitable_promise_base.hpp"
#include "net/io_env.hpp"
#include "net/io_result.hpp"
#include "net/task.hpp"

// when_all / when_any 的公共骨架。
//
// 子 awaitable 由组合子直接驱动（child<A>），不经 runner 协程：组合子把 child_env（父执行器、组合子
// 自己的 stop_token、父帧分配器）交给子 awaiter 的 await_ready(env) / await_suspend，续体是嵌在
// child 里的 completion_frame——子完成时对称转移到它，组合子取走结果（捕获 await_resume 抛出的异常）、
// 必要时向兄弟请求停止，最后一个到达者把父协程经父执行器 dispatch 恢复。子 awaiter 的 await_suspend
// 要求转移过去的句柄（task 的帧）由启动方恢复，跑到第一个挂起点为止。
//
// 计数初值 N + 1：启动方放下自己那一份时若已归零，说明全部子任务同步完成，await_suspend
// 直接返回父句柄（不挂起）。

namespace net {
namespace detail {

// ---- 子结果的载荷 ----
//
//   io_result<T>       → T            （单值解包）
//   io_result<>        → tuple<>      （无载荷）
//   io_result<T, U..>  → tuple<T, U..>
//   非 io_result 的 R  → R
//   void               → tuple<>

template <class R> struct payload_of {
    using type = R;
    static constexpr bool is_io = false;
};
template <> struct payload_of<void> {
    using type = std::tuple<>;
    static constexpr bool is_io = false;
};
template <> struct payload_of<io_result<>> {
    using type = std::tuple<>;
    static constexpr bool is_io = true;
};
template <class T> struct payload_of<io_result<T>> {
    using type = T;
    static constexpr bool is_io = true;
};
template <class T, class U, class... Rest> struct payload_of<io_result<T, U, Rest...>> {
    using type = std::tuple<T, U, Rest...>;
    static constexpr bool is_io = true;
};

// 载荷作为 tuple 片段（tuple<> 载荷贡献空片段，被 tuple_cat 吃掉）。
template <class P> struct payload_fragment {
    using type = std::tuple<P>;
};
template <> struct payload_fragment<std::tuple<>> {
    using type = std::tuple<>;
};

template <class R> using payload_t = typename payload_of<R>::type;
template <class R> using fragment_t = typename payload_fragment<payload_t<R>>::type;

// 取走一个子结果的 tuple 片段。
template <class R> struct extract_fragment {
    static fragment_t<R> apply(result_storage<R>& storage) { return fragment_t<R>(storage.take()); }
};
template <> struct extract_fragment<void> {
    static std::tuple<> apply(result_storage<void>& storage) {
        storage.take();
        return {};
    }
};
template <> struct extract_fragment<io_result<>> {
    static std::tuple<> apply(result_storage<io_result<>>& storage) {
        storage.take();
        return {};
    }
};
template <class T> struct extract_fragment<io_result<T>> {
    static std::tuple<T> apply(result_storage<io_result<T>>& storage) {
        return std::tuple<T>(std::move(storage.get().value));
    }
};
template <class T, class U, class... Rest> struct extract_fragment<io_result<T, U, Rest...>> {
    static std::tuple<std::tuple<T, U, Rest...>> apply(result_storage<io_result<T, U, Rest...>>& storage) {
        return std::tuple<std::tuple<T, U, Rest...>>(std::move(storage.get().values));
    }
};

// 子结果里的 error_code（非 io_result 的结果没有）。
template <class R> struct error_of {
    static std::error_code apply(result_storage<R>&) noexcept { return {}; }
};
template <class... Ts> struct error_of<io_result<Ts...>> {
    static std::error_code apply(result_storage<io_result<Ts...>>& storage) noexcept {
        return storage.hasValue() ? storage.get().ec : std::error_code{};
    }
};

// 由载荷 tuple 构造 io_result<Ps...>。
template <class Tuple> struct make_io_result_from;
template <> struct make_io_result_from<std::tuple<>> {
    using type = io_result<>;
    static type apply(std::error_code const ec, std::tuple<>&&) { return type{ec}; }
};
template <class P> struct make_io_result_from<std::tuple<P>> {
    using type = io_result<P>;
    static type apply(std::error_code const ec, std::tuple<P>&& values) {
        return type{ec, std::move(std::get<0>(values))};
    }
};
template <class P, class Q, class... Rest> struct make_io_result_from<std::tuple<P, Q, Rest...>> {
    using type = io_result<P, Q, Rest...>;
    static type apply(std::error_code const ec, std::tuple<P, Q, Rest...>&& values) {
        return type{ec, std::move(values)};
    }
};

template <class Tuple> struct tuple_or_void {
    using type = Tuple;
    static type apply(Tuple&& values) { return std::move(values); }
};
template <> struct tuple_or_void<std::tuple<>> {
    using type = void;
    static void apply(std::tuple<>&&) {}
};

template <bool... Bs> struct any_true : std::false_type {};
template <bool... Bs> struct any_true<true, Bs...> : std::true_type {};
template <bool... Bs> struct any_true<false, Bs...> : any_true<Bs...> {};

// ---- 直接驱动的子任务 ----

// 一个 awaitable 与它的 awaiter：本身就是 awaiter（套接字操作、task、ready()）时只存一份；否则先存
// awaitable，开始时就地取出 awaiter。
template <class A, bool = std::is_same<co2::detail::AwaiterOf<A>, A>::value> struct awaiter_holder {
    explicit awaiter_holder(A a) : awaiter(std::move(a)) {}
    awaiter_holder(awaiter_holder&& other) noexcept(std::is_nothrow_move_constructible<A>::value)
        : awaiter(std::move(other.awaiter)) {}

    void start() noexcept {}
    A& get() noexcept { return awaiter; }
    void finish() noexcept {}

    A awaiter;
};

template <class A> struct awaiter_holder<A, false> {
    using inner_type = co2::detail::AwaiterOf<A>;

    explicit awaiter_holder(A a) : awaitable(std::move(a)) {}
    awaiter_holder(awaiter_holder&& other) noexcept(std::is_nothrow_move_constructible<A>::value)
        : awaitable(std::move(other.awaitable)) {
        CO2_CONTRACT_CHECK(not other.inner.hasValue());
    }

    void start() { inner.emplace(co2::detail::getAwaiter(std::move(awaitable))); }
    inner_type& get() noexcept { return inner.get(); }
    void finish() noexcept { inner.reset(); }

    A awaitable;
    late_init<inner_type> inner;
};

template <class R> struct capture_result {
    template <class Awaiter> static void apply(Awaiter& awaiter, result_storage<R>& out) {
        out.emplace(awaiter.await_resume());
    }
};

template <> struct capture_result<void> {
    template <class Awaiter> static void apply(Awaiter& awaiter, result_storage<void>& out) {
        awaiter.await_resume();
        out.emplace();
    }
};

// 按 await_suspend 的返回类型挂起一个子 awaiter；返回 true 表示它其实已同步完成。它要求转移过去的句柄
//（task 的帧）放进 *next，由调用方恢复。
template <class Awaiter>
bool suspend_child(Awaiter& awaiter, coroutine_handle<> const self, io_env const* const env, coroutine_handle<>* const next,
                   coroutine_handle<>) {
    auto const target = awaiter.await_suspend(self, env);
    if (target == self) return true;
    if (target && target != noop_coroutine()) *next = target;
    return false;
}

template <class Awaiter>
bool suspend_child(Awaiter& awaiter, coroutine_handle<> const self, io_env const* const env, coroutine_handle<>*, bool) {
    return not awaiter.await_suspend(self, env);
}

template <class Awaiter>
bool suspend_child(Awaiter& awaiter, coroutine_handle<> const self, io_env const* const env, coroutine_handle<>*, int) {
    awaiter.await_suspend(self, env);
    return false;
}

// void → int、bool → bool，其余（coroutine_handle<>、带 promise 类型的句柄）→ coroutine_handle<>。
template <class T> struct suspend_tag {
    using type = typename std::conditional<std::is_same<T, bool>::value, bool, coroutine_handle<>>::type;
};
template <> struct suspend_tag<void> {
    using type = int;
};

template <class Awaiter>
using suspend_tag_t = typename suspend_tag<decltype(std::declval<Awaiter&>().await_suspend(
    std::declval<coroutine_handle<>>(), std::declval<io_env const*>()))>::type;

// 一个子任务：它的 awaiter、结果格与续体帧。只在启动前移动（帧的句柄交出去之后不能动）。
template <class A> struct child {
    using result_type = awaitable_result_t<A>;

    explicit child(A awaitable) : holder(std::move(awaitable)) {}
    child(child&& other) noexcept(std::is_nothrow_move_constructible<A>::value) : holder(std::move(other.holder)) {}

    child(child const&) = delete;
    child& operator=(child const&) = delete;
    child& operator=(child&&) = delete;

    // 发起：返回 true 表示已同步完成（*error 是它抛出的异常，没有则空），调用方自己记账；false 表示完成时
    // frame 的回调会被调用（回调里调 capture）。取出 awaiter、await_ready、await_suspend 抛出都算同步完成。
    // 要求转移过去的句柄在 try 之外恢复：它跑起来之后抛出什么都不再是"发起失败"（与 runner 协程时一样 terminate）。
    bool launch(io_env const* const env, std::exception_ptr& error) noexcept {
        using awaiter_type = typename std::remove_reference<decltype(holder.get())>::type;
        coroutine_handle<> next;
        try {
            holder.start();
            auto& awaiter = holder.get();
            if (await_ready_with(awaiter, env) ||
                suspend_child(awaiter, frame.handle(), env, &next, suspend_tag_t<awaiter_type>{})) {
                error = capture();
                return true;
            }
        } catch (...) {
            holder.finish();
            error = std::current_exception();
            return true;
        }
        if (next) next.resume(); // 跑到它的第一个挂起点；它直接完成时经 frame 的回调记账
        return false;
    }

    // 取走结果；返回 await_resume 抛出的异常（没有则空）。之后不再碰 awaiter。
    std::exception_ptr capture() noexcept {
        std::exception_ptr error;
        try {
            capture_result<result_type>::apply(holder.get(), storage);
        } catch (...) {
            error = std::current_exception();
        }
        holder.finish();
        return error;
    }

    awaiter_holder<A> holder;
    result_storage<result_type> storage;
    completion_frame frame;
};

// ---- 共享状态 ----

struct forward_stop {
    stop_source* source;
    void operator()() const noexcept { source->request_stop(); }
};

struct combinator_state {
    static constexpr std::size_t none = static_cast<std::size_t>(-1);

    combinator_state() = default;

    // 只允许在启动前移动。
    combinator_state(combinator_state&& other) noexcept : source(std::move(other.source)) {}

    combinator_state(combinator_state const&) = delete;
    combinator_state& operator=(combinator_state const&) = delete;
    combinator_state& operator=(combinator_state&&) = delete;

    void begin(coroutine_handle<> const awaiting, io_env const* const env, std::size_t const count) {
        parent_env = env;
        parent.h = awaiting;
        child_env.executor = env->executor;
        child_env.stop_token = source.get_token();
        child_env.frame_allocator = env->frame_allocator;
        remaining.store(count + 1U, std::memory_order_relaxed);
        if (env->stop_token.stop_possible())
            forwarding.emplace(env->stop_token, forward_stop{&source});
    }

    // 启动方放下自己那一份：返回 true 表示子任务全部已完成。
    bool finish_start() noexcept {
        return remaining.fetch_sub(1U, std::memory_order_acq_rel) == 1U;
    }

    // 一个子任务完成：返回 true 表示它是最后一个。
    bool child_done() noexcept { return remaining.fetch_sub(1U, std::memory_order_acq_rel) == 1U; }

    // 时间上第一个失败者记录。返回 true 表示本次记录成功。
    bool record_exception(std::exception_ptr error) noexcept {
        auto expected = false;
        if (not has_exception.compare_exchange_strong(expected, true, std::memory_order_acq_rel))
            return false;
        first_exception = std::move(error);
        return true;
    }

    bool record_error(std::error_code const ec) noexcept {
        auto expected = false;
        if (not has_error.compare_exchange_strong(expected, true, std::memory_order_acq_rel))
            return false;
        first_error = ec;
        return true;
    }

    // when_any：第一个成功者。返回 true 表示 index 成为赢家。
    bool claim_winner(std::size_t const index) noexcept {
        auto expected = none;
        return winner.compare_exchange_strong(expected, index, std::memory_order_acq_rel);
    }

    std::size_t winner_index() const noexcept { return winner.load(std::memory_order_acquire); }

    coroutine_handle<> resume_parent() { return parent_env->executor.dispatch(parent); }

    io_env child_env;
    io_env const* parent_env = nullptr;
    continuation parent;
    std::atomic<std::size_t> remaining{0U};
    std::atomic<std::size_t> winner{none};
    std::atomic<bool> has_exception{false};
    std::atomic<bool> has_error{false};
    std::exception_ptr first_exception;
    std::error_code first_error;
    stop_source source;
    late_init<stop_callback<forward_stop>> forwarding; // 晚于 source 构造、先于它析构
};
} // namespace detail
} // namespace net
