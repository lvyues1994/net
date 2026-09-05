#pragma once

#include <atomic>
#include <cstddef>
#include <exception>
#include <system_error>
#include <tuple>
#include <type_traits>
#include <utility>

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
// 每个子 awaitable 由一个小的 runner 协程（task<void>）co_await：runner 的 promise 把组合子
// 的 child_env（父执行器、组合子自己的 stop_token、父帧分配器）注入子 awaitable，并捕获它
// 抛出的异常。runner 的续体是嵌在组合子里的 completion_frame：子完成时对称转移到它，组合
// 子记录结果、必要时向兄弟请求停止，最后一个到达者把父协程经父执行器 dispatch 恢复。
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

// ---- runner 协程 ----

template <class A, class R>
auto run_child(A awaitable, result_storage<R>* out) CO2_BEG(task<void>, (awaitable, out)) {
    CO2_AWAIT_SET(*out, std::move(awaitable));
}
CO2_END

template <class A>
auto run_child_void(A awaitable, result_storage<void>* out) CO2_BEG(task<void>, (awaitable, out)) {
    CO2_AWAIT(std::move(awaitable));
    out->emplace();
}
CO2_END

template <class A, class R>
task<void> make_runner(A&& awaitable, result_storage<R>* const out, std::false_type) {
    return run_child<typename std::decay<A>::type, R>(std::forward<A>(awaitable), out);
}

template <class A>
task<void> make_runner(A&& awaitable, result_storage<void>* const out, std::true_type) {
    return run_child_void<typename std::decay<A>::type>(std::forward<A>(awaitable), out);
}

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

// 每个子任务一格：续体帧 + runner。
struct child_slot {
    completion_frame frame;
    task<void> runner;
    void* owner = nullptr;
    std::size_t index = 0U;
};

} // namespace detail
} // namespace net
