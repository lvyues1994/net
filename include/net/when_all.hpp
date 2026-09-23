#pragma once

#include <array>
#include <cstddef>
#include <exception>
#include <memory>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "co2/contract.hpp"

#include "net/detail/combinator.hpp"

// when_all（P4100R1 §8.7，语义见 P4124R0 §2）：并发等待一组 IoAwaitable，全部完成后一起
// 交付。组合子知道 I/O 的结果约定——error_code 在第一位——而不是按通道分派：
//
//   CO2_AWAIT_SET(r, net::when_all(s.read_some(b1), s.read_some(b2)));
//   // r : io_result<std::size_t, std::size_t>：一个 ec，两个字节数（C++17: auto [ec, n1, n2]）
//
// 行为（P4124R0 表 2.2）：
//   - 全部 !ec：返回全部结果，无取消；
//   - 任一子任务返回 ec：向兄弟请求停止（组合子自己的 stop_source），时间上第一个 ec 胜出，
//     其余结果原样保留（部分传输的字节数可见但不保证有意义）；
//   - 任一子任务抛出：捕获、请求停止，全部完成后重抛时间上第一个异常；异常优先于 ec；
//   - 父 stop_token 的请求转发给全部子任务。
//
// 结果类型：子任务返回 io_result<T>/io_result<>/io_result<T, U..> 时载荷分别为 T / 无 /
// tuple<T, U..>，结果是 io_result<载荷...>；没有任何子任务返回 io_result 时结果是
// std::tuple<载荷...>（全部 void 时为 void）。范围重载给出 io_result<std::vector<载荷>>。

namespace net {
namespace detail {

template <class... As> struct when_all_result {
    using fragments = decltype(std::tuple_cat(std::declval<fragment_t<awaitable_result_t<As>>>()...));
    static constexpr bool is_io = any_true<payload_of<awaitable_result_t<As>>::is_io...>::value;
    using type = typename std::conditional<is_io, typename make_io_result_from<fragments>::type,
                                           typename tuple_or_void<fragments>::type>::type;
};

template <class... As> struct when_all_awaitable {
    static constexpr std::size_t count = sizeof...(As);
    using result_type = typename when_all_result<As...>::type;
    using fragments = typename when_all_result<As...>::fragments;

    explicit when_all_awaitable(As... awaitables_) : children(std::move(awaitables_)...) {}

    // 只在启动前移动（进父帧的 awaiter 槽）。
    when_all_awaitable(when_all_awaitable&& other) noexcept
        : children(std::move(other.children)), state(std::move(other.state)) {}

    when_all_awaitable(when_all_awaitable const&) = delete;
    when_all_awaitable& operator=(when_all_awaitable const&) = delete;
    when_all_awaitable& operator=(when_all_awaitable&&) = delete;

    bool await_ready() const noexcept { return count == 0U; }

    coroutine_handle<> await_suspend(coroutine_handle<> const awaiting, io_env const* const env) {
        state.begin(awaiting, env, count);
        // 依次发起；已同步完成的子任务报了错会先撤兄弟，后发起的看到停止已请求就不执行。
        launch_all(std::make_index_sequence<count>{});
        if (state.finish_start()) return awaiting;
        return noop_coroutine();
    }

    result_type await_resume() {
        if (state.has_exception.load(std::memory_order_acquire))
            std::rethrow_exception(state.first_exception);
        return collect(std::make_index_sequence<count>{},
                       std::integral_constant<bool, when_all_result<As...>::is_io>{});
    }

  private:
    template <std::size_t I> using child_at = typename std::tuple_element<I, std::tuple<child<As>...>>::type;
    template <std::size_t I> using result_at = typename child_at<I>::result_type;

    template <std::size_t... I> void launch_all(std::index_sequence<I...>) noexcept {
        int const ordered[] = {0, (launch_one<I>(), 0)...};
        static_cast<void>(ordered);
    }

    template <std::size_t I> void launch_one() noexcept {
        auto& c = std::get<I>(children);
        c.frame.set(&on_child_done<I>, this);
        std::exception_ptr error;
        if (c.launch(&state.child_env, error)) static_cast<void>(child_finished<I>(std::move(error)));
    }

    template <std::size_t I> static coroutine_handle<> on_child_done(void* const user) {
        auto* const self = static_cast<when_all_awaitable*>(user);
        if (self->template child_finished<I>(std::get<I>(self->children).capture())) return self->state.resume_parent();
        return nullptr;
    }

    // 一个子任务结束（结果已取走）：异常或 ec 撤兄弟。返回 true 表示它是最后一个。
    template <std::size_t I> bool child_finished(std::exception_ptr error) noexcept {
        if (error) {
            state.record_exception(std::move(error));
            state.source.request_stop();
        } else {
            auto const ec = error_of<result_at<I>>::apply(std::get<I>(children).storage);
            if (ec) {
                state.record_error(ec);
                state.source.request_stop();
            }
        }
        return state.child_done();
    }

    template <std::size_t... I> fragments gather(std::index_sequence<I...>) {
        return std::tuple_cat(extract_fragment<result_at<I>>::apply(std::get<I>(children).storage)...);
    }

    template <std::size_t... I> result_type collect(std::index_sequence<I...> seq, std::true_type) {
        return make_io_result_from<fragments>::apply(state.first_error, gather(seq));
    }

    template <std::size_t... I> result_type collect(std::index_sequence<I...> seq, std::false_type) {
        return tuple_or_void<fragments>::apply(gather(seq));
    }

    std::tuple<child<As>...> children;
    combinator_state state;
};

template <class Vector> struct vector_or_void {
    using type = Vector;
    static type apply(Vector&& values) { return std::move(values); }
};
template <> struct vector_or_void<std::tuple<>> {
    using type = void;
    static void apply(std::tuple<>&&) {}
};

// 范围重载的结果：io 子任务 → io_result<std::vector<P>>（无载荷时 io_result<>）；
// 非 io 子任务 → std::vector<P>（void 时 void）。
template <class A> struct when_all_range_result {
    using payload = payload_t<awaitable_result_t<A>>;
    static constexpr bool is_io = payload_of<awaitable_result_t<A>>::is_io;
    static constexpr bool has_payload = not std::is_same<payload, std::tuple<>>::value;
    using vector_type = typename std::conditional<has_payload, std::vector<payload>, std::tuple<>>::type;
    using type = typename std::conditional<
        is_io, typename make_io_result_from<typename payload_fragment<vector_type>::type>::type,
        typename vector_or_void<vector_type>::type>::type;
};

template <class A> struct when_all_range_awaitable {
    using child_result = awaitable_result_t<A>;
    using traits = when_all_range_result<A>;
    using result_type = typename traits::type;

    explicit when_all_range_awaitable(std::vector<A> awaitables) {
        children.reserve(awaitables.size());
        for (auto& awaitable : awaitables)
            children.emplace_back(std::move(awaitable));
    }

    // 只在启动前移动；vector 的移动不搬元素，续体帧的地址不变。
    when_all_range_awaitable(when_all_range_awaitable&& other) noexcept
        : children(std::move(other.children)), state(std::move(other.state)) {}

    when_all_range_awaitable(when_all_range_awaitable const&) = delete;
    when_all_range_awaitable& operator=(when_all_range_awaitable const&) = delete;
    when_all_range_awaitable& operator=(when_all_range_awaitable&&) = delete;

    bool await_ready() const noexcept { return children.empty(); }

    coroutine_handle<> await_suspend(coroutine_handle<> const awaiting, io_env const* const env) {
        state.begin(awaiting, env, children.size());
        for (auto& entry : children) {
            entry.owner = this;
            entry.c.frame.set(&on_child_done, &entry);
            std::exception_ptr error;
            if (entry.c.launch(&state.child_env, error)) static_cast<void>(child_finished(entry, std::move(error)));
        }
        if (state.finish_start()) return awaiting;
        return noop_coroutine();
    }

    result_type await_resume() {
        if (state.has_exception.load(std::memory_order_acquire))
            std::rethrow_exception(state.first_exception);
        return collect(std::integral_constant<bool, traits::is_io>{},
                       std::integral_constant<bool, traits::has_payload>{});
    }

  private:
    struct entry_type {
        explicit entry_type(A awaitable) : c(std::move(awaitable)) {}
        entry_type(entry_type&& other) noexcept(std::is_nothrow_move_constructible<A>::value) : c(std::move(other.c)) {}

        child<A> c;
        when_all_range_awaitable* owner = nullptr;
    };

    static coroutine_handle<> on_child_done(void* const user) {
        auto& entry = *static_cast<entry_type*>(user);
        if (entry.owner->child_finished(entry, entry.c.capture())) return entry.owner->state.resume_parent();
        return nullptr;
    }

    bool child_finished(entry_type& entry, std::exception_ptr error) noexcept {
        if (error) {
            state.record_exception(std::move(error));
            state.source.request_stop();
        } else {
            auto const ec = error_of<child_result>::apply(entry.c.storage);
            if (ec) {
                state.record_error(ec);
                state.source.request_stop();
            }
        }
        return state.child_done();
    }

    typename traits::vector_type gather(std::true_type) {
        auto values = typename traits::vector_type{};
        values.reserve(children.size());
        for (auto& entry : children)
            values.push_back(std::get<0>(extract_fragment<child_result>::apply(entry.c.storage)));
        return values;
    }

    std::tuple<> gather(std::false_type) {
        for (auto& entry : children)
            extract_fragment<child_result>::apply(entry.c.storage);
        return {};
    }

    template <class HasPayload> result_type collect(std::true_type, HasPayload has_payload) {
        using fragment = typename payload_fragment<typename traits::vector_type>::type;
        return make_io_result_from<fragment>::apply(state.first_error, fragment(gather(has_payload)));
    }

    template <class HasPayload> result_type collect(std::false_type, HasPayload has_payload) {
        return vector_or_void<typename traits::vector_type>::apply(gather(has_payload));
    }

    std::vector<entry_type> children;
    combinator_state state;
};

} // namespace detail

template <class... As> detail::when_all_awaitable<As...> when_all(As... awaitables) {
    return detail::when_all_awaitable<As...>{std::move(awaitables)...};
}

template <class A> detail::when_all_range_awaitable<A> when_all(std::vector<A> awaitables) {
    return detail::when_all_range_awaitable<A>{std::move(awaitables)};
}

} // namespace net
