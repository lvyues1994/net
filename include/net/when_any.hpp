#pragma once

#include <array>
#include <cstddef>
#include <exception>
#include <memory>
#include <system_error>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "co2/contract.hpp"

#include "net/detail/combinator.hpp"

// when_any（P4100R1 §8.7）：并发等待一组 IoAwaitable，第一个成功完成者胜出，其余被请求
// 停止；组合子等待全部子任务结束后才恢复父协程（结构化：没有子任务逃出 co_await 的词法
// 边界）。
//
//   CO2_AWAIT_SET(r, net::when_any(sock.read_some(buf), timer.wait()));
//   if (r.index == 0) use(std::get<0>(r.value));   // 读赢了：字节数
//
// 结果 when_any_result<P>{ec, index, value}：
//   - 全部子任务载荷类型相同时 P 就是该载荷（io_result<T> → T，io_result<> / void → 无值）；
//   - 否则 P 是 std::tuple<载荷...>，只有 index 对应的位置有意义（其余是默认值）。
//
// 语义：
//   - 子任务返回 io_result 时，!ec 才算成功；返回 ec 的子任务记录时间上第一个 ec，继续等
//     其它子任务；全部失败时结果的 ec 是第一个 ec、index 是 when_any_result::none；
//   - 非 io_result 的子任务任何完成都算成功；
//   - 任一子任务抛出：捕获、请求停止，全部完成后重抛时间上第一个异常（异常优先于结果）；
//   - 父 stop_token 的请求转发给全部子任务。

namespace net {

template <class P> struct when_any_result {
    static constexpr std::size_t none = static_cast<std::size_t>(-1);

    std::error_code ec;
    std::size_t index = none;
    P value{};
};

template <> struct when_any_result<std::tuple<>> {
    static constexpr std::size_t none = static_cast<std::size_t>(-1);

    std::error_code ec;
    std::size_t index = none;
};

template <> struct when_any_result<void> : when_any_result<std::tuple<>> {};

namespace detail {

// ---- 结果类型 ----

template <class First, class... Rest> struct all_same : std::true_type {};
template <class First, class Second, class... Rest>
struct all_same<First, Second, Rest...>
    : std::integral_constant<bool, std::is_same<First, Second>::value && all_same<First, Rest...>::value> {};

template <bool Homogeneous, class... Payloads> struct when_any_payload;

template <class First, class... Rest> struct when_any_payload<true, First, Rest...> {
    using type = First;
};

template <class... Payloads> struct when_any_payload<false, Payloads...> {
    using type = std::tuple<Payloads...>;
};

template <class... Rs> struct when_any_result_of {
    static constexpr bool homogeneous = all_same<payload_t<Rs>...>::value;
    using payload = typename when_any_payload<homogeneous, payload_t<Rs>...>::type;
    using type = when_any_result<payload>;
};

// 把赢家的载荷写进结果。
template <class Result, class R, class Payload> struct store_winner {
    static void apply(Result& out, result_storage<R>& storage) {
        out.value = std::get<0>(extract_fragment<R>::apply(storage));
    }
};

template <class Result, class R> struct store_winner<Result, R, std::tuple<>> {
    static void apply(Result&, result_storage<R>& storage) { extract_fragment<R>::apply(storage); }
};

// 异构：写进 tuple 的第 I 位。
template <class Result, class R, std::size_t I, bool HasValue = not std::is_same<payload_t<R>, std::tuple<>>::value>
struct store_winner_at {
    static void apply(Result& out, result_storage<R>& storage) {
        std::get<I>(out.value) = std::get<0>(extract_fragment<R>::apply(storage));
    }
};

template <class Result, class R, std::size_t I> struct store_winner_at<Result, R, I, false> {
    static void apply(Result&, result_storage<R>& storage) { extract_fragment<R>::apply(storage); }
};

// ---- 子任务完成：赢家判定与错误记录 ----

// 一个子任务结束（结果已取走）：返回 true 表示它是最后一个。
template <class R>
bool when_any_child_done(combinator_state& state, std::exception_ptr error, result_storage<R>& storage,
                         std::size_t const index) noexcept {
    if (error) {
        state.record_exception(std::move(error));
        state.source.request_stop();
    } else {
        auto const ec = error_of<R>::apply(storage);
        if (ec) {
            state.record_error(ec);
        } else if (state.claim_winner(index)) {
            state.source.request_stop();
        }
    }
    return state.child_done();
}

template <class... As> struct when_any_awaitable {
    static_assert(sizeof...(As) != 0U, "when_any needs at least one awaitable");
    static constexpr std::size_t count = sizeof...(As);

    using traits = when_any_result_of<awaitable_result_t<As>...>;
    using result_type = typename traits::type;

    explicit when_any_awaitable(As... awaitables_) : children(std::move(awaitables_)...) {}

    // 只在启动前移动（进父帧的 awaiter 槽）。
    when_any_awaitable(when_any_awaitable&& other) noexcept
        : children(std::move(other.children)), state(std::move(other.state)) {}

    when_any_awaitable(when_any_awaitable const&) = delete;
    when_any_awaitable& operator=(when_any_awaitable const&) = delete;
    when_any_awaitable& operator=(when_any_awaitable&&) = delete;

    bool await_ready() const noexcept { return false; }

    coroutine_handle<> await_suspend(coroutine_handle<> const awaiting, io_env const* const env) {
        state.begin(awaiting, env, count);
        // 依次发起；已同步完成的赢家会先撤兄弟，后发起的看到停止已请求就不执行。
        launch_all(std::make_index_sequence<count>{});
        if (state.finish_start()) return awaiting;
        return noop_coroutine();
    }

    result_type await_resume() {
        if (state.has_exception.load(std::memory_order_acquire))
            std::rethrow_exception(state.first_exception);
        auto result = result_type{};
        auto const winner = state.winner_index();
        if (winner == combinator_state::none) {
            result.ec = state.first_error;
            return result;
        }
        result.index = winner;
        store(result, winner, std::make_index_sequence<count>{},
              std::integral_constant<bool, traits::homogeneous>{});
        return result;
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
        if (c.launch(&state.child_env, error))
            static_cast<void>(when_any_child_done(state, std::move(error), c.storage, I));
    }

    template <std::size_t I> static coroutine_handle<> on_child_done(void* const user) {
        auto* const self = static_cast<when_any_awaitable*>(user);
        auto& c = std::get<I>(self->children);
        if (when_any_child_done(self->state, c.capture(), c.storage, I)) return self->state.resume_parent();
        return nullptr;
    }

    template <std::size_t... I>
    void store(result_type& out, std::size_t const winner, std::index_sequence<I...>, std::true_type) {
        int const ordered[] = {
            0, (I == winner ? (store_winner<result_type, result_at<I>, typename traits::payload>::apply(
                                   out, std::get<I>(children).storage),
                               0)
                            : 0)...};
        static_cast<void>(ordered);
    }

    template <std::size_t... I>
    void store(result_type& out, std::size_t const winner, std::index_sequence<I...>, std::false_type) {
        int const ordered[] = {
            0, (I == winner ? (store_winner_at<result_type, result_at<I>, I>::apply(out, std::get<I>(children).storage), 0)
                            : 0)...};
        static_cast<void>(ordered);
    }

    std::tuple<child<As>...> children;
    combinator_state state;
};

template <class A> struct when_any_range_awaitable {
    using child_result = awaitable_result_t<A>;
    using result_type = when_any_result<payload_t<child_result>>;

    explicit when_any_range_awaitable(std::vector<A> awaitables) {
        children.reserve(awaitables.size());
        for (auto& awaitable : awaitables)
            children.emplace_back(std::move(awaitable));
    }

    // 只在启动前移动；vector 的移动不搬元素，续体帧的地址不变。
    when_any_range_awaitable(when_any_range_awaitable&& other) noexcept
        : children(std::move(other.children)), state(std::move(other.state)) {}

    when_any_range_awaitable(when_any_range_awaitable const&) = delete;
    when_any_range_awaitable& operator=(when_any_range_awaitable const&) = delete;
    when_any_range_awaitable& operator=(when_any_range_awaitable&&) = delete;

    bool await_ready() const noexcept { return children.empty(); }

    coroutine_handle<> await_suspend(coroutine_handle<> const awaiting, io_env const* const env) {
        state.begin(awaiting, env, children.size());
        for (auto index = std::size_t{}; index != children.size(); ++index) {
            auto& entry = children[index];
            entry.owner = this;
            entry.index = index;
            entry.c.frame.set(&on_child_done, &entry);
            std::exception_ptr error;
            if (entry.c.launch(&state.child_env, error))
                static_cast<void>(when_any_child_done(state, std::move(error), entry.c.storage, index));
        }
        if (state.finish_start()) return awaiting;
        return noop_coroutine();
    }

    result_type await_resume() {
        if (state.has_exception.load(std::memory_order_acquire))
            std::rethrow_exception(state.first_exception);
        auto result = result_type{};
        auto const winner = state.winner_index();
        if (winner == combinator_state::none) {
            result.ec = state.first_error;
            return result;
        }
        result.index = winner;
        store_winner<result_type, child_result, payload_t<child_result>>::apply(result, children[winner].c.storage);
        return result;
    }

  private:
    struct entry_type {
        explicit entry_type(A awaitable) : c(std::move(awaitable)) {}
        entry_type(entry_type&& other) noexcept(std::is_nothrow_move_constructible<A>::value) : c(std::move(other.c)) {}

        child<A> c;
        when_any_range_awaitable* owner = nullptr;
        std::size_t index = 0U;
    };

    static coroutine_handle<> on_child_done(void* const user) {
        auto& entry = *static_cast<entry_type*>(user);
        auto* const self = entry.owner;
        if (when_any_child_done(self->state, entry.c.capture(), entry.c.storage, entry.index))
            return self->state.resume_parent();
        return nullptr;
    }

    std::vector<entry_type> children;
    combinator_state state;
};

} // namespace detail

template <class... As> detail::when_any_awaitable<As...> when_any(As... awaitables) {
    return detail::when_any_awaitable<As...>{std::move(awaitables)...};
}

template <class A> detail::when_any_range_awaitable<A> when_any(std::vector<A> awaitables) {
    return detail::when_any_range_awaitable<A>{std::move(awaitables)};
}

} // namespace net
