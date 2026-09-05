#pragma once

#include <cstddef>
#include <system_error>
#include <tuple>
#include <type_traits>
#include <utility>

// I/O 复合结果（P4124R0 §2.1，P4166R0）：第一个元素恒为 error_code，其余是值。
// `!ec` 表示成功、其余元素有意义；`ec` 表示失败、其余元素存在但不保证有意义（例如
// 部分传输的字节数）。它是聚合体，`io_result<std::size_t>{ec, n}` 直接构造；提供
// tuple 协议（get / tuple_size / tuple_element），C++17 及以后可以结构化绑定
// `auto [ec, n] = ...`，C++14 下用 `r.ec` / `r.value`。
//
//   io_result<>          { ec }
//   io_result<T>         { ec, value }
//   io_result<T, U, ...> { ec, values /* std::tuple<T, U, ...> */ }

namespace net {

template <class... Ts> struct io_result;

template <> struct io_result<> {
    std::error_code ec;
};

template <class T> struct io_result<T> {
    std::error_code ec;
    T value;
};

template <class T, class U, class... Rest> struct io_result<T, U, Rest...> {
    std::error_code ec;
    std::tuple<T, U, Rest...> values;
};

template <class T> struct is_io_result : std::false_type {};
template <class... Ts> struct is_io_result<io_result<Ts...>> : std::true_type {};

// ---- tuple 协议 ----

namespace detail {

template <std::size_t I> struct io_result_get;

template <> struct io_result_get<0> {
    template <class... Ts> static std::error_code& apply(io_result<Ts...>& result) noexcept {
        return result.ec;
    }
    template <class... Ts>
    static std::error_code const& apply(io_result<Ts...> const& result) noexcept {
        return result.ec;
    }
    template <class... Ts> static std::error_code&& apply(io_result<Ts...>&& result) noexcept {
        return std::move(result.ec);
    }
};

template <std::size_t I> struct io_result_get {
    template <class T> static T& apply(io_result<T>& result) noexcept { return result.value; }
    template <class T> static T const& apply(io_result<T> const& result) noexcept {
        return result.value;
    }
    template <class T> static T&& apply(io_result<T>&& result) noexcept {
        return std::move(result.value);
    }

    template <class T, class U, class... Rest>
    static auto apply(io_result<T, U, Rest...>& result) noexcept
        -> decltype(std::get<I - 1>(result.values)) {
        return std::get<I - 1>(result.values);
    }
    template <class T, class U, class... Rest>
    static auto apply(io_result<T, U, Rest...> const& result) noexcept
        -> decltype(std::get<I - 1>(result.values)) {
        return std::get<I - 1>(result.values);
    }
    template <class T, class U, class... Rest>
    static auto apply(io_result<T, U, Rest...>&& result) noexcept
        -> decltype(std::get<I - 1>(std::move(result.values))) {
        return std::get<I - 1>(std::move(result.values));
    }
};

} // namespace detail

template <std::size_t I, class... Ts>
auto get(io_result<Ts...>& result) noexcept
    -> decltype(detail::io_result_get<I>::apply(result)) {
    static_assert(I <= sizeof...(Ts), "io_result index out of range");
    return detail::io_result_get<I>::apply(result);
}

template <std::size_t I, class... Ts>
auto get(io_result<Ts...> const& result) noexcept
    -> decltype(detail::io_result_get<I>::apply(result)) {
    static_assert(I <= sizeof...(Ts), "io_result index out of range");
    return detail::io_result_get<I>::apply(result);
}

template <std::size_t I, class... Ts>
auto get(io_result<Ts...>&& result) noexcept
    -> decltype(detail::io_result_get<I>::apply(std::move(result))) {
    static_assert(I <= sizeof...(Ts), "io_result index out of range");
    return detail::io_result_get<I>::apply(std::move(result));
}

} // namespace net

namespace std {

template <class... Ts>
struct tuple_size<::net::io_result<Ts...>>
    : integral_constant<size_t, sizeof...(Ts) + 1U> {};

template <class... Ts> struct tuple_element<0, ::net::io_result<Ts...>> {
    using type = error_code;
};

template <size_t I, class... Ts> struct tuple_element<I, ::net::io_result<Ts...>> {
    using type = typename tuple_element<I - 1, tuple<Ts...>>::type;
};

} // namespace std
