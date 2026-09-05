#pragma once

#include <system_error>
#include <tuple>
#include <type_traits>
#include <utility>

#include "net/coroutine.hpp"
#include "net/io_env.hpp"
#include "net/io_result.hpp"

// 立即完成的 IoAwaitable：await_ready 恒真，从不挂起。内存缓冲区流、测试替身、
// 解压/编码层都靠它满足 ReadStream / WriteStream（P4100R1 §8.6 "Synchronous streams
// for free"）；ready(ec, ...) 是 io_result 的便捷构造。

namespace net {

template <class T> struct immediate {
    T value;

    bool await_ready() const noexcept { return true; }

    coroutine_handle<> await_suspend(coroutine_handle<> const h, io_env const*) const noexcept {
        return h;
    }

    T await_resume() noexcept(std::is_nothrow_move_constructible<T>::value) {
        return std::move(value);
    }
};

template <> struct immediate<void> {
    bool await_ready() const noexcept { return true; }

    coroutine_handle<> await_suspend(coroutine_handle<> const h, io_env const*) const noexcept {
        return h;
    }

    void await_resume() const noexcept {}
};

template <class T> immediate<typename std::decay<T>::type> make_immediate(T&& value) {
    return {std::forward<T>(value)};
}

inline immediate<io_result<>> ready(std::error_code const ec = {}) noexcept { return {{ec}}; }

template <class T1> immediate<io_result<T1>> ready(std::error_code const ec, T1 value) {
    return {{ec, std::move(value)}};
}

template <class T1, class T2, class... Rest>
immediate<io_result<T1, T2, Rest...>> ready(std::error_code const ec, T1 first, T2 second,
                                            Rest... rest) {
    return {{ec, std::make_tuple(std::move(first), std::move(second), std::move(rest)...)}};
}

} // namespace net
