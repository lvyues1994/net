#pragma once

#include <exception>
#include <utility>

#include "net/detail/storage.hpp"
#include "net/io_context.hpp"
#include "net/run_async.hpp"
#include "net/task.hpp"

// 在调用线程上把一个 task 跑到完成并交出结果（Capy 的 test::run_blocking 同形）：测试与示例里
// "启动一条链、run() 到结束、取值"的三行合成一行。协程抛出的异常在这里重抛。
//
//   auto const r = net::test::run_blocking(ctx, echo(sock));       // 用给定的 io_context
//   auto const n = net::test::run_blocking(compute());             // 临时建一个 io_context
//   auto const a = net::test::run_blocking(ctx, net::test::stopped_token(), read_once(sock));  // 取消路径

namespace net {
namespace test {

namespace detail {

template <class T> struct blocking_result {
    net::detail::result_storage<T> value;
    std::exception_ptr error;

    T take() {
        if (error) std::rethrow_exception(error);
        return value.take();
    }
};

template <> struct blocking_result<void> {
    std::exception_ptr error;

    void take() {
        if (error) std::rethrow_exception(error);
    }
};

} // namespace detail

// 带 stop_token 的版本：测取消路径（例如令牌在启动前就已请求停止）。
template <class T> T run_blocking(io_context& context, stop_token token, task<T> t) {
    detail::blocking_result<T> result;
    run_async(
        context.get_executor(), std::move(token), nullptr, [&](T v) { result.value.emplace(std::move(v)); },
        [&](std::exception_ptr const e) { result.error = e; })(std::move(t));
    context.run();
    context.restart();
    return result.take();
}

inline void run_blocking(io_context& context, stop_token token, task<void> t) {
    detail::blocking_result<void> result;
    run_async(
        context.get_executor(), std::move(token), nullptr, [] {},
        [&](std::exception_ptr const e) { result.error = e; })(std::move(t));
    context.run();
    context.restart();
    result.take();
}

template <class T> T run_blocking(io_context& context, task<T> t) {
    return run_blocking(context, stop_token{}, std::move(t));
}

inline void run_blocking(io_context& context, task<void> t) { run_blocking(context, stop_token{}, std::move(t)); }

// 一个已请求停止的令牌。
inline stop_token stopped_token() {
    stop_source source;
    source.request_stop();
    return source.get_token();
}

template <class T> T run_blocking(task<T> t) {
    io_context context;
    return run_blocking(context, std::move(t));
}

} // namespace test
} // namespace net
