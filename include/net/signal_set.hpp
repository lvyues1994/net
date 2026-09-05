#pragma once

#include <cstddef>
#include <memory>
#include <system_error>

#include "net/coroutine.hpp"
#include "net/io_env.hpp"
#include "net/io_result.hpp"

// signal_set（P4100R1 §8.8 Paper 9）：把 POSIX 信号变成可等待的事件。
//
//   net::signal_set signals{ctx, SIGINT, SIGTERM};
//   auto [ec, signo] = co_await signals.wait();
//
// 信号到达而没有未完成的 wait 时排队，下一次 wait 立即完成。同一 signal_set 同一时刻只能有
// 一个未完成的 wait；cancel() / stop_token 以 operation_aborted 完成它。实现是经典的
// 自管道：信号处理函数把信号号写进进程唯一的管道，每个 io_context 的信号服务在反应器里
// 读它并分发给所有注册了该信号的 signal_set。

namespace net {

struct io_context;

namespace detail {
struct signal_set_impl;
}

struct signal_wait_awaitable {
    detail::signal_set_impl* impl;

    bool await_ready() noexcept;
    coroutine_handle<> await_suspend(coroutine_handle<> h, io_env const* env) noexcept;
    io_result<int> await_resume() noexcept;
};

struct signal_set {
    explicit signal_set(io_context& context);
    signal_set(io_context& context, int signal_number_1);
    signal_set(io_context& context, int signal_number_1, int signal_number_2);
    signal_set(io_context& context, int signal_number_1, int signal_number_2, int signal_number_3);

    signal_set(signal_set&&) noexcept;
    signal_set& operator=(signal_set&&) noexcept;
    signal_set(signal_set const&) = delete;
    signal_set& operator=(signal_set const&) = delete;
    ~signal_set();

    io_context& context() const noexcept;

    std::error_code add(int signal_number) noexcept;
    std::error_code remove(int signal_number) noexcept;
    std::error_code clear() noexcept;

    // 取消未完成的 wait，返回取消的个数（0 或 1）。
    std::size_t cancel() noexcept;

    signal_wait_awaitable wait() noexcept;

  private:
    std::unique_ptr<detail::signal_set_impl> impl_;
};

} // namespace net
