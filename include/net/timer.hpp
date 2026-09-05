#pragma once

#include <chrono>
#include <cstddef>
#include <memory>

#include "net/coroutine.hpp"
#include "net/io_env.hpp"
#include "net/io_result.hpp"

// steady_timer（P4100R1 §8.8 Paper 8）：基于 std::chrono::steady_clock 的异步定时器。
//
//   timer.expires_after(std::chrono::seconds{1});
//   auto [ec] = co_await timer.wait();     // ec == error::operation_aborted 表示被取消
//
// 同一定时器同一时刻只能有一个未完成的 wait；expires_at / expires_after / cancel 取消它。
// 有未完成的 wait 时不能销毁定时器（契约违规）。stop_token 请求停止时 wait 以
// operation_aborted 完成。

namespace net {

struct io_context;

namespace detail {
struct timer_impl;
}

struct timer_wait_awaitable {
    detail::timer_impl* impl;

    bool await_ready() noexcept;
    coroutine_handle<> await_suspend(coroutine_handle<> h, io_env const* env) noexcept;
    io_result<> await_resume() noexcept;
};

struct steady_timer {
    using clock_type = std::chrono::steady_clock;
    using time_point = clock_type::time_point;
    using duration = clock_type::duration;

    explicit steady_timer(io_context& context);
    steady_timer(io_context& context, time_point const& expiry);
    steady_timer(io_context& context, duration const& expiry_from_now);

    steady_timer(steady_timer&&) noexcept;
    steady_timer& operator=(steady_timer&&) noexcept;
    steady_timer(steady_timer const&) = delete;
    steady_timer& operator=(steady_timer const&) = delete;
    ~steady_timer();

    io_context& context() const noexcept;

    time_point expiry() const noexcept;

    // 设置到期时间；取消未完成的 wait，返回取消的个数（0 或 1）。
    std::size_t expires_at(time_point const& expiry) noexcept;
    std::size_t expires_after(duration const& expiry_from_now) noexcept;

    // 取消未完成的 wait（以 operation_aborted 完成），返回取消的个数。
    std::size_t cancel() noexcept;

    timer_wait_awaitable wait() noexcept;

  private:
    std::unique_ptr<detail::timer_impl> impl_;
};

} // namespace net
