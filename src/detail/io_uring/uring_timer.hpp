#pragma once

#include <linux/time_types.h>

#include "net/continuation.hpp"
#include "net/coroutine.hpp"
#include "net/detail/storage.hpp"
#include "net/io_env.hpp"

#include "detail/backend.hpp"
#include "detail/io_uring/uring_op.hpp"

// io_uring 后端的定时器：每次 wait 提交一个绝对时间的 IORING_OP_TIMEOUT
// （CLOCK_MONOTONIC，与 steady_clock 同源），取消用 IORING_OP_TIMEOUT_REMOVE。

namespace net {
namespace detail {

struct uring_backend;
struct uring_timer;

struct cancel_uring_timer {
    uring_timer* impl;
    void operator()() const noexcept;
};

struct uring_timer final : timer_impl, uring_op {
    uring_timer(io_context& context, uring_backend& backend) noexcept;
    ~uring_timer() override;

    io_context& context() const noexcept override { return *context_; }
    time_point expiry() const noexcept override { return expiry_; }
    std::size_t expires_at(time_point expiry) noexcept override;
    std::size_t cancel() noexcept override;
    bool has_pending() const noexcept override { return pending_; }

    bool ready() noexcept override;
    coroutine_handle<> suspend(coroutine_handle<> h, io_env const* env) noexcept override;
    io_result<> finish() noexcept override;

    void prepare(io_uring_sqe& sqe) noexcept override;
    void on_complete(int res, unsigned flags) noexcept override;
    void complete() noexcept override { env_->executor.post(cont_); }
    std::uint8_t cancel_opcode() const noexcept override { return IORING_OP_TIMEOUT_REMOVE; }

    uring_backend& backend() noexcept { return *backend_; }

  private:
    io_context* context_;
    uring_backend* backend_;
    time_point expiry_;
    __kernel_timespec ts_{};
    std::error_code ec_;
    continuation cont_;
    io_env const* env_ = nullptr;
    bool pending_ = false;
    late_init<stop_callback<cancel_uring_timer>> stop_cb_;
};

} // namespace detail
} // namespace net
