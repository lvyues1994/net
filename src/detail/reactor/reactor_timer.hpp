#pragma once

#include "net/continuation.hpp"
#include "net/coroutine.hpp"
#include "net/detail/storage.hpp"
#include "net/io_env.hpp"

#include "detail/backend.hpp"
#include "detail/reactor/reactor_op.hpp"

// 就绪型后端的定时器实现：一个 timer_op，挂在反应器的二叉堆上。

namespace net {
namespace detail {

struct reactor_backend;
struct reactor_timer;

struct cancel_reactor_timer {
    reactor_timer* impl;
    void operator()() const noexcept;
};

struct reactor_timer final : timer_impl, timer_op {
    reactor_timer(io_context& context, reactor_backend& backend) noexcept;
    ~reactor_timer() override;

    io_context& context() const noexcept override { return *context_; }
    time_point expiry() const noexcept override { return timer_op::expiry; }
    std::size_t expires_at(time_point expiry) noexcept override;
    std::size_t cancel() noexcept override;
    bool has_pending() const noexcept override { return pending_; }

    bool ready() noexcept override;
    coroutine_handle<> suspend(coroutine_handle<> h, io_env const* env) noexcept override;
    io_result<> finish() noexcept override;

    void complete() noexcept override { env_->executor.post(cont_); }

    reactor_backend& backend() noexcept { return *backend_; }

  private:
    io_context* context_;
    reactor_backend* backend_;
    continuation cont_;
    io_env const* env_ = nullptr;
    bool pending_ = false;
    late_init<stop_callback<cancel_reactor_timer>> stop_cb_;
};

} // namespace detail
} // namespace net
