#pragma once

#include "net/continuation.hpp"
#include "net/coroutine.hpp"
#include "net/detail/storage.hpp"
#include "net/io_env.hpp"

#include "detail/backend.hpp"
#include "detail/timer_heap.hpp"

// 建立在 timer_heap 上的 timer_impl：reactor_backend 与 iocp_backend 共用。就是一个 timer_op——
// 地址稳定、排队时挂进后端的堆。

namespace net {
namespace detail {

struct heap_timer;

struct cancel_heap_timer {
    heap_timer* impl;
    void operator()() const noexcept;
};

struct heap_timer final : timer_impl, timer_op {
    heap_timer(io_context& context, timer_scheduler& scheduler) noexcept;
    ~heap_timer() override;

    io_context& context() const noexcept override { return *context_; }
    time_point expiry() const noexcept override { return timer_op::expiry; }
    std::size_t expires_at(time_point expiry) noexcept override;
    std::size_t cancel() noexcept override;
    bool has_pending() const noexcept override { return pending_; }

    bool ready() noexcept override;
    coroutine_handle<> suspend(coroutine_handle<> h, io_env const* env) noexcept override;
    io_result<> finish() noexcept override;

    void complete() noexcept override { env_->executor.post(cont_); }

    timer_scheduler& scheduler() noexcept { return *scheduler_; }

  private:
    io_context* context_;
    timer_scheduler* scheduler_;
    continuation cont_;
    io_env const* env_ = nullptr;
    bool pending_ = false;
    late_init<stop_callback<cancel_heap_timer>> stop_cb_;
};

} // namespace detail
} // namespace net
