#pragma once

#include <chrono>
#include <cstddef>
#include <system_error>
#include <vector>

// 定时器的最小堆与它的拥有者接口。就绪型后端（reactor_backend）与 IOCP 后端都没有内核定时器对象
// 可以直接挂协程，用同一份堆：等待的超时取堆顶，到期的在事件循环里弹出并完成。io_uring 用
// IORING_OP_TIMEOUT，不走这里。

namespace net {
namespace detail {

struct timer_op {
    static constexpr std::size_t not_queued = static_cast<std::size_t>(-1);

    timer_op() = default;
    timer_op(timer_op const&) = delete;
    timer_op& operator=(timer_op const&) = delete;
    virtual ~timer_op() = default;

    virtual void complete() noexcept = 0;

    std::chrono::steady_clock::time_point expiry{};
    std::size_t heap_index = not_queued;
    std::error_code ec;
    // 停止请求在"装好 stop_callback 之后、add_timer 之前"到达：cancel_timer 找不到已排队的定时器，
    // 记在这里，add_timer 看到就以 operation_aborted 同步完成。
    bool cancel_requested = false;
};

struct timer_heap {
    bool empty() const noexcept { return timers_.empty(); }
    timer_op* front() const noexcept { return timers_.front(); }
    std::size_t size() const noexcept { return timers_.size(); }

    void push(timer_op& op) noexcept;
    void remove(std::size_t index) noexcept;
    // 清空，并把每个定时器的 heap_index 置回 not_queued。
    void clear() noexcept;
    // 弹出所有 expiry <= now 的定时器（ec 清零）到 expired。
    void pop_expired(std::chrono::steady_clock::time_point now, std::vector<timer_op*>& expired) noexcept;

  private:
    void up(std::size_t index) noexcept;
    void down(std::size_t index) noexcept;
    void swap_at(std::size_t a, std::size_t b) noexcept;

    std::vector<timer_op*> timers_;
};

// 拥有一个 timer_heap 的后端给 heap_timer 的接口。
struct timer_scheduler {
    timer_scheduler() = default;
    timer_scheduler(timer_scheduler const&) = delete;
    timer_scheduler& operator=(timer_scheduler const&) = delete;
    virtual ~timer_scheduler() = default;

    // 返回 true：已因停止请求同步以 aborted 完成（调用方直接恢复协程）。
    virtual bool add_timer(timer_op& op) noexcept = 0;
    // 返回 true：取消了一个排队中的定时器（已 complete）。
    virtual bool cancel_timer(timer_op& op, bool from_stop_token) noexcept = 0;
};

} // namespace detail
} // namespace net
