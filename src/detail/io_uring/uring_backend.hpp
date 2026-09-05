#pragma once

#include <memory>
#include <mutex>
#include <vector>

#include "net/execution_context.hpp"

#include "detail/backend.hpp"
#include "detail/io_uring/uring.hpp"
#include "detail/io_uring/uring_op.hpp"

// io_uring 后端（完成型）：一个环 + 环锁；操作在 suspend 时提交 SQE，run() 收 CQE 并把
// 完成的操作交给它们的执行器。取消是异步的（ASYNC_CANCEL / TIMEOUT_REMOVE），原操作
// 仍会收到一个 CQE（-ECANCELED）。唤醒与信号管道用两个常驻的 POLL_ADD。
//
// 工作计数：每个已提交（或在延迟队列中）的操作持有 io_context 的一份工作。

namespace net {
namespace detail {

struct uring_backend final : execution_context::service, io_backend {
    using key_type = io_backend;

    explicit uring_backend(execution_context& context, unsigned entries = 1024U);
    ~uring_backend() override;

    // ---- io_backend ----
    void run(long timeout_ms) override;
    void interrupt() noexcept override;
    std::unique_ptr<socket_impl> create_socket(io_context& context) override;
    std::unique_ptr<timer_impl> create_timer(io_context& context) override;
    std::error_code register_signal_reader(int read_fd, void (*deliver)(int signal_number)) noexcept override;
    char const* name() const noexcept override { return "io_uring"; }

    // ---- 内部协议（uring_socket / uring_timer 使用） ----

    io_context& context() noexcept { return *context_; }

    // 提交操作。返回 false 表示它在提交前已被取消（调用方自己以 operation_aborted 完成，
    // 不会再有 CQE）。环满时进入延迟队列，由 run() 补提交。
    bool submit(uring_op& op) noexcept;
    // 取消：在延迟队列里 → 摘下并立即完成；在飞 → 提交取消请求（结果随原操作的 CQE 到达）；
    // 尚未提交 → 记下，submit 时返回 false。
    void cancel(uring_op& op) noexcept;

  protected:
    void shutdown() override;

  private:
    // 常驻轮询：可读时调用 on_readable，然后由 run() 重新武装。
    struct poller final : uring_op {
        void prepare(io_uring_sqe& sqe) noexcept override;
        void on_complete(int res, unsigned flags) noexcept override;
        void complete() noexcept override {}

        int fd = -1;
        void (*on_readable)(poller& self) noexcept = nullptr;
        void (*deliver)(int) = nullptr;
    };

    static void drain_eventfd(poller& self) noexcept;
    static void drain_signal_pipe(poller& self) noexcept;

    // 环锁内。
    bool try_submit_locked(uring_op& op) noexcept;
    void push_deferred_locked(uring_op& op) noexcept;
    bool remove_deferred_locked(uring_op& op) noexcept;
    void drain_deferred_locked() noexcept;
    void submit_cancel_locked(uring_op& op) noexcept;
    void flush_locked() noexcept;

    io_context* context_;
    std::mutex mutex_;
    uring ring_;
    int event_fd_ = -1;
    poller interrupt_poller_;
    poller signal_poller_;
    uring_op* deferred_head_ = nullptr;
    uring_op* deferred_tail_ = nullptr;
    struct reaped_cqe {
        uring_op* op;
        int res;
        unsigned flags;
    };

    std::vector<reaped_cqe> reaped_;   // 只有运行 run 的线程触碰
    std::vector<uring_op*> completed_; // 同上
    std::vector<uring_op*> rearm_;     // 同上
    bool shut_down_ = false;
};

} // namespace detail
} // namespace net
