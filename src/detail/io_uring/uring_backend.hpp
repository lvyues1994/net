#pragma once

#include <memory>
#include <mutex>
#include <vector>

#include "net/execution_context.hpp"

#include "detail/backend.hpp"
#include "detail/io_uring/uring.hpp"
#include "detail/io_uring/uring_op.hpp"

// io_uring 后端（完成型）：一个环 + 环锁。操作在 suspend 时只把 SQE 写进用户态 SQ 环
// （不进内核）；run() 用一次 io_uring_enter 把积累的 SQE 与"等待至少一个完成"合并，然后收
// CQE 并把完成的操作交给它们的执行器——这是 Corosio 的 submit_sqes_op 批量提交的等价物。
// 另一个线程在 run() 阻塞期间提交时，用 eventfd 叫醒它去冲提交。
//
// 取消是异步的（ASYNC_CANCEL / TIMEOUT_REMOVE），原操作仍会收到一个 CQE（-ECANCELED）。
// 唤醒与信号管道用两个常驻的多发 POLL_ADD（老内核退回一次性 + 重新武装）。
//
// 工作计数：每个已提交（或在延迟队列中）的操作持有 io_context 的一份工作。

namespace net {
namespace detail {

struct uring_backend final : execution_context::service, io_backend {
    using key_type = io_backend;

    // single_issuer：io_context 的并发提示为 1 时为真，启用 SINGLE_ISSUER | DEFER_TASKRUN
    //（要求 run() 始终在同一个线程上调用）。
    uring_backend(execution_context& context, bool single_issuer, unsigned entries = 1024U);
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

    // 提交操作（只写 SQE；进内核推迟到 run()）。返回 false 表示它在提交前已被取消（调用方
    // 自己以 operation_aborted 完成，不会再有 CQE）。环满时进入延迟队列，由 run() 补提交。
    bool submit(uring_op& op) noexcept;
    // 取消：在延迟队列里 → 摘下并立即完成；在飞 → 提交取消请求（结果随原操作的 CQE 到达）；
    // 尚未提交 → 记下，submit 时返回 false。
    void cancel(uring_op& op) noexcept;

    // 退役一个常驻操作（多发 accept 的拥有者关闭 / 释放描述符时）：所有权转给后端。在飞则请求
    // 取消并持有到终止 CQE；在延迟队列则摘下；之后由 run() 删除。内核仍可能引用它的 user_data，
    // 所以拥有者不能自己删。调用方已把操作里指向自己的指针清空。
    void retire(std::unique_ptr<uring_op> op) noexcept;

    // 环锁（多发操作的 on_complete 在锁内被调用；它们需要在锁内提交替代操作时用 submit_locked）。
    std::mutex& mutex() noexcept { return mutex_; }
    void submit_locked(uring_op& op) noexcept;

  protected:
    void shutdown() override;

  private:
    // 常驻轮询：可读时调用 on_readable。多发模式下一个 SQE 持续产生 CQE；只在它终止
    //（CQE 无 F_MORE）或内核不支持多发（-EINVAL）时由 run() 重新武装。
    struct poller final : uring_op {
        void prepare(io_uring_sqe& sqe) noexcept override;
        void on_complete(int res, unsigned flags) noexcept override;
        void complete() noexcept override {}
        bool rearm() noexcept override { return true; }

        int fd = -1;
        bool multishot = true;
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
    void flush_pending_cancels_locked() noexcept;
    void forget_pending_cancel_locked(uring_op& op) noexcept;

    io_context* context_;
    std::mutex mutex_;
    uring ring_;
    int event_fd_ = -1;
    bool waiting_ = false;      // 有线程阻塞在 enter 里（锁内读写）：此时的提交者要叫醒它
    std::size_t inflight_ = 0;  // 已提交、CQE 未到的非常驻操作数（锁内）
    poller interrupt_poller_;
    poller signal_poller_;
    uring_op* deferred_head_ = nullptr;
    uring_op* deferred_tail_ = nullptr;
    struct reaped_cqe {
        uring_op* op;
        int res;
        unsigned flags;
    };

    std::vector<reaped_cqe> reaped_;    // 只有运行 run 的线程触碰
    std::vector<reaped_cqe> completed_; // 同上
    std::vector<uring_op*> rearm_;     // 同上
    std::vector<std::unique_ptr<uring_op>> retired_; // 锁内
    std::vector<uring_op*> pending_cancels_;          // 锁内：环满时没发出去的取消请求
    bool shut_down_ = false;
};

} // namespace detail
} // namespace net
