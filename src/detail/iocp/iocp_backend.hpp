#pragma once

#include <mutex>
#include <vector>

#include "net/execution_context.hpp"

#include "detail/backend.hpp"
#include "detail/iocp/iocp_op.hpp"
#include "detail/timer_heap.hpp"

// Windows 完成端口后端（Corosio 的 win_scheduler 形状）。
//
// 线程模型：io_context 保证同一时刻只有一个线程在 run() 里，它阻塞在 GetQueuedCompletionStatus 上，
// 超时取定时器堆顶；完成包到达后在锁外调用 op->on_complete / complete()，续体经各自执行器 post。
// 别的线程随时可以发起操作（WSARecv 等直接由内核排队，不经过本对象）、加定时器（堆锁；成为最早
// 到期且有线程在等时用 PostQueuedCompletionStatus 叫醒它重算超时）、取消（CancelIoEx）。
//
// 与 io_uring 后端不同，这里没有提交环：发起就是系统调用，完成一定经端口到达——包括 closesocket /
// CancelIoEx 之后的 ERROR_OPERATION_ABORTED。所以没有"提交前取消"的窗口要关，操作对象在发起后
// 到完成包处理前不能被触碰。

namespace net {
namespace detail {

struct iocp_backend final : execution_context::service, io_backend, timer_scheduler {
    using key_type = io_backend;

    explicit iocp_backend(execution_context& context);
    ~iocp_backend() override;

    void run(long timeout_ms) override;
    void interrupt() noexcept override;
    bool concurrent_run() const noexcept override { return true; }

    std::unique_ptr<socket_impl> create_socket(io_context& context) override;
    std::unique_ptr<timer_impl> create_timer(io_context& context) override;
    std::unique_ptr<file_impl> create_file(io_context& context) override;
    std::error_code register_signal_reader(native_socket_type read_end, void (*deliver)(int signal_number)) noexcept override;
    char const* name() const noexcept override { return "iocp"; }

    io_context& context() noexcept { return *context_; }
    HANDLE port() const noexcept { return port_; }
    // 把句柄挂到端口上（一个句柄只能挂一次）。
    std::error_code associate(HANDLE handle) noexcept;

    bool add_timer(timer_op& op) noexcept override;
    bool cancel_timer(timer_op& op, bool from_stop_token) noexcept override;

  protected:
    void shutdown() override;

  private:
    // 信号自管道读端的常驻接收：收到的每个 int 都交给 deliver，然后重新武装。
    struct signal_pump final : iocp_op {
        void on_complete(DWORD error, DWORD bytes) noexcept override;
        void complete() noexcept override;
        bool arm() noexcept;

        native_socket_type socket = invalid_socket;
        void (*deliver)(int) = nullptr;
        WSABUF wsabuf{};
        char buffer[64 * sizeof(int)] = {};
        DWORD leftover = 0; // 上一轮末尾不满一个 int 的字节数（流式套接字可能拆分）
        DWORD received = 0;
        DWORD last_error = 0;
    };

    struct completed {
        iocp_op* op;
        bool counts_as_work;
    };

    DWORD wait_timeout_ms_locked(long limit_ms) const noexcept;

    io_context* context_;
    HANDLE port_ = nullptr;
    std::mutex mutex_;
    timer_heap timers_;
    unsigned waiting_ = 0U; // 阻塞在 GetQueuedCompletionStatus 里的线程数（锁内读写；多线程并发 run）
    signal_pump signal_pump_;
};

} // namespace detail
} // namespace net
