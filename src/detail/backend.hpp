#pragma once

#include <chrono>
#include <cstddef>
#include <memory>
#include <system_error>

#include <sys/socket.h>

#include "net/buffers.hpp"
#include "net/coroutine.hpp"
#include "net/io_env.hpp"
#include "net/io_result.hpp"
#include "net/span.hpp"

// 后端接缝（仅 src/ 内可见）。公共具体层（io_context、tcp_socket、udp_socket、
// steady_timer、signal_set）只认识这里的抽象接口；某个后端（就绪型：epoll / poll /
// select；将来完成型：io_uring / IOCP）以 execution_context 服务的形式提供实现，由
// io_context(backend_tag) 在构造时选择并注册。这与 Corosio 的 detail::scheduler +
// detail::tcp_service + tcp_socket::implementation 三个抽象是同一形状。
//
// 上层对后端的全部要求：
//   - io_backend：事件循环的等待/唤醒、关闭，以及各类 I/O 实现对象的工厂；
//   - socket_impl：一个套接字的实现——同步的打开/关闭/取消 + 每个方向一个异步操作的三步
//     协议（begin_* 记参数 → ready 推测 → suspend 排队/提交 → finish 取结果）；
//   - timer_impl：一个定时器的实现。
// 操作完成时实现把续体交给 io_env 里的执行器（env->executor.post(cont)）并归还工作计数；
// 这一条对所有后端相同，与 P4003R3 §5 "there is always an owner" 一致。
//
// suspend() 的发布规则（每个后端都必须遵守，多线程 io_context 下违反即 use-after-free）：
//   1. 发布操作（登记到反应器 / 写进 SQ 环 / 置 waiting）之后不能再碰 env、op、this——
//      别的线程可能已经完成它、恢复协程、跑到结束并销毁帧（连同套接字与 run_async 状态）；
//   2. 工作计数 on_work_started 在发布之前、与发布同一把锁内：发布后立刻完成会先
//      on_work_finished，把 outstanding_work 打到 0；
//   3. stop_callback 在发布之前装好；"装好之后、发布之前"到达的停止请求由取消路径记为
//      cancel_requested，发布时看到即同步以 operation_aborted 完成——不能在发布之后再读
//      env->stop_token 来关这个窗口。

namespace net {

struct io_context;

namespace detail {

enum class op_direction : unsigned char { read = 0, write = 1 };

struct socket_impl {
    socket_impl() = default;
    socket_impl(socket_impl const&) = delete;
    socket_impl& operator=(socket_impl const&) = delete;
    virtual ~socket_impl() = default;

    virtual io_context& context() const noexcept = 0;

    // ---- 同步 ----
    virtual std::error_code open(int family, int type, int protocol) noexcept = 0;
    virtual std::error_code assign(int family, int type, int protocol, int fd) noexcept = 0;
    // ::listen；完成型后端借此武装多发 accept。
    virtual std::error_code listen(int backlog) noexcept = 0;
    // 取消两个方向的操作（以 operation_aborted 完成）并关闭描述符。
    virtual std::error_code close() noexcept = 0;
    virtual void cancel() noexcept = 0;
    // 注销并交出描述符所有权。
    virtual int release() noexcept = 0;
    virtual int native_handle() const noexcept = 0;

    // ---- 异步操作：记参数 ----
    // 前置条件：该方向没有未完成的操作。读方向：read / accept / receive_from；写方向：
    // write / connect / send_to。
    virtual void begin_read(span<mutable_buffer const> buffers) noexcept = 0;
    virtual void begin_write(span<const_buffer const> buffers) noexcept = 0;
    virtual void begin_receive_from(span<mutable_buffer const> buffers, sockaddr* sender,
                                    socklen_t capacity) noexcept = 0;
    virtual void begin_send_to(span<const_buffer const> buffers, sockaddr const* target,
                               socklen_t length) noexcept = 0;
    // 未打开时先按给定协议打开；connect 的同步结果（成功 / 失败 / EINPROGRESS）在这里确定。
    virtual void begin_connect(sockaddr const* address, socklen_t length, int family, int type,
                               int protocol) noexcept = 0;
    virtual void begin_accept() noexcept = 0;

    // ---- 异步操作：awaiter 三步 ----
    virtual bool ready(op_direction direction) noexcept = 0;
    virtual coroutine_handle<> suspend(op_direction direction, coroutine_handle<> h,
                                       io_env const* env) noexcept = 0;
    // 读/写/receive_from/send_to 的结果。
    virtual io_result<std::size_t> finish_transfer(op_direction direction) noexcept = 0;
    virtual io_result<> finish_connect() noexcept = 0;
    // accept 的结果：成功时 fd 已是非阻塞、CLOEXEC，family 是对端地址族。
    virtual std::error_code finish_accept(int& fd, int& family) noexcept = 0;

    // 是否有未完成的操作（销毁契约用）。
    virtual bool has_pending() const noexcept = 0;
};

struct timer_impl {
    using time_point = std::chrono::steady_clock::time_point;

    timer_impl() = default;
    timer_impl(timer_impl const&) = delete;
    timer_impl& operator=(timer_impl const&) = delete;
    virtual ~timer_impl() = default;

    virtual io_context& context() const noexcept = 0;
    virtual time_point expiry() const noexcept = 0;
    // 设置到期时间；取消挂起的 wait，返回取消个数。
    virtual std::size_t expires_at(time_point expiry) noexcept = 0;
    virtual std::size_t cancel() noexcept = 0;
    virtual bool has_pending() const noexcept = 0;

    virtual bool ready() noexcept = 0;
    virtual coroutine_handle<> suspend(coroutine_handle<> h, io_env const* env) noexcept = 0;
    virtual io_result<> finish() noexcept = 0;
};

struct io_backend {
    io_backend() = default;
    io_backend(io_backend const&) = delete;
    io_backend& operator=(io_backend const&) = delete;
    virtual ~io_backend() = default;

    // ---- 事件循环钩子（io_context 的调度器调用；同一时刻只有一个线程在 run 里） ----

    // 等待并处理一批事件；timeout_ms < 0 表示只受内部定时器限制。完成的操作在返回前经
    // 各自的执行器 post。
    virtual void run(long timeout_ms) = 0;
    // 唤醒阻塞在 run 里的线程（任意线程可调）。
    virtual void interrupt() noexcept = 0;

    // ---- 工厂 ----
    virtual std::unique_ptr<socket_impl> create_socket(io_context& context) = 0;
    virtual std::unique_ptr<timer_impl> create_timer(io_context& context) = 0;

    // 监视信号自管道的读端：可读时排空它，并对每个信号号调用 deliver。POSIX 后端实现；
    // 完成型后端可以用 poll-add / 等待对象实现同一语义。
    virtual std::error_code register_signal_reader(int read_fd,
                                                   void (*deliver)(int signal_number)) noexcept = 0;

    virtual char const* name() const noexcept = 0;
};

// io_context 的私有入口。
struct io_context_access {
    static io_backend& backend(io_context& context) noexcept;
};

} // namespace detail
} // namespace net
