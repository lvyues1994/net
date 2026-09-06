#pragma once

#include <atomic>

#include <sys/socket.h>
#include <sys/uio.h>

#include "net/buffers.hpp"
#include "net/continuation.hpp"
#include "net/coroutine.hpp"
#include "net/detail/storage.hpp"
#include "net/io_env.hpp"

#include "detail/backend.hpp"
#include "detail/io_uring/uring_op.hpp"

// io_uring 后端的套接字实现：每个方向一个操作，用 RECVMSG / SENDMSG / ACCEPT / CONNECT。
// iovec 数组与 msghdr 住在操作里——内核持有它们直到 CQE 到达。套接字仍是非阻塞的：ready()
// 先做一次推测系统调用，数据已就绪就不必提交（与就绪型后端一致的"同步流零开销"路径）。

namespace net {
namespace detail {

struct uring_backend;
struct uring_socket;

struct cancel_uring_socket_op {
    uring_socket* impl;
    op_direction direction;
    void operator()() const noexcept;
};

// 自适应投机（照 Corosio 的 speculative_state）：每个操作先试一次非阻塞系统调用；EAGAIN 后关掉
// 该方向的投机，直到一次异步完成证明就绪再重开；读方向连续 4 次 EAGAIN 后永久关闭——"总是
// 先等"的套接字（服务端的读、acceptor）不再为白跑的 read 付系统调用，直接走完成型路径，由
// 内核把数据拷进我们的缓冲区。字段只是提示，relaxed 原子：读到旧值最多浪费或省掉一次投机。
struct speculation_state {
    static constexpr int max_read_failures = 4;

    bool may_read() const noexcept {
        return try_read.load(std::memory_order_relaxed) && not perma_off_read.load(std::memory_order_relaxed);
    }
    bool may_write() const noexcept { return try_write.load(std::memory_order_relaxed); }

    void on_read_exhausted() noexcept {
        try_read.store(false, std::memory_order_relaxed);
        auto streak = read_eagain_streak.load(std::memory_order_relaxed);
        if (streak < max_read_failures) {
            read_eagain_streak.store(++streak, std::memory_order_relaxed);
            if (streak >= max_read_failures) perma_off_read.store(true, std::memory_order_relaxed);
        }
    }
    void on_read_success() noexcept {
        if (read_eagain_streak.load(std::memory_order_relaxed) != 0) read_eagain_streak.store(0, std::memory_order_relaxed);
    }
    void on_write_exhausted() noexcept { try_write.store(false, std::memory_order_relaxed); }
    void on_async_read_ready() noexcept {
        if (not perma_off_read.load(std::memory_order_relaxed)) try_read.store(true, std::memory_order_relaxed);
    }
    void on_async_write_ready() noexcept { try_write.store(true, std::memory_order_relaxed); }

    std::atomic<bool> try_read{true};
    std::atomic<bool> try_write{true};
    std::atomic<int> read_eagain_streak{0};
    std::atomic<bool> perma_off_read{false};
};

struct uring_socket_op final : uring_op {
    enum class kind : unsigned char { none, read, write, connect, accept, receive_from, send_to };

    uring_socket_op(uring_socket& owner_, op_direction const direction_) noexcept
        : owner{&owner_}, direction{direction_} {}

    void prepare(io_uring_sqe& sqe) noexcept override;
    void on_complete(int res, unsigned flags) noexcept override;
    void complete() noexcept override;

    uring_socket* owner;
    op_direction direction;
    kind op_kind = kind::none;

    mutable_buffer_array<> read_buffers;
    const_buffer_array<> write_buffers;
    iovec vectors[max_iovec];
    msghdr message{};

    sockaddr* address_out = nullptr; // receive_from：调用方的地址存储
    sockaddr_storage address{};       // send_to / connect 的目标；accept 的对端
    socklen_t address_length = 0;
    int accepted_fd = -1;
    int accepted_family = 0;

    std::error_code ec;
    std::size_t bytes_transferred = 0;

    continuation cont;
    io_env const* env = nullptr;
    bool pending = false;
    bool immediate = false; // connect：打开失败等同步结果
    late_init<stop_callback<cancel_uring_socket_op>> stop_cb;
};

struct uring_socket final : socket_impl {
    uring_socket(io_context& context, uring_backend& backend) noexcept;
    ~uring_socket() override;

    io_context& context() const noexcept override { return *context_; }

    std::error_code open(int family, int type, int protocol) noexcept override;
    std::error_code assign(int family, int type, int protocol, int fd) noexcept override;
    std::error_code close() noexcept override;
    void cancel() noexcept override;
    int release() noexcept override;
    int native_handle() const noexcept override { return fd_; }

    void begin_read(span<mutable_buffer const> buffers) noexcept override;
    void begin_write(span<const_buffer const> buffers) noexcept override;
    void begin_receive_from(span<mutable_buffer const> buffers, sockaddr* sender, socklen_t capacity) noexcept override;
    void begin_send_to(span<const_buffer const> buffers, sockaddr const* target, socklen_t length) noexcept override;
    void begin_connect(sockaddr const* address, socklen_t length, int family, int type, int protocol) noexcept override;
    void begin_accept() noexcept override;

    bool ready(op_direction direction) noexcept override;
    coroutine_handle<> suspend(op_direction direction, coroutine_handle<> h, io_env const* env) noexcept override;
    io_result<std::size_t> finish_transfer(op_direction direction) noexcept override;
    io_result<> finish_connect() noexcept override;
    std::error_code finish_accept(int& fd, int& family) noexcept override;

    bool has_pending() const noexcept override { return read_op_.pending || write_op_.pending; }

    uring_backend& backend() noexcept { return *backend_; }
    uring_socket_op& op_for(op_direction const direction) noexcept {
        return direction == op_direction::read ? read_op_ : write_op_;
    }
    speculation_state& speculation() noexcept { return speculation_; }

  private:
    void cancel_pending(uring_socket_op& op) noexcept;
    void finish(uring_socket_op& op) noexcept;

    io_context* context_;
    uring_backend* backend_;
    int fd_ = -1;
    speculation_state speculation_;
    uring_socket_op read_op_;
    uring_socket_op write_op_;
};

} // namespace detail
} // namespace net
