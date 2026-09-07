#pragma once

#include <sys/socket.h>

#include "net/buffers.hpp"
#include "net/continuation.hpp"
#include "net/coroutine.hpp"
#include "net/detail/storage.hpp"
#include "net/io_env.hpp"

#include "detail/backend.hpp"
#include "detail/reactor/reactor_op.hpp"

// 就绪型后端的套接字实现：描述符状态 + 每个方向一个操作；perform() 用 POSIX 系统调用。

namespace net {
namespace detail {

struct reactor_backend;
struct reactor_socket;

struct cancel_reactor_socket_op {
    reactor_socket* impl;
    op_direction direction;
    void operator()() const noexcept;
};

struct reactor_socket_op final : reactor_op {
    enum class kind : unsigned char { none, read, write, connect, accept, receive_from, send_to };

    reactor_socket_op(reactor_socket& owner_, op_direction const direction_) noexcept
        : owner{&owner_}, direction{direction_} {}

    bool perform() noexcept override;
    void complete() noexcept override;

    reactor_socket* owner;
    op_direction direction;
    kind op_kind = kind::none;

    mutable_buffer_array<> read_buffers;
    const_buffer_array<> write_buffers;

    // receive_from：address_out 指向调用方的地址存储（容量 address_length）；
    // send_to：address 是目标。
    sockaddr* address_out = nullptr;
    socklen_t* address_length_out = nullptr; // receive_from：内核报告的地址长度写到这里（可空）
    sockaddr_storage address{};
    socklen_t address_length = 0;
    int accepted_fd = -1;
    int accepted_family = 0;

    continuation cont;
    io_env const* env = nullptr;
    bool pending = false;
    bool immediate = false; // connect：结果在 begin_connect 里已同步确定
    late_init<stop_callback<cancel_reactor_socket_op>> stop_cb;
};

struct reactor_socket final : socket_impl {
    reactor_socket(io_context& context, reactor_backend& backend) noexcept;
    ~reactor_socket() override;

    io_context& context() const noexcept override { return *context_; }

    std::error_code open(int family, int type, int protocol) noexcept override;
    std::error_code assign(int family, int type, int protocol, int fd) noexcept override;
    std::error_code adopt(int family, int type, int protocol, int fd) noexcept override;
    std::error_code close() noexcept override;
    void cancel() noexcept override;
    int release() noexcept override;
    int native_handle() const noexcept override { return fd_; }
    std::error_code listen(int backlog) noexcept override;

    void begin_read(span<mutable_buffer const> buffers) noexcept override;
    void begin_write(span<const_buffer const> buffers) noexcept override;
    void begin_receive_from(span<mutable_buffer const> buffers, sockaddr* sender, socklen_t capacity,
                            socklen_t* sender_length) noexcept override;
    void begin_send_to(span<const_buffer const> buffers, sockaddr const* target,
                       socklen_t length) noexcept override;
    void begin_connect(sockaddr const* address, socklen_t length, int family, int type,
                       int protocol) noexcept override;
    void begin_accept() noexcept override;

    bool ready(op_direction direction) noexcept override;
    coroutine_handle<> suspend(op_direction direction, coroutine_handle<> h,
                               io_env const* env) noexcept override;
    io_result<std::size_t> finish_transfer(op_direction direction) noexcept override;
    io_result<> finish_connect() noexcept override;
    std::error_code finish_accept(int& fd, int& family) noexcept override;

    bool has_pending() const noexcept override { return read_op_.pending || write_op_.pending; }

    // cancel_reactor_socket_op 使用。
    reactor_backend& backend() noexcept { return *backend_; }
    descriptor_state& state() noexcept { return state_; }
    reactor_socket_op& op_for(op_direction const direction) noexcept {
        return direction == op_direction::read ? read_op_ : write_op_;
    }

  private:
    void finish(reactor_socket_op& op) noexcept;

    io_context* context_;
    reactor_backend* backend_;
    descriptor_state state_;
    int fd_ = -1;
    reactor_socket_op read_op_;
    reactor_socket_op write_op_;
};

} // namespace detail
} // namespace net
