#pragma once

#include <sys/socket.h>

#include "net/buffers.hpp"
#include "net/continuation.hpp"
#include "net/coroutine.hpp"
#include "net/detail/storage.hpp"
#include "net/io_env.hpp"

#include "detail/reactor.hpp"

// 套接字实现（仅 src/ 内可见）：描述符状态 + 每个方向一个操作。操作对象地址稳定，
// awaiter 只带 socket_impl 指针。

namespace net {

struct io_context;

namespace detail {

struct socket_impl;

struct cancel_socket_op {
    socket_impl* impl;
    op_direction direction;
    void operator()() const noexcept;
};

struct socket_op final : reactor_op {
    enum class kind : unsigned char { none, read, write, connect, accept, receive_from, send_to };

    socket_op(socket_impl& owner_, op_direction const direction_) noexcept
        : owner{&owner_}, direction{direction_} {}

    bool perform() noexcept override;
    void complete() noexcept override;

    socket_impl* owner;
    op_direction direction;
    kind op_kind = kind::none;

    mutable_buffer_array<> read_buffers;
    const_buffer_array<> write_buffers;

    // receive_from：address_out 指向调用方的地址存储（容量 address_length）；
    // send_to：address 是目标；accept：address 是对端地址。
    sockaddr* address_out = nullptr;
    sockaddr_storage address{};
    socklen_t address_length = 0;
    int accepted_fd = -1;

    continuation cont;
    io_env const* env = nullptr;
    bool pending = false;
    bool immediate = false; // connect：结果在 start_connect 里已同步确定
    late_init<stop_callback<cancel_socket_op>> stop_cb;
};

struct socket_impl {
    explicit socket_impl(io_context& context_) noexcept;
    socket_impl(socket_impl const&) = delete;
    socket_impl& operator=(socket_impl const&) = delete;
    ~socket_impl();

    std::error_code open(int family, int type, int protocol) noexcept;
    std::error_code assign(int family, int type, int protocol, int fd) noexcept;
    std::error_code close() noexcept;
    void cancel() noexcept;
    int release() noexcept;

    // ---- awaiter 协议 ----
    bool op_ready(socket_op& op) noexcept;
    coroutine_handle<> op_suspend(socket_op& op, coroutine_handle<> h, io_env const* env) noexcept;
    void op_finish(socket_op& op) noexcept;

    socket_op& op_for(op_direction const direction) noexcept {
        return direction == op_direction::read ? read_op : write_op;
    }

    io_context* context;
    reactor* reactor_;
    descriptor_state state;
    int fd = -1;
    int family = 0;
    int type = 0;
    int protocol = 0;
    socket_op read_op;
    socket_op write_op;
};

} // namespace detail
} // namespace net
