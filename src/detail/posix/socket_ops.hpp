#pragma once

#include <cstddef>
#include <system_error>

#include <sys/socket.h>
#include <sys/uio.h>

#include "net/buffers.hpp"

// POSIX 套接字系统调用的薄封装：非阻塞语义统一，EINTR 重试，errno → error_code。
// 就绪型后端在描述符就绪时调用；完成型后端（io_uring）用它做同步部分（open/bind/...）。

namespace net {
namespace detail {
namespace posix {

std::error_code last_error() noexcept;
bool would_block(int err) noexcept;

// 结果三态：完成（ec 已定）、需等待（EAGAIN）。
struct io_outcome {
    bool done;
    std::error_code ec;
    std::size_t bytes;
};

int create_socket(int family, int type, int protocol) noexcept;
std::error_code set_nonblocking_cloexec(int fd) noexcept;
std::error_code close_socket(int fd) noexcept;

io_outcome readv(int fd, mutable_buffer_array<> const& buffers) noexcept;   // 0 字节 → error::eof
io_outcome writev(int fd, const_buffer_array<> const& buffers) noexcept;
// sender_length：入为容量，出为内核报告的地址长度（sender 为空时忽略）。
io_outcome recvmsg(int fd, mutable_buffer_array<> const& buffers, sockaddr* sender,
                   socklen_t capacity, socklen_t* sender_length = nullptr) noexcept;
io_outcome sendmsg(int fd, const_buffer_array<> const& buffers, sockaddr const* target,
                   socklen_t length) noexcept;
// 返回值：fd >= 0 成功；-1 且 done 为假表示 EAGAIN；-1 且 ec 非空表示错误。
struct accept_outcome {
    bool done;
    std::error_code ec;
    int fd;
    int family;
};
accept_outcome accept(int fd) noexcept;
// 非阻塞 connect 的同步阶段：done 为假表示 EINPROGRESS（等待可写后查 SO_ERROR）。
struct connect_outcome {
    bool done;
    std::error_code ec;
};
connect_outcome connect(int fd, sockaddr const* address, socklen_t length) noexcept;
std::error_code connect_result(int fd) noexcept; // SO_ERROR
// 非阻塞 connect 之后套接字可写时调用：SO_ERROR 非零 → 失败（done）；为零且 getpeername 成功 →
// 连上（done）；为零但 ENOTCONN → 可写只是过期的就绪位（未连接套接字最初就报 EPOLLOUT|EPOLLHUP），
// 连接仍在进行（done = false，继续等）。
connect_outcome connect_completed(int fd) noexcept;

} // namespace posix
} // namespace detail
} // namespace net
