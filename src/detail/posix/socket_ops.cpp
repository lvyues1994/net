#include "detail/posix/socket_ops.hpp"

#include <cerrno>

#include <fcntl.h>
#include <unistd.h>

#include "net/error.hpp"

namespace net {
namespace detail {
namespace posix {

namespace {

template <class Buffer, std::size_t N>
std::size_t fill_iovec(iovec (&vectors)[N], buffer_array<Buffer, N> const& buffers) noexcept {
    auto count = std::size_t{};
    for (auto const& b : buffers) {
        vectors[count].iov_base = const_cast<void*>(static_cast<void const*>(b.data()));
        vectors[count].iov_len = b.size();
        ++count;
    }
    return count;
}

io_outcome transfer_outcome(ssize_t const n, bool const eof_on_zero) noexcept {
    if (n > 0) return io_outcome{true, {}, static_cast<std::size_t>(n)};
    if (n == 0) return io_outcome{true, eof_on_zero ? make_error_code(error::eof) : std::error_code{}, 0U};
    if (would_block(errno)) return io_outcome{false, {}, 0U};
    return io_outcome{true, last_error(), 0U};
}

} // namespace

std::error_code last_error() noexcept { return std::error_code{errno, std::system_category()}; }

bool would_block(int const err) noexcept { return err == EAGAIN || err == EWOULDBLOCK; }

int create_socket(int const family, int const type, int const protocol) noexcept {
    return ::socket(family, type | SOCK_NONBLOCK | SOCK_CLOEXEC, protocol);
}

std::error_code set_nonblocking_cloexec(int const fd) noexcept {
    auto const flags = ::fcntl(fd, F_GETFL, 0);
    if (flags < 0 || ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) return last_error();
    auto const fd_flags = ::fcntl(fd, F_GETFD, 0);
    if (fd_flags < 0 || ::fcntl(fd, F_SETFD, fd_flags | FD_CLOEXEC) < 0) return last_error();
    return {};
}

std::error_code close_socket(int const fd) noexcept {
    if (::close(fd) != 0 && errno != EINTR) return last_error();
    return {};
}

io_outcome readv(int const fd, mutable_buffer_array<> const& buffers) noexcept {
    iovec vectors[max_iovec];
    auto const count = fill_iovec(vectors, buffers);
    for (;;) {
        auto const n = ::readv(fd, vectors, static_cast<int>(count));
        if (n < 0 && errno == EINTR) continue;
        return transfer_outcome(n, true);
    }
}

io_outcome writev(int const fd, const_buffer_array<> const& buffers) noexcept {
    iovec vectors[max_iovec];
    auto const count = fill_iovec(vectors, buffers);
    for (;;) {
        msghdr message{};
        message.msg_iov = vectors;
        message.msg_iovlen = count;
        auto const n = ::sendmsg(fd, &message, MSG_NOSIGNAL);
        if (n < 0 && errno == EINTR) continue;
        if (n < 0 && errno == ENOTSOCK) {
            // 非套接字描述符（管道、文件）：退回 writev。
            auto const w = ::writev(fd, vectors, static_cast<int>(count));
            if (w < 0 && errno == EINTR) continue;
            return transfer_outcome(w, false);
        }
        return transfer_outcome(n, false);
    }
}

io_outcome recvmsg(int const fd, mutable_buffer_array<> const& buffers, sockaddr* const sender,
                   socklen_t const capacity, socklen_t* const sender_length) noexcept {
    iovec vectors[max_iovec];
    auto const count = fill_iovec(vectors, buffers);
    for (;;) {
        msghdr message{};
        message.msg_name = sender;
        message.msg_namelen = sender != nullptr ? capacity : 0U;
        message.msg_iov = vectors;
        message.msg_iovlen = count;
        auto const n = ::recvmsg(fd, &message, 0);
        if (n < 0 && errno == EINTR) continue;
        if (n >= 0 && sender != nullptr && sender_length != nullptr) *sender_length = message.msg_namelen;
        return transfer_outcome(n, false); // 数据报：0 字节是合法的空报文
    }
}

io_outcome sendmsg(int const fd, const_buffer_array<> const& buffers, sockaddr const* const target,
                   socklen_t const length) noexcept {
    iovec vectors[max_iovec];
    auto const count = fill_iovec(vectors, buffers);
    for (;;) {
        msghdr message{};
        message.msg_name = const_cast<sockaddr*>(target);
        message.msg_namelen = length;
        message.msg_iov = vectors;
        message.msg_iovlen = count;
        auto const n = ::sendmsg(fd, &message, MSG_NOSIGNAL);
        if (n < 0 && errno == EINTR) continue;
        return transfer_outcome(n, false);
    }
}

accept_outcome accept(int const fd) noexcept {
    for (;;) {
        sockaddr_storage peer{};
        auto length = static_cast<socklen_t>(sizeof(peer));
        auto const accepted = ::accept4(fd, reinterpret_cast<sockaddr*>(&peer), &length,
                                        SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (accepted >= 0) return accept_outcome{true, {}, accepted, peer.ss_family};
        if (errno == EINTR || errno == ECONNABORTED) continue;
        if (would_block(errno)) return accept_outcome{false, {}, -1, 0};
        return accept_outcome{true, last_error(), -1, 0};
    }
}

connect_outcome connect(int const fd, sockaddr const* const address, socklen_t const length) noexcept {
    for (;;) {
        if (::connect(fd, address, length) == 0) return connect_outcome{true, {}};
        if (errno == EINTR) continue;
        if (errno == EINPROGRESS) return connect_outcome{false, {}};
        return connect_outcome{true, last_error()};
    }
}

std::error_code connect_result(int const fd) noexcept {
    auto pending_error = 0;
    auto length = static_cast<socklen_t>(sizeof(pending_error));
    if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &pending_error, &length) != 0) pending_error = errno;
    return pending_error == 0 ? std::error_code{} : std::error_code{pending_error, std::system_category()};
}

connect_outcome connect_completed(int const fd) noexcept {
    auto const ec = connect_result(fd);
    if (ec) return connect_outcome{true, ec};
    sockaddr_storage peer{};
    auto length = static_cast<socklen_t>(sizeof(peer));
    if (::getpeername(fd, reinterpret_cast<sockaddr*>(&peer), &length) == 0) return connect_outcome{true, {}};
    if (errno == ENOTCONN) return connect_outcome{false, {}}; // 还在 SYN_SENT：可写位是过期的
    return connect_outcome{true, last_error()};
}

} // namespace posix
} // namespace detail
} // namespace net
