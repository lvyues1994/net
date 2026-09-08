#include "net/socket_base.hpp"

#include <cerrno>

#if !NET_PLATFORM_WINDOWS
#include <sys/ioctl.h>
#include <unistd.h>
#endif

#include "co2/contract.hpp"

#include "net/error.hpp"
#include "net/io_context.hpp"

#include "detail/backend.hpp"
#if NET_PLATFORM_WINDOWS
#include "detail/iocp/iocp_op.hpp"
#endif

// 具体层：套接字的同步操作直接对描述符做系统调用；异步操作经抽象的 socket_impl 交给后端。

namespace net {

namespace {

std::error_code last_error() noexcept {
#if NET_PLATFORM_WINDOWS
    return detail::iocp_error(static_cast<DWORD>(::WSAGetLastError()));
#else
    return std::error_code{errno, std::system_category()};
#endif
}

} // namespace

namespace detail {

void close_native_socket(native_socket_type const s) noexcept {
#if NET_PLATFORM_WINDOWS
    ::closesocket(s);
#else
    ::close(s);
#endif
}

std::error_code last_socket_error() noexcept { return last_error(); }

void ensure_networking_initialized() noexcept {
#if NET_PLATFORM_WINDOWS
    struct winsock_session {
        winsock_session() noexcept {
            WSADATA data{};
            ::WSAStartup(MAKEWORD(2, 2), &data);
        }
    };
    static winsock_session const session;
#endif
}

} // namespace detail

// ---- awaiter：一个指针宽，定义在库内 ----

bool socket_read_awaitable::await_ready() noexcept { return impl->ready(detail::op_direction::read); }

coroutine_handle<> socket_read_awaitable::await_suspend(coroutine_handle<> const h,
                                                        io_env const* const env) noexcept {
    return impl->suspend(detail::op_direction::read, h, env);
}

io_result<std::size_t> socket_read_awaitable::await_resume() noexcept {
    return impl->finish_transfer(detail::op_direction::read);
}

bool socket_write_awaitable::await_ready() noexcept { return impl->ready(detail::op_direction::write); }

coroutine_handle<> socket_write_awaitable::await_suspend(coroutine_handle<> const h,
                                                         io_env const* const env) noexcept {
    return impl->suspend(detail::op_direction::write, h, env);
}

io_result<std::size_t> socket_write_awaitable::await_resume() noexcept {
    return impl->finish_transfer(detail::op_direction::write);
}

bool socket_connect_awaitable::await_ready() noexcept { return impl->ready(detail::op_direction::write); }

coroutine_handle<> socket_connect_awaitable::await_suspend(coroutine_handle<> const h,
                                                           io_env const* const env) noexcept {
    return impl->suspend(detail::op_direction::write, h, env);
}

io_result<> socket_connect_awaitable::await_resume() noexcept { return impl->finish_connect(); }

// ---- socket_base ----

socket_base::socket_base() noexcept = default;

socket_base::socket_base(io_context& context)
    : impl_{detail::io_context_access::backend(context).create_socket(context)} {}

socket_base::socket_base(socket_base&& other) noexcept = default;

socket_base& socket_base::operator=(socket_base&& other) noexcept {
    if (this != &other) impl_ = std::move(other.impl_);
    return *this;
}

socket_base::~socket_base() = default;

io_context& socket_base::context() const noexcept {
    CO2_CONTRACT_CHECK(impl_ != nullptr);
    return impl_->context();
}

bool socket_base::is_open() const noexcept { return impl_ != nullptr && socket_is_valid(impl_->native_handle()); }

std::error_code socket_base::close() noexcept {
    if (impl_ == nullptr) return {};
    return impl_->close();
}

void socket_base::cancel() noexcept {
    if (impl_ != nullptr) impl_->cancel();
}

socket_base::native_handle_type socket_base::native_handle() const noexcept {
    return impl_ != nullptr ? impl_->native_handle() : invalid_socket;
}

socket_base::native_handle_type socket_base::release() noexcept {
    return impl_ != nullptr ? impl_->release() : invalid_socket;
}

std::size_t socket_base::available(std::error_code& ec) const noexcept {
    if (not is_open()) {
        ec = make_error_code(error::not_open);
        return 0U;
    }
#if NET_PLATFORM_WINDOWS
    u_long count = 0;
    if (::ioctlsocket(impl_->native_handle(), FIONREAD, &count) != 0) {
        ec = last_error();
        return 0U;
    }
#else
    auto count = 0;
    if (::ioctl(impl_->native_handle(), FIONREAD, &count) != 0) {
        ec = last_error();
        return 0U;
    }
#endif
    ec.clear();
    return static_cast<std::size_t>(count);
}

std::error_code socket_base::open_raw(int const family, int const type, int const protocol) noexcept {
    CO2_CONTRACT_CHECK(impl_ != nullptr);
    return impl_->open(family, type, protocol);
}

std::error_code socket_base::adopt_raw(int const family, int const type, int const protocol,
                                       native_handle_type const fd) noexcept {
    return impl_->adopt(family, type, protocol, fd);
}

std::error_code socket_base::assign_raw(int const family, int const type, int const protocol,
                                        native_handle_type const fd) noexcept {
    CO2_CONTRACT_CHECK(impl_ != nullptr);
    return impl_->assign(family, type, protocol, fd);
}

std::error_code socket_base::bind_raw(sockaddr const* const address, socklen_t const length) noexcept {
    if (not is_open()) return make_error_code(error::not_open);
    if (::bind(impl_->native_handle(), address, length) != 0) return last_error();
    return {};
}

std::error_code socket_base::listen_raw(int const backlog) noexcept {
    if (not is_open()) return make_error_code(error::not_open);
    return impl_->listen(backlog);
}

std::error_code socket_base::shutdown_raw(int const how) noexcept {
    if (not is_open()) return make_error_code(error::not_open);
    if (::shutdown(impl_->native_handle(), how) != 0) return last_error();
    return {};
}

std::error_code socket_base::local_endpoint_raw(sockaddr* const address,
                                                socklen_t* const length) const noexcept {
    if (not is_open()) return make_error_code(error::not_open);
    if (::getsockname(impl_->native_handle(), address, length) != 0) return last_error();
    return {};
}

std::error_code socket_base::remote_endpoint_raw(sockaddr* const address,
                                                 socklen_t* const length) const noexcept {
    if (not is_open()) return make_error_code(error::not_open);
    if (::getpeername(impl_->native_handle(), address, length) != 0) return last_error();
    return {};
}

std::error_code socket_base::set_option_raw(int const level, int const name, void const* const data,
                                            std::size_t const size) noexcept {
    if (not is_open()) return make_error_code(error::not_open);
    if (::setsockopt(impl_->native_handle(), level, name, static_cast<detail::sockopt_pointer>(data),
                     static_cast<socklen_t>(size)) != 0)
        return last_error();
    return {};
}

std::error_code socket_base::get_option_raw(int const level, int const name, void* const data,
                                            socklen_t* const size) const noexcept {
    if (not is_open()) return make_error_code(error::not_open);
    if (::getsockopt(impl_->native_handle(), level, name, static_cast<detail::sockopt_mutable_pointer>(data), size) != 0)
        return last_error();
    return {};
}

std::error_code socket_base::connect_raw(sockaddr const* const address, socklen_t const length) noexcept {
    if (not is_open()) return make_error_code(error::not_open);
    for (;;) {
        if (::connect(impl_->native_handle(), address, length) == 0) return {};
#if !NET_PLATFORM_WINDOWS
        if (errno == EINTR) continue;
#endif
        return last_error();
    }
}

socket_read_awaitable socket_base::start_read(span<mutable_buffer const> const buffers) noexcept {
    CO2_CONTRACT_CHECK(impl_ != nullptr);
    impl_->begin_read(buffers);
    return socket_read_awaitable{impl_.get()};
}

socket_write_awaitable socket_base::start_write(span<const_buffer const> const buffers) noexcept {
    CO2_CONTRACT_CHECK(impl_ != nullptr);
    impl_->begin_write(buffers);
    return socket_write_awaitable{impl_.get()};
}

socket_connect_awaitable socket_base::start_connect(sockaddr const* const address, socklen_t const length,
                                                    int const family, int const type,
                                                    int const protocol) noexcept {
    CO2_CONTRACT_CHECK(impl_ != nullptr);
    impl_->begin_connect(address, length, family, type, protocol);
    return socket_connect_awaitable{impl_.get()};
}

socket_read_awaitable socket_base::start_receive_from(span<mutable_buffer const> const buffers,
                                                      sockaddr* const sender, socklen_t const capacity,
                                                      socklen_t* const sender_length) noexcept {
    CO2_CONTRACT_CHECK(impl_ != nullptr);
    impl_->begin_receive_from(buffers, sender, capacity, sender_length);
    return socket_read_awaitable{impl_.get()};
}

socket_write_awaitable socket_base::start_send_to(span<const_buffer const> const buffers,
                                                  sockaddr const* const target,
                                                  socklen_t const length) noexcept {
    CO2_CONTRACT_CHECK(impl_ != nullptr);
    impl_->begin_send_to(buffers, target, length);
    return socket_write_awaitable{impl_.get()};
}

} // namespace net
