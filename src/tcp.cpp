#include "net/tcp.hpp"

#include <cstring>

#include <unistd.h>

#include "co2/contract.hpp"

#include "net/error.hpp"
#include "net/io_context.hpp"

#include "detail/backend.hpp"

namespace net {

namespace {

template <class Endpoint>
Endpoint endpoint_from(sockaddr_storage const& storage, socklen_t const length) noexcept {
    auto endpoint = Endpoint{};
    if (length <= endpoint.capacity()) std::memcpy(endpoint.data(), &storage, length);
    return endpoint;
}

} // namespace

// ---- tcp_socket ----

tcp_socket::tcp_socket(io_context& context, protocol_type const& protocol) : socket_base{context} {
    auto const ec = open(protocol);
    if (ec) throw std::system_error{ec, "tcp_socket::open"};
}

tcp_socket::tcp_socket(io_context& context, protocol_type const& protocol,
                       native_handle_type const fd)
    : socket_base{context} {
    auto const ec = assign(protocol, fd);
    if (ec) throw std::system_error{ec, "tcp_socket::assign"};
}

tcp_socket::endpoint_type tcp_socket::local_endpoint(std::error_code& ec) const noexcept {
    sockaddr_storage storage{};
    auto length = static_cast<socklen_t>(sizeof(storage));
    ec = local_endpoint_raw(reinterpret_cast<sockaddr*>(&storage), &length);
    return ec ? endpoint_type{} : endpoint_from<endpoint_type>(storage, length);
}

tcp_socket::endpoint_type tcp_socket::remote_endpoint(std::error_code& ec) const noexcept {
    sockaddr_storage storage{};
    auto length = static_cast<socklen_t>(sizeof(storage));
    ec = remote_endpoint_raw(reinterpret_cast<sockaddr*>(&storage), &length);
    return ec ? endpoint_type{} : endpoint_from<endpoint_type>(storage, length);
}

// ---- tcp_acceptor ----

tcp_acceptor::tcp_acceptor(io_context& context, endpoint_type const& endpoint,
                           bool const reuse_address)
    : socket_base{context} {
    auto ec = open(endpoint.protocol());
    if (not ec && reuse_address) ec = set_option(socket_option::reuse_address{true});
    if (not ec) ec = bind(endpoint);
    if (not ec) ec = listen();
    if (ec) throw std::system_error{ec, "tcp_acceptor"};
}

tcp_acceptor::endpoint_type tcp_acceptor::local_endpoint(std::error_code& ec) const noexcept {
    sockaddr_storage storage{};
    auto length = static_cast<socklen_t>(sizeof(storage));
    ec = local_endpoint_raw(reinterpret_cast<sockaddr*>(&storage), &length);
    return ec ? endpoint_type{} : endpoint_from<endpoint_type>(storage, length);
}

tcp_accept_awaitable tcp_acceptor::accept() noexcept {
    CO2_CONTRACT_CHECK(impl() != nullptr);
    impl()->begin_accept();
    return tcp_accept_awaitable{this};
}

bool tcp_accept_awaitable::await_ready() noexcept {
    return acceptor->impl()->ready(detail::op_direction::read);
}

coroutine_handle<> tcp_accept_awaitable::await_suspend(coroutine_handle<> const h,
                                                       io_env const* const env) noexcept {
    return acceptor->impl()->suspend(detail::op_direction::read, h, env);
}

io_result<tcp_socket> tcp_accept_awaitable::await_resume() noexcept {
    auto* const impl = acceptor->impl();
    auto fd = -1;
    auto family = 0;
    auto result = io_result<tcp_socket>{impl->finish_accept(fd, family), tcp_socket{}};
    if (result.ec || fd < 0) return result;
    auto const protocol = family == AF_INET6 ? ip::tcp::v6() : ip::tcp::v4();
    auto peer = tcp_socket{impl->context()};
    result.ec = peer.assign(protocol, fd);
    if (result.ec) {
        ::close(fd);
        return result;
    }
    result.value = std::move(peer);
    return result;
}

} // namespace net
