#include "net/tcp.hpp"

#include <cstring>

#include <unistd.h>

#include "co2/contract.hpp"

#include "net/error.hpp"
#include "net/io_context.hpp"

#include "detail/socket_impl.hpp"

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
    auto& op = impl()->read_op;
    CO2_CONTRACT_CHECK(not op.pending);
    op.op_kind = detail::socket_op::kind::accept;
    op.accepted_fd = -1;
    return tcp_accept_awaitable{this};
}

bool tcp_accept_awaitable::await_ready() noexcept {
    auto* const impl = acceptor->impl();
    return impl->op_ready(impl->read_op);
}

coroutine_handle<> tcp_accept_awaitable::await_suspend(coroutine_handle<> const h,
                                                       io_env const* const env) noexcept {
    auto* const impl = acceptor->impl();
    return impl->op_suspend(impl->read_op, h, env);
}

io_result<tcp_socket> tcp_accept_awaitable::await_resume() noexcept {
    auto* const impl = acceptor->impl();
    auto& op = impl->read_op;
    impl->op_finish(op);
    auto result = io_result<tcp_socket>{op.ec, tcp_socket{}};
    if (op.ec || op.accepted_fd < 0) return result;
    auto const fd = op.accepted_fd;
    op.accepted_fd = -1;
    auto const protocol = op.address.ss_family == AF_INET6 ? ip::tcp::v6() : ip::tcp::v4();
    auto peer = tcp_socket{*impl->context};
    result.ec = peer.assign(protocol, fd);
    if (result.ec) {
        ::close(fd);
        return result;
    }
    result.value = std::move(peer);
    return result;
}

} // namespace net
