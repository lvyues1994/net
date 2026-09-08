#include "net/local.hpp"

#include <cstdio>
#include <cstring>

#include "co2/contract.hpp"

#include "net/error.hpp"
#include "net/io_context.hpp"

#include "detail/backend.hpp"

namespace net {

namespace {

template <class Endpoint> Endpoint endpoint_from(sockaddr_storage const& storage, socklen_t const length) noexcept {
    auto endpoint = Endpoint{};
    if (static_cast<std::size_t>(length) <= endpoint.capacity()) {
        std::memcpy(endpoint.data(), &storage, static_cast<std::size_t>(length));
        endpoint.resize(static_cast<std::size_t>(length));
    }
    return endpoint;
}

// 绑定前删掉已有的套接字文件（抽象命名空间 / 未命名端点没有文件）。Windows 上套接字文件是重解析点，
// 普通的 remove 就能删。
template <class Protocol> void unlink_socket_file(local::basic_endpoint<Protocol> const& endpoint) noexcept {
    auto const path = endpoint.path();
    if (not path.empty() && path[0] != '\0') std::remove(path.c_str());
}

} // namespace

// ---- local_stream_socket ----

local_stream_socket::local_stream_socket(io_context& context, protocol_type const& protocol) : socket_base{context} {
    auto const ec = open(protocol);
    if (ec) throw std::system_error{ec, "local_stream_socket::open"};
}

local_stream_socket::local_stream_socket(io_context& context, protocol_type const& protocol, native_handle_type const fd)
    : socket_base{context} {
    auto const ec = assign(protocol, fd);
    if (ec) throw std::system_error{ec, "local_stream_socket::assign"};
}

local_stream_socket::endpoint_type local_stream_socket::local_endpoint(std::error_code& ec) const noexcept {
    sockaddr_storage storage{};
    auto length = static_cast<socklen_t>(sizeof(storage));
    ec = local_endpoint_raw(reinterpret_cast<sockaddr*>(&storage), &length);
    return ec ? endpoint_type{} : endpoint_from<endpoint_type>(storage, length);
}

local_stream_socket::endpoint_type local_stream_socket::remote_endpoint(std::error_code& ec) const noexcept {
    sockaddr_storage storage{};
    auto length = static_cast<socklen_t>(sizeof(storage));
    ec = remote_endpoint_raw(reinterpret_cast<sockaddr*>(&storage), &length);
    return ec ? endpoint_type{} : endpoint_from<endpoint_type>(storage, length);
}

// ---- local_stream_acceptor ----

local_stream_acceptor::local_stream_acceptor(io_context& context, endpoint_type const& endpoint, bool const unlink_existing)
    : socket_base{context} {
    auto ec = open(endpoint.protocol());
    if (not ec && unlink_existing) unlink_socket_file(endpoint);
    if (not ec) ec = bind(endpoint);
    if (not ec) ec = listen();
    if (ec) throw std::system_error{ec, "local_stream_acceptor"};
}

local_stream_acceptor::endpoint_type local_stream_acceptor::local_endpoint(std::error_code& ec) const noexcept {
    sockaddr_storage storage{};
    auto length = static_cast<socklen_t>(sizeof(storage));
    ec = local_endpoint_raw(reinterpret_cast<sockaddr*>(&storage), &length);
    return ec ? endpoint_type{} : endpoint_from<endpoint_type>(storage, length);
}

local_accept_awaitable local_stream_acceptor::accept() noexcept {
    CO2_CONTRACT_CHECK(impl() != nullptr);
    impl()->begin_accept();
    return local_accept_awaitable{this};
}

bool local_accept_awaitable::await_ready() noexcept { return acceptor->impl()->ready(detail::op_direction::read); }

coroutine_handle<> local_accept_awaitable::await_suspend(coroutine_handle<> const h, io_env const* const env) noexcept {
    return acceptor->impl()->suspend(detail::op_direction::read, h, env);
}

io_result<local_stream_socket> local_accept_awaitable::await_resume() noexcept {
    auto* const impl = acceptor->impl();
    auto fd = invalid_socket;
    auto family = 0;
    auto result = io_result<local_stream_socket>{impl->finish_accept(fd, family), local_stream_socket{}};
    if (result.ec || not socket_is_valid(fd)) return result;
    auto peer = local_stream_socket{impl->context()};
    result.ec = peer.adopt_accepted(local::stream_protocol{}, fd);
    if (result.ec) {
        detail::close_native_socket(fd);
        return result;
    }
    result.value = std::move(peer);
    return result;
}

// ---- local_datagram_socket（POSIX） ----

#if !NET_PLATFORM_WINDOWS

local_datagram_socket::local_datagram_socket(io_context& context, protocol_type const& protocol) : socket_base{context} {
    auto const ec = open(protocol);
    if (ec) throw std::system_error{ec, "local_datagram_socket::open"};
}

local_datagram_socket::local_datagram_socket(io_context& context, endpoint_type const& endpoint, bool const unlink_existing)
    : socket_base{context} {
    auto ec = open(endpoint.protocol());
    if (not ec && unlink_existing) unlink_socket_file(endpoint);
    if (not ec) ec = bind(endpoint);
    if (ec) throw std::system_error{ec, "local_datagram_socket"};
}

local_datagram_socket::endpoint_type local_datagram_socket::local_endpoint(std::error_code& ec) const noexcept {
    sockaddr_storage storage{};
    auto length = static_cast<socklen_t>(sizeof(storage));
    ec = local_endpoint_raw(reinterpret_cast<sockaddr*>(&storage), &length);
    return ec ? endpoint_type{} : endpoint_from<endpoint_type>(storage, length);
}

local_datagram_socket::endpoint_type local_datagram_socket::remote_endpoint(std::error_code& ec) const noexcept {
    sockaddr_storage storage{};
    auto length = static_cast<socklen_t>(sizeof(storage));
    ec = remote_endpoint_raw(reinterpret_cast<sockaddr*>(&storage), &length);
    return ec ? endpoint_type{} : endpoint_from<endpoint_type>(storage, length);
}

#endif // !NET_PLATFORM_WINDOWS

} // namespace net
