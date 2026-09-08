#include "net/udp.hpp"

#include <cstring>

#include "net/error.hpp"
#include "net/io_context.hpp"

namespace net {

namespace {

udp_socket::endpoint_type endpoint_from(sockaddr_storage const& storage,
                                        socklen_t const length) noexcept {
    auto endpoint = udp_socket::endpoint_type{};
    if (static_cast<std::size_t>(length) <= endpoint.capacity()) std::memcpy(endpoint.data(), &storage, static_cast<std::size_t>(length));
    return endpoint;
}

} // namespace

udp_socket::udp_socket(io_context& context, protocol_type const& protocol) : socket_base{context} {
    auto const ec = open(protocol);
    if (ec) throw std::system_error{ec, "udp_socket::open"};
}

udp_socket::udp_socket(io_context& context, endpoint_type const& endpoint) : socket_base{context} {
    auto ec = open(endpoint.protocol());
    if (not ec) ec = bind(endpoint);
    if (ec) throw std::system_error{ec, "udp_socket"};
}

udp_socket::endpoint_type udp_socket::local_endpoint(std::error_code& ec) const noexcept {
    sockaddr_storage storage{};
    auto length = static_cast<socklen_t>(sizeof(storage));
    ec = local_endpoint_raw(reinterpret_cast<sockaddr*>(&storage), &length);
    return ec ? endpoint_type{} : endpoint_from(storage, length);
}

udp_socket::endpoint_type udp_socket::remote_endpoint(std::error_code& ec) const noexcept {
    sockaddr_storage storage{};
    auto length = static_cast<socklen_t>(sizeof(storage));
    ec = remote_endpoint_raw(reinterpret_cast<sockaddr*>(&storage), &length);
    return ec ? endpoint_type{} : endpoint_from(storage, length);
}

} // namespace net
