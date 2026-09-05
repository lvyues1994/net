#pragma once

#include <cstddef>
#include <system_error>

#include "net/buffers.hpp"
#include "net/ip.hpp"
#include "net/socket_base.hpp"

// UDP（P4100R1 §8.8 Paper 13）：数据报套接字。
//
//   auto [ec, n] = co_await sock.send_to(buf, target);
//   auto [ec, n] = co_await sock.receive_from(buf, sender);   // sender 必须比操作活得久
//
// 已 connect 的套接字可用 send / receive（它们与 write_some / read_some 同形，因此
// udp_socket 也满足 Stream）。

namespace net {

struct udp_socket : socket_base {
    using protocol_type = ip::udp;
    using endpoint_type = ip::udp::endpoint;

    udp_socket() noexcept = default;
    explicit udp_socket(io_context& context) : socket_base{context} {}
    udp_socket(io_context& context, protocol_type const& protocol);
    // 打开并绑定；失败抛 std::system_error。
    udp_socket(io_context& context, endpoint_type const& endpoint);

    udp_socket(udp_socket&&) noexcept = default;
    udp_socket& operator=(udp_socket&&) noexcept = default;

    std::error_code open(protocol_type const& protocol = protocol_type::v4()) noexcept {
        return open_raw(protocol.family(), protocol.type(), protocol.protocol());
    }

    std::error_code assign(protocol_type const& protocol, native_handle_type const fd) noexcept {
        return assign_raw(protocol.family(), protocol.type(), protocol.protocol(), fd);
    }

    std::error_code bind(endpoint_type const& endpoint) noexcept {
        return bind_raw(endpoint.data(), static_cast<socklen_t>(endpoint.size()));
    }

    // 数据报套接字的 connect 立即完成：固定默认目标并过滤来源。
    std::error_code connect(endpoint_type const& endpoint) noexcept {
        if (not is_open()) {
            auto const ec = open(endpoint.protocol());
            if (ec) return ec;
        }
        return connect_raw(endpoint.data(), static_cast<socklen_t>(endpoint.size()));
    }

    endpoint_type local_endpoint(std::error_code& ec) const noexcept;
    endpoint_type remote_endpoint(std::error_code& ec) const noexcept;

    template <class ConstBufferSequence>
    socket_write_awaitable send_to(ConstBufferSequence const& buffers,
                                   endpoint_type const& target) noexcept {
        static_assert(is_const_buffer_sequence<ConstBufferSequence>::value,
                      "send_to requires a ConstBufferSequence");
        return start_send_to(const_buffer_array<>{buffers}.to_span(), target.data(),
                             static_cast<socklen_t>(target.size()));
    }

    // sender 在操作完成前必须保持有效（通常是协程帧里的局部）。
    template <class MutableBufferSequence>
    socket_read_awaitable receive_from(MutableBufferSequence const& buffers,
                                       endpoint_type& sender) noexcept {
        static_assert(is_mutable_buffer_sequence<MutableBufferSequence>::value,
                      "receive_from requires a MutableBufferSequence");
        sender = endpoint_type{protocol_type::v6(), 0}; // 预留最大容量；家族由内核回填
        return start_receive_from(mutable_buffer_array<>{buffers}.to_span(), sender.data(),
                                  static_cast<socklen_t>(sender.capacity()));
    }

    template <class ConstBufferSequence>
    socket_write_awaitable send(ConstBufferSequence const& buffers) noexcept {
        static_assert(is_const_buffer_sequence<ConstBufferSequence>::value,
                      "send requires a ConstBufferSequence");
        return start_write(const_buffer_array<>{buffers}.to_span());
    }

    template <class MutableBufferSequence>
    socket_read_awaitable receive(MutableBufferSequence const& buffers) noexcept {
        static_assert(is_mutable_buffer_sequence<MutableBufferSequence>::value,
                      "receive requires a MutableBufferSequence");
        return start_read(mutable_buffer_array<>{buffers}.to_span());
    }

    template <class ConstBufferSequence>
    socket_write_awaitable write_some(ConstBufferSequence const& buffers) noexcept {
        return send(buffers);
    }

    template <class MutableBufferSequence>
    socket_read_awaitable read_some(MutableBufferSequence const& buffers) noexcept {
        return receive(buffers);
    }
};

} // namespace net
