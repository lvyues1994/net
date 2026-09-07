#pragma once

#include <cstddef>
#include <system_error>

#include "net/buffers.hpp"
#include "net/coroutine.hpp"
#include "net/io_env.hpp"
#include "net/io_result.hpp"
#include "net/ip.hpp"
#include "net/socket_base.hpp"
#include "net/source_sink.hpp"

// TCP（P4100R1 §8.8 Paper 11）：tcp_socket 满足 Stream；tcp_acceptor 的 accept() 交出新的
// tcp_socket。API 形态与 Networking TS 一致，去掉 async_ 前缀与完成令牌：
//
//   auto [ec] = co_await sock.connect(endpoint);
//   auto [ec, n] = co_await sock.read_some(buf);
//   auto [ec, peer] = co_await acceptor.accept();

namespace net {

struct tcp_socket : socket_base {
    using protocol_type = ip::tcp;
    using endpoint_type = ip::tcp::endpoint;

    // 未绑定 io_context 的空套接字：只能被移动赋值或销毁（io_result<tcp_socket> 需要它）。
    tcp_socket() noexcept = default;
    explicit tcp_socket(io_context& context) : socket_base{context} {}
    // 打开。
    tcp_socket(io_context& context, protocol_type const& protocol);
    // 接管已有描述符（设为非阻塞并注册）。
    tcp_socket(io_context& context, protocol_type const& protocol, native_handle_type fd);

    tcp_socket(tcp_socket&&) noexcept = default;
    tcp_socket& operator=(tcp_socket&&) noexcept = default;

    std::error_code open(protocol_type const& protocol = protocol_type::v4()) noexcept {
        return open_raw(protocol.family(), protocol.type(), protocol.protocol());
    }

    std::error_code assign(protocol_type const& protocol, native_handle_type const fd) noexcept {
        return assign_raw(protocol.family(), protocol.type(), protocol.protocol(), fd);
    }

    // 内部：接受器交来的、已非阻塞 + CLOEXEC 的描述符。
    std::error_code adopt_accepted(protocol_type const& protocol, native_handle_type const fd) noexcept {
        return adopt_raw(protocol.family(), protocol.type(), protocol.protocol(), fd);
    }

    std::error_code bind(endpoint_type const& endpoint) noexcept {
        return bind_raw(endpoint.data(), static_cast<socklen_t>(endpoint.size()));
    }

    std::error_code shutdown(shutdown_type const what) noexcept {
        return shutdown_raw(static_cast<int>(what));
    }

    endpoint_type local_endpoint(std::error_code& ec) const noexcept;
    endpoint_type remote_endpoint(std::error_code& ec) const noexcept;

    // 未打开时按端点的协议自动打开。
    socket_connect_awaitable connect(endpoint_type const& endpoint) noexcept {
        auto const protocol = endpoint.protocol();
        return start_connect(endpoint.data(), static_cast<socklen_t>(endpoint.size()),
                             protocol.family(), protocol.type(), protocol.protocol());
    }

    template <class MutableBufferSequence>
    socket_read_awaitable read_some(MutableBufferSequence const& buffers) noexcept {
        static_assert(is_mutable_buffer_sequence<MutableBufferSequence>::value,
                      "read_some requires a MutableBufferSequence");
        return start_read(mutable_buffer_array<>{buffers}.to_span());
    }

    template <class ConstBufferSequence>
    socket_write_awaitable write_some(ConstBufferSequence const& buffers) noexcept {
        static_assert(is_const_buffer_sequence<ConstBufferSequence>::value,
                      "write_some requires a ConstBufferSequence");
        return start_write(const_buffer_array<>{buffers}.to_span());
    }
};

struct tcp_acceptor;

struct tcp_accept_awaitable {
    tcp_acceptor* acceptor;

    bool await_ready() noexcept;
    coroutine_handle<> await_suspend(coroutine_handle<> h, io_env const* env) noexcept;
    io_result<tcp_socket> await_resume() noexcept;
};

struct tcp_acceptor : socket_base {
    using protocol_type = ip::tcp;
    using endpoint_type = ip::tcp::endpoint;

    static constexpr int max_listen_connections = SOMAXCONN;

    explicit tcp_acceptor(io_context& context) : socket_base{context} {}
    // 打开、设 reuse_address、绑定、监听；失败抛 std::system_error。
    tcp_acceptor(io_context& context, endpoint_type const& endpoint, bool reuse_address = true);

    tcp_acceptor(tcp_acceptor&&) noexcept = default;
    tcp_acceptor& operator=(tcp_acceptor&&) noexcept = default;

    std::error_code open(protocol_type const& protocol = protocol_type::v4()) noexcept {
        return open_raw(protocol.family(), protocol.type(), protocol.protocol());
    }

    // 接管一个现成的描述符（已在监听的描述符可以直接 accept()）。
    std::error_code assign(protocol_type const& protocol, native_handle_type const fd) noexcept {
        return assign_raw(protocol.family(), protocol.type(), protocol.protocol(), fd);
    }

    std::error_code bind(endpoint_type const& endpoint) noexcept {
        return bind_raw(endpoint.data(), static_cast<socklen_t>(endpoint.size()));
    }

    std::error_code listen(int const backlog = max_listen_connections) noexcept {
        return listen_raw(backlog);
    }

    endpoint_type local_endpoint(std::error_code& ec) const noexcept;

    // 接受一个连接；新套接字绑定到本接受器的 io_context。
    tcp_accept_awaitable accept() noexcept;

  private:
    friend struct tcp_accept_awaitable;
};

// WriteSink 适配器（source_sink.hpp）的流结束定制点：TCP 的 EOF 就是 shutdown(send)。
inline auto signal_stream_eof(tcp_socket& socket, detail::eof_preferred) CO2_BEG((task<io_result<>>), (socket)) {
    CO2_RETURN((io_result<>{socket.shutdown(shutdown_type::send)}));
}
CO2_END

} // namespace net
