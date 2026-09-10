#pragma once

#include <cstddef>
#include <cstring>
#include <string>
#include <system_error>

#include "net/config.hpp"
#include "net/detail/socket_types.hpp"

#if NET_PLATFORM_WINDOWS
#include <afunix.h> // Windows 10 1803+：AF_UNIX 流套接字（sockaddr_un / UNIX_PATH_MAX）；没有数据报，没有抽象命名空间
#else
#include <sys/un.h>
#endif

#include "net/buffers.hpp"
#include "net/socket_base.hpp"
#include "net/source_sink.hpp"
#include "net/task.hpp"

// Unix 域套接字（P4100R1 §8.8 Paper 11 "TCP, UDP and Unix sockets"；形态与 Asio 的 local:: 一致）。
//
//   net::local::stream_protocol::endpoint ep{"/tmp/app.sock"};
//   net::local_stream_acceptor acceptor{ctx, ep};          // 已有的套接字文件先 unlink
//   auto [ec, peer] = co_await acceptor.accept();          // peer 是 local_stream_socket，满足 Stream
//   net::local_datagram_socket dgram{ctx, net::local::datagram_protocol::endpoint{"/tmp/app.dgram"}};
//
// 端点是 sockaddr_un；路径以 '\0' 开头是 Linux 的抽象命名空间（不在文件系统里，长度决定身份）。
// 流 / 数据报套接字都建立在 socket_base 上，与 TCP / UDP 共用同一套后端实现（reactor / io_uring / IOCP）。
// Windows（afunix.h）只有流套接字：local_datagram_socket 与 datagram_protocol 在 Windows 上不提供，
// 抽象命名空间 bind 会失败。

namespace net {
namespace local {

template <class Protocol> struct basic_endpoint {
    using protocol_type = Protocol;

    basic_endpoint() noexcept : data_{} { data_.sun_family = AF_UNIX; }

    basic_endpoint(char const* const path) : basic_endpoint{std::string{path}} {}

    basic_endpoint(std::string const& path) : basic_endpoint{} {
        if (path.size() > sizeof(data_.sun_path) - 1U)
            throw std::system_error{std::make_error_code(std::errc::filename_too_long), "local endpoint path"};
        std::memcpy(data_.sun_path, path.data(), path.size());
        length_ = static_cast<socklen_t>(offsetof(sockaddr_un, sun_path) + path.size());
    }

    protocol_type protocol() const noexcept { return protocol_type{}; }

    // 路径（抽象命名空间的端点以 '\0' 开头，含有效字节）。
    std::string path() const {
        if (length_ <= static_cast<socklen_t>(offsetof(sockaddr_un, sun_path))) return {};
        auto const n = static_cast<std::size_t>(length_) - offsetof(sockaddr_un, sun_path);
#if NET_PLATFORM_WINDOWS
        // Windows 没有抽象命名空间。getsockname / getpeername 对未命名端点仍回
        // sizeof(sockaddr_un)、sun_path 全 0；按 POSIX 长度应只有 sun_family。
        if (n == 0U || data_.sun_path[0] == '\0') return {};
#endif
        // 内核回填的文件系统路径可能带结尾 '\0'
        auto const len = n != 0U && data_.sun_path[0] != '\0' && data_.sun_path[n - 1U] == '\0' ? n - 1U : n;
        return std::string{data_.sun_path, len};
    }

    sockaddr* data() noexcept { return reinterpret_cast<sockaddr*>(&data_); }
    sockaddr const* data() const noexcept { return reinterpret_cast<sockaddr const*>(&data_); }
    std::size_t size() const noexcept { return static_cast<std::size_t>(length_); }
    void resize(std::size_t const n) noexcept { length_ = static_cast<socklen_t>(n <= sizeof(data_) ? n : sizeof(data_)); }
    std::size_t capacity() const noexcept { return sizeof(data_); }
    socklen_t* length_storage() noexcept { return &length_; }

    friend bool operator==(basic_endpoint const& a, basic_endpoint const& b) noexcept {
        return a.length_ == b.length_ && std::memcmp(&a.data_, &b.data_, static_cast<std::size_t>(a.length_)) == 0;
    }
    friend bool operator!=(basic_endpoint const& a, basic_endpoint const& b) noexcept { return not(a == b); }

  private:
    sockaddr_un data_;
    socklen_t length_ = static_cast<socklen_t>(offsetof(sockaddr_un, sun_path)); // 未命名
};

struct stream_protocol {
    using endpoint = basic_endpoint<stream_protocol>;
    int family() const noexcept { return AF_UNIX; }
    int type() const noexcept { return SOCK_STREAM; }
    int protocol() const noexcept { return 0; }
    friend bool operator==(stream_protocol, stream_protocol) noexcept { return true; }
};

#if !NET_PLATFORM_WINDOWS
struct datagram_protocol {
    using endpoint = basic_endpoint<datagram_protocol>;
    int family() const noexcept { return AF_UNIX; }
    int type() const noexcept { return SOCK_DGRAM; }
    int protocol() const noexcept { return 0; }
    friend bool operator==(datagram_protocol, datagram_protocol) noexcept { return true; }
};
#endif

} // namespace local

// ---- 流 ----

struct local_stream_socket : socket_base {
    using protocol_type = local::stream_protocol;
    using endpoint_type = protocol_type::endpoint;

    local_stream_socket() noexcept = default;
    explicit local_stream_socket(io_context& context) : socket_base{context} {}
    local_stream_socket(io_context& context, protocol_type const& protocol);
    local_stream_socket(io_context& context, protocol_type const& protocol, native_handle_type fd);
    local_stream_socket(local_stream_socket&&) noexcept = default;
    local_stream_socket& operator=(local_stream_socket&&) noexcept = default;

    std::error_code open(protocol_type const& protocol = protocol_type{}) noexcept {
        return open_raw(protocol.family(), protocol.type(), protocol.protocol());
    }
    std::error_code assign(protocol_type const& protocol, native_handle_type const fd) noexcept {
        return assign_raw(protocol.family(), protocol.type(), protocol.protocol(), fd);
    }
    std::error_code adopt_accepted(protocol_type const& protocol, native_handle_type const fd) noexcept {
        return adopt_raw(protocol.family(), protocol.type(), protocol.protocol(), fd);
    }
    std::error_code bind(endpoint_type const& endpoint) noexcept {
        return bind_raw(endpoint.data(), static_cast<socklen_t>(endpoint.size()));
    }
    std::error_code shutdown(shutdown_type const what) noexcept { return shutdown_raw(static_cast<int>(what)); }

    endpoint_type local_endpoint(std::error_code& ec) const noexcept;
    endpoint_type remote_endpoint(std::error_code& ec) const noexcept;

    socket_connect_awaitable connect(endpoint_type const& endpoint) noexcept {
        auto const protocol = endpoint.protocol();
        return start_connect(endpoint.data(), static_cast<socklen_t>(endpoint.size()), protocol.family(), protocol.type(),
                             protocol.protocol());
    }

    template <class MutableBufferSequence> socket_read_awaitable read_some(MutableBufferSequence const& buffers) noexcept {
        static_assert(is_mutable_buffer_sequence<MutableBufferSequence>::value, "read_some requires a MutableBufferSequence");
        return start_read(mutable_buffer_array<>{buffers}.to_span());
    }

    template <class ConstBufferSequence> socket_write_awaitable write_some(ConstBufferSequence const& buffers) noexcept {
        static_assert(is_const_buffer_sequence<ConstBufferSequence>::value, "write_some requires a ConstBufferSequence");
        return start_write(const_buffer_array<>{buffers}.to_span());
    }
};

struct local_stream_acceptor;

struct local_accept_awaitable {
    local_stream_acceptor* acceptor;

    bool await_ready() noexcept;
    coroutine_handle<> await_suspend(coroutine_handle<> h, io_env const* env) noexcept;
    io_result<local_stream_socket> await_resume() noexcept;
};

struct local_stream_acceptor : socket_base {
    using protocol_type = local::stream_protocol;
    using endpoint_type = protocol_type::endpoint;

    static constexpr int max_listen_connections = SOMAXCONN;

    local_stream_acceptor() noexcept = default;
    explicit local_stream_acceptor(io_context& context) : socket_base{context} {}
    // 打开、（unlink_existing 时先删掉已有的套接字文件）绑定、监听；失败抛 system_error。
    local_stream_acceptor(io_context& context, endpoint_type const& endpoint, bool unlink_existing = true);
    local_stream_acceptor(local_stream_acceptor&&) noexcept = default;
    local_stream_acceptor& operator=(local_stream_acceptor&&) noexcept = default;

    std::error_code open(protocol_type const& protocol = protocol_type{}) noexcept {
        return open_raw(protocol.family(), protocol.type(), protocol.protocol());
    }
    std::error_code assign(protocol_type const& protocol, native_handle_type const fd) noexcept {
        return assign_raw(protocol.family(), protocol.type(), protocol.protocol(), fd);
    }
    std::error_code bind(endpoint_type const& endpoint) noexcept {
        return bind_raw(endpoint.data(), static_cast<socklen_t>(endpoint.size()));
    }
    std::error_code listen(int const backlog = max_listen_connections) noexcept { return listen_raw(backlog); }
    endpoint_type local_endpoint(std::error_code& ec) const noexcept;

    local_accept_awaitable accept() noexcept;

  private:
    friend struct local_accept_awaitable;
};

// ---- 数据报（POSIX） ----

#if !NET_PLATFORM_WINDOWS
struct local_datagram_socket : socket_base {
    using protocol_type = local::datagram_protocol;
    using endpoint_type = protocol_type::endpoint;

    local_datagram_socket() noexcept = default;
    explicit local_datagram_socket(io_context& context) : socket_base{context} {}
    local_datagram_socket(io_context& context, protocol_type const& protocol);
    // 打开并绑定（unlink_existing 时先删掉已有的套接字文件）；失败抛 system_error。
    local_datagram_socket(io_context& context, endpoint_type const& endpoint, bool unlink_existing = true);
    local_datagram_socket(local_datagram_socket&&) noexcept = default;
    local_datagram_socket& operator=(local_datagram_socket&&) noexcept = default;

    std::error_code open(protocol_type const& protocol = protocol_type{}) noexcept {
        return open_raw(protocol.family(), protocol.type(), protocol.protocol());
    }
    std::error_code assign(protocol_type const& protocol, native_handle_type const fd) noexcept {
        return assign_raw(protocol.family(), protocol.type(), protocol.protocol(), fd);
    }
    std::error_code bind(endpoint_type const& endpoint) noexcept {
        return bind_raw(endpoint.data(), static_cast<socklen_t>(endpoint.size()));
    }
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
    socket_write_awaitable send_to(ConstBufferSequence const& buffers, endpoint_type const& target) noexcept {
        static_assert(is_const_buffer_sequence<ConstBufferSequence>::value, "send_to requires a ConstBufferSequence");
        return start_send_to(const_buffer_array<>{buffers}.to_span(), target.data(), static_cast<socklen_t>(target.size()));
    }

    template <class MutableBufferSequence>
    socket_read_awaitable receive_from(MutableBufferSequence const& buffers, endpoint_type& sender) noexcept {
        static_assert(is_mutable_buffer_sequence<MutableBufferSequence>::value, "receive_from requires a MutableBufferSequence");
        sender = endpoint_type{};
        return start_receive_from(mutable_buffer_array<>{buffers}.to_span(), sender.data(),
                                  static_cast<socklen_t>(sender.capacity()), sender.length_storage());
    }

    template <class ConstBufferSequence> socket_write_awaitable send(ConstBufferSequence const& buffers) noexcept {
        static_assert(is_const_buffer_sequence<ConstBufferSequence>::value, "send requires a ConstBufferSequence");
        return start_write(const_buffer_array<>{buffers}.to_span());
    }

    template <class MutableBufferSequence> socket_read_awaitable receive(MutableBufferSequence const& buffers) noexcept {
        static_assert(is_mutable_buffer_sequence<MutableBufferSequence>::value, "receive requires a MutableBufferSequence");
        return start_read(mutable_buffer_array<>{buffers}.to_span());
    }

    template <class ConstBufferSequence> socket_write_awaitable write_some(ConstBufferSequence const& buffers) noexcept {
        return send(buffers);
    }
    template <class MutableBufferSequence> socket_read_awaitable read_some(MutableBufferSequence const& buffers) noexcept {
        return receive(buffers);
    }
};
#endif

// WriteSink 适配器（source_sink.hpp）的流结束定制点：与 TCP 一样是 shutdown(send)。
inline auto signal_stream_eof(local_stream_socket& socket, detail::eof_preferred) CO2_BEG((task<io_result<>>), (socket)) {
    CO2_RETURN((io_result<>{socket.shutdown(shutdown_type::send)}));
}
CO2_END

} // namespace net
