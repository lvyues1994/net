#pragma once

#include <array>
#include <cstdint>
#include <cstring>
#include <string>
#include <system_error>
#include <typeinfo>

#include <netinet/in.h>
#include <sys/socket.h>

// IP 地址与端点（P4100R1 §8.8 Paper 11：套接字、接受器、端点、IP 地址），形态取自
// Networking TS 的 ip::address_v4 / address_v6 / address / basic_endpoint / tcp / udp。

namespace net {

struct tcp_socket;
struct tcp_acceptor;
struct udp_socket;

namespace ip {

using port_type = std::uint16_t;
using scope_id_type = std::uint32_t;

struct address_v4 {
    using bytes_type = std::array<unsigned char, 4>;
    using uint_type = std::uint32_t;

    address_v4() noexcept : bytes_{{0, 0, 0, 0}} {}
    explicit address_v4(bytes_type const& bytes) noexcept : bytes_{bytes} {}
    // 主机字节序整数。
    explicit address_v4(uint_type value) noexcept;

    bool is_unspecified() const noexcept { return to_uint() == 0U; }
    bool is_loopback() const noexcept { return (to_uint() & 0xFF000000U) == 0x7F000000U; }
    bool is_multicast() const noexcept { return (to_uint() & 0xF0000000U) == 0xE0000000U; }

    bytes_type to_bytes() const noexcept { return bytes_; }
    uint_type to_uint() const noexcept;
    std::string to_string() const;

    static address_v4 any() noexcept { return address_v4{}; }
    static address_v4 loopback() noexcept { return address_v4{0x7F000001U}; }
    static address_v4 broadcast() noexcept { return address_v4{0xFFFFFFFFU}; }

    friend bool operator==(address_v4 const& a, address_v4 const& b) noexcept {
        return a.bytes_ == b.bytes_;
    }
    friend bool operator!=(address_v4 const& a, address_v4 const& b) noexcept { return not(a == b); }
    friend bool operator<(address_v4 const& a, address_v4 const& b) noexcept {
        return a.to_uint() < b.to_uint();
    }

  private:
    bytes_type bytes_;
};

struct address_v6 {
    using bytes_type = std::array<unsigned char, 16>;

    address_v6() noexcept : bytes_{}, scope_id_{0} {}
    explicit address_v6(bytes_type const& bytes, scope_id_type const scope_id = 0) noexcept
        : bytes_{bytes}, scope_id_{scope_id} {}

    scope_id_type scope_id() const noexcept { return scope_id_; }
    void scope_id(scope_id_type const id) noexcept { scope_id_ = id; }

    bool is_unspecified() const noexcept;
    bool is_loopback() const noexcept;
    bool is_v4_mapped() const noexcept;
    bool is_multicast() const noexcept { return bytes_[0] == 0xFF; }

    bytes_type to_bytes() const noexcept { return bytes_; }
    std::string to_string() const;

    static address_v6 any() noexcept { return address_v6{}; }
    static address_v6 loopback() noexcept;

    friend bool operator==(address_v6 const& a, address_v6 const& b) noexcept {
        return a.bytes_ == b.bytes_ && a.scope_id_ == b.scope_id_;
    }
    friend bool operator!=(address_v6 const& a, address_v6 const& b) noexcept { return not(a == b); }
    friend bool operator<(address_v6 const& a, address_v6 const& b) noexcept {
        auto const order = std::memcmp(a.bytes_.data(), b.bytes_.data(), 16U);
        return order < 0 || (order == 0 && a.scope_id_ < b.scope_id_);
    }

  private:
    bytes_type bytes_;
    scope_id_type scope_id_;
};

struct bad_address_cast : std::bad_cast {
    char const* what() const noexcept override { return "bad address cast"; }
};

struct address {
    address() noexcept : type_{kind::v4}, v4_{}, v6_{} {}
    address(address_v4 const& v4) noexcept : type_{kind::v4}, v4_{v4}, v6_{} {}
    address(address_v6 const& v6) noexcept : type_{kind::v6}, v4_{}, v6_{v6} {}

    bool is_v4() const noexcept { return type_ == kind::v4; }
    bool is_v6() const noexcept { return type_ == kind::v6; }

    address_v4 to_v4() const {
        if (not is_v4()) throw bad_address_cast{};
        return v4_;
    }

    address_v6 to_v6() const {
        if (not is_v6()) throw bad_address_cast{};
        return v6_;
    }

    bool is_unspecified() const noexcept { return is_v4() ? v4_.is_unspecified() : v6_.is_unspecified(); }
    bool is_loopback() const noexcept { return is_v4() ? v4_.is_loopback() : v6_.is_loopback(); }
    bool is_multicast() const noexcept { return is_v4() ? v4_.is_multicast() : v6_.is_multicast(); }

    std::string to_string() const { return is_v4() ? v4_.to_string() : v6_.to_string(); }

    friend bool operator==(address const& a, address const& b) noexcept {
        if (a.type_ != b.type_) return false;
        return a.is_v4() ? a.v4_ == b.v4_ : a.v6_ == b.v6_;
    }
    friend bool operator!=(address const& a, address const& b) noexcept { return not(a == b); }
    friend bool operator<(address const& a, address const& b) noexcept {
        if (a.type_ != b.type_) return a.is_v4();
        return a.is_v4() ? a.v4_ < b.v4_ : a.v6_ < b.v6_;
    }

  private:
    enum class kind : unsigned char { v4, v6 };
    kind type_;
    address_v4 v4_;
    address_v6 v6_;
};

address_v4 make_address_v4(char const* text, std::error_code& ec) noexcept;
address_v4 make_address_v4(char const* text);
inline address_v4 make_address_v4(std::string const& text, std::error_code& ec) noexcept {
    return make_address_v4(text.c_str(), ec);
}
inline address_v4 make_address_v4(std::string const& text) { return make_address_v4(text.c_str()); }

address_v6 make_address_v6(char const* text, std::error_code& ec) noexcept;
address_v6 make_address_v6(char const* text);
inline address_v6 make_address_v6(std::string const& text, std::error_code& ec) noexcept {
    return make_address_v6(text.c_str(), ec);
}
inline address_v6 make_address_v6(std::string const& text) { return make_address_v6(text.c_str()); }

address make_address(char const* text, std::error_code& ec) noexcept;
address make_address(char const* text);
inline address make_address(std::string const& text, std::error_code& ec) noexcept {
    return make_address(text.c_str(), ec);
}
inline address make_address(std::string const& text) { return make_address(text.c_str()); }

// ---- 端点 ----

template <class Protocol> struct basic_endpoint {
    using protocol_type = Protocol;

    basic_endpoint() noexcept : data_{} { data_.v4.sin_family = AF_INET; }

    basic_endpoint(Protocol const& protocol, port_type const port) noexcept : data_{} {
        if (protocol.family() == AF_INET6) {
            data_.v6.sin6_family = AF_INET6;
            data_.v6.sin6_port = htons(port);
        } else {
            data_.v4.sin_family = AF_INET;
            data_.v4.sin_port = htons(port);
        }
    }

    basic_endpoint(ip::address const& addr, port_type const port) noexcept : data_{} {
        address(addr);
        this->port(port);
    }

    Protocol protocol() const noexcept {
        return data_.base.sa_family == AF_INET6 ? Protocol::v6() : Protocol::v4();
    }

    ip::address address() const noexcept {
        if (data_.base.sa_family == AF_INET6) {
            address_v6::bytes_type bytes;
            std::memcpy(bytes.data(), &data_.v6.sin6_addr, 16U);
            return address_v6{bytes, data_.v6.sin6_scope_id};
        }
        address_v4::bytes_type bytes;
        std::memcpy(bytes.data(), &data_.v4.sin_addr, 4U);
        return address_v4{bytes};
    }

    void address(ip::address const& addr) noexcept {
        auto const port = this->port();
        std::memset(&data_, 0, sizeof(data_));
        if (addr.is_v6()) {
            auto const v6 = addr.to_v6();
            data_.v6.sin6_family = AF_INET6;
            data_.v6.sin6_port = htons(port);
            data_.v6.sin6_scope_id = v6.scope_id();
            auto const bytes = v6.to_bytes();
            std::memcpy(&data_.v6.sin6_addr, bytes.data(), 16U);
        } else {
            auto const bytes = addr.to_v4().to_bytes();
            data_.v4.sin_family = AF_INET;
            data_.v4.sin_port = htons(port);
            std::memcpy(&data_.v4.sin_addr, bytes.data(), 4U);
        }
    }

    port_type port() const noexcept {
        return ntohs(data_.base.sa_family == AF_INET6 ? data_.v6.sin6_port : data_.v4.sin_port);
    }

    void port(port_type const value) noexcept {
        if (data_.base.sa_family == AF_INET6)
            data_.v6.sin6_port = htons(value);
        else
            data_.v4.sin_port = htons(value);
    }

    sockaddr* data() noexcept { return &data_.base; }
    sockaddr const* data() const noexcept { return &data_.base; }

    std::size_t size() const noexcept {
        return data_.base.sa_family == AF_INET6 ? sizeof(sockaddr_in6) : sizeof(sockaddr_in);
    }

    void resize(std::size_t const) noexcept {}

    std::size_t capacity() const noexcept { return sizeof(data_); }

    friend bool operator==(basic_endpoint const& a, basic_endpoint const& b) noexcept {
        return a.address() == b.address() && a.port() == b.port();
    }
    friend bool operator!=(basic_endpoint const& a, basic_endpoint const& b) noexcept {
        return not(a == b);
    }
    friend bool operator<(basic_endpoint const& a, basic_endpoint const& b) noexcept {
        if (a.address() != b.address()) return a.address() < b.address();
        return a.port() < b.port();
    }

  private:
    union {
        sockaddr base;
        sockaddr_in v4;
        sockaddr_in6 v6;
    } data_;
};

template <class Protocol> std::string to_string(basic_endpoint<Protocol> const& endpoint) {
    auto const addr = endpoint.address();
    if (addr.is_v6()) return "[" + addr.to_string() + "]:" + std::to_string(endpoint.port());
    return addr.to_string() + ":" + std::to_string(endpoint.port());
}

// ---- 协议 ----

template <class Protocol> struct basic_resolver;

struct tcp {
    using endpoint = basic_endpoint<tcp>;
    using socket = tcp_socket;
    using acceptor = tcp_acceptor;
    using resolver = basic_resolver<tcp>;

    static tcp v4() noexcept { return tcp{AF_INET}; }
    static tcp v6() noexcept { return tcp{AF_INET6}; }

    int family() const noexcept { return family_; }
    int type() const noexcept { return SOCK_STREAM; }
    int protocol() const noexcept { return IPPROTO_TCP; }

    friend bool operator==(tcp const& a, tcp const& b) noexcept { return a.family_ == b.family_; }
    friend bool operator!=(tcp const& a, tcp const& b) noexcept { return a.family_ != b.family_; }

  private:
    explicit tcp(int const family) noexcept : family_{family} {}
    int family_;
};

struct udp {
    using endpoint = basic_endpoint<udp>;
    using socket = udp_socket;
    using resolver = basic_resolver<udp>;

    static udp v4() noexcept { return udp{AF_INET}; }
    static udp v6() noexcept { return udp{AF_INET6}; }

    int family() const noexcept { return family_; }
    int type() const noexcept { return SOCK_DGRAM; }
    int protocol() const noexcept { return IPPROTO_UDP; }

    friend bool operator==(udp const& a, udp const& b) noexcept { return a.family_ == b.family_; }
    friend bool operator!=(udp const& a, udp const& b) noexcept { return a.family_ != b.family_; }

  private:
    explicit udp(int const family) noexcept : family_{family} {}
    int family_;
};

} // namespace ip
} // namespace net
