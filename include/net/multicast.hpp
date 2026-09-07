#pragma once

#include <cstddef>
#include <cstring>

#include <netinet/in.h>
#include <sys/socket.h>

#include "net/ip.hpp"

// 组播套接字选项（Networking TS §21.15 ip::multicast，P4100R1 Paper 11 的 UDP 组播）。
// 每个选项同时支持 v4 与 v6：按构造时给的地址族选择 IPPROTO_IP / IPPROTO_IPV6 一侧的 level / name / 数据。
//
//   net::udp_socket sock{ctx, net::ip::udp::endpoint{net::ip::address_v4::any(), 30001}};
//   sock.set_option(net::ip::multicast::join_group{net::ip::make_address("239.255.0.1")});
//   sock.set_option(net::ip::multicast::hops{4});
//   sock.set_option(net::ip::multicast::enable_loopback{true});
//   sock.set_option(net::ip::multicast::outbound_interface{net::ip::address_v4::loopback()});

namespace net {
namespace ip {
namespace multicast {

namespace detail {

// join / leave：ip_mreq（v4）或 ipv6_mreq（v6）。
template <int V4Name, int V6Name> struct group_request {
    group_request() noexcept : v4_{}, v6_{} {}

    // v4：组地址 + 本地接口地址（默认任意）；v6：组地址 + 接口索引（默认 0 = 任意）。
    explicit group_request(address const& group) : group_request{} {
        if (group.is_v4())
            set_v4(group.to_v4(), address_v4::any());
        else
            set_v6(group.to_v6(), group.to_v6().scope_id());
    }
    group_request(address_v4 const& group, address_v4 const& network_interface) noexcept : group_request{} {
        set_v4(group, network_interface);
    }
    group_request(address_v6 const& group, unsigned long const network_interface) noexcept : group_request{} {
        set_v6(group, network_interface);
    }

    int level() const noexcept { return is_v6_ ? IPPROTO_IPV6 : IPPROTO_IP; }
    int name() const noexcept { return is_v6_ ? V6Name : V4Name; }
    void const* data() const noexcept { return is_v6_ ? static_cast<void const*>(&v6_) : static_cast<void const*>(&v4_); }
    void* data() noexcept { return is_v6_ ? static_cast<void*>(&v6_) : static_cast<void*>(&v4_); }
    std::size_t size() const noexcept { return is_v6_ ? sizeof(v6_) : sizeof(v4_); }
    void resize(std::size_t) noexcept {}

  private:
    void set_v4(address_v4 const& group, address_v4 const& network_interface) noexcept {
        is_v6_ = false;
        auto const g = group.to_bytes();
        auto const i = network_interface.to_bytes();
        std::memcpy(&v4_.imr_multiaddr, g.data(), 4U);
        std::memcpy(&v4_.imr_interface, i.data(), 4U);
    }
    void set_v6(address_v6 const& group, unsigned long const network_interface) noexcept {
        is_v6_ = true;
        auto const g = group.to_bytes();
        std::memcpy(&v6_.ipv6mr_multiaddr, g.data(), 16U);
        v6_.ipv6mr_interface = static_cast<unsigned int>(network_interface);
    }

    ip_mreq v4_;
    ipv6_mreq v6_;
    bool is_v6_ = false;
};

// 整数值选项，v4 与 v6 各一个 name。v4 的 TTL / loop 在内核里是 int（Linux 也接受 unsigned char）。
template <int V4Name, int V6Name> struct integer_pair {
    integer_pair() noexcept = default;
    explicit integer_pair(int const v) noexcept : value_{v} {}
    // 选择地址族（默认按 v4 应用；set_option 前用 for_v6() 切换）。
    integer_pair& for_v4() noexcept {
        is_v6_ = false;
        return *this;
    }
    integer_pair& for_v6() noexcept {
        is_v6_ = true;
        return *this;
    }
    bool is_v6() const noexcept { return is_v6_; }

    int value() const noexcept { return value_; }

    int level() const noexcept { return is_v6_ ? IPPROTO_IPV6 : IPPROTO_IP; }
    int name() const noexcept { return is_v6_ ? V6Name : V4Name; }
    int* data() noexcept { return &value_; }
    int const* data() const noexcept { return &value_; }
    std::size_t size() const noexcept { return sizeof(value_); }
    void resize(std::size_t) noexcept {}

  protected:
    int value_ = 0;
    bool is_v6_ = false;
};

} // namespace detail

// 加入 / 离开组播组。
using join_group = detail::group_request<IP_ADD_MEMBERSHIP, IPV6_JOIN_GROUP>;
using leave_group = detail::group_request<IP_DROP_MEMBERSHIP, IPV6_LEAVE_GROUP>;

// 出向组播的接口：v4 用接口地址（in_addr），v6 用接口索引（unsigned int）。
struct outbound_interface {
    outbound_interface() noexcept : v4_{}, v6_{0} {}
    explicit outbound_interface(address_v4 const& network_interface) noexcept : outbound_interface{} {
        auto const b = network_interface.to_bytes();
        std::memcpy(&v4_, b.data(), 4U);
        is_v6_ = false;
    }
    explicit outbound_interface(unsigned int const v6_interface_index) noexcept : outbound_interface{} {
        v6_ = v6_interface_index;
        is_v6_ = true;
    }

    int level() const noexcept { return is_v6_ ? IPPROTO_IPV6 : IPPROTO_IP; }
    int name() const noexcept { return is_v6_ ? IPV6_MULTICAST_IF : IP_MULTICAST_IF; }
    void const* data() const noexcept { return is_v6_ ? static_cast<void const*>(&v6_) : static_cast<void const*>(&v4_); }
    void* data() noexcept { return is_v6_ ? static_cast<void*>(&v6_) : static_cast<void*>(&v4_); }
    std::size_t size() const noexcept { return is_v6_ ? sizeof(v6_) : sizeof(v4_); }
    void resize(std::size_t) noexcept {}

  private:
    in_addr v4_;
    unsigned int v6_;
    bool is_v6_ = false;
};

// 出向组播的 TTL（v4）/ hop limit（v6）。
struct hops : detail::integer_pair<IP_MULTICAST_TTL, IPV6_MULTICAST_HOPS> {
    hops() noexcept = default;
    explicit hops(int const v) noexcept : integer_pair{v} {}
};

// 本机是否收到自己发出的组播（默认开）。
struct enable_loopback : detail::integer_pair<IP_MULTICAST_LOOP, IPV6_MULTICAST_LOOP> {
    enable_loopback() noexcept = default;
    explicit enable_loopback(bool const v) noexcept : integer_pair{v ? 1 : 0} {}
    explicit operator bool() const noexcept { return value_ != 0; }
};

} // namespace multicast

// 单播 TTL / hop limit（Networking TS §21.14 ip::unicast）。
namespace unicast {
struct hops : multicast::detail::integer_pair<IP_TTL, IPV6_UNICAST_HOPS> {
    hops() noexcept = default;
    explicit hops(int const v) noexcept : integer_pair{v} {}
};
} // namespace unicast

} // namespace ip
} // namespace net
