// UDP：send_to / receive_from 回环、connect 后的 send / receive、取消。

#include <string>

#include "net/io_context.hpp"
#include "net/error.hpp"
#include "net/ip.hpp"
#include "net/multicast.hpp"
#include "net/run_async.hpp"
#include "net/stream.hpp"
#include "net/task.hpp"
#include "net/udp.hpp"

#include "check.hpp"

namespace {

static_assert(net::is_stream<net::udp_socket>::value, "connected udp_socket satisfies Stream");

net::ip::udp::endpoint local_of(net::udp_socket const& sock) {
    std::error_code ec;
    auto ep = sock.local_endpoint(ec);
    CHECK(not ec);
    ep.address(net::ip::address_v4::loopback());
    return ep;
}

auto receive_one(net::udp_socket* sock, net::ip::udp::endpoint* sender)
    CO2_BEG(net::task<std::string>, (sock, sender), char buf[64]; net::io_result<std::size_t> r;) {
    CO2_AWAIT_SET(r, sock->receive_from(net::buffer(buf), *sender));
    CHECK(not r.ec);
    CO2_RETURN(std::string(buf, r.value));
}
CO2_END

auto send_one(net::udp_socket* sock, net::ip::udp::endpoint target, std::string text)
    CO2_BEG(net::task<std::size_t>, (sock, target, text), net::io_result<std::size_t> r;) {
    CO2_AWAIT_SET(r, sock->send_to(net::buffer(text), target));
    CHECK(not r.ec);
    CO2_RETURN(r.value);
}
CO2_END

void datagram_roundtrip() {
    test_context ctx;
    net::udp_socket receiver{ctx, net::ip::udp::endpoint{net::ip::address_v4::loopback(), 0}};
    net::udp_socket sender{ctx, net::ip::udp::endpoint{net::ip::address_v4::loopback(), 0}};
    net::ip::udp::endpoint from;
    std::string got;
    auto sent = std::size_t{};
    net::run_async(ctx.get_executor(), [&](std::string s) { got = std::move(s); }, [](std::exception_ptr) { CHECK(false); })(receive_one(&receiver, &from));
    net::run_async(ctx.get_executor(), [&](std::size_t n) { sent = n; }, [](std::exception_ptr) { CHECK(false); })(send_one(&sender, local_of(receiver), "ping"));
    ctx.run();
    CHECK_EQ(sent, 4U);
    CHECK_EQ(got, "ping");
    CHECK_EQ(from.port(), local_of(sender).port());
    CHECK(from.address().is_loopback());
}

auto echo_connected(net::udp_socket* sock) CO2_BEG(net::task<std::string>, (sock), char buf[16];
                                                    std::string const message{"hello"};
                                                    net::io_result<std::size_t> w; net::io_result<std::size_t> r;) {
    CO2_AWAIT_SET(w, sock->send(net::buffer(message)));
    CHECK(not w.ec);
    CO2_AWAIT_SET(r, sock->receive(net::buffer(buf)));
    CHECK(not r.ec);
    CO2_RETURN(std::string(buf, r.value));
}
CO2_END

auto reflect(net::udp_socket* sock) CO2_BEG(net::task<>, (sock), char buf[16]; net::ip::udp::endpoint from;
                                            net::io_result<std::size_t> r; net::io_result<std::size_t> w;) {
    CO2_AWAIT_SET(r, sock->receive_from(net::buffer(buf), from));
    CHECK(not r.ec);
    CO2_AWAIT_SET(w, sock->send_to(net::buffer(buf, r.value), from));
    CHECK(not w.ec);
}
CO2_END

void connected_socket_uses_send_and_receive() {
    test_context ctx;
    net::udp_socket server{ctx, net::ip::udp::endpoint{net::ip::address_v4::loopback(), 0}};
    net::udp_socket client{ctx};
    CHECK(not client.connect(local_of(server)));
    std::string reply;
    net::run_async(ctx.get_executor())(reflect(&server));
    net::run_async(ctx.get_executor(), [&](std::string s) { reply = std::move(s); }, [](std::exception_ptr) { CHECK(false); })(echo_connected(&client));
    ctx.run();
    CHECK_EQ(reply, "hello");
}

auto blocked_receive(net::udp_socket* sock) CO2_BEG((net::task<std::error_code>), (sock), char buf[8];
                                                    net::io_result<std::size_t> r;) {
    CO2_AWAIT_SET(r, sock->receive(net::buffer(buf)));
    CO2_RETURN(r.ec);
}
CO2_END

void cancel_aborts_a_pending_receive() {
    test_context ctx;
    net::udp_socket sock{ctx, net::ip::udp::endpoint{net::ip::address_v4::loopback(), 0}};
    std::error_code ec;
    net::run_async(ctx.get_executor(), [&](std::error_code e) { ec = e; }, [](std::exception_ptr) { CHECK(false); })(blocked_receive(&sock));
    CHECK_EQ(ctx.poll(), 1U);
    sock.cancel();
    ctx.run();
    CHECK(ec == net::error::operation_aborted);
}

// ---- 组播选项（Paper 11）：在回环接口上加入 239.255.0.1，自己发自己收；hops / loopback / 出向接口 ----

void multicast_options_join_send_receive_leave() {
    test_context ctx;
    auto const group = net::ip::make_address("239.255.0.1");
    CHECK(group.to_v4().is_multicast());
    net::udp_socket receiver{ctx};
    CHECK(not receiver.open(net::ip::udp::v4()));
    CHECK(not receiver.set_option(net::socket_option::reuse_address{true}));
    CHECK(not receiver.bind(net::ip::udp::endpoint{net::ip::address_v4::any(), 0}));
    // 在回环接口上加入组
    auto const join_ec = receiver.set_option(net::ip::multicast::join_group{group.to_v4(), net::ip::address_v4::loopback()});
    if (join_ec) {
        std::cout << "multicast join not permitted here (" << join_ec.message() << "), skipping\n";
        return;
    }
    net::udp_socket sender{ctx, net::ip::udp::endpoint{net::ip::address_v4::loopback(), 0}};
    CHECK(not sender.set_option(net::ip::multicast::outbound_interface{net::ip::address_v4::loopback()}));
    CHECK(not sender.set_option(net::ip::multicast::hops{1}));
    CHECK(not sender.set_option(net::ip::multicast::enable_loopback{true}));
    // 读回：hops 与 loopback
    net::ip::multicast::hops hops_back;
    CHECK(not sender.get_option(hops_back));
    CHECK_EQ(hops_back.value(), 1);
    net::ip::multicast::enable_loopback loop_back;
    CHECK(not sender.get_option(loop_back));
    CHECK(static_cast<bool>(loop_back));
    // 单播 TTL 也在同一族里
    CHECK(not sender.set_option(net::ip::unicast::hops{7}));
    net::ip::unicast::hops ttl_back;
    CHECK(not sender.get_option(ttl_back));
    CHECK_EQ(ttl_back.value(), 7);

    std::error_code ec;
    auto const port = receiver.local_endpoint(ec).port();
    net::ip::udp::endpoint const target{group, port};
    net::ip::udp::endpoint from;
    std::string got;
    auto sent = std::size_t{};
    net::run_async(ctx.get_executor(), [&](std::string s) { got = std::move(s); }, [](std::exception_ptr) { CHECK(false); })(
        receive_one(&receiver, &from));
    net::run_async(ctx.get_executor(), [&](std::size_t n) { sent = n; }, [](std::exception_ptr) { CHECK(false); })(
        send_one(&sender, target, "multicast"));
    ctx.run();
    CHECK_EQ(sent, 9U);
    CHECK_EQ(got, "multicast");
    CHECK(from.address().is_loopback());
    // 离开组
    CHECK(not receiver.set_option(net::ip::multicast::leave_group{group.to_v4(), net::ip::address_v4::loopback()}));
    // 再离开一次：不在组里 → EADDRNOTAVAIL
    auto const twice = receiver.set_option(net::ip::multicast::leave_group{group.to_v4(), net::ip::address_v4::loopback()});
    CHECK(twice == std::errc::address_not_available);
    // v6 形态的选项至少构造正确（level / name / size）
    net::ip::multicast::join_group const v6_join{net::ip::make_address("ff02::1").to_v6(), 1UL};
    CHECK_EQ(v6_join.level(), IPPROTO_IPV6);
    CHECK_EQ(v6_join.name(), IPV6_JOIN_GROUP);
    CHECK_EQ(v6_join.size(), sizeof(ipv6_mreq));
    net::ip::multicast::outbound_interface const v6_if{1U};
    CHECK_EQ(v6_if.level(), IPPROTO_IPV6);
    CHECK_EQ(v6_if.size(), sizeof(unsigned int));
    auto v6_hops = net::ip::multicast::hops{3};
    v6_hops.for_v6();
    CHECK_EQ(v6_hops.name(), IPV6_MULTICAST_HOPS);
}

} // namespace

int main() {
    datagram_roundtrip();
    connected_socket_uses_send_and_receive();
    cancel_aborts_a_pending_receive();
    multicast_options_join_send_receive_leave();
    std::cout << "udp tests passed\n";
    return 0;
}
