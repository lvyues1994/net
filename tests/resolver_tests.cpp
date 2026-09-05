// DNS：数字地址解析（不依赖网络）、localhost、反向解析、失败、取消。

#include <string>

#include "net/io_context.hpp"
#include "net/error.hpp"
#include "net/ip.hpp"
#include "net/resolver.hpp"
#include "net/run_async.hpp"
#include "net/task.hpp"

#include "check.hpp"

namespace {

using results_type = net::ip::tcp::resolver::results_type;

auto resolve(net::ip::tcp::resolver* resolver, std::string host, std::string service, net::ip::resolver_flags flags)
    CO2_BEG((net::task<net::io_result<results_type>>), (resolver, host, service, flags), net::io_result<results_type> r;) {
    CO2_AWAIT_SET(r, resolver->resolve(host, service, flags));
    CO2_RETURN(r);
}
CO2_END

net::io_result<results_type> run_resolve(net::io_context& ctx, net::ip::tcp::resolver& resolver, std::string host,
                                         std::string service, net::ip::resolver_flags flags = net::ip::resolver_flags::none) {
    net::io_result<results_type> result;
    net::run_async(ctx.get_executor(), [&](net::io_result<results_type> v) { result = std::move(v); },
                   [](std::exception_ptr) { CHECK(false); })(resolve(&resolver, std::move(host), std::move(service), flags));
    ctx.run();
    return result;
}

void numeric_host_and_service() {
    net::io_context ctx;
    net::ip::tcp::resolver resolver{ctx};
    auto const r = run_resolve(ctx, resolver, "127.0.0.1", "8080",
                               net::ip::resolver_flags::numeric_host | net::ip::resolver_flags::numeric_service);
    CHECK(not r.ec);
    CHECK_EQ(r.value.size(), 1U);
    CHECK(r.value[0].endpoint.address() == net::ip::address_v4::loopback());
    CHECK_EQ(r.value[0].endpoint.port(), 8080);
    CHECK_EQ(r.value[0].service_name, "8080");
}

void ipv6_numeric_host() {
    net::io_context ctx;
    net::ip::tcp::resolver resolver{ctx};
    auto const r = run_resolve(ctx, resolver, "::1", "1", net::ip::resolver_flags::numeric_host | net::ip::resolver_flags::numeric_service);
    CHECK(not r.ec);
    CHECK_EQ(r.value.size(), 1U);
    CHECK(r.value[0].endpoint.address().is_v6());
    CHECK(r.value[0].endpoint.address().to_v6().is_loopback());
    CHECK_EQ(net::ip::to_string(r.value[0].endpoint), "[::1]:1");
}

void localhost_resolves_to_loopback() {
    net::io_context ctx;
    net::ip::tcp::resolver resolver{ctx};
    auto const r = run_resolve(ctx, resolver, "localhost", "80", net::ip::resolver_flags::numeric_service);
    CHECK(not r.ec);
    CHECK(not r.value.empty());
    for (auto const& entry : r.value) {
        CHECK(entry.endpoint.address().is_loopback());
        CHECK_EQ(entry.endpoint.port(), 80);
    }
}

void invalid_numeric_host_fails() {
    net::io_context ctx;
    net::ip::tcp::resolver resolver{ctx};
    auto const r = run_resolve(ctx, resolver, "not-an-address", "80",
                               net::ip::resolver_flags::numeric_host | net::ip::resolver_flags::numeric_service);
    CHECK(r.ec == net::error::host_not_found);
    CHECK(r.value.empty());
}

void passive_resolution_for_binding() {
    net::io_context ctx;
    net::ip::tcp::resolver resolver{ctx};
    auto const r = run_resolve(ctx, resolver, "", "9000",
                               net::ip::resolver_flags::passive | net::ip::resolver_flags::numeric_service);
    CHECK(not r.ec);
    CHECK(not r.value.empty());
    CHECK(r.value[0].endpoint.address().is_unspecified());
    CHECK_EQ(r.value[0].endpoint.port(), 9000);
}

auto reverse(net::ip::tcp::resolver* resolver, net::ip::tcp::endpoint ep)
    CO2_BEG((net::task<net::io_result<results_type>>), (resolver, ep), net::io_result<results_type> r;) {
    CO2_AWAIT_SET(r, resolver->resolve(ep));
    CO2_RETURN(r);
}
CO2_END

void reverse_resolution_of_loopback() {
    net::io_context ctx;
    net::ip::tcp::resolver resolver{ctx};
    net::io_result<results_type> result;
    net::run_async(ctx.get_executor(), [&](net::io_result<results_type> v) { result = std::move(v); },
                   [](std::exception_ptr) { CHECK(false); })(reverse(&resolver, net::ip::tcp::endpoint{net::ip::address_v4::loopback(), 22}));
    ctx.run();
    CHECK(not result.ec);
    CHECK_EQ(result.value.size(), 1U);
    CHECK(not result.value[0].host_name.empty());
    CHECK(result.value[0].endpoint.address() == net::ip::address_v4::loopback());
}

void stop_token_cancels_a_resolution() {
    net::io_context ctx;
    net::ip::tcp::resolver resolver{ctx};
    net::stop_source source;
    source.request_stop();
    net::io_result<results_type> result;
    net::run_async(ctx.get_executor(), source.get_token(), nullptr,
                   [&](net::io_result<results_type> v) { result = std::move(v); },
                   [](std::exception_ptr) { CHECK(false); })(resolve(&resolver, "localhost", "80", net::ip::resolver_flags::none));
    ctx.run();
    CHECK(result.ec == net::error::operation_aborted);
}

void address_parsing() {
    std::error_code ec;
    auto const v4 = net::ip::make_address("192.168.1.10", ec);
    CHECK(not ec && v4.is_v4());
    CHECK_EQ(v4.to_string(), "192.168.1.10");
    CHECK_EQ(v4.to_v4().to_uint(), 0xC0A8010AU);
    auto const v6 = net::ip::make_address("fe80::1%3", ec);
    CHECK(not ec && v6.is_v6());
    CHECK_EQ(v6.to_v6().scope_id(), 3U);
    CHECK_EQ(v6.to_string(), "fe80::1%3");
    net::ip::make_address("nope", ec);
    CHECK(ec == net::error::invalid_address);
    auto threw = false;
    try {
        net::ip::make_address_v4("300.1.1.1");
    } catch (std::system_error const&) {
        threw = true;
    }
    CHECK(threw);
    CHECK(net::ip::address_v6::loopback().is_loopback());
    CHECK(net::ip::address_v4::any().is_unspecified());
    CHECK(net::ip::address_v4{0xE0000001U}.is_multicast());
    net::ip::tcp::endpoint ep{net::ip::make_address("::1"), 443};
    CHECK(ep.protocol() == net::ip::tcp::v6());
    CHECK_EQ(ep.port(), 443);
    ep.port(80);
    CHECK_EQ(net::ip::to_string(ep), "[::1]:80");
    CHECK(net::ip::tcp::endpoint{} < ep);
}

} // namespace

int main() {
    numeric_host_and_service();
    ipv6_numeric_host();
    localhost_resolves_to_loopback();
    invalid_numeric_host_fails();
    passive_resolution_for_binding();
    reverse_resolution_of_loopback();
    stop_token_cancels_a_resolution();
    address_parsing();
    std::cout << "resolver tests passed\n";
    return 0;
}
