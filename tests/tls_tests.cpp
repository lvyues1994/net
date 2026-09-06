// TLS：回环握手 / 回显 / 干净关闭、证书与主机名校验失败、传输截断、ALPN、大块传输经
// any_stream（类型擦除的 TLS）、取消、协议版本不匹配、上下文配置错误。OpenSSL 与 BoringSSL
// 共用（差异处按 is_boringssl() 分支）。

#include <chrono>
#include <cstring>
#include <string>
#include <vector>

#include "net/any_stream.hpp"
#include "net/error.hpp"
#include "net/io_context.hpp"
#include "net/ip.hpp"
#include "net/run_async.hpp"
#include "net/stream.hpp"
#include "net/task.hpp"
#include "net/tcp.hpp"
#include "net/timer.hpp"
#include "net/tls/context.hpp"
#include "net/tls/error.hpp"
#include "net/tls/stream.hpp"
#include "net/when_all.hpp"

#include "check.hpp"
#include "tls_test_certs.hpp"

namespace {

using namespace std::chrono;

static_assert(net::is_stream<net::tls::stream>::value, "tls::stream must satisfy Stream");
static_assert(net::is_stream<net::tls::openssl_stream>::value, "openssl_stream must satisfy Stream");

net::tls::context server_context() {
    net::tls::context ctx;
    CHECK(not ctx.use_certificate(net_test_certs::server_certificate(), net::tls::file_format::pem));
    CHECK(not ctx.use_private_key(net_test_certs::server_private_key(), net::tls::file_format::pem));
    return ctx;
}

net::tls::context client_context(bool const trust_server = true) {
    net::tls::context ctx;
    CHECK(not ctx.set_verify_mode(net::tls::verify_mode::peer));
    if (trust_server) CHECK(not ctx.add_certificate_authority(net_test_certs::server_certificate()));
    return ctx;
}

net::ip::tcp::endpoint loopback_endpoint(net::tcp_acceptor const& acceptor) {
    std::error_code ec;
    auto endpoint = acceptor.local_endpoint(ec);
    CHECK(not ec);
    endpoint.address(net::ip::address_v4::loopback());
    return endpoint;
}

auto accept_into(net::tcp_acceptor* acceptor, net::tcp_socket* out)
    CO2_BEG(net::task<>, (acceptor, out), net::io_result<net::tcp_socket> accepted;) {
    CO2_AWAIT_SET(accepted, acceptor->accept());
    CHECK(not accepted.ec);
    *out = std::move(accepted.value);
}
CO2_END

auto connect_to(net::tcp_socket* sock, net::ip::tcp::endpoint ep) CO2_BEG(net::task<>, (sock, ep), net::io_result<> c;) {
    CO2_AWAIT_SET(c, sock->connect(ep));
    CHECK(not c.ec);
}
CO2_END

// 已连接的一对 TCP 套接字。
struct connected_pair {
    test_context ctx;
    net::tcp_acceptor acceptor{ctx, net::ip::tcp::endpoint{net::ip::address_v4::loopback(), 0}};
    net::tcp_socket client{ctx};
    net::tcp_socket server;

    connected_pair() {
        net::run_async(ctx.get_executor())(accept_into(&acceptor, &server));
        net::run_async(ctx.get_executor())(connect_to(&client, loopback_endpoint(acceptor)));
        ctx.run();
        CHECK(server.is_open() && client.is_open());
    }
};

// ---------------------------------------------------------------------------

// 服务端：握手、回显一条消息、等待对端 close_notify（读到 eof）、shutdown。
auto tls_echo_server(net::tls::stream* tls)
    CO2_BEG((net::task<std::error_code>), (tls), net::io_result<> h; char buf[256]; net::io_result<std::size_t> r;
            net::io_result<std::size_t> w; net::io_result<> s;) {
    CO2_AWAIT_SET(h, tls->handshake(net::tls::role::server));
    if (h.ec) CO2_RETURN(h.ec);
    CO2_AWAIT_SET(r, tls->read_some(net::buffer(buf)));
    if (r.ec) CO2_RETURN(r.ec);
    CO2_AWAIT_SET(w, net::write(*tls, net::buffer(buf, r.value)));
    if (w.ec) CO2_RETURN(w.ec);
    CO2_AWAIT_SET(r, tls->read_some(net::buffer(buf)));
    if (r.ec != net::error::eof) CO2_RETURN(r.ec ? r.ec : std::make_error_code(std::errc::protocol_error));
    CO2_AWAIT_SET(s, tls->shutdown());
    CO2_RETURN(s.ec);
}
CO2_END

// 客户端：握手、发一条、读回、shutdown。返回读回的文本；错误经 out 报告。
auto tls_echo_client(net::tls::stream* tls, std::string message, std::error_code* out)
    CO2_BEG(net::task<std::string>, (tls, message, out), net::io_result<> h; net::io_result<std::size_t> w;
            net::io_result<std::size_t> r; net::io_result<> s; std::string reply;) {
    CO2_AWAIT_SET(h, tls->handshake(net::tls::role::client));
    if (h.ec) {
        *out = h.ec;
        CO2_RETURN(reply);
    }
    CO2_AWAIT_SET(w, net::write(*tls, net::buffer(message)));
    if (w.ec) {
        *out = w.ec;
        CO2_RETURN(reply);
    }
    reply.resize(message.size());
    CO2_AWAIT_SET(r, net::read(*tls, net::buffer(reply)));
    if (r.ec) {
        *out = r.ec;
        CO2_RETURN(reply);
    }
    CO2_AWAIT_SET(s, tls->shutdown());
    *out = s.ec;
    CO2_RETURN(reply);
}
CO2_END

void loopback_handshake_echo_and_clean_shutdown() {
    connected_pair pair;
    auto server_ctx = server_context();
    auto client_ctx = client_context();
    net::tls::openssl_stream server{&pair.server, server_ctx};
    net::tls::openssl_stream client{&pair.client, client_ctx};
    client.set_hostname("localhost");
    std::error_code server_ec;
    std::error_code client_ec;
    std::string reply;
    net::run_async(pair.ctx.get_executor(), [&](std::error_code e) { server_ec = e; },
                   [](std::exception_ptr) { CHECK(false); })(tls_echo_server(&server));
    net::run_async(pair.ctx.get_executor(), [&](std::string s) { reply = std::move(s); },
                   [](std::exception_ptr) { CHECK(false); })(tls_echo_client(&client, "hello over tls", &client_ec));
    pair.ctx.run();
    CHECK(not server_ec);
    CHECK(not client_ec);
    CHECK_EQ(reply, "hello over tls");
    CHECK(client.native_handle() != nullptr);
}

void ip_literal_hostname_matches_the_certificate() {
    connected_pair pair;
    auto server_ctx = server_context();
    auto client_ctx = client_context();
    net::tls::openssl_stream server{&pair.server, server_ctx};
    net::tls::openssl_stream client{&pair.client, client_ctx};
    client.set_hostname("127.0.0.1"); // SAN 里有 IP:127.0.0.1；不发 SNI
    std::error_code server_ec;
    std::error_code client_ec;
    std::string reply;
    net::run_async(pair.ctx.get_executor(), [&](std::error_code e) { server_ec = e; }, [](std::exception_ptr) { CHECK(false); })(tls_echo_server(&server));
    net::run_async(pair.ctx.get_executor(), [&](std::string s) { reply = std::move(s); },
                   [](std::exception_ptr) { CHECK(false); })(tls_echo_client(&client, "ip", &client_ec));
    pair.ctx.run();
    CHECK(not server_ec);
    CHECK(not client_ec);
    CHECK_EQ(reply, "ip");
}

// ---------------------------------------------------------------------------

auto server_handshake_only(net::tls::stream* tls) CO2_BEG((net::task<std::error_code>), (tls), net::io_result<> h;) {
    CO2_AWAIT_SET(h, tls->handshake(net::tls::role::server));
    CO2_RETURN(h.ec);
}
CO2_END

auto client_handshake_only(net::tls::stream* tls) CO2_BEG((net::task<std::error_code>), (tls), net::io_result<> h;) {
    CO2_AWAIT_SET(h, tls->handshake(net::tls::role::client));
    CO2_RETURN(h.ec);
}
CO2_END

void untrusted_certificate_fails_verification() {
    connected_pair pair;
    auto server_ctx = server_context();
    auto client_ctx = client_context(false); // 没有信任锚
    net::tls::openssl_stream server{&pair.server, server_ctx};
    net::tls::openssl_stream client{&pair.client, client_ctx};
    client.set_hostname("localhost");
    std::error_code server_ec;
    std::error_code client_ec;
    net::run_async(pair.ctx.get_executor(), [&](std::error_code e) { server_ec = e; }, [](std::exception_ptr) { CHECK(false); })(server_handshake_only(&server));
    net::run_async(pair.ctx.get_executor(), [&](std::error_code e) { client_ec = e; }, [](std::exception_ptr) { CHECK(false); })(client_handshake_only(&client));
    pair.ctx.run();
    CHECK(client_ec);
    CHECK(client_ec.category() == net::tls::verify_category());
    CHECK(not client_ec.message().empty());
    CHECK(server_ec); // 对端以告警中止
}

void wrong_hostname_fails_verification() {
    connected_pair pair;
    auto server_ctx = server_context();
    auto client_ctx = client_context();
    net::tls::openssl_stream server{&pair.server, server_ctx};
    net::tls::openssl_stream client{&pair.client, client_ctx};
    client.set_hostname("not-localhost.example");
    std::error_code server_ec;
    std::error_code client_ec;
    net::run_async(pair.ctx.get_executor(), [&](std::error_code e) { server_ec = e; }, [](std::exception_ptr) { CHECK(false); })(server_handshake_only(&server));
    net::run_async(pair.ctx.get_executor(), [&](std::error_code e) { client_ec = e; }, [](std::exception_ptr) { CHECK(false); })(client_handshake_only(&client));
    pair.ctx.run();
    CHECK(client_ec);
    CHECK(client_ec.category() == net::tls::verify_category());
}

void verify_callback_can_override_the_decision() {
    connected_pair pair;
    auto server_ctx = server_context();
    auto client_ctx = client_context(false);
    auto calls = 0;
    auto saw_failure = false;
    std::string subject;
    client_ctx.set_verify_callback([&](bool preverified, net::tls::verify_context& vc) {
        ++calls;
        subject = vc.subject();
        if (not preverified) saw_failure = true; // 没有信任锚：自签证书预验证失败
        return true;                              // 接受
    });
    net::tls::openssl_stream server{&pair.server, server_ctx};
    net::tls::openssl_stream client{&pair.client, client_ctx};
    std::error_code server_ec;
    std::error_code client_ec;
    net::run_async(pair.ctx.get_executor(), [&](std::error_code e) { server_ec = e; }, [](std::exception_ptr) { CHECK(false); })(server_handshake_only(&server));
    net::run_async(pair.ctx.get_executor(), [&](std::error_code e) { client_ec = e; }, [](std::exception_ptr) { CHECK(false); })(client_handshake_only(&client));
    pair.ctx.run();
    CHECK(not client_ec);
    CHECK(not server_ec);
    CHECK(calls >= 1);
    CHECK(saw_failure);
    CHECK(subject.find("localhost") != std::string::npos);
}

// ---------------------------------------------------------------------------

// 服务端握手后直接关闭 TCP（不发 close_notify）。
auto server_then_truncate(net::tls::stream* tls, net::tcp_socket* raw)
    CO2_BEG((net::task<std::error_code>), (tls, raw), net::io_result<> h;) {
    CO2_AWAIT_SET(h, tls->handshake(net::tls::role::server));
    raw->close();
    CO2_RETURN(h.ec);
}
CO2_END

auto client_read_after_handshake(net::tls::stream* tls)
    CO2_BEG((net::task<std::error_code>), (tls), net::io_result<> h; char buf[16]; net::io_result<std::size_t> r;) {
    CO2_AWAIT_SET(h, tls->handshake(net::tls::role::client));
    if (h.ec) CO2_RETURN(h.ec);
    CO2_AWAIT_SET(r, tls->read_some(net::buffer(buf)));
    CO2_RETURN(r.ec);
}
CO2_END

void transport_close_without_close_notify_is_truncation() {
    connected_pair pair;
    auto server_ctx = server_context();
    auto client_ctx = client_context();
    net::tls::openssl_stream server{&pair.server, server_ctx};
    net::tls::openssl_stream client{&pair.client, client_ctx};
    client.set_hostname("localhost");
    std::error_code server_ec;
    std::error_code client_ec;
    net::run_async(pair.ctx.get_executor(), [&](std::error_code e) { server_ec = e; }, [](std::exception_ptr) { CHECK(false); })(server_then_truncate(&server, &pair.server));
    net::run_async(pair.ctx.get_executor(), [&](std::error_code e) { client_ec = e; }, [](std::exception_ptr) { CHECK(false); })(client_read_after_handshake(&client));
    pair.ctx.run();
    CHECK(not server_ec);
    CHECK(client_ec == net::error::stream_truncated);
}

// ---------------------------------------------------------------------------

void alpn_is_negotiated() {
    connected_pair pair;
    auto server_ctx = server_context();
    auto client_ctx = client_context();
    CHECK(not server_ctx.set_alpn({"h2", "http/1.1"}));
    CHECK(not client_ctx.set_alpn({"http/1.1"}));
    net::tls::openssl_stream server{&pair.server, server_ctx};
    net::tls::openssl_stream client{&pair.client, client_ctx};
    client.set_hostname("localhost");
    std::error_code server_ec;
    std::error_code client_ec;
    net::run_async(pair.ctx.get_executor(), [&](std::error_code e) { server_ec = e; }, [](std::exception_ptr) { CHECK(false); })(server_handshake_only(&server));
    net::run_async(pair.ctx.get_executor(), [&](std::error_code e) { client_ec = e; }, [](std::exception_ptr) { CHECK(false); })(client_handshake_only(&client));
    pair.ctx.run();
    CHECK(not server_ec);
    CHECK(not client_ec);
    CHECK_EQ(client.alpn_selected(), "http/1.1");
    CHECK_EQ(server.alpn_selected(), "http/1.1");
}

// ---------------------------------------------------------------------------

// 大块传输：业务逻辑只看 any_stream&（TLS 被类型擦除）。
auto sink_all(net::any_stream& stream, std::size_t expected)
    CO2_BEG(net::task<std::size_t>, (stream, expected), std::vector<char> buf = std::vector<char>(64 * 1024);
            net::io_result<std::size_t> r;
            std::size_t total{}; std::size_t i{}; unsigned char expected_byte{};) {
    while (total < expected) {
        CO2_AWAIT_SET(r, stream.read_some(net::buffer(buf)));
        if (r.ec) break;
        for (i = 0; i != r.value; ++i) {
            expected_byte = static_cast<unsigned char>((total + i) % 251U);
            CHECK(static_cast<unsigned char>(buf[i]) == expected_byte);
        }
        total += r.value;
    }
    CO2_RETURN(total);
}
CO2_END

auto source_all(net::any_stream& stream, std::size_t count)
    CO2_BEG(net::task<std::size_t>, (stream, count), std::vector<char> data; net::io_result<std::size_t> w; std::size_t i{};) {
    data.resize(count);
    for (i = 0; i != count; ++i)
        data[i] = static_cast<char>(i % 251U);
    CO2_AWAIT_SET(w, net::write(stream, net::buffer(data)));
    CHECK(not w.ec);
    CO2_RETURN(w.value);
}
CO2_END

auto server_bulk(net::tls::openssl_stream* tls, std::size_t count)
    CO2_BEG(net::task<std::size_t>, (tls, count), net::io_result<> h; net::any_stream erased{tls}; std::size_t n{};
            net::io_result<> s;) {
    CO2_AWAIT_SET(h, tls->handshake(net::tls::role::server));
    CHECK(not h.ec);
    CO2_AWAIT_SET(n, sink_all(erased, count));
    CO2_AWAIT_SET(s, tls->shutdown());
    CHECK(not s.ec);
    CO2_RETURN(n);
}
CO2_END

auto client_bulk(net::tls::openssl_stream* tls, std::size_t count)
    CO2_BEG(net::task<std::size_t>, (tls, count), net::io_result<> h; net::any_stream erased{tls}; std::size_t n{};
            net::io_result<> s; char buf[8]; net::io_result<std::size_t> r;) {
    CO2_AWAIT_SET(h, tls->handshake(net::tls::role::client));
    CHECK(not h.ec);
    CO2_AWAIT_SET(n, source_all(erased, count));
    CO2_AWAIT_SET(s, tls->shutdown());
    CHECK(not s.ec);
    // 对端已经 close_notify：再读是干净的 EOF。
    CO2_AWAIT_SET(r, tls->read_some(net::buffer(buf)));
    CHECK(r.ec == net::error::eof);
    CO2_RETURN(n);
}
CO2_END

void bulk_transfer_through_type_erased_tls() {
    connected_pair pair;
    auto server_ctx = server_context();
    auto client_ctx = client_context();
    net::tls::openssl_stream server{&pair.server, server_ctx};
    net::tls::openssl_stream client{&pair.client, client_ctx};
    client.set_hostname("localhost");
    constexpr std::size_t count = 3U * 1024U * 1024U + 777U;
    auto received = std::size_t{};
    auto sent = std::size_t{};
    net::run_async(pair.ctx.get_executor(), [&](std::size_t n) { received = n; }, [](std::exception_ptr) { CHECK(false); })(server_bulk(&server, count));
    net::run_async(pair.ctx.get_executor(), [&](std::size_t n) { sent = n; }, [](std::exception_ptr) { CHECK(false); })(client_bulk(&client, count));
    pair.ctx.run();
    CHECK_EQ(sent, count);
    CHECK_EQ(received, count);
}

// ---------------------------------------------------------------------------

void handshake_can_be_cancelled_by_stop_token() {
    connected_pair pair; // 服务端从不握手：客户端一直等 ServerHello
    auto client_ctx = client_context();
    net::tls::openssl_stream client{&pair.client, client_ctx};
    net::stop_source source;
    std::error_code client_ec;
    net::run_async(pair.ctx.get_executor(), source.get_token(), nullptr, [&](std::error_code e) { client_ec = e; },
                   [](std::exception_ptr) { CHECK(false); })(client_handshake_only(&client));
    pair.ctx.poll();
    source.request_stop();
    auto const start = steady_clock::now();
    pair.ctx.run();
    CHECK(client_ec == net::error::operation_aborted);
    CHECK(steady_clock::now() - start < seconds{2});
}

void protocol_version_mismatch_fails() {
    connected_pair pair;
    auto server_ctx = server_context();
    auto client_ctx = client_context();
    CHECK(not server_ctx.set_max_protocol_version(net::tls::version::tls_1_2));
    CHECK(not client_ctx.set_min_protocol_version(net::tls::version::tls_1_3));
    net::tls::openssl_stream server{&pair.server, server_ctx};
    net::tls::openssl_stream client{&pair.client, client_ctx};
    client.set_hostname("localhost");
    std::error_code server_ec;
    std::error_code client_ec;
    net::run_async(pair.ctx.get_executor(), [&](std::error_code e) { server_ec = e; }, [](std::exception_ptr) { CHECK(false); })(server_handshake_only(&server));
    net::run_async(pair.ctx.get_executor(), [&](std::error_code e) { client_ec = e; }, [](std::exception_ptr) { CHECK(false); })(client_handshake_only(&client));
    pair.ctx.run();
    CHECK(server_ec || client_ec);
}

void context_reports_configuration_errors() {
    net::tls::context ctx;
    CHECK(ctx.use_certificate("not a certificate", net::tls::file_format::pem));
    CHECK(ctx.use_private_key("not a key", net::tls::file_format::pem));
    CHECK(ctx.add_certificate_authority("garbage"));
    CHECK(ctx.use_certificate_file("/nonexistent/cert.pem", net::tls::file_format::pem));
    CHECK(ctx.set_alpn({""}));
    CHECK(not ctx.set_alpn({"h2"}));
    CHECK(not ctx.set_ciphersuites("HIGH:!aNULL"));
    CHECK(not ctx.set_verify_depth(4));
    auto const tls13 = ctx.set_ciphersuites_tls13("TLS_AES_256_GCM_SHA384");
    if (net::tls::is_boringssl())
        CHECK(tls13 == std::errc::function_not_supported);
    else
        CHECK(not tls13);
    CHECK(ctx.native_handle() != nullptr);
    CHECK(std::strlen(net::tls::provider_name()) > 0U);
    std::cout << "provider: " << net::tls::provider_name() << '\n';
}

void owning_stream_and_move() {
    connected_pair pair;
    auto server_ctx = server_context();
    auto client_ctx = client_context();
    net::tls::openssl_stream server{&pair.server, server_ctx};
    net::tls::openssl_stream moved_from{std::move(pair.client), client_ctx}; // 拥有底层套接字
    net::tls::openssl_stream client{std::move(moved_from)};
    client.set_hostname("localhost");
    CHECK(client.next_layer().has_value());
    std::error_code server_ec;
    std::error_code client_ec;
    std::string reply;
    net::run_async(pair.ctx.get_executor(), [&](std::error_code e) { server_ec = e; }, [](std::exception_ptr) { CHECK(false); })(tls_echo_server(&server));
    net::run_async(pair.ctx.get_executor(), [&](std::string s) { reply = std::move(s); },
                   [](std::exception_ptr) { CHECK(false); })(tls_echo_client(&client, "owned", &client_ec));
    pair.ctx.run();
    CHECK(not server_ec);
    CHECK(not client_ec);
    CHECK_EQ(reply, "owned");
}

} // namespace

int main() {
    loopback_handshake_echo_and_clean_shutdown();
    ip_literal_hostname_matches_the_certificate();
    untrusted_certificate_fails_verification();
    wrong_hostname_fails_verification();
    verify_callback_can_override_the_decision();
    transport_close_without_close_notify_is_truncation();
    alpn_is_negotiated();
    bulk_transfer_through_type_erased_tls();
    handshake_can_be_cancelled_by_stop_token();
    protocol_version_mismatch_fails();
    context_reports_configuration_errors();
    owning_stream_and_move();
    std::cout << "tls tests passed (" << net::tls::provider_name() << ")\n";
    return 0;
}
