// TLS 基准：回环握手延迟、加密回环往返、单向吞吐。提供者由构建期决定（OpenSSL / BoringSSL），
// 表头打印 provider_name()，两个构建的输出可以并排比较。

#include <cstdio>
#include <string>
#include <vector>

#include "net/buffers.hpp"
#include "net/io_context.hpp"
#include "net/ip.hpp"
#include "net/run_async.hpp"
#include "net/stream.hpp"
#include "net/task.hpp"
#include "net/tcp.hpp"
#include "net/tls/context.hpp"
#include "net/tls/stream.hpp"

#include "bench.hpp"
#include "tls_bench_certs.hpp"

namespace {

net::ip::tcp::endpoint loopback_endpoint(net::tcp_acceptor const& acceptor) {
    std::error_code ec;
    auto endpoint = acceptor.local_endpoint(ec);
    endpoint.address(net::ip::address_v4::loopback());
    return endpoint;
}

auto accept_into(net::tcp_acceptor* acceptor, net::tcp_socket* out)
    CO2_BEG(net::task<>, (acceptor, out), net::io_result<net::tcp_socket> accepted;) {
    CO2_AWAIT_SET(accepted, acceptor->accept());
    *out = std::move(accepted.value);
}
CO2_END

auto connect_to(net::tcp_socket* sock, net::ip::tcp::endpoint ep) CO2_BEG(net::task<>, (sock, ep), net::io_result<> c;) {
    CO2_AWAIT_SET(c, sock->connect(ep));
}
CO2_END

struct connected_pair {
    net::io_context ctx;
    net::tcp_acceptor acceptor{ctx, net::ip::tcp::endpoint{net::ip::address_v4::loopback(), 0}};
    net::tcp_socket client{ctx};
    net::tcp_socket server;

    connected_pair() {
        net::run_async(ctx.get_executor())(accept_into(&acceptor, &server));
        net::run_async(ctx.get_executor())(connect_to(&client, loopback_endpoint(acceptor)));
        ctx.run();
        client.set_option(net::socket_option::no_delay{true});
        server.set_option(net::socket_option::no_delay{true});
    }
};

net::tls::context server_context() {
    net::tls::context ctx;
    ctx.use_certificate(net_bench_certs::server_certificate(), net::tls::file_format::pem);
    ctx.use_private_key(net_bench_certs::server_private_key(), net::tls::file_format::pem);
    return ctx;
}

net::tls::context client_context() {
    net::tls::context ctx;
    ctx.set_verify_mode(net::tls::verify_mode::peer);
    ctx.add_certificate_authority(net_bench_certs::server_certificate());
    return ctx;
}

auto handshake(net::tls::stream* tls, net::tls::role r) CO2_BEG(net::task<>, (tls, r), net::io_result<> h;) {
    CO2_AWAIT_SET(h, tls->handshake(r));
    if (h.ec) {
        std::fprintf(stderr, "handshake failed: %s\n", h.ec.message().c_str());
        std::abort();
    }
}
CO2_END

auto client_handshake(net::tcp_socket* sock, net::tls::context ctx)
    CO2_BEG(net::task<>, (sock, ctx), net::tls::openssl_stream tls{sock, ctx}; net::io_result<> h;) {
    tls.set_hostname("localhost");
    CO2_AWAIT_SET(h, tls.handshake(net::tls::role::client));
    if (h.ec) {
        std::fprintf(stderr, "client handshake failed: %s\n", h.ec.message().c_str());
        std::abort();
    }
}
CO2_END

// 一个服务端会话：拥有套接字的 TLS 流做一次握手。
auto server_session(net::tcp_socket sock, net::tls::context ctx)
    CO2_BEG(net::task<>, (sock, ctx), net::tls::openssl_stream tls{std::move(sock), ctx}; net::io_result<> h;) {
    CO2_AWAIT_SET(h, tls.handshake(net::tls::role::server));
    if (h.ec) {
        std::fprintf(stderr, "server handshake failed: %s\n", h.ec.message().c_str());
        std::abort();
    }
}
CO2_END

// 服务端：接受 n 个连接，每个握手一次。
auto accept_and_handshake_n(net::tcp_acceptor* acceptor, net::tls::context ctx, std::size_t n)
    CO2_BEG(net::task<>, (acceptor, ctx, n), std::size_t i{}; net::io_result<net::tcp_socket> accepted;) {
    for (i = 0; i != n; ++i) {
        CO2_AWAIT_SET(accepted, acceptor->accept());
        if (accepted.ec) std::abort();
        CO2_AWAIT(server_session(std::move(accepted.value), ctx));
    }
}
CO2_END

// 一个客户端会话：TCP 连接 + TLS 握手。
auto client_session(net::io_context* ioc, net::ip::tcp::endpoint ep, net::tls::context ctx)
    CO2_BEG(net::task<>, (ioc, ep, ctx), net::tcp_socket sock{*ioc}; net::io_result<> c; net::io_result<> h;) {
    CO2_AWAIT_SET(c, sock.connect(ep));
    if (c.ec) std::abort();
    CO2_AWAIT(client_handshake(&sock, ctx));
}
CO2_END

auto connect_and_handshake_n(net::io_context* ioc, net::ip::tcp::endpoint ep, net::tls::context ctx, std::size_t n)
    CO2_BEG(net::task<>, (ioc, ep, ctx, n), std::size_t i{};) {
    for (i = 0; i != n; ++i)
        CO2_AWAIT(client_session(ioc, ep, ctx));
}
CO2_END

auto echo_n(net::tls::stream* tls, std::size_t size, std::size_t n)
    CO2_BEG(net::task<>, (tls, size, n), std::vector<char> buf; std::size_t i{}; net::io_result<std::size_t> r;
            net::io_result<std::size_t> w;) {
    buf.resize(size);
    for (i = 0; i != n; ++i) {
        CO2_AWAIT_SET(r, net::read(*tls, net::buffer(buf)));
        if (r.ec) break;
        CO2_AWAIT_SET(w, net::write(*tls, net::buffer(buf)));
        if (w.ec) break;
    }
}
CO2_END

auto ping_n(net::tls::stream* tls, std::size_t size, std::size_t n)
    CO2_BEG(net::task<>, (tls, size, n), std::vector<char> buf; std::size_t i{}; net::io_result<std::size_t> r;
            net::io_result<std::size_t> w;) {
    buf.assign(size, 'x');
    for (i = 0; i != n; ++i) {
        CO2_AWAIT_SET(w, net::write(*tls, net::buffer(buf)));
        if (w.ec) break;
        CO2_AWAIT_SET(r, net::read(*tls, net::buffer(buf)));
        if (r.ec) break;
    }
}
CO2_END

auto send_total(net::tls::stream* tls, std::size_t chunk, std::size_t total)
    CO2_BEG(net::task<>, (tls, chunk, total), std::vector<char> buf; std::size_t sent{}; net::io_result<std::size_t> w;) {
    buf.assign(chunk, 'y');
    while (sent < total) {
        CO2_AWAIT_SET(w, net::write(*tls, net::buffer(buf)));
        if (w.ec) break;
        sent += w.value;
    }
}
CO2_END

auto receive_total(net::tls::stream* tls, std::size_t total)
    CO2_BEG(net::task<>, (tls, total), std::vector<char> buf = std::vector<char>(64 * 1024); std::size_t got{};
            net::io_result<std::size_t> r;) {
    while (got < total) {
        CO2_AWAIT_SET(r, tls->read_some(net::buffer(buf)));
        if (r.ec) break;
        got += r.value;
    }
}
CO2_END

} // namespace

int main(int argc, char** argv) {
    auto const o = bench::options::parse(argc, argv);
    std::vector<bench::result> results;
    auto const provider = std::string{net::tls::provider_name()};

    {
        net::io_context ioc;
        net::tcp_acceptor acceptor{ioc, net::ip::tcp::endpoint{net::ip::address_v4::loopback(), 0}};
        auto const ep = loopback_endpoint(acceptor);
        auto sctx = server_context();
        auto cctx = client_context();
        results.push_back(bench::run(o, "tcp connect + tls handshake (TLS 1.3, P-256)", o.scale(2000U),
                                     [&](std::size_t n) {
                                         net::run_async(ioc.get_executor())(accept_and_handshake_n(&acceptor, sctx, n));
                                         net::run_async(ioc.get_executor())(connect_and_handshake_n(&ioc, ep, cctx, n));
                                         ioc.run();
                                     },
                                     "ns per connection, both sides on one thread"));
    }

    for (auto const size : {std::size_t{64}, std::size_t{4096}}) {
        connected_pair pair;
        auto sctx = server_context();
        auto cctx = client_context();
        net::tls::openssl_stream server{&pair.server, sctx};
        net::tls::openssl_stream client{&pair.client, cctx};
        client.set_hostname("localhost");
        net::run_async(pair.ctx.get_executor())(handshake(&server, net::tls::role::server));
        net::run_async(pair.ctx.get_executor())(handshake(&client, net::tls::role::client));
        pair.ctx.run();
        results.push_back(bench::run(o, "tls echo round trip, " + std::to_string(size) + " B", o.scale(20000U),
                                     [&](std::size_t n) {
                                         net::run_async(pair.ctx.get_executor())(echo_n(&server, size, n));
                                         net::run_async(pair.ctx.get_executor())(ping_n(&client, size, n));
                                         pair.ctx.run();
                                     },
                                     "ns per round trip"));
    }

    {
        connected_pair pair;
        auto sctx = server_context();
        auto cctx = client_context();
        net::tls::openssl_stream server{&pair.server, sctx};
        net::tls::openssl_stream client{&pair.client, cctx};
        client.set_hostname("localhost");
        net::run_async(pair.ctx.get_executor())(handshake(&server, net::tls::role::server));
        net::run_async(pair.ctx.get_executor())(handshake(&client, net::tls::role::client));
        pair.ctx.run();
        constexpr std::size_t chunk = 16U * 1024U;
        auto const total = o.scale(64U) * 1024U * 1024U;
        auto r = bench::run(o, "tls throughput, 16 KiB writes", 1U, [&](std::size_t) {
            net::run_async(pair.ctx.get_executor())(receive_total(&server, total));
            net::run_async(pair.ctx.get_executor())(send_total(&client, chunk, total));
            pair.ctx.run();
        });
        auto const seconds = r.ns_per_op / 1e9;
        r.note = std::to_string(static_cast<long>(static_cast<double>(total) / (1024.0 * 1024.0) / seconds)) + " MiB/s";
        r.ns_per_op = r.ns_per_op / static_cast<double>(total / chunk);
        r.iterations = total / chunk;
        results.push_back(std::move(r));
    }

    bench::print_table(("net TLS benchmarks: " + provider).c_str(), results);
    return 0;
}
