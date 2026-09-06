// 最小 HTTPS GET：resolver + tcp_socket + tls::openssl_stream + read_until + any_stream。
//
//   ./https_get example.com [/path] [--insecure]
//
// 请求/响应逻辑 http_get 与 http_get.cpp 完全相同——它只看 any_stream&，这次里面是 TLS。
// 证书用系统默认信任锚验证（set_default_verify_paths），主机名经 SNI 与证书校验。

#include <cstdio>
#include <cstring>
#include <exception>
#include <string>

#include "net/net.hpp"

namespace {

auto http_get(net::any_stream& stream, std::string host, std::string path)
    CO2_BEG(net::task<int>, (stream, host, path), std::string request; std::string response;
            net::dynamic_container_buffer<std::string> buffer{response}; net::io_result<std::size_t> w;
            net::io_result<std::size_t> r; net::io_result<std::size_t> body; char chunk[4096];) {
    request = "GET " + path + " HTTP/1.0\r\nHost: " + host + "\r\nConnection: close\r\n\r\n";
    CO2_AWAIT_SET(w, net::write(stream, net::buffer(request)));
    if (w.ec) {
        std::fprintf(stderr, "write: %s\n", w.ec.message().c_str());
        CO2_RETURN(1);
    }
    CO2_AWAIT_SET(r, net::read_until(stream, buffer, "\r\n\r\n"));
    if (r.ec) {
        std::fprintf(stderr, "read headers: %s\n", r.ec.message().c_str());
        CO2_RETURN(1);
    }
    std::printf("%.*s", static_cast<int>(r.value), response.data());
    buffer.consume(r.value);
    for (;;) {
        CO2_AWAIT_SET(body, stream.read_some(net::buffer(chunk)));
        if (body.ec) break;
        std::fwrite(chunk, 1, body.value, stdout);
    }
    std::fwrite(response.data(), 1, response.size(), stdout);
    std::printf("\n");
    // 对端可能先 close_notify（eof），也可能不发直接关（stream_truncated）：HTTP/1.0 两者都常见。
    CO2_RETURN(body.ec == net::error::eof || body.ec == net::error::stream_truncated ? 0 : 1);
}
CO2_END

auto fetch(net::io_context* ctx, net::tls::context tls_ctx, std::string host, std::string path)
    CO2_BEG(net::task<int>, (ctx, tls_ctx, host, path), net::ip::tcp::resolver resolver{*ctx};
            net::io_result<net::ip::tcp::resolver::results_type> resolved; net::tcp_socket sock{*ctx};
            net::io_result<> connected; net::tls::openssl_stream tls{&sock, tls_ctx}; net::io_result<> handshake;
            net::any_stream stream{&tls}; int status{}; net::io_result<> closed;) {
    CO2_AWAIT_SET(resolved, resolver.resolve(host, "443"));
    if (resolved.ec || resolved.value.empty()) {
        std::fprintf(stderr, "resolve %s: %s\n", host.c_str(), resolved.ec.message().c_str());
        CO2_RETURN(1);
    }
    CO2_AWAIT_SET(connected, sock.connect(resolved.value[0].endpoint));
    if (connected.ec) {
        std::fprintf(stderr, "connect: %s\n", connected.ec.message().c_str());
        CO2_RETURN(1);
    }
    tls.set_hostname(host);
    CO2_AWAIT_SET(handshake, tls.handshake(net::tls::role::client));
    if (handshake.ec) {
        std::fprintf(stderr, "tls handshake: %s (%s)\n", handshake.ec.message().c_str(), handshake.ec.category().name());
        CO2_RETURN(1);
    }
    std::fprintf(stderr, "[%s, alpn=%s]\n", net::tls::provider_name(),
                 tls.alpn_selected().empty() ? "-" : tls.alpn_selected().c_str());
    CO2_AWAIT_SET(status, http_get(stream, host, path));
    CO2_AWAIT_SET(closed, tls.shutdown());
    CO2_RETURN(status);
}
CO2_END

} // namespace

int main(int argc, char** argv) {
    if (argc < 2) {
        std::fprintf(stderr, "usage: https_get host [path] [--insecure]\n");
        return 2;
    }
    auto insecure = false;
    std::string path = "/";
    for (auto i = 2; i < argc; ++i) {
        if (std::strcmp(argv[i], "--insecure") == 0)
            insecure = true;
        else
            path = argv[i];
    }
    try {
        net::tls::context tls_ctx;
        if (not insecure) {
            if (auto const ec = tls_ctx.set_verify_mode(net::tls::verify_mode::peer)) throw std::system_error{ec};
            if (auto const ec = tls_ctx.set_default_verify_paths()) throw std::system_error{ec};
        }
        tls_ctx.set_alpn({"http/1.1"});

        net::io_context ctx;
        auto status = 1;
        net::run_async(ctx.get_executor(), [&](int v) { status = v; },
                       [](std::exception_ptr) { std::fprintf(stderr, "unexpected exception\n"); })(
            fetch(&ctx, tls_ctx, argv[1], path));
        ctx.run();
        return status;
    } catch (std::exception const& error) {
        std::fprintf(stderr, "fatal: %s\n", error.what());
        return 1;
    }
}
