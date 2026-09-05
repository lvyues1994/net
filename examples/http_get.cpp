// 最小 HTTP/1.0 GET：resolver + tcp_socket + read_until + dynamic_buffer + any_stream。
//
//   ./http_get example.com [/path]
//
// 请求/响应逻辑 http_get 只依赖 any_stream&，与传输层无关。

#include <cstdio>
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
    // 头部到空行为止。
    CO2_AWAIT_SET(r, net::read_until(stream, buffer, "\r\n\r\n"));
    if (r.ec) {
        std::fprintf(stderr, "read headers: %s\n", r.ec.message().c_str());
        CO2_RETURN(1);
    }
    std::printf("%.*s", static_cast<int>(r.value), response.data());
    buffer.consume(r.value);
    // 其余是正文：读到 EOF。
    for (;;) {
        CO2_AWAIT_SET(body, stream.read_some(net::buffer(chunk)));
        if (body.ec) break;
        std::fwrite(chunk, 1, body.value, stdout);
    }
    std::fwrite(response.data(), 1, response.size(), stdout);
    std::printf("\n");
    CO2_RETURN(body.ec == net::error::eof ? 0 : 1);
}
CO2_END

auto fetch(net::io_context* ctx, std::string host, std::string path)
    CO2_BEG(net::task<int>, (ctx, host, path), net::ip::tcp::resolver resolver{*ctx};
            net::io_result<net::ip::tcp::resolver::results_type> resolved; net::tcp_socket sock{*ctx};
            net::io_result<> connected; net::any_stream stream{&sock}; int status{};) {
    CO2_AWAIT_SET(resolved, resolver.resolve(host, "80"));
    if (resolved.ec || resolved.value.empty()) {
        std::fprintf(stderr, "resolve %s: %s\n", host.c_str(), resolved.ec.message().c_str());
        CO2_RETURN(1);
    }
    CO2_AWAIT_SET(connected, sock.connect(resolved.value[0].endpoint));
    if (connected.ec) {
        std::fprintf(stderr, "connect: %s\n", connected.ec.message().c_str());
        CO2_RETURN(1);
    }
    CO2_AWAIT_SET(status, http_get(stream, host, path));
    CO2_RETURN(status);
}
CO2_END

} // namespace

int main(int argc, char** argv) {
    if (argc < 2) {
        std::fprintf(stderr, "usage: http_get host [path]\n");
        return 2;
    }
    try {
        net::io_context ctx;
        auto status = 1;
        net::run_async(ctx.get_executor(), [&](int v) { status = v; },
                       [](std::exception_ptr) { std::fprintf(stderr, "unexpected exception\n"); })(
            fetch(&ctx, argv[1], argc > 2 ? argv[2] : "/"));
        ctx.run();
        return status;
    } catch (std::exception const& error) {
        std::fprintf(stderr, "fatal: %s\n", error.what());
        return 1;
    }
}
