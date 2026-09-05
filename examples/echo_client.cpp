// 回显客户端：解析、连接、发送一行、读回、带超时。
//
//   ./echo_client [host] [port] [message]

#include <chrono>
#include <cstdio>
#include <exception>
#include <string>

#include "net/net.hpp"

namespace {

using namespace std::chrono;

// 带超时的读：when_any(read, timer)。
auto read_with_timeout(net::io_context* ctx, net::tcp_socket* sock, net::mutable_buffer buf, milliseconds timeout)
    CO2_BEG((net::task<net::io_result<std::size_t>>), (ctx, sock, buf, timeout), net::steady_timer timer{*ctx};
            net::when_any_result<std::tuple<std::size_t, std::tuple<>>> r;) {
    timer.expires_after(timeout);
    CO2_AWAIT_SET(r, net::when_any(sock->read_some(buf), timer.wait()));
    if (r.index == 0) CO2_RETURN((net::io_result<std::size_t>{{}, std::get<0>(r.value)}));
    CO2_RETURN((net::io_result<std::size_t>{std::make_error_code(std::errc::timed_out), 0U}));
}
CO2_END

auto client(net::io_context* ctx, std::string host, std::string port, std::string message)
    CO2_BEG(net::task<int>, (ctx, host, port, message), net::ip::tcp::resolver resolver{*ctx};
            net::io_result<net::ip::tcp::resolver::results_type> resolved; net::tcp_socket sock{*ctx};
            net::io_result<> connected; net::io_result<std::size_t> w; net::io_result<std::size_t> r; char buf[4096];) {
    CO2_AWAIT_SET(resolved, resolver.resolve(host, port));
    if (resolved.ec || resolved.value.empty()) {
        std::fprintf(stderr, "resolve: %s\n", resolved.ec.message().c_str());
        CO2_RETURN(1);
    }
    CO2_AWAIT_SET(connected, sock.connect(resolved.value[0].endpoint));
    if (connected.ec) {
        std::fprintf(stderr, "connect: %s\n", connected.ec.message().c_str());
        CO2_RETURN(1);
    }
    CO2_AWAIT_SET(w, net::write(sock, net::buffer(message)));
    if (w.ec) {
        std::fprintf(stderr, "write: %s\n", w.ec.message().c_str());
        CO2_RETURN(1);
    }
    CO2_AWAIT_SET(r, read_with_timeout(ctx, &sock, net::buffer(buf), seconds{3}));
    if (r.ec) {
        std::fprintf(stderr, "read: %s\n", r.ec.message().c_str());
        CO2_RETURN(1);
    }
    std::printf("echo: %.*s\n", static_cast<int>(r.value), buf);
    CO2_RETURN(0);
}
CO2_END

} // namespace

int main(int argc, char** argv) {
    auto const host = std::string{argc > 1 ? argv[1] : "127.0.0.1"};
    auto const port = std::string{argc > 2 ? argv[2] : "7777"};
    auto const message = std::string{argc > 3 ? argv[3] : "hello, net!"};
    try {
        net::io_context ctx;
        auto status = 1;
        net::run_async(ctx.get_executor(), [&](int v) { status = v; },
                       [](std::exception_ptr e) {
                           try {
                               std::rethrow_exception(e);
                           } catch (std::exception const& error) {
                               std::fprintf(stderr, "error: %s\n", error.what());
                           }
                       })(client(&ctx, host, port, message));
        ctx.run();
        return status;
    } catch (std::exception const& error) {
        std::fprintf(stderr, "fatal: %s\n", error.what());
        return 1;
    }
}
