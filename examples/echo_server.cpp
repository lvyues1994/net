// 回显服务器：accept 循环 + 每个连接一条协程链；SIGINT / SIGTERM 优雅退出。
//
//   ./echo_server [port]        （默认 7777）
//
// 业务逻辑 echo_session 只依赖 any_stream&：换成 TLS 或别的传输不需要改它。

#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <string>

#include "net/net.hpp"

namespace {

// 回显直到对端关闭或出错。
auto echo_session(net::any_stream& stream) CO2_BEG(net::task<>, (stream), char buf[4096]; net::io_result<std::size_t> r;
                                                    net::io_result<std::size_t> w;) {
    for (;;) {
        CO2_AWAIT_SET(r, stream.read_some(net::buffer(buf)));
        if (r.ec) break;
        CO2_AWAIT_SET(w, net::write(stream, net::buffer(buf, r.value)));
        if (w.ec) break;
    }
}
CO2_END

// 每个连接：把套接字包成 any_stream 交给业务逻辑。
auto serve(net::tcp_socket sock) CO2_BEG(net::task<>, (sock), net::any_stream stream{&sock}; std::error_code ec;) {
    std::printf("[+] %s\n", net::ip::to_string(sock.remote_endpoint(ec)).c_str());
    CO2_AWAIT(echo_session(stream));
    std::printf("[-] %s\n", net::ip::to_string(sock.remote_endpoint(ec)).c_str());
}
CO2_END

// accept 循环：每个连接用 run_async 启动独立的链（fire and forget），直到 acceptor 被关闭。
// 会话链可能比本链活得久，所以用 io_context 的执行器副本而不是 this_coro::executor 给出的
// 非拥有 executor_ref 来启动它们。
auto accept_loop(net::io_context* ctx, net::tcp_acceptor* acceptor)
    CO2_BEG(net::task<>, (ctx, acceptor), net::io_result<net::tcp_socket> accepted;) {
    for (;;) {
        CO2_AWAIT_SET(accepted, acceptor->accept());
        if (accepted.ec) {
            if (accepted.ec != net::error::operation_aborted)
                std::fprintf(stderr, "accept: %s\n", accepted.ec.message().c_str());
            break;
        }
        net::run_async(ctx->get_executor(), [] {}, [](std::exception_ptr) { std::fprintf(stderr, "session failed\n"); })(
            serve(std::move(accepted.value)));
    }
}
CO2_END

// 等待 SIGINT / SIGTERM，然后请求停止。
auto wait_for_shutdown(net::signal_set* signals, net::stop_source* stop, net::tcp_acceptor* acceptor)
    CO2_BEG(net::task<>, (signals, stop, acceptor), net::io_result<int> r;) {
    CO2_AWAIT_SET(r, signals->wait());
    if (not r.ec) std::printf("signal %d: shutting down\n", r.value);
    stop->request_stop();
    acceptor->close();
}
CO2_END

} // namespace

int main(int argc, char** argv) {
    auto const port = static_cast<net::ip::port_type>(argc > 1 ? std::atoi(argv[1]) : 7777);
    try {
        net::io_context ctx;
        net::tcp_acceptor acceptor{ctx, net::ip::tcp::endpoint{net::ip::address_v4::any(), port}};
        net::signal_set signals{ctx, SIGINT, SIGTERM};
        net::stop_source stop;
        std::printf("echo server listening on port %u\n", static_cast<unsigned>(port));

        net::run_async(ctx.get_executor(), stop.get_token())(accept_loop(&ctx, &acceptor));
        net::run_async(ctx.get_executor())(wait_for_shutdown(&signals, &stop, &acceptor));
        ctx.run();
        return 0;
    } catch (std::exception const& error) {
        std::fprintf(stderr, "fatal: %s\n", error.what());
        return 1;
    }
}
