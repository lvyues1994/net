// 平台基准：每种后端（epoll / poll / select / io_uring）的 TCP 回环往返延迟、吞吐与定时器
// 调度开销。单线程 io_context，一个连接。

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>

#include "net/backend.hpp"
#include "net/buffers.hpp"
#include "net/io_context.hpp"
#include "net/ip.hpp"
#include "net/run_async.hpp"
#include "net/stream.hpp"
#include "net/task.hpp"
#include "net/tcp.hpp"
#include "net/timer.hpp"

#include "bench.hpp"

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

    explicit connected_pair(net::backend_kind const kind) : ctx{kind, 1} {
        net::run_async(ctx.get_executor())(accept_into(&acceptor, &server));
        net::run_async(ctx.get_executor())(connect_to(&client, loopback_endpoint(acceptor)));
        ctx.run();
        client.set_option(net::socket_option::no_delay{true});
        server.set_option(net::socket_option::no_delay{true});
    }
};

// 回显 n 条固定长度消息。
auto echo_n(net::tcp_socket* sock, std::size_t size, std::size_t n)
    CO2_BEG(net::task<>, (sock, size, n), std::vector<char> buf; std::size_t i{}; net::io_result<std::size_t> r;
            net::io_result<std::size_t> w;) {
    buf.resize(size);
    for (i = 0; i != n; ++i) {
        CO2_AWAIT_SET(r, net::read(*sock, net::buffer(buf)));
        if (r.ec) break;
        CO2_AWAIT_SET(w, net::write(*sock, net::buffer(buf)));
        if (w.ec) break;
    }
}
CO2_END

// 发 n 条并等回显：往返。
auto ping_n(net::tcp_socket* sock, std::size_t size, std::size_t n)
    CO2_BEG(net::task<>, (sock, size, n), std::vector<char> buf; std::size_t i{}; net::io_result<std::size_t> r;
            net::io_result<std::size_t> w;) {
    buf.assign(size, 'x');
    for (i = 0; i != n; ++i) {
        CO2_AWAIT_SET(w, net::write(*sock, net::buffer(buf)));
        if (w.ec) break;
        CO2_AWAIT_SET(r, net::read(*sock, net::buffer(buf)));
        if (r.ec) break;
    }
}
CO2_END

// 单向吞吐：发送方连续写 total 字节，接收方读完。
auto send_total(net::tcp_socket* sock, std::size_t chunk, std::size_t total)
    CO2_BEG(net::task<>, (sock, chunk, total), std::vector<char> buf; std::size_t sent{}; net::io_result<std::size_t> w;) {
    buf.assign(chunk, 'y');
    while (sent < total) {
        CO2_AWAIT_SET(w, net::write(*sock, net::buffer(buf)));
        if (w.ec) break;
        sent += w.value;
    }
}
CO2_END

auto receive_total(net::tcp_socket* sock, std::size_t total)
    CO2_BEG(net::task<>, (sock, total), std::vector<char> buf = std::vector<char>(64 * 1024); std::size_t got{};
            net::io_result<std::size_t> r;) {
    while (got < total) {
        CO2_AWAIT_SET(r, sock->read_some(net::buffer(buf)));
        if (r.ec) break;
        got += r.value;
    }
}
CO2_END

// n 个已到期的定时器依次等待。
auto timers_n(net::io_context* ctx, std::size_t n) CO2_BEG(net::task<>, (ctx, n), net::steady_timer timer{*ctx}; std::size_t i{};
                                                             net::io_result<> r;) {
    for (i = 0; i != n; ++i) {
        timer.expires_after(std::chrono::microseconds{1});
        CO2_AWAIT_SET(r, timer.wait());
    }
}
CO2_END

void bench_backend(bench::options const& o, net::backend_kind const kind, std::vector<bench::result>& results) {
    if (not net::backend_available(kind)) {
        std::printf("skipping unavailable backend %s\n", net::to_string(kind));
        return;
    }
    auto const name = std::string{net::to_string(kind)};

    for (auto const size : {std::size_t{64}, std::size_t{4096}}) {
        connected_pair pair{kind};
        results.push_back(bench::run(o, name + ": tcp echo round trip, " + std::to_string(size) + " B", o.scale(20000U),
                                     [&](std::size_t n) {
                                         net::run_async(pair.ctx.get_executor())(echo_n(&pair.server, size, n));
                                         net::run_async(pair.ctx.get_executor())(ping_n(&pair.client, size, n));
                                         pair.ctx.run();
                                     },
                                     "ns per round trip (4 syscalls + 2 wakeups)"));
    }

    {
        connected_pair pair{kind};
        constexpr std::size_t chunk = 64U * 1024U;
        auto const total = o.scale(64U) * 1024U * 1024U; // 64 MiB（quick: 3 MiB）
        auto r = bench::run(o, name + ": tcp throughput, 64 KiB writes", 1U, [&](std::size_t) {
            net::run_async(pair.ctx.get_executor())(receive_total(&pair.server, total));
            net::run_async(pair.ctx.get_executor())(send_total(&pair.client, chunk, total));
            pair.ctx.run();
        });
        auto const seconds = r.ns_per_op / 1e9;
        r.note = std::to_string(static_cast<long>(static_cast<double>(total) / (1024.0 * 1024.0) / seconds)) + " MiB/s";
        r.ns_per_op = r.ns_per_op / static_cast<double>(total / chunk); // ns per 64 KiB chunk
        r.iterations = total / chunk;
        results.push_back(std::move(r));
    }

    {
        net::io_context ctx{kind, 1};
        results.push_back(bench::run(o, name + ": timer expire + resume", o.scale(200000U), [&](std::size_t n) {
            net::run_async(ctx.get_executor())(timers_n(&ctx, n));
            ctx.run();
        }));
    }
}

} // namespace

int main(int argc, char** argv) {
    auto const o = bench::options::parse(argc, argv);
    std::vector<bench::result> results;
    for (auto const kind : {net::backend_kind::epoll, net::backend_kind::poll, net::backend_kind::select,
                            net::backend_kind::io_uring})
        bench_backend(o, kind, results);
    bench::print_table("net backend benchmarks (single-threaded io_context, loopback)", results);
    return 0;
}
