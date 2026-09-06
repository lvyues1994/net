// 与 Boost.Asio 的对照基准：同一台机器、同一个 harness（bench.hpp：多轮中位数 + 全局 operator
// new 计数）、同样的场景（bench_core / bench_net 的行）。两种 Asio 写法：
//   - callbacks：Asio 最快的写法（完成处理器，无协程帧）；
//   - awaitable：C++20 协程（co_spawn + use_awaitable），与 net::task 的模型最接近。
// 单线程 io_context{1}（对应 net 的 single_thread_hint）。本文件以 C++20 编译（awaitable 需要）。

#include <cstdio>
#include <memory>
#include <string>
#include <vector>

#include <boost/asio.hpp>

#include "bench.hpp"

namespace asio = boost::asio;
using tcp = asio::ip::tcp;

namespace {

// ---- callbacks：post hop ----

struct hop_chain : std::enable_shared_from_this<hop_chain> {
    asio::io_context& ioc;
    std::size_t remaining;
    hop_chain(asio::io_context& c, std::size_t n) : ioc{c}, remaining{n} {}
    void start() {
        if (remaining-- == 0U) return;
        asio::post(ioc, [self = shared_from_this()] { self->start(); });
    }
};

// ---- callbacks：timer chain ----

struct timer_chain : std::enable_shared_from_this<timer_chain> {
    asio::steady_timer timer;
    std::size_t remaining;
    timer_chain(asio::io_context& c, std::size_t n) : timer{c}, remaining{n} {}
    void start() {
        if (remaining-- == 0U) return;
        timer.expires_after(std::chrono::microseconds{1});
        timer.async_wait([self = shared_from_this()](boost::system::error_code) { self->start(); });
    }
};

// ---- callbacks：echo ----

struct echo_server_cb : std::enable_shared_from_this<echo_server_cb> {
    tcp::socket& sock;
    std::vector<char> buf;
    std::size_t remaining;
    echo_server_cb(tcp::socket& s, std::size_t size, std::size_t n) : sock{s}, buf(size), remaining{n} {}
    void start() {
        if (remaining-- == 0U) return;
        asio::async_read(sock, asio::buffer(buf), [self = shared_from_this()](boost::system::error_code ec, std::size_t) {
            if (ec) return;
            asio::async_write(self->sock, asio::buffer(self->buf), [self](boost::system::error_code ec2, std::size_t) {
                if (ec2) return;
                self->start();
            });
        });
    }
};

struct echo_client_cb : std::enable_shared_from_this<echo_client_cb> {
    tcp::socket& sock;
    std::vector<char> buf;
    std::size_t remaining;
    echo_client_cb(tcp::socket& s, std::size_t size, std::size_t n) : sock{s}, buf(size, 'x'), remaining{n} {}
    void start() {
        if (remaining-- == 0U) return;
        asio::async_write(sock, asio::buffer(buf), [self = shared_from_this()](boost::system::error_code ec, std::size_t) {
            if (ec) return;
            asio::async_read(self->sock, asio::buffer(self->buf), [self](boost::system::error_code ec2, std::size_t) {
                if (ec2) return;
                self->start();
            });
        });
    }
};

// ---- awaitable ----

asio::awaitable<int> leaf(int v) { co_return v; }

asio::awaitable<long> await_children(std::size_t n) {
    long sum = 0;
    for (std::size_t i = 0; i != n; ++i) sum += co_await leaf(static_cast<int>(i));
    co_return sum;
}

asio::awaitable<void> hops(std::size_t n) {
    for (std::size_t i = 0; i != n; ++i) co_await asio::post(asio::use_awaitable);
}

asio::awaitable<void> timers_n(std::size_t n) {
    asio::steady_timer timer{co_await asio::this_coro::executor};
    for (std::size_t i = 0; i != n; ++i) {
        timer.expires_after(std::chrono::microseconds{1});
        co_await timer.async_wait(asio::use_awaitable);
    }
}

asio::awaitable<void> echo_n(tcp::socket& sock, std::size_t size, std::size_t n) {
    std::vector<char> buf(size);
    for (std::size_t i = 0; i != n; ++i) {
        co_await asio::async_read(sock, asio::buffer(buf), asio::use_awaitable);
        co_await asio::async_write(sock, asio::buffer(buf), asio::use_awaitable);
    }
}

asio::awaitable<void> ping_n(tcp::socket& sock, std::size_t size, std::size_t n) {
    std::vector<char> buf(size, 'x');
    for (std::size_t i = 0; i != n; ++i) {
        co_await asio::async_write(sock, asio::buffer(buf), asio::use_awaitable);
        co_await asio::async_read(sock, asio::buffer(buf), asio::use_awaitable);
    }
}

asio::awaitable<void> send_total(tcp::socket& sock, std::size_t chunk, std::size_t total) {
    std::vector<char> buf(chunk, 'y');
    std::size_t sent = 0;
    while (sent < total) sent += co_await asio::async_write(sock, asio::buffer(buf), asio::use_awaitable);
}

asio::awaitable<void> receive_total(tcp::socket& sock, std::size_t total) {
    std::vector<char> buf(64 * 1024);
    std::size_t got = 0;
    while (got < total) got += co_await sock.async_read_some(asio::buffer(buf), asio::use_awaitable);
}

asio::awaitable<void> accept_n(tcp::acceptor& acceptor, std::size_t n) {
    for (std::size_t i = 0; i != n; ++i) {
        tcp::socket s = co_await acceptor.async_accept(asio::use_awaitable);
        static_cast<void>(s);
    }
}

asio::awaitable<void> connect_n(tcp::endpoint ep, std::size_t n) {
    auto ex = co_await asio::this_coro::executor;
    for (std::size_t i = 0; i != n; ++i) {
        tcp::socket s{ex};
        s.open(tcp::v4());
        s.set_option(asio::socket_base::linger{true, 0}); // 与 bench_net 相同：不进 TIME_WAIT
        co_await s.async_connect(ep, asio::use_awaitable);
        s.close(); // 显式 close：Asio 的析构路径会先清掉用户设置的 linger，那样就没有 RST 了
    }
}

struct connected_pair {
    asio::io_context ioc{1};
    tcp::acceptor acceptor{ioc, tcp::endpoint{asio::ip::address_v4::loopback(), 0}};
    tcp::socket client{ioc};
    tcp::socket server{ioc};

    connected_pair() {
        acceptor.async_accept(server, [](boost::system::error_code) {});
        client.async_connect(tcp::endpoint{asio::ip::address_v4::loopback(), acceptor.local_endpoint().port()},
                             [](boost::system::error_code) {});
        ioc.run();
        ioc.restart();
        client.set_option(tcp::no_delay{true});
        server.set_option(tcp::no_delay{true});
    }
};

struct abort_on_exception {
    void operator()(std::exception_ptr const e) const {
        if (e) {
            std::fprintf(stderr, "asio coroutine threw\n");
            std::abort();
        }
    }
    template <class T> void operator()(std::exception_ptr const e, T&&) const { (*this)(e); }
};
constexpr abort_on_exception detached_or_abort{};

} // namespace

int main(int argc, char** argv) {
    auto const o = bench::options::parse(argc, argv);
    std::vector<bench::result> results;

    {
        asio::io_context ioc{1};
        results.push_back(bench::run(o, "asio awaitable: co_await child", o.scale(2000000U), [&](std::size_t n) {
            asio::co_spawn(ioc, await_children(n), detached_or_abort);
            ioc.run();
            ioc.restart();
        }, "one awaitable frame per op"));
        results.push_back(bench::run(o, "asio awaitable: co_spawn + run round trip", o.scale(300000U), [&](std::size_t n) {
            for (std::size_t i = 0; i != n; ++i) {
                asio::co_spawn(ioc, leaf(1), detached_or_abort);
                ioc.run();
                ioc.restart();
            }
        }));
        results.push_back(bench::run(o, "asio callbacks: post hop", o.scale(2000000U), [&](std::size_t n) {
            std::make_shared<hop_chain>(ioc, n)->start();
            ioc.run();
            ioc.restart();
        }));
        results.push_back(bench::run(o, "asio awaitable: post hop", o.scale(2000000U), [&](std::size_t n) {
            asio::co_spawn(ioc, hops(n), detached_or_abort);
            ioc.run();
            ioc.restart();
        }));
        results.push_back(bench::run(o, "asio callbacks: timer expire + resume", o.scale(200000U), [&](std::size_t n) {
            std::make_shared<timer_chain>(ioc, n)->start();
            ioc.run();
            ioc.restart();
        }));
        results.push_back(bench::run(o, "asio awaitable: timer expire + resume", o.scale(200000U), [&](std::size_t n) {
            asio::co_spawn(ioc, timers_n(n), detached_or_abort);
            ioc.run();
            ioc.restart();
        }));
    }

    for (auto const size : {std::size_t{64}, std::size_t{4096}}) {
        connected_pair pair;
        results.push_back(bench::run(o, "asio callbacks: tcp echo round trip, " + std::to_string(size) + " B", o.scale(20000U),
                                     [&](std::size_t n) {
                                         std::make_shared<echo_server_cb>(pair.server, size, n)->start();
                                         std::make_shared<echo_client_cb>(pair.client, size, n)->start();
                                         pair.ioc.run();
                                         pair.ioc.restart();
                                     },
                                     "ns per round trip, both peers on one thread"));
    }
    for (auto const size : {std::size_t{64}, std::size_t{4096}}) {
        connected_pair pair;
        results.push_back(bench::run(o, "asio awaitable: tcp echo round trip, " + std::to_string(size) + " B", o.scale(20000U),
                                     [&](std::size_t n) {
                                         asio::co_spawn(pair.ioc, echo_n(pair.server, size, n), detached_or_abort);
                                         asio::co_spawn(pair.ioc, ping_n(pair.client, size, n), detached_or_abort);
                                         pair.ioc.run();
                                         pair.ioc.restart();
                                     },
                                     "ns per round trip, both peers on one thread"));
    }

    {
        connected_pair pair;
        constexpr std::size_t chunk = 64U * 1024U;
        auto const total = o.scale(64U) * 1024U * 1024U;
        auto r = bench::run(o, "asio awaitable: tcp throughput, 64 KiB writes", 1U, [&](std::size_t) {
            asio::co_spawn(pair.ioc, receive_total(pair.server, total), detached_or_abort);
            asio::co_spawn(pair.ioc, send_total(pair.client, chunk, total), detached_or_abort);
            pair.ioc.run();
            pair.ioc.restart();
        });
        auto const seconds = r.ns_per_op / 1e9;
        r.note = std::to_string(static_cast<long>(static_cast<double>(total) / (1024.0 * 1024.0) / seconds)) + " MiB/s";
        r.ns_per_op = r.ns_per_op / static_cast<double>(total / chunk);
        r.iterations = total / chunk;
        results.push_back(std::move(r));
    }

    {
        asio::io_context ioc{1};
        tcp::acceptor acceptor{ioc, tcp::endpoint{asio::ip::address_v4::loopback(), 0}};
        tcp::endpoint const ep{asio::ip::address_v4::loopback(), acceptor.local_endpoint().port()};
        results.push_back(bench::run(o, "asio awaitable: tcp connect + accept", o.scale(20000U), [&](std::size_t n) {
            asio::co_spawn(ioc, accept_n(acceptor, n), detached_or_abort);
            asio::co_spawn(ioc, connect_n(ep, n), detached_or_abort);
            ioc.run();
            ioc.restart();
        }, "ns per connection, both peers on one thread"));
    }

    bench::print_table(("Boost.Asio " + std::to_string(BOOST_VERSION / 100000) + "." + std::to_string(BOOST_VERSION / 100 % 1000) +
                        " (epoll, io_context{1})")
                           .c_str(),
                       results);
    return 0;
}
