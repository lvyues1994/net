// TCP：接受器 / 连接 / 读写回环、部分传输、EOF、取消、stop_token、类型擦除的 any_stream、
// 多线程 run。

#include <atomic>
#include <chrono>
#include <cstring>
#include <string>
#include <thread>
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
#include "net/when_all.hpp"
#include "net/when_any.hpp"

#include "check.hpp"

namespace {

using namespace std::chrono;

static_assert(net::is_stream<net::tcp_socket>::value, "tcp_socket must satisfy Stream");

net::ip::tcp::endpoint loopback_endpoint(net::tcp_acceptor const& acceptor) {
    std::error_code ec;
    auto endpoint = acceptor.local_endpoint(ec);
    CHECK(not ec);
    endpoint.address(net::ip::address_v4::loopback());
    return endpoint;
}

// 回显一个连接直到对端关闭；返回回显的字节数。
auto echo_session(net::tcp_socket sock) CO2_BEG(net::task<std::size_t>, (sock), char buf[64];
                                                 net::io_result<std::size_t> r; net::io_result<std::size_t> w;
                                                 std::size_t total{};) {
    for (;;) {
        CO2_AWAIT_SET(r, sock.read_some(net::buffer(buf)));
        if (r.ec) break;
        CO2_AWAIT_SET(w, net::write(sock, net::buffer(buf, r.value)));
        if (w.ec) break;
        total += w.value;
    }
    CO2_RETURN(total);
}
CO2_END

// 接受一个连接并回显。
auto echo_once(net::tcp_acceptor* acceptor) CO2_BEG(net::task<std::size_t>, (acceptor), net::io_result<net::tcp_socket> accepted;
                                                    std::size_t n{};) {
    CO2_AWAIT_SET(accepted, acceptor->accept());
    CHECK(not accepted.ec);
    CHECK(accepted.value.is_open());
    CO2_AWAIT_SET(n, echo_session(std::move(accepted.value)));
    CO2_RETURN(n);
}
CO2_END

// 客户端：连接、写、读回、关闭。返回读回的内容。
auto client_roundtrip(net::io_context* ctx, net::ip::tcp::endpoint ep, std::string message)
    CO2_BEG(net::task<std::string>, (ctx, ep, message), net::tcp_socket sock{*ctx}; net::io_result<> c;
            net::io_result<std::size_t> w; net::io_result<std::size_t> r; std::string reply;) {
    CO2_AWAIT_SET(c, sock.connect(ep));
    CHECK(not c.ec);
    CO2_AWAIT_SET(w, net::write(sock, net::buffer(message)));
    CHECK(not w.ec);
    CHECK_EQ(w.value, message.size());
    reply.resize(message.size());
    CO2_AWAIT_SET(r, net::read(sock, net::buffer(reply)));
    CHECK(not r.ec);
    sock.shutdown(net::shutdown_type::send);
    // 对端在我们关闭后结束会话：再读一次拿到 EOF。
    CO2_AWAIT_SET(r, sock.read_some(net::buffer(reply)));
    CHECK(r.ec == net::error::eof);
    CO2_RETURN(reply);
}
CO2_END

void loopback_echo() {
    net::io_context ctx;
    net::tcp_acceptor acceptor{ctx, net::ip::tcp::endpoint{net::ip::address_v4::loopback(), 0}};
    auto const ep = loopback_endpoint(acceptor);
    auto echoed = std::size_t{};
    std::string reply;
    net::run_async(ctx.get_executor(), [&](std::size_t n) { echoed = n; }, [](std::exception_ptr) { CHECK(false); })(echo_once(&acceptor));
    auto const message = std::string(1000, 'x') + "tail";
    net::run_async(ctx.get_executor(), [&](std::string s) { reply = std::move(s); },
                   [](std::exception_ptr) { CHECK(false); })(client_roundtrip(&ctx, ep, message));
    ctx.run();
    CHECK_EQ(echoed, message.size());
    CHECK_EQ(reply, message);
}

// ---------------------------------------------------------------------------

auto connect_refused(net::io_context* ctx, net::ip::tcp::endpoint ep)
    CO2_BEG((net::task<std::error_code>), (ctx, ep), net::tcp_socket sock{*ctx}; net::io_result<> c;) {
    CO2_AWAIT_SET(c, sock.connect(ep));
    CO2_RETURN(c.ec);
}
CO2_END

void connect_to_a_closed_port_fails() {
    net::io_context ctx;
    // 找一个空闲端口：绑定后立刻关闭。
    net::tcp_acceptor probe{ctx, net::ip::tcp::endpoint{net::ip::address_v4::loopback(), 0}};
    auto const ep = loopback_endpoint(probe);
    probe.close();
    std::error_code ec;
    net::run_async(ctx.get_executor(), [&](std::error_code e) { ec = e; }, [](std::exception_ptr) { CHECK(false); })(connect_refused(&ctx, ep));
    ctx.run();
    CHECK(ec == std::errc::connection_refused);
}

// ---------------------------------------------------------------------------

// 读一个永远不会有数据的连接；被 cancel() / close() / stop_token 打断。
auto blocked_read(net::tcp_socket* sock) CO2_BEG((net::task<std::error_code>), (sock), char buf[8];
                                                 net::io_result<std::size_t> r;) {
    CO2_AWAIT_SET(r, sock->read_some(net::buffer(buf)));
    CO2_RETURN(r.ec);
}
CO2_END

auto accept_and_hold(net::tcp_acceptor* acceptor, net::tcp_socket* out)
    CO2_BEG(net::task<>, (acceptor, out), net::io_result<net::tcp_socket> accepted;) {
    CO2_AWAIT_SET(accepted, acceptor->accept());
    CHECK(not accepted.ec);
    *out = std::move(accepted.value);
}
CO2_END

auto connect_only(net::tcp_socket* sock, net::ip::tcp::endpoint ep) CO2_BEG(net::task<>, (sock, ep), net::io_result<> c;) {
    CO2_AWAIT_SET(c, sock->connect(ep));
    CHECK(not c.ec);
}
CO2_END

struct connected_pair {
    net::io_context ctx;
    net::tcp_acceptor acceptor{ctx, net::ip::tcp::endpoint{net::ip::address_v4::loopback(), 0}};
    net::tcp_socket client{ctx};
    net::tcp_socket server;

    connected_pair() {
        net::run_async(ctx.get_executor())(accept_and_hold(&acceptor, &server));
        net::run_async(ctx.get_executor())(connect_only(&client, loopback_endpoint(acceptor)));
        ctx.run();
        CHECK(server.is_open() && client.is_open());
    }
};

void cancel_aborts_a_pending_read() {
    connected_pair pair;
    std::error_code ec;
    net::run_async(pair.ctx.get_executor(), [&](std::error_code e) { ec = e; }, [](std::exception_ptr) { CHECK(false); })(blocked_read(&pair.client));
    CHECK_EQ(pair.ctx.poll(), 1U); // 挂起在读上
    pair.client.cancel();
    pair.ctx.run();
    CHECK(ec == net::error::operation_aborted);
    CHECK(pair.client.is_open());
}

void close_aborts_a_pending_read() {
    connected_pair pair;
    std::error_code ec;
    net::run_async(pair.ctx.get_executor(), [&](std::error_code e) { ec = e; }, [](std::exception_ptr) { CHECK(false); })(blocked_read(&pair.client));
    CHECK_EQ(pair.ctx.poll(), 1U);
    pair.client.close();
    pair.ctx.run();
    CHECK(ec == net::error::operation_aborted);
    CHECK(not pair.client.is_open());
}

void stop_token_aborts_a_pending_read() {
    connected_pair pair;
    net::stop_source source;
    std::error_code ec;
    net::run_async(pair.ctx.get_executor(), source.get_token(), nullptr, [&](std::error_code e) { ec = e; },
                   [](std::exception_ptr) { CHECK(false); })(blocked_read(&pair.client));
    CHECK_EQ(pair.ctx.poll(), 1U);
    source.request_stop();
    pair.ctx.run();
    CHECK(ec == net::error::operation_aborted);
}

void peer_close_yields_eof() {
    connected_pair pair;
    std::error_code ec;
    net::run_async(pair.ctx.get_executor(), [&](std::error_code e) { ec = e; }, [](std::exception_ptr) { CHECK(false); })(blocked_read(&pair.client));
    CHECK_EQ(pair.ctx.poll(), 1U);
    pair.server.close();
    pair.ctx.run();
    CHECK(ec == net::error::eof);
}

// 读超时模式：when_any(read, timer)。两个子结果类型不同（io_result<size_t> / io_result<>），
// 结果是 when_any_result<std::tuple<std::size_t, std::tuple<>>>，只有赢家下标处有意义。
using read_or_timeout_result = net::when_any_result<std::tuple<std::size_t, std::tuple<>>>;

auto read_with_timeout(net::io_context* ctx, net::tcp_socket* sock, bool write_first)
    CO2_BEG((net::task<read_or_timeout_result>), (ctx, sock, write_first), char buf[8]; net::steady_timer timer{*ctx};
            read_or_timeout_result r;) {
    timer.expires_after(milliseconds{write_first ? 2000 : 20});
    CO2_AWAIT_SET(r, net::when_any(sock->read_some(net::buffer(buf)), timer.wait()));
    CO2_RETURN(r);
}
CO2_END

// 注意：缓冲区描述符不拥有内存，co_await 表达式里的临时对象活不过挂起点——要发送的数据必须
// 是帧局部（或参数）。
auto write_soon(net::io_context* ctx, net::tcp_socket* sock) CO2_BEG(net::task<>, (ctx, sock), net::steady_timer t{*ctx};
                                                                     net::io_result<> r; net::io_result<std::size_t> w;
                                                                     std::string const payload{"abc"};) {
    t.expires_after(milliseconds{5});
    CO2_AWAIT_SET(r, t.wait());
    CO2_AWAIT_SET(w, net::write(*sock, net::buffer(payload)));
    CHECK(not w.ec);
}
CO2_END

void when_any_read_or_timeout() {
    {
        connected_pair pair;
        read_or_timeout_result r;
        net::run_async(pair.ctx.get_executor(), [&](read_or_timeout_result v) { r = v; },
                       [](std::exception_ptr) { CHECK(false); })(read_with_timeout(&pair.ctx, &pair.client, false));
        pair.ctx.run();
        CHECK_EQ(r.index, 1U); // 定时器先完成，读被取消（operation_aborted 不算赢家）
        CHECK(not r.ec);
    }
    {
        connected_pair pair;
        read_or_timeout_result r;
        net::run_async(pair.ctx.get_executor(), [&](read_or_timeout_result v) { r = v; },
                       [](std::exception_ptr) { CHECK(false); })(read_with_timeout(&pair.ctx, &pair.client, true));
        net::run_async(pair.ctx.get_executor())(write_soon(&pair.ctx, &pair.server));
        auto const start = steady_clock::now();
        pair.ctx.run();
        CHECK_EQ(r.index, 0U); // 数据先到：读赢，定时器被取消
        CHECK(not r.ec);
        CHECK_EQ(std::get<0>(r.value), 3U);
        CHECK(steady_clock::now() - start < seconds{1});
    }
}

// ---------------------------------------------------------------------------

// 业务逻辑只依赖 any_stream&：与 TCP 传输编译无关。
auto shout(net::any_stream& stream) CO2_BEG(net::task<std::string>, (stream), char buf[32]; net::io_result<std::size_t> r;
                                             net::io_result<std::size_t> w; std::string got;) {
    CO2_AWAIT_SET(r, net::read(stream, net::buffer(buf, 5)));
    CHECK(not r.ec);
    got.assign(buf, r.value);
    for (auto& c : got)
        c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    CO2_AWAIT_SET(w, net::write(stream, net::buffer(got)));
    CHECK(not w.ec);
    CO2_RETURN(got);
}
CO2_END

auto send_and_receive(net::tcp_socket* sock) CO2_BEG(net::task<std::string>, (sock), std::string reply;
                                                     std::string const message{"hello"};
                                                     net::io_result<std::size_t> w; net::io_result<std::size_t> r;) {
    CO2_AWAIT_SET(w, net::write(*sock, net::buffer(message)));
    CHECK(not w.ec);
    reply.resize(5);
    CO2_AWAIT_SET(r, net::read(*sock, net::buffer(reply)));
    CHECK(not r.ec);
    CO2_RETURN(reply);
}
CO2_END

void tcp_socket_behind_any_stream() {
    connected_pair pair;
    net::any_stream erased{&pair.server};
    std::string reply;
    net::run_async(pair.ctx.get_executor())(shout(erased));
    net::run_async(pair.ctx.get_executor(), [&](std::string s) { reply = std::move(s); },
                   [](std::exception_ptr) { CHECK(false); })(send_and_receive(&pair.client));
    pair.ctx.run();
    CHECK_EQ(reply, "HELLO");
}

// ---------------------------------------------------------------------------

auto serve_n(net::tcp_acceptor* acceptor, int n, std::atomic<int>* sessions)
    CO2_BEG(net::task<>, (acceptor, n, sessions), std::vector<net::task<std::size_t>> pending; int i{};
            net::io_result<net::tcp_socket> accepted; std::vector<std::size_t> totals;) {
    for (i = 0; i != n; ++i) {
        CO2_AWAIT_SET(accepted, acceptor->accept());
        CHECK(not accepted.ec);
        pending.push_back(echo_session(std::move(accepted.value)));
    }
    CO2_AWAIT_SET(totals, net::when_all(std::move(pending)));
    sessions->fetch_add(static_cast<int>(totals.size()));
}
CO2_END

void many_clients_on_several_threads() {
    net::io_context ctx;
    net::tcp_acceptor acceptor{ctx, net::ip::tcp::endpoint{net::ip::address_v4::loopback(), 0}};
    auto const ep = loopback_endpoint(acceptor);
    constexpr auto clients = 16;
    std::atomic<int> sessions{0};
    std::atomic<int> replies{0};
    net::run_async(ctx.get_executor())(serve_n(&acceptor, clients, &sessions));
    for (auto i = 0; i != clients; ++i)
        net::run_async(ctx.get_executor(), [&](std::string s) { if (s.size() == 100U) replies.fetch_add(1); },
                       [](std::exception_ptr) { CHECK(false); })(client_roundtrip(&ctx, ep, std::string(100, static_cast<char>('a' + i))));
    std::vector<std::thread> threads;
    for (auto i = 0; i != 4; ++i)
        threads.emplace_back([&] { ctx.run(); });
    for (auto& t : threads)
        t.join();
    CHECK_EQ(sessions.load(), clients);
    CHECK_EQ(replies.load(), clients);
}

void endpoints_and_options() {
    connected_pair pair;
    std::error_code ec;
    auto const local = pair.client.local_endpoint(ec);
    CHECK(not ec);
    auto const remote = pair.client.remote_endpoint(ec);
    CHECK(not ec);
    CHECK(local.address().is_loopback());
    CHECK_EQ(remote.port(), loopback_endpoint(pair.acceptor).port());
    CHECK(not pair.client.set_option(net::socket_option::no_delay{true}));
    net::socket_option::no_delay option;
    CHECK(not pair.client.get_option(option));
    CHECK(option.value());
    CHECK_EQ(net::ip::to_string(remote), "127.0.0.1:" + std::to_string(remote.port()));
}

} // namespace

int main() {
    loopback_echo();
    connect_to_a_closed_port_fails();
    cancel_aborts_a_pending_read();
    close_aborts_a_pending_read();
    stop_token_aborts_a_pending_read();
    peer_close_yields_eof();
    when_any_read_or_timeout();
    tcp_socket_behind_any_stream();
    many_clients_on_several_threads();
    endpoints_and_options();
    std::cout << "tcp tests passed\n";
    return 0;
}
