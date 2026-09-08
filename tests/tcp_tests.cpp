// TCP：接受器 / 连接 / 读写回环、部分传输、EOF、取消、stop_token、类型擦除的 any_stream、
// 多线程 run。

#include <atomic>
#include <chrono>
#include <cstring>
#include <set>
#include <string>
#include <thread>
#include <vector>


#include "net/any_source_sink.hpp"
#include "net/any_stream.hpp"
#include "net/error.hpp"
#include "net/io_context.hpp"
#include "net/ip.hpp"
#include "net/run_async.hpp"
#include "net/source_sink.hpp"
#include "net/stream.hpp"
#include "net/task.hpp"
#include "net/tcp.hpp"
#include "net/timer.hpp"
#include "net/when_all.hpp"
#include "net/when_any.hpp"

#include "check.hpp"
#include "platform.hpp"

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
    test_context ctx;
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
    test_context ctx;
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

// 先 open()，等一会（让反应器看到未连接套接字最初的 EPOLLOUT|EPOLLHUP 边沿并记下"可写"），再
// connect 一个 accept 队列已满的监听端口：内核直接丢弃 SYN（tcp_abort_on_overflow=0，不回 RST），
// 连接停在 SYN_SENT。connect 必须一直在进行（定时器赢），绝不能"成功"——过期的可写位曾让
// connect 在 SYN 还在路上时就报成功，之后第一次写才看到错误。
using connect_or_timeout = net::when_any_result<std::tuple<>>;

#if !NET_PLATFORM_WINDOWS
// backlog 0 的监听套接字 + 一个已完成的连接把队列填满；返回监听端口（描述符由调用方持有）。
// （Windows 的 backlog 0 不是"队列满"的语义，这个回归只在 POSIX 上跑。）
struct full_backlog {
    int listener = -1;
    int filler = -1;
    net::ip::tcp::endpoint endpoint;

    full_backlog() {
        listener = net_test::raw_tcp_socket();
        CHECK(listener >= 0);
        sockaddr_in local{};
        local.sin_family = AF_INET;
        local.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        CHECK(::bind(listener, reinterpret_cast<sockaddr const*>(&local), sizeof(local)) == 0);
        CHECK(::listen(listener, 0) == 0);
        socklen_t length = sizeof(local);
        CHECK(::getsockname(listener, reinterpret_cast<sockaddr*>(&local), &length) == 0);
        endpoint = net::ip::tcp::endpoint{net::ip::address_v4::loopback(), ntohs(local.sin_port)};
        filler = net_test::raw_tcp_socket();
        CHECK(filler >= 0);
        CHECK(::connect(filler, reinterpret_cast<sockaddr const*>(&local), sizeof(local)) == 0); // 占满队列
    }
    ~full_backlog() {
        net_test::close_raw_socket(filler);
        net_test::close_raw_socket(listener);
    }
};

auto open_idle_then_connect(net::io_context* ctx, net::ip::tcp::endpoint ep, connect_or_timeout* out, bool* really_connected)
    CO2_BEG(net::task<>, (ctx, ep, out, really_connected), net::tcp_socket sock{*ctx}; net::steady_timer timer{*ctx};
            net::io_result<> t; std::error_code ec;) {
    CHECK(not sock.open(net::ip::tcp::v4()));
    timer.expires_after(milliseconds{5});
    CO2_AWAIT_SET(t, timer.wait());
    timer.expires_after(milliseconds{150});
    CO2_AWAIT_SET(*out, net::when_any(sock.connect(ep), timer.wait()));
    sock.remote_endpoint(ec);
    *really_connected = not ec;
}
CO2_END

void connect_after_idle_open_never_reports_a_false_success() {
    test_context ctx{2}; // 第二个线程让反应器在 open 与 connect 之间有机会轮询
    full_backlog blackhole;
    connect_or_timeout result;
    auto connected = true;
    net::run_async(ctx.get_executor())(open_idle_then_connect(&ctx, blackhole.endpoint, &result, &connected));
    std::thread second{[&] { ctx.run(); }};
    ctx.run();
    second.join();
    CHECK_EQ(result.index, 1U); // 定时器赢：连接仍在进行
    CHECK(not connected);
}
#endif

struct connected_pair {
    test_context ctx;
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
    test_context ctx;
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

// single_thread_hint：调用方承诺单线程，io_uring 以 SINGLE_ISSUER | DEFER_TASKRUN 运行。
// 覆盖：批量提交（多个客户端在一轮里各发一个 SQE）、取消（定时器 TIMEOUT_REMOVE）、投机路径
// 之外的完成路径（服务端的读总是先等），以及第二次 run() 仍在同一线程。
auto cancel_soon(net::steady_timer* timer) CO2_BEG(net::task<>, (timer), net::steady_timer delay{timer->context()};
                                                    net::io_result<> r;) {
    delay.expires_after(std::chrono::milliseconds{5});
    CO2_AWAIT_SET(r, delay.wait());
    timer->cancel();
}
CO2_END

auto wait_then_cancel(net::io_context* ctx, std::error_code* out)
    CO2_BEG(net::task<>, (ctx, out), net::steady_timer timer{*ctx}; net::io_result<> r;) {
    timer.expires_after(std::chrono::hours{1});
    net::run_async(ctx->get_executor())(cancel_soon(&timer));
    CO2_AWAIT_SET(r, timer.wait());
    *out = r.ec;
}
CO2_END

void single_thread_hint_promise() {
    test_context ctx{net::single_thread_hint};
    net::tcp_acceptor acceptor{ctx, net::ip::tcp::endpoint{net::ip::address_v4::loopback(), 0}};
    auto const ep = loopback_endpoint(acceptor);
    constexpr auto clients = 8;
    std::atomic<int> sessions{0};
    std::atomic<int> replies{0};
    std::error_code timer_ec;
    net::run_async(ctx.get_executor())(serve_n(&acceptor, clients, &sessions));
    for (auto i = 0; i != clients; ++i)
        net::run_async(ctx.get_executor(), [&](std::string s) { if (s.size() == 100U) replies.fetch_add(1); },
                       [](std::exception_ptr) { CHECK(false); })(client_roundtrip(&ctx, ep, std::string(100, static_cast<char>('a' + i))));
    net::run_async(ctx.get_executor())(wait_then_cancel(&ctx, &timer_ec));
    ctx.run();
    CHECK_EQ(sessions.load(), clients);
    CHECK_EQ(replies.load(), clients);
    CHECK(timer_ec == net::error::operation_aborted);
    // 同一线程再跑一轮。
    ctx.restart();
    replies = 0;
    net::run_async(ctx.get_executor())(serve_n(&acceptor, 1, &sessions));
    net::run_async(ctx.get_executor(), [&](std::string s) { if (s.size() == 100U) replies.fetch_add(1); },
                   [](std::exception_ptr) { CHECK(false); })(client_roundtrip(&ctx, ep, std::string(100, 'z')));
    ctx.run();
    CHECK_EQ(replies.load(), 1);
}

// ---- 接受器语义（io_uring 下走多发 accept：parked 队列 / waiter / 退役；其它后端语义相同） ----

auto accept_n_into(net::tcp_acceptor* acceptor, int n, std::vector<net::tcp_socket>* out)
    CO2_BEG(net::task<>, (acceptor, n, out), int i{}; net::io_result<net::tcp_socket> accepted;) {
    for (i = 0; i != n; ++i) {
        CO2_AWAIT_SET(accepted, acceptor->accept());
        CHECK(not accepted.ec);
        out->push_back(std::move(accepted.value));
    }
}
CO2_END

// 连接先于 accept() 到达：内核 / 多发 CQE 先把它们排好，之后的 accept() 逐个取走。
void connections_arriving_before_accept_are_queued() {
    test_context ctx;
    net::tcp_acceptor acceptor{ctx, net::ip::tcp::endpoint{net::ip::address_v4::loopback(), 0}};
    auto const ep = loopback_endpoint(acceptor);
    constexpr auto count = 6;
    std::vector<net::tcp_socket> clients;
    for (auto i = 0; i != count; ++i) clients.emplace_back(ctx);
    for (auto& c : clients) net::run_async(ctx.get_executor())(connect_only(&c, ep));
    ctx.run(); // 全部连上，还没有人 accept
    ctx.restart();
    std::vector<net::tcp_socket> accepted;
    net::run_async(ctx.get_executor())(accept_n_into(&acceptor, count, &accepted));
    ctx.run();
    CHECK_EQ(accepted.size(), static_cast<std::size_t>(count));
    // 接受到的套接字的对端就是客户端们的本地端点（地址不经 accept 传递，由 remote_endpoint 取）。
    std::set<unsigned short> client_ports;
    std::set<unsigned short> peer_ports;
    std::error_code ec;
    for (auto& c : clients) client_ports.insert(c.local_endpoint(ec).port());
    for (auto& a : accepted) {
        auto const peer = a.remote_endpoint(ec);
        CHECK(not ec);
        CHECK(peer.address() == net::ip::address_v4::loopback());
        peer_ports.insert(peer.port());
    }
    CHECK(client_ports == peer_ports);
}

auto accept_with_token(net::tcp_acceptor* acceptor, std::error_code* out)
    CO2_BEG(net::task<>, (acceptor, out), net::io_result<net::tcp_socket> accepted;) {
    CO2_AWAIT_SET(accepted, acceptor->accept());
    *out = accepted.ec;
}
CO2_END

// 停着的 accept 被 stop_token 取消；之后到达的连接照常被下一个 accept() 取走。
void pending_accept_is_cancelled_by_stop_token_then_acceptor_keeps_working() {
    test_context ctx;
    net::tcp_acceptor acceptor{ctx, net::ip::tcp::endpoint{net::ip::address_v4::loopback(), 0}};
    auto const ep = loopback_endpoint(acceptor);
    net::stop_source source;
    std::error_code first;
    net::run_async(ctx.get_executor(), source.get_token(), nullptr, [] {}, [](std::exception_ptr) { CHECK(false); })(
        accept_with_token(&acceptor, &first));
    ctx.poll();
    source.request_stop();
    ctx.run();
    CHECK(first == net::error::operation_aborted);
    ctx.restart();
    net::tcp_socket client{ctx};
    net::tcp_socket server;
    net::run_async(ctx.get_executor())(accept_and_hold(&acceptor, &server));
    net::run_async(ctx.get_executor())(connect_only(&client, ep));
    ctx.run();
    CHECK(server.is_open());
}

auto sleep_for(net::io_context* ctx, milliseconds d) CO2_BEG(net::task<>, (ctx, d), net::steady_timer timer{*ctx};
                                                              net::io_result<> r;) {
    timer.expires_after(d);
    CO2_AWAIT_SET(r, timer.wait());
}
CO2_END

// 关闭带着排队连接的接受器：排队的连接被关闭，客户端读到 EOF；上下文继续运行其它工作
//（退役的多发操作的终止 CQE 在这期间被安全处理）。
void closing_acceptor_drops_queued_connections() {
    test_context ctx;
    std::vector<net::tcp_socket> clients;
    {
        net::tcp_acceptor acceptor{ctx, net::ip::tcp::endpoint{net::ip::address_v4::loopback(), 0}};
        auto const ep = loopback_endpoint(acceptor);
        for (auto i = 0; i != 3; ++i) clients.emplace_back(ctx);
        for (auto& c : clients) net::run_async(ctx.get_executor())(connect_only(&c, ep));
        ctx.run();
        ctx.restart();
    } // 接受器销毁：监听描述符与排队的连接一起关闭
    std::vector<std::error_code> results(3);
    for (auto i = 0U; i != 3U; ++i)
        net::run_async(ctx.get_executor(), [&, i](std::error_code e) { results[i] = e; },
                       [](std::exception_ptr) { CHECK(false); })(blocked_read(&clients[i]));
    net::run_async(ctx.get_executor())(sleep_for(&ctx, milliseconds{20}));
    ctx.run();
    for (auto const& ec : results)
        CHECK(ec == net::error::eof || ec == std::errc::connection_reset);
}

// assign 一个已在监听的描述符：accept() 直接可用。
void assigning_a_listening_descriptor() {
    test_context ctx;
    auto const raw = net_test::raw_tcp_socket();
    CHECK(net::socket_is_valid(raw));
    net_test::set_reuse_address(raw);
    sockaddr_in local{};
    local.sin_family = AF_INET;
    local.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    CHECK(::bind(raw, reinterpret_cast<sockaddr const*>(&local), sizeof(local)) == 0);
    CHECK(::listen(raw, 16) == 0);
    net::tcp_acceptor acceptor{ctx};
    CHECK(not acceptor.assign(net::ip::tcp::v4(), raw));
    auto const ep = loopback_endpoint(acceptor);
    net::tcp_socket client{ctx};
    net::tcp_socket server;
    net::run_async(ctx.get_executor())(accept_and_hold(&acceptor, &server));
    net::run_async(ctx.get_executor())(connect_only(&client, ep));
    ctx.run();
    CHECK(server.is_open());
    // release：描述符交回调用方，接受器不再拥有它。
    auto const released = acceptor.release();
    CHECK(released == raw);
    CHECK(not acceptor.is_open());
    net_test::close_raw_socket(released);
}

// ---- 第二族流概念在 TCP 上：write_eof → shutdown(send)，对端读到 eof；BufferSink 的 commit_eof 同样 ----

auto read_all_until_eof(net::tcp_socket* sock, std::string* out)
    CO2_BEG((net::task<std::error_code>), (sock, out), char buf[1024]; net::io_result<std::size_t> r;) {
    for (;;) {
        CO2_AWAIT_SET(r, sock->read_some(net::buffer(buf)));
        if (r.ec) CO2_RETURN(r.ec);
        out->append(buf, r.value);
    }
}
CO2_END

auto erased_sink_send(net::any_write_sink* sink, std::string payload)
    CO2_BEG((net::task<net::io_result<std::size_t>>), (sink, payload), net::io_result<std::size_t> r;) {
    CO2_AWAIT_SET(r, sink->write_eof(net::buffer(payload)));
    CO2_RETURN(r);
}
CO2_END

auto source_to_tcp_sink(net::memory_source* source, net::any_buffer_sink* sink)
    CO2_BEG((net::task<net::io_result<std::size_t>>), (source, sink), net::io_result<std::size_t> r;) {
    CO2_AWAIT_SET(r, net::transfer_to_sink(*source, *sink));
    CO2_RETURN(r);
}
CO2_END

void write_sink_eof_is_tcp_shutdown() {
    connected_pair pair;
    auto adapter = net::as_write_sink(pair.client);
    net::any_write_sink sink{&adapter};
    std::string const payload(200U * 1024U, 'w');
    std::string received;
    std::error_code read_ec;
    net::io_result<std::size_t> sent;
    net::run_async(pair.ctx.get_executor(), [&](std::error_code e) { read_ec = e; }, [](std::exception_ptr) { CHECK(false); })(
        read_all_until_eof(&pair.server, &received));
    net::run_async(pair.ctx.get_executor(), [&](net::io_result<std::size_t> v) { sent = v; }, [](std::exception_ptr) { CHECK(false); })(
        erased_sink_send(&sink, payload));
    pair.ctx.run();
    CHECK(not sent.ec);
    CHECK_EQ(sent.value, payload.size());
    CHECK(read_ec == net::error::eof); // write_eof → shutdown(send) → 对端 eof
    CHECK(received == payload);
}

void buffer_sink_commit_eof_is_tcp_shutdown() {
    connected_pair pair;
    auto adapter = net::as_buffer_sink(pair.client, 4096U);
    net::any_buffer_sink sink{&adapter};
    std::string const payload(100U * 1024U + 17U, 'b');
    net::memory_source source{net::buffer(payload)};
    std::string received;
    std::error_code read_ec;
    net::io_result<std::size_t> moved;
    net::run_async(pair.ctx.get_executor(), [&](std::error_code e) { read_ec = e; }, [](std::exception_ptr) { CHECK(false); })(
        read_all_until_eof(&pair.server, &received));
    net::run_async(pair.ctx.get_executor(), [&](net::io_result<std::size_t> v) { moved = v; }, [](std::exception_ptr) { CHECK(false); })(
        source_to_tcp_sink(&source, &sink));
    pair.ctx.run();
    CHECK(not moved.ec);
    CHECK_EQ(moved.value, payload.size());
    CHECK(read_ec == net::error::eof); // commit_eof → shutdown(send)
    CHECK(received == payload);
    CHECK(adapter.finished());
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
#if !NET_PLATFORM_WINDOWS
    connect_after_idle_open_never_reports_a_false_success();
#endif
    cancel_aborts_a_pending_read();
    close_aborts_a_pending_read();
    stop_token_aborts_a_pending_read();
    peer_close_yields_eof();
    when_any_read_or_timeout();
    tcp_socket_behind_any_stream();
    many_clients_on_several_threads();
    single_thread_hint_promise();
    connections_arriving_before_accept_are_queued();
    pending_accept_is_cancelled_by_stop_token_then_acceptor_keeps_working();
    closing_acceptor_drops_queued_connections();
    assigning_a_listening_descriptor();
    write_sink_eof_is_tcp_shutdown();
    buffer_sink_commit_eof_is_tcp_shutdown();
    endpoints_and_options();
    std::cout << "tcp tests passed\n";
    return 0;
}
