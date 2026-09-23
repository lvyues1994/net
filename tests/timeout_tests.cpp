// timeout / delay 与可移植错误条件 cond：到期取消操作、操作先完成撤定时器、已就绪不建定时器、父 stop_token
// 透传、异常透传、绝对截止；delay 可被 stop_token 打断。

#include <chrono>
#include <exception>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "net/error.hpp"
#include "net/immediate.hpp"
#include "net/io_context.hpp"
#include "net/ip.hpp"
#include "net/run_async.hpp"
#include "net/stream.hpp"
#include "net/task.hpp"
#include "net/tcp.hpp"
#include "net/test/run_blocking.hpp"
#include "net/timeout.hpp"
#include "net/timer.hpp"

#include "check.hpp"
#include "platform.hpp"

namespace {

using namespace std::chrono;

net::ip::tcp::endpoint loopback_endpoint(net::tcp_acceptor const& acceptor) {
    std::error_code ec;
    auto endpoint = acceptor.local_endpoint(ec);
    CHECK(not ec);
    endpoint.address(net::ip::address_v4::loopback());
    return endpoint;
}

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
    test_context ctx;
    net::tcp_acceptor acceptor{ctx, net::ip::tcp::endpoint{net::ip::address_v4::loopback(), 0}};
    net::tcp_socket client{ctx};
    net::tcp_socket server;

    connected_pair() {
        net::run_async(ctx.get_executor())(accept_and_hold(&acceptor, &server));
        net::run_async(ctx.get_executor())(connect_only(&client, loopback_endpoint(acceptor)));
        ctx.run();
        ctx.restart();
        CHECK(server.is_open() && client.is_open());
    }
};

auto write_once(net::tcp_socket* sock, std::string* data)
    CO2_BEG((net::task<net::io_result<std::size_t>>), (sock, data), net::io_result<std::size_t> w;) {
    CO2_AWAIT_SET(w, net::write(*sock, net::buffer(*data)));
    CO2_RETURN(w);
}
CO2_END

// 读一个永远没数据的套接字，带时限。
auto timed_read(net::tcp_socket* sock, milliseconds limit)
    CO2_BEG((net::task<net::io_result<std::size_t>>), (sock, limit), char buf[16]; net::io_result<std::size_t> r;) {
    CO2_AWAIT_SET(r, net::timeout(sock->read_some(net::buffer(buf)), limit));
    CO2_RETURN(r);
}
CO2_END

void deadline_cancels_the_operation() {
    connected_pair pair;
    auto const start = steady_clock::now();
    auto const r = net::test::run_blocking(pair.ctx, timed_read(&pair.client, milliseconds{30}));
    auto const elapsed = steady_clock::now() - start;
    CHECK(r.ec == net::error::timed_out);
    CHECK(r.ec == net::cond::timeout);
    CHECK(r.ec == std::errc::timed_out);
    CHECK_EQ(r.value, 0U);
    CHECK(elapsed >= milliseconds{25});
    CHECK(elapsed < seconds{2});
    CHECK(pair.client.is_open()); // 只取消了操作，套接字照旧
    // 之后同一套接字还能正常读
    auto payload = std::string{"after"};
    auto const w = net::test::run_blocking(pair.ctx, write_once(&pair.server, &payload));
    CHECK(not w.ec);
    auto const r2 = net::test::run_blocking(pair.ctx, timed_read(&pair.client, seconds{5}));
    CHECK(not r2.ec);
    CHECK_EQ(r2.value, 5U);
}

// 数据在时限之内到达：原样返回读的结果，定时器被撤（run() 不必等到时限）。
auto write_after(net::tcp_socket* sock, milliseconds wait, net::io_context* ctx)
    CO2_BEG(net::task<>, (sock, wait, ctx), net::steady_timer timer{*ctx}; net::io_result<> t; net::io_result<std::size_t> w;
            std::string payload = "hello";) {
    timer.expires_after(wait);
    CO2_AWAIT_SET(t, timer.wait());
    CO2_AWAIT_SET(w, net::write(*sock, net::buffer(payload)));
    CHECK(not w.ec);
}
CO2_END

void operation_completing_first_wins() {
    connected_pair pair;
    net::run_async(pair.ctx.get_executor())(write_after(&pair.server, milliseconds{10}, &pair.ctx));
    auto const start = steady_clock::now();
    auto const r = net::test::run_blocking(pair.ctx, timed_read(&pair.client, seconds{10}));
    CHECK(not r.ec);
    CHECK_EQ(r.value, 5U);
    CHECK(steady_clock::now() - start < seconds{5}); // 定时器被撤，没等 10 秒
}

// 操作在 await_ready 就完成：不建定时器，结果直接返回。
auto immediate_under_timeout() CO2_BEG((net::task<net::io_result<int>>), (), net::io_result<int> r;) {
    CO2_AWAIT_SET(r, net::timeout(net::ready(std::error_code{}, 42), seconds{1}));
    CO2_RETURN(r);
}
CO2_END

void ready_operation_needs_no_timer() {
    test_context ctx;
    auto const r = net::test::run_blocking(ctx, immediate_under_timeout());
    CHECK(not r.ec);
    CHECK_EQ(r.value, 42);
}

// 父 stop_token 请求停止：操作以 operation_aborted 结束，不是超时。
void parent_stop_is_not_a_timeout() {
    connected_pair pair;
    net::stop_source source;
    net::io_result<std::size_t> r{};
    net::run_async(pair.ctx.get_executor(), source.get_token(), nullptr, [&](net::io_result<std::size_t> v) { r = v; },
                   [](std::exception_ptr) { CHECK(false); })(timed_read(&pair.client, seconds{10}));
    CHECK_EQ(pair.ctx.poll(), 1U); // 挂起在读上
    source.request_stop();
    pair.ctx.run();
    CHECK(r.ec == net::error::operation_aborted);
    CHECK(r.ec == net::cond::canceled);
    CHECK(not(r.ec == net::cond::timeout));
}

// 操作的 await_resume 抛出：异常穿过 timeout。
struct throwing_awaitable {
    bool await_ready() const noexcept { return false; }
    net::coroutine_handle<> await_suspend(net::coroutine_handle<> const h, net::io_env const*) const noexcept { return h; }
    net::io_result<int> await_resume() const { throw std::runtime_error{"boom"}; }
};

auto throwing_under_timeout() CO2_BEG((net::task<net::io_result<int>>), (), net::io_result<int> r;) {
    CO2_AWAIT_SET(r, net::timeout(throwing_awaitable{}, seconds{1}));
    CO2_RETURN(r);
}
CO2_END

void exception_propagates_through_timeout() {
    test_context ctx;
    auto threw = false;
    try {
        net::test::run_blocking(ctx, throwing_under_timeout());
    } catch (std::runtime_error const& e) {
        threw = std::string{e.what()} == "boom";
    }
    CHECK(threw);
}

// 绝对截止时刻的重载。
auto timed_read_until(net::tcp_socket* sock, steady_clock::time_point deadline)
    CO2_BEG((net::task<net::io_result<std::size_t>>), (sock, deadline), char buf[16]; net::io_result<std::size_t> r;) {
    CO2_AWAIT_SET(r, net::timeout(sock->read_some(net::buffer(buf)), deadline));
    CO2_RETURN(r);
}
CO2_END

void absolute_deadline() {
    connected_pair pair;
    auto const r = net::test::run_blocking(pair.ctx, timed_read_until(&pair.client, steady_clock::now() + milliseconds{20}));
    CHECK(r.ec == net::cond::timeout);
    // 已经过去的截止时刻：立刻超时
    auto const r2 = net::test::run_blocking(pair.ctx, timed_read_until(&pair.client, steady_clock::now() - seconds{1}));
    CHECK(r2.ec == net::cond::timeout);
}

// delay：睡够时长；stop_token 打断时 operation_aborted。
auto sleep_for(milliseconds d) CO2_BEG((net::task<net::io_result<>>), (d), net::io_result<> r;) {
    CO2_AWAIT_SET(r, net::delay(d));
    CO2_RETURN(r);
}
CO2_END

void delay_waits_and_is_cancellable() {
    test_context ctx;
    auto const start = steady_clock::now();
    auto const r = net::test::run_blocking(ctx, sleep_for(milliseconds{30}));
    CHECK(not r.ec);
    CHECK(steady_clock::now() - start >= milliseconds{25});

    net::stop_source source;
    net::io_result<> cancelled{};
    net::run_async(ctx.get_executor(), source.get_token(), nullptr, [&](net::io_result<> v) { cancelled = v; },
                   [](std::exception_ptr) { CHECK(false); })(sleep_for(seconds{30}));
    CHECK_EQ(ctx.poll(), 1U);
    source.request_stop();
    auto const before = steady_clock::now();
    ctx.run();
    CHECK(cancelled.ec == net::cond::canceled);
    CHECK(steady_clock::now() - before < seconds{5});
}

// 停止在开始前已请求：delay 不等、timeout 里的操作不执行（数据已就绪也一样），都是 operation_aborted。
void stopped_before_start() {
    test_context ctx;
    auto const d = net::test::run_blocking(ctx, net::test::stopped_token(), sleep_for(milliseconds{0}));
    CHECK(d.ec == net::error::operation_aborted);

    connected_pair pair;
    std::string payload = "ready";
    CHECK(not net::test::run_blocking(pair.ctx, write_once(&pair.server, &payload)).ec);
    auto const r = net::test::run_blocking(pair.ctx, net::test::stopped_token(), timed_read(&pair.client, seconds{1}));
    CHECK(r.ec == net::error::operation_aborted);
    CHECK(not(r.ec == net::cond::timeout));
    auto const again = net::test::run_blocking(pair.ctx, timed_read(&pair.client, seconds{1}));
    CHECK(not again.ec);
    CHECK_EQ(again.value, 5U);
}

// 被限时的是一个 task：它的 await_suspend 返回子协程句柄，要转移过去它才开始跑。
auto read_task(net::tcp_socket* sock)
    CO2_BEG((net::task<net::io_result<std::size_t>>), (sock), char buf[16]; net::io_result<std::size_t> r;) {
    CO2_AWAIT_SET(r, sock->read_some(net::buffer(buf)));
    CO2_RETURN(r);
}
CO2_END

auto timed_task(net::tcp_socket* sock, milliseconds limit)
    CO2_BEG((net::task<net::io_result<std::size_t>>), (sock, limit), net::io_result<std::size_t> r;) {
    CO2_AWAIT_SET(r, net::timeout(read_task(sock), limit));
    CO2_RETURN(r);
}
CO2_END

void task_under_timeout() {
    connected_pair pair;
    std::string payload = "task";
    CHECK(not net::test::run_blocking(pair.ctx, write_once(&pair.server, &payload)).ec);
    auto const r = net::test::run_blocking(pair.ctx, timed_task(&pair.client, seconds{5}));
    CHECK(not r.ec);
    CHECK_EQ(r.value, 4U);
    // 时限先到：停止经子协程的环境传到它里面的读
    auto const start = steady_clock::now();
    auto const late = net::test::run_blocking(pair.ctx, timed_task(&pair.client, milliseconds{20}));
    CHECK(late.ec == net::cond::timeout);
    CHECK(steady_clock::now() - start < seconds{5});
}

// 多线程：被限时的操作（一个 delay）与时限几乎同时到期，两边常在不同线程上完成。操作先到时撤定时器可能晚于
// 定时器自己收尾，留下的过期取消标记不能让复用这份状态的下一次 timeout 失效。
struct race_record {
    int unexpected = 0;
    net::io_result<> last{};
};

auto racing_timeouts(int rounds, race_record* out) CO2_BEG(net::task<>, (rounds, out), int i{}; net::io_result<> r;) {
    for (i = 0; i != rounds; ++i) {
        CO2_AWAIT_SET(r, net::timeout(net::delay(microseconds{50}), microseconds{50}));
        if (r.ec && not(r.ec == net::cond::timeout)) ++out->unexpected;
    }
    CO2_AWAIT_SET(out->last, net::timeout(net::delay(seconds{2}), milliseconds{20}));
}
CO2_END

void racing_timeouts_leave_the_next_deadline_intact() {
    test_context ctx{4};
    std::vector<race_record> records(8U);
    for (auto& record : records)
        net::run_async(ctx.get_executor())(racing_timeouts(200, &record));
    std::vector<std::thread> threads;
    for (auto t = 0; t != 3; ++t)
        threads.emplace_back([&ctx] { ctx.run(); });
    ctx.run();
    for (auto& thread : threads)
        thread.join();
    for (auto const& record : records) {
        CHECK_EQ(record.unexpected, 0);
        CHECK(record.last.ec == net::cond::timeout);
    }
}

// cond 与各来源错误码的等价关系。
void portable_conditions() {
    CHECK(make_error_code(net::error::eof) == net::cond::eof);
    CHECK(make_error_code(net::error::operation_aborted) == net::cond::canceled);
    CHECK(make_error_code(net::error::operation_aborted) == std::errc::operation_canceled);
    CHECK(make_error_code(net::error::stream_truncated) == net::cond::stream_truncated);
    CHECK(make_error_code(net::error::timed_out) == net::cond::timeout);
    CHECK(make_error_code(net::error::timed_out) == std::errc::timed_out);
    CHECK(std::make_error_code(std::errc::operation_canceled) == net::cond::canceled);
    CHECK(std::make_error_code(std::errc::timed_out) == net::cond::timeout);
#if !NET_PLATFORM_WINDOWS
    CHECK((std::error_code{ECANCELED, std::system_category()} == net::cond::canceled));
    CHECK((std::error_code{ETIMEDOUT, std::system_category()} == net::cond::timeout));
#endif
    CHECK(not(make_error_code(net::error::eof) == net::cond::canceled));
    CHECK(not(make_error_code(net::error::not_open) == net::cond::eof));
    CHECK(not(std::error_code{} == net::cond::eof));
    CHECK_EQ(std::string{net::cond_category().name()}, "net.cond");
    CHECK(not make_error_condition(net::cond::timeout).message().empty());
}

} // namespace

int main() {
    portable_conditions();
    deadline_cancels_the_operation();
    operation_completing_first_wins();
    ready_operation_needs_no_timer();
    parent_stop_is_not_a_timeout();
    exception_propagates_through_timeout();
    absolute_deadline();
    delay_waits_and_is_cancellable();
    stopped_before_start();
    task_under_timeout();
    racing_timeouts_leave_the_next_deadline_intact();
    std::cout << "timeout tests passed\n";
    return 0;
}
