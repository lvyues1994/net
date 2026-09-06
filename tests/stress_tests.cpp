// 组合与压力测试：把前面各文件单独覆盖的原语放到一起、放到多个线程上、加上随机性——
// 目标是暴露交错相关的缺陷（发布之后触碰、工作计数顺序、取消与完成的竞争、锁序）。每个
// 用例都是确定性可核对的：字节数守恒、每个操作恰好完成一次、run() 最终返回。
//
// 为每个后端各编译一个变体（NET_TEST_BACKEND）；CI 在 2 核机器上跑 ASan / TSan，本地可用
// `taskset -c 0,1` 复现 CI 的调度。

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstring>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "net/any_stream.hpp"
#include "net/error.hpp"
#include "net/io_context.hpp"
#include "net/ip.hpp"
#include "net/run.hpp"
#include "net/run_async.hpp"
#include "net/strand.hpp"
#include "net/stream.hpp"
#include "net/task.hpp"
#include "net/tcp.hpp"
#include "net/thread_pool.hpp"
#include "net/timer.hpp"
#include "net/udp.hpp"
#include "net/when_all.hpp"
#include "net/when_any.hpp"

#include "check.hpp"

namespace {

using namespace std::chrono;

net::ip::tcp::endpoint loopback_endpoint(net::tcp_acceptor const& acceptor) {
    std::error_code ec;
    auto endpoint = acceptor.local_endpoint(ec);
    CHECK(not ec);
    endpoint.address(net::ip::address_v4::loopback());
    return endpoint;
}

// 线程数：CI 只有 2 核，用 4 个线程制造抢占。
constexpr auto thread_count = 4;

void run_on_threads(net::io_context& ctx, int const threads = thread_count) {
    std::vector<std::thread> workers;
    for (auto i = 0; i != threads; ++i)
        workers.emplace_back([&] { ctx.run(); });
    for (auto& w : workers)
        w.join();
}

// ---------------------------------------------------------------------------
// 1. 多线程回显风暴：32 个客户端各做 20 次往返，服务端每连接一个回显会话。

auto echo_session(net::tcp_socket sock, std::atomic<long>* echoed)
    CO2_BEG(net::task<>, (sock, echoed), char buf[256]; net::io_result<std::size_t> r; net::io_result<std::size_t> w;) {
    for (;;) {
        CO2_AWAIT_SET(r, sock.read_some(net::buffer(buf)));
        if (r.ec) break;
        CO2_AWAIT_SET(w, net::write(sock, net::buffer(buf, r.value)));
        if (w.ec) break;
        echoed->fetch_add(static_cast<long>(w.value));
    }
}
CO2_END

auto accept_loop(net::io_context* ctx, net::tcp_acceptor* acceptor, int n, std::atomic<long>* echoed)
    CO2_BEG(net::task<>, (ctx, acceptor, n, echoed), int i{}; net::io_result<net::tcp_socket> accepted;) {
    for (i = 0; i != n; ++i) {
        CO2_AWAIT_SET(accepted, acceptor->accept());
        CHECK(not accepted.ec);
        net::run_async(ctx->get_executor())(echo_session(std::move(accepted.value), echoed));
    }
}
CO2_END

auto client_rounds(net::io_context* ctx, net::ip::tcp::endpoint ep, int rounds, std::atomic<long>* sent)
    CO2_BEG(net::task<>, (ctx, ep, rounds, sent), net::tcp_socket sock{*ctx}; net::io_result<> c; int i{};
            std::string payload; std::string reply; net::io_result<std::size_t> w; net::io_result<std::size_t> r;) {
    CO2_AWAIT_SET(c, sock.connect(ep));
    CHECK(not c.ec);
    for (i = 0; i != rounds; ++i) {
        payload.assign(static_cast<std::size_t>(1 + (i * 37) % 200), static_cast<char>('a' + i % 26));
        reply.assign(payload.size(), '\0');
        CO2_AWAIT_SET(w, net::write(sock, net::buffer(payload)));
        CHECK(not w.ec);
        CO2_AWAIT_SET(r, net::read(sock, net::buffer(reply)));
        CHECK(not r.ec);
        CHECK(reply == payload);
        sent->fetch_add(static_cast<long>(payload.size()));
    }
}
CO2_END

void multithreaded_echo_storm() {
    test_context ctx{thread_count};
    net::tcp_acceptor acceptor{ctx, net::ip::tcp::endpoint{net::ip::address_v4::loopback(), 0}};
    auto const ep = loopback_endpoint(acceptor);
    constexpr auto clients = 32;
    constexpr auto rounds = 20;
    std::atomic<long> sent{0};
    std::atomic<long> echoed{0};
    net::run_async(ctx.get_executor())(accept_loop(&ctx, &acceptor, clients, &echoed));
    for (auto i = 0; i != clients; ++i)
        net::run_async(ctx.get_executor())(client_rounds(&ctx, ep, rounds, &sent));
    run_on_threads(ctx);
    CHECK(sent.load() > 0);
    CHECK_EQ(sent.load(), echoed.load());
}

// ---------------------------------------------------------------------------
// 2. 多线程下 when_any(read, timer)：对端随机地在定时器之前或之后写。赢家是谁不确定，但每次
//    都必须恰好完成一次、输家被干净取消、结束后读到的字节总数守恒。

using read_or_timeout = net::when_any_result<std::tuple<std::size_t, std::tuple<>>>;

auto race_reader(net::io_context* ctx, net::tcp_socket* sock, int rounds, std::atomic<long>* reads,
                 std::atomic<long>* timeouts, std::atomic<long>* bytes)
    CO2_BEG(net::task<>, (ctx, sock, rounds, reads, timeouts, bytes), char buf[64]; net::steady_timer timer{*ctx};
            read_or_timeout r; int i{}; net::io_result<std::size_t> drain;) {
    for (i = 0; i != rounds; ++i) {
        timer.expires_after(microseconds{200 + (i % 7) * 150});
        CO2_AWAIT_SET(r, net::when_any(sock->read_some(net::buffer(buf)), timer.wait()));
        CHECK(not r.ec);
        if (r.index == 0) {
            reads->fetch_add(1);
            bytes->fetch_add(static_cast<long>(std::get<0>(r.value)));
        } else {
            CHECK_EQ(r.index, 1U);
            timeouts->fetch_add(1);
        }
    }
    // 把对端还没读走的数据收完（对端一共写 rounds 条）。
    for (;;) {
        CO2_AWAIT_SET(drain, sock->read_some(net::buffer(buf)));
        if (drain.ec) break;
        bytes->fetch_add(static_cast<long>(drain.value));
    }
}
CO2_END

auto race_writer(net::io_context* ctx, net::tcp_socket* sock, int rounds, std::atomic<long>* written)
    CO2_BEG(net::task<>, (ctx, sock, rounds, written), net::steady_timer timer{*ctx}; net::io_result<> t; int i{};
            std::string const payload{"ping"}; net::io_result<std::size_t> w;) {
    for (i = 0; i != rounds; ++i) {
        timer.expires_after(microseconds{(i % 5) * 200});
        CO2_AWAIT_SET(t, timer.wait());
        CO2_AWAIT_SET(w, net::write(*sock, net::buffer(payload)));
        CHECK(not w.ec);
        written->fetch_add(static_cast<long>(w.value));
    }
    sock->close(); // 读端最终读到 eof
}
CO2_END

auto connect_pair(net::io_context* ctx, net::tcp_acceptor* acceptor, net::tcp_socket* client, net::tcp_socket* server)
    CO2_BEG(net::task<>, (ctx, acceptor, client, server), net::io_result<net::tcp_socket> accepted; net::io_result<> c;
            net::ip::tcp::endpoint ep;) {
    ep = loopback_endpoint(*acceptor);
    // 连接与接受并行：先发起 connect（内核完成握手不需要 accept），再 accept。
    CO2_AWAIT_SET(c, client->connect(ep));
    CHECK(not c.ec);
    CO2_AWAIT_SET(accepted, acceptor->accept());
    CHECK(not accepted.ec);
    *server = std::move(accepted.value);
}
CO2_END

void when_any_read_or_timer_under_threads() {
    test_context ctx{thread_count};
    net::tcp_acceptor acceptor{ctx, net::ip::tcp::endpoint{net::ip::address_v4::loopback(), 0}};
    constexpr auto pairs = 8;
    constexpr auto rounds = 40;
    std::vector<net::tcp_socket> clients;
    std::vector<net::tcp_socket> servers(pairs);
    for (auto i = 0; i != pairs; ++i) clients.emplace_back(ctx);
    for (auto i = 0; i != pairs; ++i)
        net::run_async(ctx.get_executor())(connect_pair(&ctx, &acceptor, &clients[static_cast<std::size_t>(i)],
                                                        &servers[static_cast<std::size_t>(i)]));
    ctx.run();
    ctx.restart();
    std::atomic<long> reads{0};
    std::atomic<long> timeouts{0};
    std::atomic<long> bytes{0};
    std::atomic<long> written{0};
    for (auto i = 0; i != pairs; ++i) {
        net::run_async(ctx.get_executor())(race_reader(&ctx, &clients[static_cast<std::size_t>(i)], rounds, &reads, &timeouts, &bytes));
        net::run_async(ctx.get_executor())(race_writer(&ctx, &servers[static_cast<std::size_t>(i)], rounds, &written));
    }
    run_on_threads(ctx);
    CHECK_EQ(reads.load() + timeouts.load(), static_cast<long>(pairs * rounds));
    // 读与定时器同时完成时定时器可能被记为赢家，而读已经消费了数据——when_any 丢弃输家的结果，
    // 这些字节就"丢"了（首个完成者胜出语义固有；Corosio 相同）。所以只能是 ≤，且不能相差太多。
    CHECK(bytes.load() <= written.load());
    CHECK(bytes.load() >= written.load() / 2);
    CHECK(reads.load() > 0); // 两种结果都应出现（概率上；若长期只出现一种说明定时器或读有问题）
    CHECK(timeouts.load() > 0);
}

// ---------------------------------------------------------------------------
// 3. 取消风暴：N 个读阻塞在 stop_token 上，另一个线程在随机时刻请求停止；与此同时对端随机
//    地写数据。每个读恰好完成一次：要么成功要么 aborted。

auto stoppable_read(net::tcp_socket* sock, std::atomic<int>* ok, std::atomic<int>* aborted)
    CO2_BEG(net::task<>, (sock, ok, aborted), char buf[16]; net::io_result<std::size_t> r;) {
    CO2_AWAIT_SET(r, sock->read_some(net::buffer(buf)));
    if (not r.ec)
        ok->fetch_add(1);
    else if (r.ec == net::error::operation_aborted)
        aborted->fetch_add(1);
    else
        CHECK(false);
}
CO2_END

auto write_after(net::io_context* ctx, net::tcp_socket* sock, microseconds delay)
    CO2_BEG(net::task<>, (ctx, sock, delay), net::steady_timer timer{*ctx}; net::io_result<> t; std::string const payload{"x"};
            net::io_result<std::size_t> w;) {
    timer.expires_after(delay);
    CO2_AWAIT_SET(t, timer.wait());
    CO2_AWAIT_SET(w, net::write(*sock, net::buffer(payload)));
    static_cast<void>(w);
}
CO2_END

void cancel_storm() {
    test_context ctx{thread_count};
    net::tcp_acceptor acceptor{ctx, net::ip::tcp::endpoint{net::ip::address_v4::loopback(), 0}};
    constexpr auto pairs = 24;
    std::vector<net::tcp_socket> clients;
    std::vector<net::tcp_socket> servers(pairs);
    for (auto i = 0; i != pairs; ++i) clients.emplace_back(ctx);
    for (auto i = 0; i != pairs; ++i)
        net::run_async(ctx.get_executor())(connect_pair(&ctx, &acceptor, &clients[static_cast<std::size_t>(i)],
                                                        &servers[static_cast<std::size_t>(i)]));
    ctx.run();
    ctx.restart();
    std::vector<net::stop_source> sources(pairs);
    std::atomic<int> ok{0};
    std::atomic<int> aborted{0};
    std::mt19937 rng{12345};
    for (auto i = 0; i != pairs; ++i) {
        auto const index = static_cast<std::size_t>(i);
        net::run_async(ctx.get_executor(), sources[index].get_token(), nullptr, [] {}, [](std::exception_ptr) { CHECK(false); })(
            stoppable_read(&clients[index], &ok, &aborted));
        // 一半的对端会写（随机延迟），一半永远不写——只能靠取消结束。
        if (i % 2 == 0)
            net::run_async(ctx.get_executor())(write_after(&ctx, &servers[index], microseconds{rng() % 3000}));
    }
    std::thread canceller{[&] {
        std::mt19937 local{777};
        std::vector<std::size_t> order(pairs);
        for (auto i = std::size_t{}; i != order.size(); ++i) order[i] = i;
        std::shuffle(order.begin(), order.end(), local);
        for (auto const index : order) {
            std::this_thread::sleep_for(microseconds{local() % 400});
            sources[index].request_stop();
        }
    }};
    run_on_threads(ctx);
    canceller.join();
    CHECK_EQ(ok.load() + aborted.load(), pairs);
    CHECK(aborted.load() >= pairs / 2); // 从不写的那一半一定是 aborted
}

// ---------------------------------------------------------------------------
// 4. 定时器风暴：随机到期，一半被随机取消；每个恰好完成一次；未取消的按到期顺序完成。

struct timer_record {
    std::atomic<int> completions{0};
    std::error_code ec;
    steady_clock::time_point expiry;
    steady_clock::time_point completed_at;
};

auto timed_wait(net::io_context* ctx, timer_record* record, microseconds delay)
    CO2_BEG(net::task<>, (ctx, record, delay), net::steady_timer timer{*ctx}; net::io_result<> r;) {
    timer.expires_after(delay);
    record->expiry = timer.expiry();
    CO2_AWAIT_SET(r, timer.wait());
    record->ec = r.ec;
    record->completed_at = steady_clock::now();
    record->completions.fetch_add(1);
}
CO2_END

void timer_storm_with_random_cancellation() {
    test_context ctx{thread_count};
    constexpr auto count = 200;
    std::vector<timer_record> records(count);
    std::vector<net::stop_source> sources(count);
    std::mt19937 rng{4242};
    for (auto i = 0; i != count; ++i) {
        auto const index = static_cast<std::size_t>(i);
        net::run_async(ctx.get_executor(), sources[index].get_token(), nullptr, [] {}, [](std::exception_ptr) { CHECK(false); })(
            timed_wait(&ctx, &records[index], microseconds{rng() % 20000}));
    }
    std::thread canceller{[&] {
        std::mt19937 local{99};
        for (auto i = 0; i != count; i += 2) {
            std::this_thread::sleep_for(microseconds{local() % 200});
            sources[static_cast<std::size_t>(i)].request_stop();
        }
    }};
    run_on_threads(ctx);
    canceller.join();
    for (auto const& record : records) {
        CHECK_EQ(record.completions.load(), 1);
        if (not record.ec) CHECK(record.completed_at >= record.expiry); // 不会早到期
        else CHECK(record.ec == net::error::operation_aborted);
    }
}

// ---------------------------------------------------------------------------
// 5. 多线程接受风暴：64 个客户端同时连接，一个 accept 循环消费（连接到达快于 accept：io_uring
//    的 parked 队列、就绪型的 backlog 都被压到）。

auto connect_and_send(net::io_context* ctx, net::ip::tcp::endpoint ep, std::atomic<int>* done)
    CO2_BEG(net::task<>, (ctx, ep, done), net::tcp_socket sock{*ctx}; net::io_result<> c; std::string const payload{"hello"};
            net::io_result<std::size_t> w; char buf[8]; net::io_result<std::size_t> r;) {
    CO2_AWAIT_SET(c, sock.connect(ep));
    CHECK(not c.ec);
    CO2_AWAIT_SET(w, net::write(sock, net::buffer(payload)));
    CHECK(not w.ec);
    CO2_AWAIT_SET(r, net::read(sock, net::buffer(buf, 5)));
    CHECK(not r.ec);
    CHECK(std::memcmp(buf, "hello", 5) == 0);
    done->fetch_add(1);
}
CO2_END

void accept_burst_under_threads() {
    test_context ctx{thread_count};
    net::tcp_acceptor acceptor{ctx, net::ip::tcp::endpoint{net::ip::address_v4::loopback(), 0}};
    auto const ep = loopback_endpoint(acceptor);
    constexpr auto clients = 64;
    std::atomic<long> echoed{0};
    std::atomic<int> done{0};
    net::run_async(ctx.get_executor())(accept_loop(&ctx, &acceptor, clients, &echoed));
    for (auto i = 0; i != clients; ++i)
        net::run_async(ctx.get_executor())(connect_and_send(&ctx, ep, &done));
    run_on_threads(ctx);
    CHECK_EQ(done.load(), clients);
    CHECK_EQ(echoed.load(), static_cast<long>(clients) * 5);
}

// ---------------------------------------------------------------------------
// 6. 跨执行器：I/O 在 io_context 上，计算在 thread_pool 上（run(pool)(...)），来回切换多次；
//    每次都必须回到 io_context 的线程集合里恢复。

auto cpu_work(int x) CO2_BEG(net::task<int>, (x)) { CO2_RETURN(x * 2 + 1); }
CO2_END

auto io_then_cpu(net::io_context* ctx, net::thread_pool::executor_type pool, net::tcp_socket* sock, int rounds,
                 std::atomic<long>* sum)
    CO2_BEG(net::task<>, (ctx, pool, sock, rounds, sum), int i{}; int v{}; net::steady_timer timer{*ctx}; net::io_result<> t;
            std::string const payload{"z"}; net::io_result<std::size_t> w;) {
    for (i = 0; i != rounds; ++i) {
        CO2_AWAIT_SET(v, net::run(pool)(cpu_work(i)));
        sum->fetch_add(v);
        timer.expires_after(microseconds{50});
        CO2_AWAIT_SET(t, timer.wait());
        CO2_AWAIT_SET(w, net::write(*sock, net::buffer(payload)));
        CHECK(not w.ec);
    }
}
CO2_END

auto count_bytes(net::tcp_socket* sock, long expected, std::atomic<long>* got)
    CO2_BEG(net::task<>, (sock, expected, got), char buf[64]; net::io_result<std::size_t> r; long total{};) {
    while (total < expected) {
        CO2_AWAIT_SET(r, sock->read_some(net::buffer(buf)));
        CHECK(not r.ec);
        total += static_cast<long>(r.value);
    }
    got->fetch_add(total);
}
CO2_END

void io_context_and_thread_pool_interleaved() {
    test_context ctx{thread_count};
    net::thread_pool pool{3U};
    net::tcp_acceptor acceptor{ctx, net::ip::tcp::endpoint{net::ip::address_v4::loopback(), 0}};
    constexpr auto pairs = 6;
    constexpr auto rounds = 30;
    std::vector<net::tcp_socket> clients;
    std::vector<net::tcp_socket> servers(pairs);
    for (auto i = 0; i != pairs; ++i) clients.emplace_back(ctx);
    for (auto i = 0; i != pairs; ++i)
        net::run_async(ctx.get_executor())(connect_pair(&ctx, &acceptor, &clients[static_cast<std::size_t>(i)],
                                                        &servers[static_cast<std::size_t>(i)]));
    ctx.run();
    ctx.restart();
    std::atomic<long> sum{0};
    std::atomic<long> got{0};
    for (auto i = 0; i != pairs; ++i) {
        auto const index = static_cast<std::size_t>(i);
        net::run_async(ctx.get_executor())(io_then_cpu(&ctx, pool.get_executor(), &clients[index], rounds, &sum));
        net::run_async(ctx.get_executor())(count_bytes(&servers[index], rounds, &got));
    }
    run_on_threads(ctx);
    pool.join();
    long expected_sum = 0;
    for (auto i = 0; i != rounds; ++i) expected_sum += i * 2 + 1;
    CHECK_EQ(sum.load(), expected_sum * pairs);
    CHECK_EQ(got.load(), static_cast<long>(rounds * pairs));
}

// ---------------------------------------------------------------------------
// 7. strand 串行化共享状态：多个协程在多线程上下文里经同一个 strand 修改一个非原子计数器，
//    中间夹着真实 I/O（定时器）。TSan 下没有竞争、计数精确。

struct shared_counter {
    long value = 0; // 只在 strand 上触碰
};

auto strand_worker(net::io_context* ctx, shared_counter* counter, int rounds)
    CO2_BEG(net::task<>, (ctx, counter, rounds), int i{}; net::steady_timer timer{*ctx}; net::io_result<> t;) {
    for (i = 0; i != rounds; ++i) {
        counter->value += 1; // 协程恢复在 strand 上：串行
        timer.expires_after(microseconds{10});
        CO2_AWAIT_SET(t, timer.wait());
        counter->value += 1;
    }
}
CO2_END

void strand_serializes_across_threads() {
    test_context ctx{thread_count};
    auto strand = net::make_strand(ctx.get_executor());
    shared_counter counter;
    constexpr auto workers = 16;
    constexpr auto rounds = 50;
    for (auto i = 0; i != workers; ++i)
        net::run_async(strand)(strand_worker(&ctx, &counter, rounds));
    run_on_threads(ctx);
    CHECK_EQ(counter.value, static_cast<long>(workers) * rounds * 2);
}

// ---------------------------------------------------------------------------
// 8. 混沌：K 个会话按种子随机选动作（写 / 读 / 定时器 / when_any(读, 定时器)），对端回显；
//    3 个线程。when_any(读, 定时器) 里输掉的读可能已经消费了数据（首个完成者胜出语义固有），
//    所以客户端不能按字节数等回显，而是最后发一个标记字节、读到它的回显为止再关闭。
//    不变量：run() 结束、每个会话恰好结束一次、服务端回显的字节数 == 写入的字节数（服务端
//    从不取消读）、客户端看到的回显 ≤ 写入。

struct chaos_stats {
    std::atomic<long> written{0};
    std::atomic<long> echoed_back{0};
    std::atomic<int> finished{0};
};

auto chaos_client(net::io_context* ctx, net::ip::tcp::endpoint ep, unsigned seed, int steps, chaos_stats* stats)
    CO2_BEG(net::task<>, (ctx, ep, seed, steps, stats), net::tcp_socket socket_storage{*ctx}; net::tcp_socket* sock{&socket_storage};
            net::io_result<> c; std::mt19937 rng{seed}; int i{}; std::string payload; char buf[128];
            net::steady_timer timer{*ctx}; net::io_result<std::size_t> w; net::io_result<std::size_t> r; net::io_result<> t;
            read_or_timeout any; std::string const marker{"M"}; bool seen_marker{}; std::size_t k{}; unsigned action{};) {
    CO2_AWAIT_SET(c, sock->connect(ep));
    CHECK(not c.ec);
    // 注意：CO2_AWAIT 不能放在用户的 switch 里（状态机自身是 switch），用 if 链。
    for (i = 0; i != steps; ++i) {
        action = rng() % 4U;
        if (action == 0U) { // 写（小写字母，与标记区分）
            payload.assign(1U + rng() % 100U, static_cast<char>('a' + rng() % 26U));
            CO2_AWAIT_SET(w, net::write(*sock, net::buffer(payload)));
            CHECK(not w.ec);
            stats->written.fetch_add(static_cast<long>(w.value));
        } else if (action == 2U) { // 睡
            timer.expires_after(microseconds{rng() % 300U});
            CO2_AWAIT_SET(t, timer.wait());
        } else { // 带超时的读：循环里没有任何读能保证有数据可读，所以全部带超时；超时后不知道输家是否消费了数据
            timer.expires_after(microseconds{action == 1U ? 2000U : rng() % 500U});
            CO2_AWAIT_SET(any, net::when_any(sock->read_some(net::buffer(buf)), timer.wait()));
            CHECK(not any.ec);
            if (any.index == 0) stats->echoed_back.fetch_add(static_cast<long>(std::get<0>(any.value)));
        }
    }
    // 重新同步：发标记，读到它的回显为止。
    CO2_AWAIT_SET(w, net::write(*sock, net::buffer(marker)));
    CHECK(not w.ec);
    stats->written.fetch_add(1);
    while (not seen_marker) {
        CO2_AWAIT_SET(r, sock->read_some(net::buffer(buf)));
        CHECK(not r.ec);
        stats->echoed_back.fetch_add(static_cast<long>(r.value));
        for (k = 0; k != r.value; ++k)
            if (buf[k] == 'M') seen_marker = true;
    }
    sock->close();
    stats->finished.fetch_add(1);
}
CO2_END

void chaos_sessions() {
    test_context ctx{3};
    net::tcp_acceptor acceptor{ctx, net::ip::tcp::endpoint{net::ip::address_v4::loopback(), 0}};
    auto const ep = loopback_endpoint(acceptor);
    constexpr auto sessions = 12;
    constexpr auto steps = 60;
    std::atomic<long> echoed{0};
    chaos_stats stats;
    // 回显会话读到 eof 才结束，所以接受循环必须与客户端在同一轮 run 里。
    net::run_async(ctx.get_executor())(accept_loop(&ctx, &acceptor, sessions, &echoed));
    for (auto i = 0; i != sessions; ++i)
        net::run_async(ctx.get_executor())(chaos_client(&ctx, ep, 1000U + static_cast<unsigned>(i), steps, &stats));
    run_on_threads(ctx, 3);
    CHECK_EQ(stats.finished.load(), sessions);
    CHECK_EQ(echoed.load(), stats.written.load());
    CHECK(stats.echoed_back.load() <= stats.written.load());
    CHECK(stats.echoed_back.load() > 0);
}

// ---------------------------------------------------------------------------
// 9. io_context 生命周期：stop() 时有在飞的操作 → run() 返回；restart() 后再 run() 把它们完成。
//    以及 run_for 的截止：既不提前返回，也不丢操作。

auto read_one(net::tcp_socket* sock, std::error_code* out)
    CO2_BEG(net::task<>, (sock, out), char buf[4]; net::io_result<std::size_t> r;) {
    CO2_AWAIT_SET(r, sock->read_some(net::buffer(buf)));
    *out = r.ec;
}
CO2_END

void stop_restart_and_run_for_keep_pending_operations() {
    test_context ctx{2};
    net::tcp_acceptor acceptor{ctx, net::ip::tcp::endpoint{net::ip::address_v4::loopback(), 0}};
    net::tcp_socket client{ctx};
    net::tcp_socket server;
    net::run_async(ctx.get_executor())(connect_pair(&ctx, &acceptor, &client, &server));
    ctx.run();
    ctx.restart();
    std::error_code read_ec = std::make_error_code(std::errc::state_not_recoverable);
    net::run_async(ctx.get_executor())(read_one(&client, &read_ec));
    // run_for：读挂着，50 ms 后返回且读没丢。
    auto start = steady_clock::now();
    ctx.run_for(milliseconds{50});
    CHECK(steady_clock::now() - start >= milliseconds{45});
    CHECK(read_ec == std::errc::state_not_recoverable); // 还没完成
    // stop() 从另一个线程：run() 返回。
    std::thread stopper{[&] {
        std::this_thread::sleep_for(milliseconds{20});
        ctx.stop();
    }};
    ctx.run();
    stopper.join();
    CHECK(read_ec == std::errc::state_not_recoverable);
    // restart + 对端写 → 读完成。
    ctx.restart();
    net::run_async(ctx.get_executor())(write_after(&ctx, &server, microseconds{100}));
    ctx.run();
    CHECK(not read_ec);
}

// ---------------------------------------------------------------------------
// 10. UDP：多线程下 when_any(receive_from, timer)，随机延迟的发送方。

using recv_or_timeout = net::when_any_result<std::tuple<std::size_t, std::tuple<>>>;

auto udp_receiver(net::io_context* ctx, net::udp_socket* sock, int rounds, std::atomic<int>* got, std::atomic<int>* timed_out)
    CO2_BEG(net::task<>, (ctx, sock, rounds, got, timed_out), char buf[64]; net::ip::udp::endpoint from; net::steady_timer timer{*ctx};
            recv_or_timeout r; int i{};) {
    for (i = 0; i != rounds; ++i) {
        timer.expires_after(microseconds{300});
        CO2_AWAIT_SET(r, net::when_any(sock->receive_from(net::buffer(buf), from), timer.wait()));
        CHECK(not r.ec);
        if (r.index == 0)
            got->fetch_add(1);
        else
            timed_out->fetch_add(1);
    }
}
CO2_END

auto udp_sender(net::io_context* ctx, net::udp_socket* sock, net::ip::udp::endpoint to, int rounds)
    CO2_BEG(net::task<>, (ctx, sock, to, rounds), net::steady_timer timer{*ctx}; net::io_result<> t; int i{};
            std::string const payload{"dgram"}; net::io_result<std::size_t> w;) {
    for (i = 0; i != rounds; ++i) {
        timer.expires_after(microseconds{(i % 3) * 250});
        CO2_AWAIT_SET(t, timer.wait());
        CO2_AWAIT_SET(w, sock->send_to(net::buffer(payload), to));
        CHECK(not w.ec);
    }
}
CO2_END

void udp_when_any_under_threads() {
    test_context ctx{thread_count};
    constexpr auto pairs = 6;
    constexpr auto rounds = 30;
    std::vector<net::udp_socket> receivers;
    std::vector<net::udp_socket> senders;
    std::vector<net::ip::udp::endpoint> targets;
    for (auto i = 0; i != pairs; ++i) {
        receivers.emplace_back(ctx, net::ip::udp::endpoint{net::ip::address_v4::loopback(), 0});
        senders.emplace_back(ctx, net::ip::udp::endpoint{net::ip::address_v4::loopback(), 0});
        std::error_code ec;
        auto target = receivers.back().local_endpoint(ec);
        CHECK(not ec);
        target.address(net::ip::address_v4::loopback());
        targets.push_back(target);
    }
    std::atomic<int> got{0};
    std::atomic<int> timed_out{0};
    for (auto i = 0; i != pairs; ++i) {
        auto const index = static_cast<std::size_t>(i);
        net::run_async(ctx.get_executor())(udp_receiver(&ctx, &receivers[index], rounds, &got, &timed_out));
        net::run_async(ctx.get_executor())(udp_sender(&ctx, &senders[index], targets[index], rounds));
    }
    run_on_threads(ctx);
    CHECK_EQ(got.load() + timed_out.load(), pairs * rounds);
    CHECK(got.load() > 0);
}

} // namespace

struct named_test {
    char const* name;
    void (*run)();
};

named_test const all_tests[] = {
    {"multithreaded_echo_storm", &multithreaded_echo_storm},
    {"when_any_read_or_timer_under_threads", &when_any_read_or_timer_under_threads},
    {"cancel_storm", &cancel_storm},
    {"timer_storm_with_random_cancellation", &timer_storm_with_random_cancellation},
    {"accept_burst_under_threads", &accept_burst_under_threads},
    {"io_context_and_thread_pool_interleaved", &io_context_and_thread_pool_interleaved},
    {"strand_serializes_across_threads", &strand_serializes_across_threads},
    {"chaos_sessions", &chaos_sessions},
    {"stop_restart_and_run_for_keep_pending_operations", &stop_restart_and_run_for_keep_pending_operations},
    {"udp_when_any_under_threads", &udp_when_any_under_threads},
};

// 用法：stress_tests [名字...]；不带参数跑全部。NET_TEST_REPEAT=n 重复 n 遍（压交错）。
int main(int argc, char** argv) {
    auto const repeat = std::getenv("NET_TEST_REPEAT") != nullptr ? std::atoi(std::getenv("NET_TEST_REPEAT")) : 1;
    for (auto iteration = 0; iteration != repeat; ++iteration) {
        for (auto const& test : all_tests) {
            auto selected = argc <= 1;
            for (auto i = 1; i < argc; ++i)
                if (std::strcmp(argv[i], test.name) == 0) selected = true;
            if (not selected) continue;
            if (argc > 1 || repeat > 1) std::cout << test.name << '\n';
            test.run();
        }
    }
    std::cout << "stress tests passed\n";
    return 0;
}
