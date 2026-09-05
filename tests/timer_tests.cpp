// steady_timer：到期、取消、expires_* 取消挂起的 wait、stop_token 取消、多个定时器的顺序。

#include <chrono>
#include <vector>

#include "net/io_context.hpp"
#include "net/error.hpp"
#include "net/run_async.hpp"
#include "net/task.hpp"
#include "net/timer.hpp"
#include "net/when_all.hpp"

#include "check.hpp"

namespace {

using namespace std::chrono;

template <class T> T run_task(net::io_context& ctx, net::task<T> t) {
    T result{};
    auto failed = false;
    net::run_async(ctx.get_executor(), [&](T v) { result = std::move(v); },
                   [&](std::exception_ptr) { failed = true; })(std::move(t));
    ctx.run();
    CHECK(not failed);
    return result;
}

auto wait_for(net::io_context* ctx, milliseconds d)
    CO2_BEG((net::task<std::error_code>), (ctx, d), net::steady_timer timer{*ctx}; net::io_result<> r;) {
    timer.expires_after(d);
    CO2_AWAIT_SET(r, timer.wait());
    CO2_RETURN(r.ec);
}
CO2_END

void timer_expires_after_the_duration() {
    test_context ctx;
    auto const start = steady_clock::now();
    auto const ec = run_task(ctx, wait_for(&ctx, milliseconds{30}));
    CHECK(not ec);
    CHECK(steady_clock::now() - start >= milliseconds{28});
}

void expired_timer_completes_immediately() {
    test_context ctx;
    auto const start = steady_clock::now();
    auto const ec = run_task(ctx, wait_for(&ctx, milliseconds{0}));
    CHECK(not ec);
    CHECK(steady_clock::now() - start < milliseconds{50});
}

// 外部取消：另一个协程稍后 cancel()。
auto cancel_later(net::io_context* ctx, net::steady_timer* victim)
    CO2_BEG(net::task<>, (ctx, victim), net::steady_timer trigger{*ctx}; net::io_result<> r;) {
    trigger.expires_after(milliseconds{10});
    CO2_AWAIT_SET(r, trigger.wait());
    CHECK_EQ(victim->cancel(), 1U);
}
CO2_END

auto wait_on(net::steady_timer* timer) CO2_BEG((net::task<std::error_code>), (timer), net::io_result<> r;) {
    CO2_AWAIT_SET(r, timer->wait());
    CO2_RETURN(r.ec);
}
CO2_END

void cancel_completes_the_wait_with_operation_aborted() {
    test_context ctx;
    net::steady_timer victim{ctx, seconds{10}};
    std::error_code ec;
    net::run_async(ctx.get_executor(), [&](std::error_code e) { ec = e; }, [](std::exception_ptr) { CHECK(false); })(wait_on(&victim));
    net::run_async(ctx.get_executor())(cancel_later(&ctx, &victim));
    auto const start = steady_clock::now();
    ctx.run();
    CHECK(ec == net::error::operation_aborted);
    CHECK(steady_clock::now() - start < seconds{2});
    CHECK_EQ(victim.cancel(), 0U);
}

auto reschedule_later(net::io_context* ctx, net::steady_timer* victim)
    CO2_BEG(net::task<>, (ctx, victim), net::steady_timer trigger{*ctx}; net::io_result<> r;) {
    trigger.expires_after(milliseconds{10});
    CO2_AWAIT_SET(r, trigger.wait());
    CHECK_EQ(victim->expires_after(milliseconds{5}), 1U); // 取消挂起的 wait
}
CO2_END

// 被 expires_after 取消后再等一次：按新的到期时间完成。
auto wait_twice(net::steady_timer* timer) CO2_BEG(net::task<int>, (timer), net::io_result<> first; net::io_result<> second;) {
    CO2_AWAIT_SET(first, timer->wait());
    CO2_AWAIT_SET(second, timer->wait());
    CO2_RETURN((first.ec == net::error::operation_aborted ? 1 : 0) + (not second.ec ? 10 : 0));
}
CO2_END

void expires_after_cancels_a_pending_wait() {
    test_context ctx;
    net::steady_timer victim{ctx, seconds{10}};
    auto result = 0;
    net::run_async(ctx.get_executor(), [&](int v) { result = v; }, [](std::exception_ptr) { CHECK(false); })(wait_twice(&victim));
    net::run_async(ctx.get_executor())(reschedule_later(&ctx, &victim));
    auto const start = steady_clock::now();
    ctx.run();
    CHECK_EQ(result, 11);
    CHECK(steady_clock::now() - start < seconds{2});
}

void stop_token_cancels_the_wait() {
    test_context ctx;
    net::stop_source source;
    std::error_code ec;
    net::run_async(ctx.get_executor(), source.get_token(), nullptr, [&](std::error_code e) { ec = e; },
                   [](std::exception_ptr) { CHECK(false); })(wait_for(&ctx, seconds{10}));
    CHECK_EQ(ctx.poll(), 1U); // 协程启动并挂起在定时器上
    source.request_stop();     // 取消回调把续体 post 回上下文
    auto const start = steady_clock::now();
    ctx.run();
    CHECK(ec == net::error::operation_aborted);
    CHECK(steady_clock::now() - start < seconds{2});
}

void already_stopped_token_completes_without_waiting() {
    test_context ctx;
    net::stop_source source;
    source.request_stop();
    std::error_code ec;
    net::run_async(ctx.get_executor(), source.get_token(), nullptr, [&](std::error_code e) { ec = e; },
                   [](std::exception_ptr) { CHECK(false); })(wait_for(&ctx, seconds{10}));
    ctx.run();
    CHECK(ec == net::error::operation_aborted);
}

auto record_order(net::io_context* ctx, milliseconds d, int id, std::vector<int>* order)
    CO2_BEG(net::task<>, (ctx, d, id, order), net::steady_timer timer{*ctx}; net::io_result<> r;) {
    timer.expires_after(d);
    CO2_AWAIT_SET(r, timer.wait());
    order->push_back(id);
}
CO2_END

void timers_fire_in_expiry_order() {
    test_context ctx;
    std::vector<int> order;
    net::run_async(ctx.get_executor())(record_order(&ctx, milliseconds{30}, 3, &order));
    net::run_async(ctx.get_executor())(record_order(&ctx, milliseconds{10}, 1, &order));
    net::run_async(ctx.get_executor())(record_order(&ctx, milliseconds{20}, 2, &order));
    ctx.run();
    CHECK_EQ(order.size(), 3U);
    CHECK_EQ(order[0], 1);
    CHECK_EQ(order[1], 2);
    CHECK_EQ(order[2], 3);
}

auto all_timers(std::vector<net::task<std::error_code>> ts)
    CO2_BEG((net::task<std::vector<std::error_code>>), (ts), std::vector<std::error_code> r;) {
    CO2_AWAIT_SET(r, net::when_all(std::move(ts)));
    CO2_RETURN(r);
}
CO2_END

void many_timers_via_when_all() {
    test_context ctx;
    std::vector<net::task<std::error_code>> tasks;
    for (auto i = 0; i != 50; ++i)
        tasks.push_back(wait_for(&ctx, milliseconds{i % 5}));
    auto const results = run_task(ctx, all_timers(std::move(tasks)));
    CHECK_EQ(results.size(), 50U);
    for (auto const& ec : results)
        CHECK(not ec);
}

} // namespace

int main() {
    timer_expires_after_the_duration();
    expired_timer_completes_immediately();
    cancel_completes_the_wait_with_operation_aborted();
    expires_after_cancels_a_pending_wait();
    stop_token_cancels_the_wait();
    already_stopped_token_completes_without_waiting();
    timers_fire_in_expiry_order();
    many_timers_via_when_all();
    std::cout << "timer tests passed\n";
    return 0;
}
