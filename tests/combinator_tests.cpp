// when_all / when_any：结果类型拼接、I/O 感知的错误传播与兄弟取消、异常优先、范围重载、
// 与真实定时器的并发。

#include <chrono>
#include <stdexcept>
#include <string>
#include <tuple>
#include <vector>

#include "net/io_context.hpp"
#include "net/error.hpp"
#include "net/run_async.hpp"
#include "net/task.hpp"
#include "net/this_coro.hpp"
#include "net/timer.hpp"
#include "net/when_all.hpp"
#include "net/when_any.hpp"

#include "check.hpp"

namespace {

struct expected_error : std::runtime_error {
    expected_error() : std::runtime_error{"expected"} {}
};

template <class T> T run_task(net::io_context& ctx, net::task<T> t) {
    T result{};
    auto failed = false;
    net::run_async(ctx.get_executor(), [&](T v) { result = std::move(v); },
                   [&](std::exception_ptr) { failed = true; })(std::move(t));
    ctx.run();
    CHECK(not failed);
    return result;
}

auto value(int v) CO2_BEG(net::task<int>, (v)) { CO2_RETURN(v); }
CO2_END

auto text(std::string s) CO2_BEG(net::task<std::string>, (s)) { CO2_RETURN(s); }
CO2_END

auto nothing() CO2_BEG(net::task<>, ()) { CO2_RETURN(); }
CO2_END

auto io_value(std::size_t v, std::error_code ec = {})
    CO2_BEG((net::task<net::io_result<std::size_t>>), (v, ec)) {
    CO2_RETURN((net::io_result<std::size_t>{ec, v}));
}
CO2_END

auto io_void(std::error_code ec = {}) CO2_BEG((net::task<net::io_result<>>), (ec)) {
    CO2_RETURN((net::io_result<>{ec}));
}
CO2_END

// 等待 delay 后返回 v；被取消时返回 -1。
auto delayed(net::io_context* ctx, std::chrono::milliseconds delay, int v)
    CO2_BEG(net::task<int>, (ctx, delay, v), net::steady_timer timer{*ctx}; net::io_result<> r;) {
    timer.expires_after(delay);
    CO2_AWAIT_SET(r, timer.wait());
    CO2_RETURN(r.ec ? -1 : v);
}
CO2_END

auto io_delayed(net::io_context* ctx, std::chrono::milliseconds delay, std::size_t v, std::error_code ec = {})
    CO2_BEG((net::task<net::io_result<std::size_t>>), (ctx, delay, v, ec), net::steady_timer timer{*ctx};
            net::io_result<> r;) {
    timer.expires_after(delay);
    CO2_AWAIT_SET(r, timer.wait());
    if (r.ec) CO2_RETURN((net::io_result<std::size_t>{r.ec, 0U}));
    CO2_RETURN((net::io_result<std::size_t>{ec, v}));
}
CO2_END

// ---------------------------------------------------------------------------

auto generic_tuple() CO2_BEG((net::task<std::tuple<int, std::string>>), (), std::tuple<int, std::string> t;) {
    CO2_AWAIT_SET(t, net::when_all(value(1), nothing(), text("two"))); // void 不占位
    CO2_RETURN(t);
}
CO2_END

void when_all_concatenates_non_io_values() {
    net::io_context ctx;
    auto const t = run_task(ctx, generic_tuple());
    CHECK_EQ(std::get<0>(t), 1);
    CHECK_EQ(std::get<1>(t), "two");
}

auto all_void() CO2_BEG(net::task<int>, ()) {
    CO2_AWAIT(net::when_all(nothing(), nothing()));
    CO2_RETURN(1);
}
CO2_END

void when_all_of_void_tasks_is_void() {
    net::io_context ctx;
    CHECK_EQ(run_task(ctx, all_void()), 1);
}

auto io_pair(net::io_context* ctx) CO2_BEG((net::task<net::io_result<std::size_t, std::size_t>>), (ctx),
                                           net::io_result<std::size_t, std::size_t> r;) {
    CO2_AWAIT_SET(r, net::when_all(io_delayed(ctx, std::chrono::milliseconds{5}, 3U), io_value(4U)));
    CO2_RETURN(r);
}
CO2_END

void when_all_lifts_error_code_out_of_io_results() {
    net::io_context ctx;
    auto const r = run_task(ctx, io_pair(&ctx));
    CHECK(not r.ec);
    CHECK_EQ(std::get<0>(r.values), 3U);
    CHECK_EQ(std::get<1>(r.values), 4U);
}

auto io_mixed() CO2_BEG((net::task<net::io_result<std::size_t>>), (), net::io_result<std::size_t> r;) {
    CO2_AWAIT_SET(r, net::when_all(io_void(), io_value(9U), nothing())); // io_result<> 与 void 不占位
    CO2_RETURN(r);
}
CO2_END

void when_all_skips_empty_payloads() {
    net::io_context ctx;
    auto const r = run_task(ctx, io_mixed());
    CHECK(not r.ec);
    CHECK_EQ(r.value, 9U);
}

// 一个子任务立刻失败：兄弟（等 1 秒的定时器）被取消，整体很快完成，第一个 ec 胜出。
auto io_failure(net::io_context* ctx) CO2_BEG((net::task<net::io_result<std::size_t, std::size_t>>), (ctx),
                                              net::io_result<std::size_t, std::size_t> r;) {
    CO2_AWAIT_SET(r, net::when_all(io_delayed(ctx, std::chrono::seconds{5}, 1U),
                                   io_value(2U, make_error_code(net::error::eof))));
    CO2_RETURN(r);
}
CO2_END

void when_all_cancels_siblings_on_error() {
    net::io_context ctx;
    auto const start = std::chrono::steady_clock::now();
    auto const r = run_task(ctx, io_failure(&ctx));
    CHECK(r.ec == net::error::eof);
    CHECK_EQ(std::get<1>(r.values), 2U);
    CHECK(std::chrono::steady_clock::now() - start < std::chrono::seconds{2});
}

auto throwing() CO2_BEG(net::task<int>, ()) {
    throw expected_error{};
    CO2_RETURN(0);
}
CO2_END

auto exception_wins(net::io_context* ctx) CO2_BEG(net::task<int>, (ctx), std::tuple<int, int> t;) {
    CO2_AWAIT_SET(t, net::when_all(delayed(ctx, std::chrono::seconds{5}, 1), throwing()));
    CO2_RETURN(std::get<0>(t));
}
CO2_END

void when_all_rethrows_the_first_exception_after_all_complete() {
    net::io_context ctx;
    auto thrown = false;
    net::run_async(ctx.get_executor(), [](int) { CHECK(false); },
                   [&](std::exception_ptr const e) {
                       try {
                           std::rethrow_exception(e);
                       } catch (expected_error const&) {
                           thrown = true;
                       }
                   })(exception_wins(&ctx));
    auto const start = std::chrono::steady_clock::now();
    ctx.run();
    CHECK(thrown);
    CHECK(std::chrono::steady_clock::now() - start < std::chrono::seconds{2});
}

auto ranged(net::io_context* ctx) CO2_BEG((net::task<net::io_result<std::vector<std::size_t>>>), (ctx),
                                          std::vector<net::task<net::io_result<std::size_t>>> tasks;
                                          net::io_result<std::vector<std::size_t>> r;) {
    tasks.push_back(io_delayed(ctx, std::chrono::milliseconds{5}, 1U));
    tasks.push_back(io_value(2U));
    tasks.push_back(io_delayed(ctx, std::chrono::milliseconds{1}, 3U));
    CO2_AWAIT_SET(r, net::when_all(std::move(tasks)));
    CO2_RETURN(r);
}
CO2_END

void when_all_range_keeps_order() {
    net::io_context ctx;
    auto const r = run_task(ctx, ranged(&ctx));
    CHECK(not r.ec);
    CHECK_EQ(r.value.size(), 3U);
    CHECK_EQ(r.value[0], 1U);
    CHECK_EQ(r.value[1], 2U);
    CHECK_EQ(r.value[2], 3U);
}

auto ranged_void() CO2_BEG(net::task<int>, (), std::vector<net::task<>> tasks;) {
    tasks.push_back(nothing());
    tasks.push_back(nothing());
    CO2_AWAIT(net::when_all(std::move(tasks)));
    CO2_RETURN(2);
}
CO2_END

void when_all_range_of_void() {
    net::io_context ctx;
    CHECK_EQ(run_task(ctx, ranged_void()), 2);
}

// ---------------------------------------------------------------------------

auto race(net::io_context* ctx) CO2_BEG((net::task<net::when_any_result<int>>), (ctx), net::when_any_result<int> r;) {
    CO2_AWAIT_SET(r, net::when_any(delayed(ctx, std::chrono::seconds{5}, 1), delayed(ctx, std::chrono::milliseconds{5}, 2)));
    CO2_RETURN(r);
}
CO2_END

void when_any_returns_the_first_and_cancels_the_rest() {
    net::io_context ctx;
    auto const start = std::chrono::steady_clock::now();
    auto const r = run_task(ctx, race(&ctx));
    CHECK_EQ(r.index, 1U);
    CHECK_EQ(r.value, 2);
    CHECK(std::chrono::steady_clock::now() - start < std::chrono::seconds{2});
}

auto io_race(net::io_context* ctx) CO2_BEG((net::task<net::when_any_result<std::size_t>>), (ctx),
                                           net::when_any_result<std::size_t> r;) {
    // 第一个完成的带错误：不算赢家；第二个成功者胜出。
    CO2_AWAIT_SET(r, net::when_any(io_value(1U, make_error_code(net::error::eof)),
                                   io_delayed(ctx, std::chrono::milliseconds{5}, 2U)));
    CO2_RETURN(r);
}
CO2_END

void when_any_skips_failed_children() {
    net::io_context ctx;
    auto const r = run_task(ctx, io_race(&ctx));
    CHECK(not r.ec);
    CHECK_EQ(r.index, 1U);
    CHECK_EQ(r.value, 2U);
}

auto all_fail() CO2_BEG((net::task<net::when_any_result<std::size_t>>), (), net::when_any_result<std::size_t> r;) {
    CO2_AWAIT_SET(r, net::when_any(io_value(1U, make_error_code(net::error::eof)),
                                   io_value(2U, make_error_code(net::error::not_open))));
    CO2_RETURN(r);
}
CO2_END

void when_any_reports_the_first_error_when_all_fail() {
    net::io_context ctx;
    auto const r = run_task(ctx, all_fail());
    CHECK(r.ec == net::error::eof);
    CHECK_EQ(r.index, net::when_any_result<std::size_t>::none);
}

auto any_range(net::io_context* ctx) CO2_BEG((net::task<net::when_any_result<int>>), (ctx),
                                             std::vector<net::task<int>> tasks; net::when_any_result<int> r;) {
    tasks.push_back(delayed(ctx, std::chrono::seconds{5}, 1));
    tasks.push_back(delayed(ctx, std::chrono::milliseconds{2}, 2));
    tasks.push_back(delayed(ctx, std::chrono::seconds{5}, 3));
    CO2_AWAIT_SET(r, net::when_any(std::move(tasks)));
    CO2_RETURN(r);
}
CO2_END

void when_any_range() {
    net::io_context ctx;
    auto const r = run_task(ctx, any_range(&ctx));
    CHECK_EQ(r.index, 1U);
    CHECK_EQ(r.value, 2);
}

// 父 stop_token 转发给子任务：请求停止后 when_all 很快完成。
auto forwards_stop(net::io_context* ctx) CO2_BEG(net::task<int>, (ctx), std::tuple<int, int> t;) {
    CO2_AWAIT_SET(t, net::when_all(delayed(ctx, std::chrono::seconds{5}, 1), delayed(ctx, std::chrono::seconds{5}, 2)));
    CO2_RETURN(std::get<0>(t) + std::get<1>(t));
}
CO2_END

auto fire_after(net::io_context* ctx, net::stop_source* source)
    CO2_BEG(net::task<>, (ctx, source), net::steady_timer trigger{*ctx}; net::io_result<> r;) {
    trigger.expires_after(std::chrono::milliseconds{10});
    CO2_AWAIT_SET(r, trigger.wait());
    source->request_stop();
}
CO2_END

void parent_stop_token_is_forwarded_to_children() {
    net::io_context ctx;
    net::stop_source source;
    auto result = 0;
    net::run_async(ctx.get_executor(), source.get_token(), nullptr, [&](int v) { result = v; },
                   [](std::exception_ptr) { CHECK(false); })(forwards_stop(&ctx));
    net::run_async(ctx.get_executor())(fire_after(&ctx, &source));
    auto const start = std::chrono::steady_clock::now();
    ctx.run();
    CHECK_EQ(result, -2);
    CHECK(std::chrono::steady_clock::now() - start < std::chrono::seconds{2});
}

} // namespace

int main() {
    when_all_concatenates_non_io_values();
    when_all_of_void_tasks_is_void();
    when_all_lifts_error_code_out_of_io_results();
    when_all_skips_empty_payloads();
    when_all_cancels_siblings_on_error();
    when_all_rethrows_the_first_exception_after_all_complete();
    when_all_range_keeps_order();
    when_all_range_of_void();
    when_any_returns_the_first_and_cancels_the_rest();
    when_any_skips_failed_children();
    when_any_reports_the_first_error_when_all_fail();
    when_any_range();
    parent_stop_token_is_forwarded_to_children();
    std::cout << "combinator tests passed\n";
    return 0;
}
