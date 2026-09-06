// signal_set：raise 后 wait 完成、先到的信号排队、取消、多个 signal_set 都收到、remove。

#include <chrono>
#include <csignal>

#include "net/io_context.hpp"
#include "net/error.hpp"
#include "net/run_async.hpp"
#include "net/signal_set.hpp"
#include "net/task.hpp"
#include "net/timer.hpp"

#include "check.hpp"

namespace {

auto wait_signal(net::signal_set* signals) CO2_BEG((net::task<net::io_result<int>>), (signals), net::io_result<int> r;) {
    CO2_AWAIT_SET(r, signals->wait());
    CO2_RETURN(r);
}
CO2_END

auto raise_later(net::io_context* ctx, int signo) CO2_BEG(net::task<>, (ctx, signo), net::steady_timer timer{*ctx};
                                                          net::io_result<> r;) {
    timer.expires_after(std::chrono::milliseconds{10});
    CO2_AWAIT_SET(r, timer.wait());
    CHECK_EQ(std::raise(signo), 0);
}
CO2_END

net::io_result<int> wait_for(net::io_context& ctx, net::signal_set& signals) {
    net::io_result<int> result{};
    net::run_async(ctx.get_executor(), [&](net::io_result<int> v) { result = v; }, [](std::exception_ptr) { CHECK(false); })(wait_signal(&signals));
    ctx.run();
    return result;
}

void raised_signal_completes_the_wait() {
    test_context ctx;
    net::signal_set signals{ctx, SIGUSR1};
    net::run_async(ctx.get_executor())(raise_later(&ctx, SIGUSR1));
    auto const r = wait_for(ctx, signals);
    CHECK(not r.ec);
    CHECK_EQ(r.value, SIGUSR1);
}

void early_signal_is_queued() {
    test_context ctx;
    net::signal_set signals{ctx, SIGUSR2};
    CHECK_EQ(std::raise(SIGUSR2), 0);
    // 信号处理函数已把信号号写进管道；反应器读到后排队；wait 立即完成。
    auto const r = wait_for(ctx, signals);
    CHECK(not r.ec);
    CHECK_EQ(r.value, SIGUSR2);
}

void cancel_aborts_the_wait() {
    test_context ctx;
    net::signal_set signals{ctx, SIGUSR1};
    net::io_result<int> result{};
    net::run_async(ctx.get_executor(), [&](net::io_result<int> v) { result = v; }, [](std::exception_ptr) { CHECK(false); })(wait_signal(&signals));
    CHECK_EQ(ctx.poll(), 1U);
    CHECK_EQ(signals.cancel(), 1U);
    ctx.run();
    CHECK(result.ec == net::error::operation_aborted);
}

// 没在等时 cancel() 返回 0，且不能给下一次 wait() 留下"已取消"标记。
void cancel_without_a_pending_wait_is_a_no_op() {
    test_context ctx;
    net::signal_set signals{ctx, SIGUSR1};
    CHECK_EQ(signals.cancel(), 0U);
    net::io_result<int> result{};
    net::run_async(ctx.get_executor(), [&](net::io_result<int> v) { result = v; }, [](std::exception_ptr) { CHECK(false); })(wait_signal(&signals));
    net::run_async(ctx.get_executor())(raise_later(&ctx, SIGUSR1));
    ctx.run();
    CHECK(not result.ec);
    CHECK_EQ(result.value, SIGUSR1);
}

void stop_token_aborts_the_wait() {
    test_context ctx;
    net::signal_set signals{ctx, SIGUSR1};
    net::stop_source source;
    net::io_result<int> result{};
    net::run_async(ctx.get_executor(), source.get_token(), nullptr, [&](net::io_result<int> v) { result = v; },
                   [](std::exception_ptr) { CHECK(false); })(wait_signal(&signals));
    CHECK_EQ(ctx.poll(), 1U);
    source.request_stop();
    ctx.run();
    CHECK(result.ec == net::error::operation_aborted);
}

void every_registered_set_receives_the_signal() {
    test_context ctx;
    net::signal_set a{ctx, SIGUSR1};
    net::signal_set b{ctx, SIGUSR1, SIGUSR2};
    net::io_result<int> ra{};
    net::io_result<int> rb{};
    net::run_async(ctx.get_executor(), [&](net::io_result<int> v) { ra = v; }, [](std::exception_ptr) { CHECK(false); })(wait_signal(&a));
    net::run_async(ctx.get_executor(), [&](net::io_result<int> v) { rb = v; }, [](std::exception_ptr) { CHECK(false); })(wait_signal(&b));
    net::run_async(ctx.get_executor())(raise_later(&ctx, SIGUSR1));
    ctx.run();
    CHECK_EQ(ra.value, SIGUSR1);
    CHECK_EQ(rb.value, SIGUSR1);
}

void removed_signal_is_not_delivered() {
    test_context ctx;
    net::signal_set signals{ctx, SIGUSR1, SIGUSR2};
    CHECK(not signals.remove(SIGUSR2));
    // SIGUSR2 现在恢复默认处置——默认会终止进程，所以这里只验证它不再在集合里（用忽略处置）。
    struct sigaction ignore{};
    ignore.sa_handler = SIG_IGN;
    CHECK_EQ(::sigaction(SIGUSR2, &ignore, nullptr), 0);
    CHECK_EQ(std::raise(SIGUSR2), 0);
    net::run_async(ctx.get_executor())(raise_later(&ctx, SIGUSR1));
    auto const r = wait_for(ctx, signals);
    CHECK_EQ(r.value, SIGUSR1);
}

} // namespace

int main() {
    raised_signal_completes_the_wait();
    early_signal_is_queued();
    cancel_aborts_the_wait();
    cancel_without_a_pending_wait_is_a_no_op();
    stop_token_aborts_the_wait();
    every_registered_set_receives_the_signal();
    removed_signal_is_not_delivered();
    std::cout << "signal tests passed\n";
    return 0;
}
