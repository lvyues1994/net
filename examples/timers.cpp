// 定时器、when_all / when_any、线程池上的 run、stop_token 取消。

#include <chrono>
#include <cstdio>
#include <exception>
#include <thread>

#include "net/net.hpp"

namespace {

using namespace std::chrono;

auto tick(net::io_context* ctx, char const* name, milliseconds period, int count)
    CO2_BEG(net::task<int>, (ctx, name, period, count), net::steady_timer timer{*ctx}; net::io_result<> r; int i{};) {
    for (i = 0; i != count; ++i) {
        timer.expires_after(period);
        CO2_AWAIT_SET(r, timer.wait());
        if (r.ec) {
            std::printf("%s cancelled after %d ticks\n", name, i);
            CO2_RETURN(i);
        }
        std::printf("%s tick %d\n", name, i + 1);
    }
    CO2_RETURN(count);
}
CO2_END

auto cpu_work(int n) CO2_BEG(net::task<long>, (n), long sum{}; int i{};) {
    for (i = 0; i != n; ++i)
        sum += i;
    CO2_RETURN(sum);
}
CO2_END

// 到期后请求停止。
auto stop_after(net::io_context* ctx, milliseconds delay, net::stop_source* stop)
    CO2_BEG(net::task<>, (ctx, delay, stop), net::steady_timer deadline{*ctx}; net::io_result<> r;) {
    deadline.expires_after(delay);
    CO2_AWAIT_SET(r, deadline.wait());
    stop->request_stop();
}
CO2_END

auto main_task(net::io_context* ctx, net::thread_pool* pool)
    CO2_BEG(net::task<>, (ctx, pool), std::tuple<int, int> both; net::when_any_result<int> first; long sum{};
            net::stop_source stop; std::tuple<int> cancelled;) {
    // 两个定时器并发，全部完成后继续。
    CO2_AWAIT_SET(both, net::when_all(tick(ctx, "fast", milliseconds{50}, 3), tick(ctx, "slow", milliseconds{80}, 2)));
    std::printf("when_all: fast=%d slow=%d\n", std::get<0>(both), std::get<1>(both));

    // 先完成者胜出，另一个被取消。
    CO2_AWAIT_SET(first, net::when_any(tick(ctx, "hare", milliseconds{30}, 2), tick(ctx, "tortoise", milliseconds{200}, 2)));
    std::printf("when_any: winner index=%zu ticks=%d\n", first.index, first.value);

    // 计算跳到线程池，完成后回到 io_context 线程。
    CO2_AWAIT_SET(sum, net::run(pool->get_executor())(cpu_work(1000000)));
    std::printf("cpu_work on pool: %ld (back on io_context: %s)\n", sum,
                ctx->get_executor().running_in_this_thread() ? "yes" : "no");

    // 给子链一个自己的 stop_token，100ms 后请求停止：定时器以 operation_aborted 完成。
    CO2_AWAIT_SET(cancelled, net::when_all(net::run(stop.get_token())(tick(ctx, "cancellable", milliseconds{40}, 100)),
                                           stop_after(ctx, milliseconds{100}, &stop)));
    std::printf("cancellable finished with %d ticks\n", std::get<0>(cancelled));
}
CO2_END

} // namespace

int main() {
    try {
        net::io_context ctx;
        net::thread_pool pool{2};
        net::run_async(ctx.get_executor(), [] { std::printf("done\n"); },
                       [](std::exception_ptr) { std::fprintf(stderr, "failed\n"); })(main_task(&ctx, &pool));
        ctx.run();
        pool.join();
        return 0;
    } catch (std::exception const& error) {
        std::fprintf(stderr, "fatal: %s\n", error.what());
        return 1;
    }
}
