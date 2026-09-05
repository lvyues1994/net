// io_context 事件循环、执行器概念、executor_ref / any_executor、strand、thread_pool、
// execution_context 服务与帧分配器。

#include <atomic>
#include <chrono>
#include <cstddef>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "net/any_executor.hpp"
#include "net/error.hpp"
#include "net/executor_ref.hpp"
#include "net/io_context.hpp"
#include "net/memory_resource.hpp"
#include "net/run.hpp"
#include "net/run_async.hpp"
#include "net/strand.hpp"
#include "net/task.hpp"
#include "net/thread_pool.hpp"
#include "net/timer.hpp"

#include "check.hpp"

namespace {

static_assert(net::is_executor<net::io_context::executor_type>::value, "");
static_assert(net::is_executor<net::thread_pool::executor_type>::value, "");
static_assert(net::is_executor<net::strand<net::io_context::executor_type>>::value, "");
static_assert(net::is_executor<net::any_executor>::value, "");
static_assert(net::is_executor<net::executor_ref>::value, "");
static_assert(not net::is_executor<int>::value, "");

auto noop() CO2_BEG(net::task<>, ()) { CO2_RETURN(); }
CO2_END

void run_returns_immediately_without_work() {
    net::io_context ctx;
    CHECK_EQ(ctx.run(), 0U);
    CHECK_EQ(ctx.poll(), 0U);
    CHECK(not ctx.stopped());
}

auto tick(net::io_context* ctx) CO2_BEG(net::task<int>, (ctx), net::steady_timer timer{*ctx}; net::io_result<> r;) {
    timer.expires_after(std::chrono::milliseconds{5});
    CO2_AWAIT_SET(r, timer.wait());
    CO2_RETURN(r.ec ? 0 : 1);
}
CO2_END

// 每种后端都能被选中、报告自己的名字，并跑通一个定时器。
void every_backend_can_be_selected() {
    {
        net::io_context ctx;
        CHECK(ctx.backend() == net::backend_kind::epoll);
        CHECK_EQ(std::string{ctx.backend_name()}, "epoll");
    }
    CHECK(net::backend_available(net::backend_kind::epoll));
    CHECK(net::backend_available(net::backend_kind::poll));
    CHECK(net::backend_available(net::backend_kind::select));
    net::backend_kind const kinds[] = {net::backend_kind::epoll, net::backend_kind::poll, net::backend_kind::select,
                                       net::backend_kind::io_uring};
    for (auto const kind : kinds) {
        if (not net::backend_available(kind)) {
            std::cout << "skipping unavailable backend " << net::to_string(kind) << '\n';
            continue;
        }
        net::io_context ctx{kind, 1};
        CHECK(ctx.backend() == kind);
        CHECK_EQ(std::string{ctx.backend_name()}, std::string{net::to_string(kind)});
        auto ticks = 0;
        net::run_async(ctx.get_executor(), [&](int v) { ticks = v; }, [](std::exception_ptr) { CHECK(false); })(tick(&ctx));
        ctx.run();
        CHECK_EQ(ticks, 1);
    }
    net::io_context by_tag_poll{net::poll};
    CHECK(by_tag_poll.backend() == net::backend_kind::poll);
    net::io_context by_tag_select{net::select, 2};
    CHECK(by_tag_select.backend() == net::backend_kind::select);
    if (net::backend_available(net::backend_kind::io_uring)) {
        net::io_context by_tag_uring{net::io_uring};
        CHECK(by_tag_uring.backend() == net::backend_kind::io_uring);
        CHECK_EQ(std::string{by_tag_uring.backend_name()}, "io_uring");
    }
}

void stop_and_restart() {
    net::io_context ctx;
    ctx.stop();
    CHECK(ctx.stopped());
    net::run_async(ctx.get_executor())(noop());
    CHECK_EQ(ctx.run(), 0U); // 已停止：不运行
    ctx.restart();
    CHECK(not ctx.stopped());
    CHECK_EQ(ctx.run(), 1U);
}

// ---------------------------------------------------------------------------

auto record_thread(std::mutex* m, std::set<std::thread::id>* ids, std::atomic<int>* count)
    CO2_BEG(net::task<>, (m, ids, count)) {
    {
        std::lock_guard<std::mutex> lock{*m};
        ids->insert(std::this_thread::get_id());
    }
    count->fetch_add(1);
    CO2_RETURN();
}
CO2_END

void io_context_runs_on_multiple_threads() {
    net::io_context ctx;
    std::mutex m;
    std::set<std::thread::id> ids;
    std::atomic<int> count{0};
    for (auto i = 0; i != 200; ++i)
        net::run_async(ctx.get_executor())(record_thread(&m, &ids, &count));
    std::vector<std::thread> threads;
    for (auto i = 0; i != 4; ++i)
        threads.emplace_back([&] { ctx.run(); });
    for (auto& t : threads)
        t.join();
    CHECK_EQ(count.load(), 200);
    CHECK(not ids.empty());
}

// ---------------------------------------------------------------------------

// strand：并发提交的协程串行执行——用一个非原子计数器检测数据竞争。
auto bump(int* counter, std::atomic<int>* concurrent, std::atomic<int>* max_concurrent)
    CO2_BEG(net::task<>, (counter, concurrent, max_concurrent)) {
    {
        auto const now = concurrent->fetch_add(1) + 1;
        auto expected = max_concurrent->load();
        while (now > expected && not max_concurrent->compare_exchange_weak(expected, now)) {}
        ++*counter;
        std::this_thread::sleep_for(std::chrono::microseconds{20});
        concurrent->fetch_sub(1);
    }
    CO2_RETURN();
}
CO2_END

void strand_serializes_on_a_thread_pool() {
    net::thread_pool pool{4U};
    auto strand = net::make_strand(pool.get_executor());
    auto counter = 0;
    std::atomic<int> concurrent{0};
    std::atomic<int> max_concurrent{0};
    for (auto i = 0; i != 500; ++i)
        net::run_async(strand)(bump(&counter, &concurrent, &max_concurrent));
    pool.join();
    CHECK_EQ(counter, 500);
    CHECK_EQ(max_concurrent.load(), 1);
}

auto in_strand(net::strand<net::io_context::executor_type> s) CO2_BEG(net::task<bool>, (s)) {
    CO2_RETURN(s.running_in_this_thread());
}
CO2_END

void strand_dispatch_is_inline_inside_the_strand() {
    net::io_context ctx;
    auto strand = net::make_strand(ctx.get_executor());
    auto inside = false;
    net::run_async(strand, [&](bool v) { inside = v; }, [](std::exception_ptr) { CHECK(false); })(in_strand(strand));
    ctx.run();
    CHECK(inside);
    CHECK(not strand.running_in_this_thread());
}

// ---------------------------------------------------------------------------

void any_executor_equality_and_target() {
    net::io_context a;
    net::io_context b;
    net::any_executor ea{a.get_executor()};
    net::any_executor ea2{a.get_executor()};
    net::any_executor eb{b.get_executor()};
    CHECK(ea == ea2);
    CHECK(ea != eb);
    CHECK(ea.target<net::io_context::executor_type>() != nullptr);
    CHECK(ea.target<net::thread_pool::executor_type>() == nullptr);
    CHECK(&ea.context() == &a);
    net::executor_ref ref{ea};
    CHECK(&ref.context() == &a);
}

// ---------------------------------------------------------------------------

struct counter_service final : net::execution_context::service {
    explicit counter_service(net::execution_context&) { ++constructed; }
    void shutdown() override { ++shut_down; }
    static int constructed;
    static int shut_down;
};
int counter_service::constructed = 0;
int counter_service::shut_down = 0;

struct dependent_service final : net::execution_context::service {
    explicit dependent_service(net::execution_context& ctx) : dependency{&ctx.use_service<counter_service>()} {}
    void shutdown() override { CHECK_EQ(counter_service::shut_down, 0); } // 后注册者先 shutdown
    counter_service* dependency;
};

void services_are_singletons_with_ordered_shutdown() {
    {
        net::io_context ctx;
        CHECK(not ctx.has_service<counter_service>());
        auto& d = ctx.use_service<dependent_service>();
        auto& c = ctx.use_service<counter_service>();
        CHECK(d.dependency == &c);
        CHECK(&ctx.use_service<counter_service>() == &c);
        CHECK_EQ(counter_service::constructed, 1);
        auto threw = false;
        try {
            ctx.make_service<counter_service>();
        } catch (std::logic_error const&) {
            threw = true;
        }
        CHECK(threw);
    }
    CHECK_EQ(counter_service::shut_down, 1);
}

void recycling_resource_reuses_blocks() {
    net::recycling_memory_resource resource;
    auto* const first = resource.allocate(256U);
    resource.deallocate(first, 256U);
    CHECK_EQ(resource.cached_blocks(), 1U);
    auto* const second = resource.allocate(256U);
    CHECK(first == second);
    CHECK_EQ(resource.cached_blocks(), 0U);
    resource.deallocate(second, 256U);
    resource.release();
    CHECK_EQ(resource.cached_bytes(), 0U);
}

void safe_resume_restores_the_cached_frame_allocator() {
    net::io_context ctx;
    net::recycling_memory_resource marker;
    net::set_cached_frame_allocator(&marker);
    net::run_async(ctx.get_executor())(noop());
    ctx.run();
    CHECK(net::get_cached_frame_allocator() == &marker);
    net::set_cached_frame_allocator(nullptr);
}

// ---------------------------------------------------------------------------

auto waits(net::steady_timer* timer) CO2_BEG(net::task<>, (timer), net::io_result<> r;) {
    CO2_AWAIT_SET(r, timer->wait());
}
CO2_END

void run_for_returns_on_timeout_with_pending_work() {
    net::io_context ctx;
    net::steady_timer timer{ctx, std::chrono::seconds{5}};
    net::run_async(ctx.get_executor())(waits(&timer));
    auto const start = std::chrono::steady_clock::now();
    ctx.run_for(std::chrono::milliseconds{30});
    auto const elapsed = std::chrono::steady_clock::now() - start;
    CHECK(elapsed >= std::chrono::milliseconds{25});
    CHECK(elapsed < std::chrono::seconds{2});
    timer.cancel();
    ctx.run();
}

void poll_processes_ready_work_only() {
    net::io_context ctx;
    net::steady_timer timer{ctx, std::chrono::seconds{5}};
    net::run_async(ctx.get_executor())(waits(&timer));
    net::run_async(ctx.get_executor())(noop());
    CHECK_EQ(ctx.poll(), 2U); // waits 启动后挂起（1 次恢复）、noop 完成（1 次恢复）
    CHECK_EQ(ctx.poll(), 0U);
    timer.cancel();
    CHECK_EQ(ctx.run(), 1U);
}

} // namespace

int main() {
    run_returns_immediately_without_work();
    every_backend_can_be_selected();
    stop_and_restart();
    io_context_runs_on_multiple_threads();
    strand_serializes_on_a_thread_pool();
    strand_dispatch_is_inline_inside_the_strand();
    any_executor_equality_and_target();
    services_are_singletons_with_ordered_shutdown();
    recycling_resource_reuses_blocks();
    safe_resume_restores_the_cached_frame_allocator();
    run_for_returns_on_timeout_with_pending_work();
    poll_processes_ready_work_only();
    std::cout << "executor tests passed\n";
    return 0;
}
