// task<T>、IoAwaitable 协议、环境传播、帧分配器、run_async / run。

#include <atomic>
#include <cstddef>
#include <exception>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "net/immediate.hpp"
#include "net/error.hpp"
#include "net/io_context.hpp"
#include "net/memory_resource.hpp"
#include "net/run.hpp"
#include "net/run_async.hpp"
#include "net/task.hpp"
#include "net/this_coro.hpp"
#include "net/thread_pool.hpp"

#include "check.hpp"

namespace {

struct expected_error : std::runtime_error {
    expected_error() : std::runtime_error{"expected"} {}
};

// 计数分配器：验证帧真的从 io_env 的分配器来。
struct counting_resource final : net::memory_resource {
    std::atomic<int> allocations{0};
    std::atomic<int> deallocations{0};

    void* do_allocate(std::size_t const bytes, std::size_t const alignment) override {
        ++allocations;
        return net::new_delete_resource()->allocate(bytes, alignment);
    }
    void do_deallocate(void* const p, std::size_t const bytes, std::size_t const alignment) noexcept override {
        ++deallocations;
        net::new_delete_resource()->deallocate(p, bytes, alignment);
    }
    bool do_is_equal(net::memory_resource const& other) const noexcept override { return &other == this; }
};

// ---------------------------------------------------------------------------

auto twice(int v) CO2_BEG(net::task<int>, (v)) { CO2_RETURN(v * 2); }
CO2_END

auto nothing() CO2_BEG(net::task<>, ()) { CO2_RETURN(); }
CO2_END

auto chain() CO2_BEG(net::task<int>, (), int a{}; int b{}; net::task<int> kept;) {
    CO2_AWAIT_SET(a, twice(1)); // 右值：拥有
    kept = twice(10);
    CO2_AWAIT_SET(b, kept); // 左值：借用
    CO2_AWAIT(nothing());
    CO2_RETURN(a + b);
}
CO2_END

void task_chain_delivers_results() {
    net::io_context ctx;
    auto result = 0;
    net::run_async(ctx.get_executor(), [&](int v) { result = v; }, [](std::exception_ptr) { CHECK(false); })(chain());
    CHECK(ctx.run() >= 1U);
    CHECK_EQ(result, 22);
}

// ---------------------------------------------------------------------------

auto throws() CO2_BEG(net::task<int>, ()) {
    throw expected_error{};
    CO2_RETURN(0);
}
CO2_END

// 子协程的异常在父协程的 await_resume 重抛，父协程没有处理就沿链向上直到 on_error。
auto rethrows() CO2_BEG(net::task<int>, (), int v{};) {
    CO2_AWAIT_SET(v, throws());
    CO2_RETURN(v);
}
CO2_END

void exceptions_propagate_to_the_awaiter_and_to_on_error() {
    net::io_context ctx;
    auto routed = false;
    net::run_async(ctx.get_executor(), [](int) { CHECK(false); },
                   [&](std::exception_ptr const e) {
                       try {
                           std::rethrow_exception(e);
                       } catch (expected_error const&) {
                           routed = true;
                       }
                   })(rethrows());
    ctx.run();
    CHECK(routed);
}

void unhandled_exceptions_rethrow_from_run() {
    net::io_context ctx;
    net::run_async(ctx.get_executor())(throws());
    auto thrown = false;
    try {
        ctx.run();
    } catch (expected_error const&) {
        thrown = true;
    }
    CHECK(thrown);
    // 链已完成，工作计数已归零：再次 run 立即返回。
    CHECK_EQ(ctx.run(), 0U);
}

// ---------------------------------------------------------------------------

// depth 层子协程都看到同一个环境（depth == 0 时不再递归）。
auto inspect_env(net::io_env const* expected, net::memory_resource* expected_mr, int depth)
    CO2_BEG(net::task<bool>, (expected, expected_mr, depth), net::io_env const* env{}; net::executor_ref ex;
            net::stop_token token; net::memory_resource* mr{}; bool child_ok{true};) {
    CO2_AWAIT_SET(env, net::this_coro::environment);
    CO2_AWAIT_SET(ex, net::this_coro::executor);
    CO2_AWAIT_SET(token, net::this_coro::stop_token);
    CO2_AWAIT_SET(mr, net::this_coro::frame_allocator);
    if (depth > 0) CO2_AWAIT_SET(child_ok, inspect_env(env, mr, depth - 1));
    CO2_RETURN((expected == nullptr || env == expected) && ex == env->executor && mr == expected_mr &&
               mr == env->frame_allocator && token == env->stop_token && child_ok);
}
CO2_END

void environment_propagates_to_children() {
    net::io_context ctx;
    counting_resource resource;
    ctx.set_frame_allocator(&resource);
    auto ok = false;
    // 工厂形态：C++14 不保证 run_async(...) 先于实参 inspect_env(...) 求值（MSVC 先算实参，顶层帧就落到
    // 默认分配器上），要数帧分配必须让任务在启动器之后创建。
    net::run_async(ctx.get_executor(), [&](bool v) { ok = v; }, [](std::exception_ptr) { CHECK(false); })(
        [&] { return inspect_env(nullptr, &resource, 3); });
    ctx.run();
    CHECK(ok);
    CHECK_EQ(resource.allocations.load(), 4); // 1 个父帧 + 3 层子帧
    CHECK_EQ(resource.allocations.load(), resource.deallocations.load());
}

// ---------------------------------------------------------------------------

auto sees_stop(bool* requested) CO2_BEG(net::task<>, (requested), net::stop_token token;) {
    CO2_AWAIT_SET(token, net::this_coro::stop_token);
    *requested = token.stop_requested();
}
CO2_END

void stop_token_is_visible_in_the_chain() {
    net::io_context ctx;
    net::stop_source source;
    source.request_stop();
    auto requested = false;
    net::run_async(ctx.get_executor(), source.get_token())(sees_stop(&requested));
    ctx.run();
    CHECK(requested);
}

// ---------------------------------------------------------------------------

auto which_pool(net::thread_pool* pool) CO2_BEG(net::task<bool>, (pool)) {
    CO2_RETURN(pool->get_executor().running_in_this_thread());
}
CO2_END

auto hop(net::thread_pool* pool, net::io_context* ctx)
    CO2_BEG(net::task<int>, (pool, ctx), bool on_pool{}; bool back{}; int v{};) {
    CO2_AWAIT_SET(on_pool, net::run(pool->get_executor())(which_pool(pool)));
    // run 完成后经父执行器恢复：回到 io_context 线程。
    back = ctx->get_executor().running_in_this_thread();
    CO2_AWAIT_SET(v, net::run(pool->get_executor())(twice(21)));
    CO2_RETURN((on_pool ? 1 : 0) + (back ? 10 : 0) + v);
}
CO2_END

void run_switches_executor_and_returns_to_the_parent() {
    net::io_context ctx;
    net::thread_pool pool{2U};
    auto result = 0;
    net::run_async(ctx.get_executor(), [&](int v) { result = v; }, [](std::exception_ptr) { CHECK(false); })(hop(&pool, &ctx));
    ctx.run();
    pool.join();
    CHECK_EQ(result, 1 + 10 + 42);
}

// ---------------------------------------------------------------------------

auto uses_resource(counting_resource* r) CO2_BEG(net::task<int>, (r), int v{};) {
    CO2_AWAIT_SET(v, twice(3)); // 子帧从 r 分配
    CO2_RETURN(v);
}
CO2_END

void run_with_frame_allocator_allocates_child_frames_from_it() {
    net::io_context ctx;
    counting_resource resource;
    auto result = 0;
    net::run_async(ctx.get_executor(), net::stop_token{}, &resource, [&](int v) { result = v; },
                   [](std::exception_ptr) { CHECK(false); })([&] { return uses_resource(&resource); });
    ctx.run();
    CHECK_EQ(result, 6);
    CHECK_EQ(resource.allocations.load(), 2); // uses_resource 的帧 + twice 的帧
    CHECK_EQ(resource.deallocations.load(), 2);
}

void factory_form_allocates_under_the_launcher() {
    net::io_context ctx;
    counting_resource resource;
    auto result = 0;
    net::run_async(ctx.get_executor(), net::stop_token{}, &resource, [&](int v) { result = v; },
                   [](std::exception_ptr) { CHECK(false); })([] { return twice(4); });
    ctx.run();
    CHECK_EQ(result, 8);
    CHECK_EQ(resource.allocations.load(), 1);
}

// ---------------------------------------------------------------------------

auto immediate_values() CO2_BEG(net::task<int>, (), net::io_result<std::size_t> r; net::io_result<> e;
                                net::io_result<int, int> two; int v{};) {
    CO2_AWAIT_SET(r, net::ready(std::error_code{}, std::size_t{5}));
    CO2_AWAIT_SET(e, net::ready(make_error_code(net::error::eof)));
    CO2_AWAIT_SET(two, net::ready(std::error_code{}, 1, 2));
    CO2_AWAIT_SET(v, net::make_immediate(100));
    CO2_RETURN(static_cast<int>(r.value) + (e.ec == net::error::eof ? 10 : 0) +
               std::get<0>(two.values) + std::get<1>(two.values) + v);
}
CO2_END

void immediate_awaitables_never_suspend() {
    net::io_context ctx;
    auto result = 0;
    net::run_async(ctx.get_executor(), [&](int v) { result = v; }, [](std::exception_ptr) { CHECK(false); })(immediate_values());
    ctx.run();
    CHECK_EQ(result, 5 + 10 + 3 + 100);
}

void io_result_tuple_protocol() {
    net::io_result<int, std::string> r{make_error_code(net::error::eof), std::make_tuple(1, std::string{"x"})};
    CHECK(net::get<0>(r) == net::error::eof);
    CHECK_EQ(net::get<1>(r), 1);
    CHECK_EQ(net::get<2>(r), "x");
    static_assert(std::tuple_size<net::io_result<int, std::string>>::value == 3U, "");
    static_assert(std::is_same<std::tuple_element<2, net::io_result<int, std::string>>::type, std::string>::value, "");
    net::io_result<std::size_t> s{{}, 3U};
    CHECK_EQ(net::get<1>(s), 3U);
}

} // namespace

int main() {
    task_chain_delivers_results();
    exceptions_propagate_to_the_awaiter_and_to_on_error();
    unhandled_exceptions_rethrow_from_run();
    environment_propagates_to_children();
    stop_token_is_visible_in_the_chain();
    run_switches_executor_and_returns_to_the_parent();
    run_with_frame_allocator_allocates_child_frames_from_it();
    factory_form_allocates_under_the_launcher();
    immediate_awaitables_never_suspend();
    io_result_tuple_protocol();
    std::cout << "task tests passed\n";
    return 0;
}
