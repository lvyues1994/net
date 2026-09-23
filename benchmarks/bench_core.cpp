// 核心路径基准：task 对称转移、启动、执行器 hop、组合子、类型擦除（P4088R1 §1.1 的表）、
// 帧分配器（P4003R3 §3.5 的表）。全部与平台无关。

#include <cstdio>
#include <string>
#include <vector>

#include "net/any_executor.hpp"
#include "net/any_stream.hpp"
#include "net/buffers.hpp"
#include "net/immediate.hpp"
#include "net/io_context.hpp"
#include "net/memory_resource.hpp"
#include "net/run.hpp"
#include "net/run_async.hpp"
#include "net/strand.hpp"
#include "net/stream.hpp"
#include "net/task.hpp"
#include "net/thread_pool.hpp"
#include "net/timeout.hpp"
#include "net/when_all.hpp"

#include "bench.hpp"

namespace {

// ---- 被测协程 ----

auto leaf(int v) CO2_BEG(net::task<int>, (v)) { CO2_RETURN(v); }
CO2_END

// N 次等待子 task：每次一个子帧（回收式分配器命中缓存）+ 两次对称转移。
auto await_children(std::size_t n) CO2_BEG(net::task<long>, (n), long sum{}; std::size_t i{}; int v{};) {
    for (i = 0; i != n; ++i) {
        CO2_AWAIT_SET(v, leaf(static_cast<int>(i)));
        sum += v;
    }
    CO2_RETURN(sum);
}
CO2_END

// 把自己 post 到当前执行器：一次队列往返。
struct hop_awaitable {
    net::continuation cont;
    bool await_ready() const noexcept { return false; }
    net::coroutine_handle<> await_suspend(net::coroutine_handle<> h, net::io_env const* env) noexcept {
        cont.h = h;
        env->executor.post(cont);
        return net::noop_coroutine();
    }
    void await_resume() const noexcept {}
};

auto hops(std::size_t n) CO2_BEG(net::task<>, (n), std::size_t i{};) {
    for (i = 0; i != n; ++i)
        CO2_AWAIT(hop_awaitable{});
}
CO2_END

// 切到线程池执行 hops：hop 在池线程上完成（池内 post），最后经 io_context 执行器回来。
auto on_pool(net::thread_pool::executor_type ex, std::size_t n) CO2_BEG(net::task<>, (ex, n)) {
    CO2_AWAIT(net::run(ex)(hops(n)));
}
CO2_END

// 经执行器 post 一跳后以 io_result 完成的操作；像套接字操作一样状态放在 I/O 对象里、在环境的 stop_token 上登记
// 取消回调。没有系统调用，timeout() 自己的成本看得清。
struct posted_object {
    struct on_stop {
        void operator()() const noexcept {}
    };
    net::continuation cont;
    net::detail::late_init<net::stop_callback<on_stop>> stop_cb;
};

struct posted_op {
    posted_object* object;

    bool await_ready() const noexcept { return false; }
    net::coroutine_handle<> await_suspend(net::coroutine_handle<> h, net::io_env const* env) {
        object->cont.h = h;
        if (env->stop_token.stop_possible()) object->stop_cb.emplace(env->stop_token, posted_object::on_stop{});
        env->executor.post(object->cont);
        return net::noop_coroutine();
    }
    net::io_result<std::size_t> await_resume() noexcept {
        object->stop_cb.reset();
        return {std::error_code{}, 1U};
    }
};

auto posted_ops(std::size_t n) CO2_BEG(net::task<>, (n), std::size_t i{}; posted_object object; net::io_result<std::size_t> r;) {
    for (i = 0; i != n; ++i)
        CO2_AWAIT_SET(r, posted_op{&object});
}
CO2_END

auto posted_ops_under_timeout(std::size_t n)
    CO2_BEG(net::task<>, (n), std::size_t i{}; posted_object object; net::io_result<std::size_t> r;) {
    for (i = 0; i != n; ++i)
        CO2_AWAIT_SET(r, net::timeout(posted_op{&object}, std::chrono::seconds{1}));
}
CO2_END

auto when_all_pairs(std::size_t n) CO2_BEG(net::task<long>, (n), std::size_t i{}; std::tuple<int, int> t; long sum{};) {
    for (i = 0; i != n; ++i) {
        CO2_AWAIT_SET(t, net::when_all(leaf(1), leaf(2)));
        sum += std::get<0>(t) + std::get<1>(t);
    }
    CO2_RETURN(sum);
}
CO2_END

// 内存流：read_some 立即完成。
struct memory_stream {
    template <class MB> net::immediate<net::io_result<std::size_t>> read_some(MB const& buffers) noexcept {
        return {{std::error_code{}, net::buffer_size(buffers)}};
    }
    template <class CB> net::immediate<net::io_result<std::size_t>> write_some(CB const& buffers) noexcept {
        return {{std::error_code{}, net::buffer_size(buffers)}};
    }
};

// 原生：具体类型。
auto read_native(memory_stream& s, std::size_t n) CO2_BEG(net::task<std::size_t>, (s, n), char buf[64]; std::size_t i{};
                                                             net::io_result<std::size_t> r; std::size_t total{};) {
    for (i = 0; i != n; ++i) {
        CO2_AWAIT_SET(r, s.read_some(net::buffer(buf)));
        total += r.value;
    }
    CO2_RETURN(total);
}
CO2_END

// 抽象：对 Stream 泛型（模板，编译期已知具体类型）。
template <class Stream>
auto read_generic(Stream& s, std::size_t n) CO2_BEG(net::task<std::size_t>, (s, n), char buf[64]; std::size_t i{};
                                                       net::io_result<std::size_t> r; std::size_t total{};) {
    for (i = 0; i != n; ++i) {
        CO2_AWAIT_SET(r, s.read_some(net::buffer(buf)));
        total += r.value;
    }
    CO2_RETURN(total);
}
CO2_END

// 类型擦除：any_stream&（编译一次，vtable 派发，awaitable 就地构造）。
auto read_erased(net::any_stream& s, std::size_t n) CO2_BEG(net::task<std::size_t>, (s, n), char buf[64]; std::size_t i{};
                                                               net::io_result<std::size_t> r; std::size_t total{};) {
    for (i = 0; i != n; ++i) {
        CO2_AWAIT_SET(r, s.read_some(net::buffer(buf)));
        total += r.value;
    }
    CO2_RETURN(total);
}
CO2_END

// 帧分配器基准：一棵深度 3、每层扇出 4 的 task 树（85 个帧），重复 n 次。
auto tree(int depth) CO2_BEG(net::task<int>, (depth), int a{}; int b{}; int c{}; int d{};) {
    if (depth == 0) CO2_RETURN(1);
    CO2_AWAIT_SET(a, tree(depth - 1));
    CO2_AWAIT_SET(b, tree(depth - 1));
    CO2_AWAIT_SET(c, tree(depth - 1));
    CO2_AWAIT_SET(d, tree(depth - 1));
    CO2_RETURN(a + b + c + d);
}
CO2_END

auto trees(std::size_t n) CO2_BEG(net::task<long>, (n), std::size_t i{}; int v{}; long sum{};) {
    for (i = 0; i != n; ++i) {
        CO2_AWAIT_SET(v, tree(3));
        sum += v;
    }
    CO2_RETURN(sum);
}
CO2_END

// ---- 驱动 ----

template <class T> T run_on(net::io_context& ctx, net::task<T> t, net::memory_resource* const resource = nullptr) {
    T result{};
    net::run_async(ctx.get_executor(), net::stop_token{}, resource, [&](T v) { result = v; },
                   [](std::exception_ptr) { std::abort(); })(std::move(t));
    ctx.run();
    return result;
}

void run_void_on(net::io_context& ctx, net::task<> t) {
    net::run_async(ctx.get_executor())(std::move(t));
    ctx.run();
}

} // namespace

int main(int argc, char** argv) {
    auto const o = bench::options::parse(argc, argv);
    std::vector<bench::result> results;
    net::io_context ctx;

    results.push_back(bench::run(o, "task: await child (symmetric transfer)", o.scale(2000000U),
                                 [&](std::size_t n) { run_on(ctx, await_children(n)); },
                                 "one child frame per op from the context's default frame allocator"));

    results.push_back(bench::run(o, "run_async + io_context::run round trip", o.scale(300000U), [&](std::size_t n) {
        for (auto i = std::size_t{}; i != n; ++i)
            run_on(ctx, leaf(1));
    }, "heap state per launch"));

    results.push_back(bench::run(o, "io_context post hop (single thread)", o.scale(2000000U),
                                 [&](std::size_t n) { run_void_on(ctx, hops(n)); }));

    {
        net::thread_pool pool{2U};
        results.push_back(bench::run(o, "thread_pool post hop (on pool thread)", o.scale(1000000U),
                                     [&](std::size_t n) { run_void_on(ctx, on_pool(pool.get_executor(), n)); },
                                     "run(pool)(hops): hop is pool-local, mutex queue"));
        pool.join();
    }

    {
        auto strand = net::make_strand(ctx.get_executor());
        results.push_back(bench::run(o, "strand post hop (single thread)", o.scale(1000000U), [&](std::size_t n) {
            net::run_async(strand)(hops(n));
            ctx.run();
        }));
    }

    results.push_back(bench::run(o, "posted op (io_result, stop callback)", o.scale(1000000U),
                                 [&](std::size_t n) { run_void_on(ctx, posted_ops(n)); }, "one post hop per op"));
    results.push_back(bench::run(o, "timeout(posted op), never fires", o.scale(300000U),
                                 [&](std::size_t n) { run_void_on(ctx, posted_ops_under_timeout(n)); },
                                 "timer armed and cancelled per op; the gap to the row above is timeout()"));

    results.push_back(bench::run(o, "when_all(2 ready tasks)", o.scale(300000U),
                                 [&](std::size_t n) { run_on(ctx, when_all_pairs(n)); },
                                 "2 leaf + 2 runner frames, awaiter box, stop state"));

    // ---- 类型擦除（P4088R1 §1.1） ----
    {
        memory_stream native;
        net::any_stream erased{&native};
        results.push_back(bench::run(o, "read_some: native (concrete type)", o.scale(5000000U),
                                     [&](std::size_t n) { run_on(ctx, read_native(native, n)); }));
        results.push_back(bench::run(o, "read_some: abstract (template on Stream)", o.scale(5000000U),
                                     [&](std::size_t n) { run_on(ctx, read_generic(native, n)); }));
        results.push_back(bench::run(o, "read_some: type-erased (any_stream&)", o.scale(5000000U),
                                     [&](std::size_t n) { run_on(ctx, read_erased(erased, n)); },
                                     "vtable + in-place awaitable, expect 0 allocs"));
    }

    // ---- 帧分配器（P4003R3 §3.5） ----
    {
        net::recycling_memory_resource recycling;
        results.push_back(bench::run(o, "85-frame task tree: recycling allocator", o.scale(20000U),
                                     [&](std::size_t n) { run_on(ctx, trees(n), &recycling); }, "ns per tree"));
        results.push_back(bench::run(o, "85-frame task tree: new_delete_resource", o.scale(20000U),
                                     [&](std::size_t n) { run_on(ctx, trees(n), net::new_delete_resource()); },
                                     "ns per tree"));
    }

    bench::print_table("net core benchmarks (ns/op, median of rounds)", results);
    return 0;
}
