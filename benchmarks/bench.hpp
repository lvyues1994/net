#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

// 最小基准 harness（无第三方依赖）：多轮取中位数，报告 ns/op 与 allocs/op（全局 operator
// new 计数，见 bench_alloc.cpp）。`--quick` 把迭代数缩到 1/20，供 CI 用。

namespace bench {

std::atomic<long>& allocation_counter() noexcept;

inline long allocations() noexcept { return allocation_counter().load(std::memory_order_relaxed); }

struct result {
    std::string name;
    double ns_per_op;
    double allocs_per_op;
    std::size_t iterations;
    std::string note;
};

struct options {
    bool quick = false;
    std::size_t rounds = 5;
    std::string only; // 只跑名字包含它的行（便于 strace / perf 单独观察）

    static options parse(int const argc, char** const argv) {
        options o;
        for (auto i = 1; i < argc; ++i) {
            if (std::strcmp(argv[i], "--quick") == 0) o.quick = true;
            if (std::strcmp(argv[i], "--rounds") == 0 && i + 1 < argc) o.rounds = static_cast<std::size_t>(std::atoi(argv[++i]));
            if (std::strcmp(argv[i], "--only") == 0 && i + 1 < argc) o.only = argv[++i];
        }
        return o;
    }

    bool selected(std::string const& name) const noexcept { return only.empty() || name.find(only) != std::string::npos; }

    std::size_t scale(std::size_t const n) const noexcept { return quick ? std::max<std::size_t>(n / 20U, 1U) : n; }
};

// body(iterations) 执行 iterations 次操作；预热一轮后测 rounds 轮取中位数。
template <class Body>
result run(options const& o, std::string name, std::size_t const iterations, Body&& body, std::string note = {}) {
    if (not o.selected(name)) return result{std::move(name), 0.0, 0.0, 0U, "skipped"};
    body(iterations / 10U + 1U); // 预热（帧分配器缓存、页）
    std::vector<double> samples;
    auto total_allocs = 0.0;
    for (auto round = std::size_t{}; round != o.rounds; ++round) {
        auto const allocs_before = allocations();
        auto const start = std::chrono::steady_clock::now();
        body(iterations);
        auto const elapsed = std::chrono::steady_clock::now() - start;
        total_allocs += static_cast<double>(allocations() - allocs_before);
        samples.push_back(static_cast<double>(std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count()) /
                          static_cast<double>(iterations));
    }
    std::sort(samples.begin(), samples.end());
    auto const median = samples[samples.size() / 2U];
    return result{std::move(name), median, total_allocs / static_cast<double>(o.rounds * iterations), iterations,
                  std::move(note)};
}

inline void print_table(char const* const title, std::vector<result> const& results) {
    std::printf("\n%s\n", title);
    std::printf("%-46s %12s %10s %10s  %s\n", "benchmark", "ns/op", "allocs/op", "iters", "note");
    std::printf("%-46s %12s %10s %10s  %s\n", "---------", "-----", "---------", "-----", "----");
    for (auto const& r : results) {
        if (r.iterations == 0U) continue; // --only 过滤掉的
        std::printf("%-46s %12.1f %10.3f %10zu  %s\n", r.name.c_str(), r.ns_per_op, r.allocs_per_op, r.iterations,
                    r.note.c_str());
    }
    std::fflush(stdout);
}

} // namespace bench
