#include "bench.hpp"

#include <cstdlib>
#include <new>

// 全局 operator new 计数：把"零分配热路径"变成可测量的数字。

namespace bench {

std::atomic<long>& allocation_counter() noexcept {
    static std::atomic<long> counter{0};
    return counter;
}

} // namespace bench

void* operator new(std::size_t const size) {
    bench::allocation_counter().fetch_add(1, std::memory_order_relaxed);
    if (auto* const p = std::malloc(size == 0U ? 1U : size)) return p;
    throw std::bad_alloc{};
}

void* operator new[](std::size_t const size) {
    bench::allocation_counter().fetch_add(1, std::memory_order_relaxed);
    if (auto* const p = std::malloc(size == 0U ? 1U : size)) return p;
    throw std::bad_alloc{};
}

void operator delete(void* const p) noexcept { std::free(p); }
void operator delete(void* const p, std::size_t) noexcept { std::free(p); }
void operator delete[](void* const p) noexcept { std::free(p); }
void operator delete[](void* const p, std::size_t) noexcept { std::free(p); }
