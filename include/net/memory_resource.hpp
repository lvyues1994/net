#pragma once

#include <cstddef>
#include <mutex>
#include <vector>

#include "net/coroutine.hpp"

// 帧分配器（P4003R3 §3.5 / §4.5，P4172R1 §6.3 / §8）。
//
// C++14 没有 std::pmr，这里给出同形的 memory_resource：类型擦除的分配接口，供协程帧
// 分配使用。协议规定两个访问函数——get_cached_frame_allocator / set_cached_frame_
// allocator——把当前链的帧分配器"带外"交给 promise 的 operator new：帧在协程被调用时
// 就分配，参数列表之外只有这条通道（P4127R0）。存储机制由实现选择，这里是线程局部
// 存储：每次协程恢复（await_resume）都把 io_env 里的分配器写回槽位，operator new 只在
// 协程体执行期间读取它。
//
// safe_resume 是 §8.3 规定的执行循环义务：恢复一个句柄前保存槽位、之后恢复，让槽位
// 像栈一样工作，避免不同链在同一线程交错时污染彼此的分配器。

namespace net {

struct memory_resource {
    memory_resource() = default;
    memory_resource(memory_resource const&) = default;
    memory_resource& operator=(memory_resource const&) = default;
    virtual ~memory_resource() = default;

    void* allocate(std::size_t const bytes,
                   std::size_t const alignment = alignof(std::max_align_t)) {
        return do_allocate(bytes, alignment);
    }

    void deallocate(void* const pointer, std::size_t const bytes,
                    std::size_t const alignment = alignof(std::max_align_t)) noexcept {
        do_deallocate(pointer, bytes, alignment);
    }

    bool is_equal(memory_resource const& other) const noexcept {
        return do_is_equal(other);
    }

  protected:
    virtual void* do_allocate(std::size_t bytes, std::size_t alignment) = 0;
    virtual void do_deallocate(void* pointer, std::size_t bytes,
                               std::size_t alignment) noexcept = 0;
    virtual bool do_is_equal(memory_resource const& other) const noexcept = 0;
};

inline bool operator==(memory_resource const& left, memory_resource const& right) noexcept {
    return &left == &right || left.is_equal(right);
}

inline bool operator!=(memory_resource const& left, memory_resource const& right) noexcept {
    return not(left == right);
}

// ::operator new / ::operator delete 之上的资源；进程唯一。
memory_resource* new_delete_resource() noexcept;

// 回收式帧分配器（P4172R1 §6.3.1）。协程帧的尺寸重复、生命周期嵌套、释放顺序与分配
// 顺序镜像；按尺寸分类缓存最近释放的块，稳态下每次帧分配都命中缓存，不再进入通用
// 分配器。缓存有上限，超出的块交还上游。线程安全。
struct recycling_memory_resource final : memory_resource {
    struct config {
        std::size_t max_size_classes = 32;        // 最多缓存多少种（尺寸, 对齐）
        std::size_t max_blocks_per_class = 64;    // 每种最多缓存多少块
        std::size_t max_block_size = 256U * 1024U; // 超过的块不缓存
    };

    explicit recycling_memory_resource(memory_resource* upstream = new_delete_resource());
    recycling_memory_resource(memory_resource* upstream, config const& options);
    ~recycling_memory_resource() override;

    recycling_memory_resource(recycling_memory_resource const&) = delete;
    recycling_memory_resource& operator=(recycling_memory_resource const&) = delete;

    memory_resource* upstream() const noexcept { return upstream_; }

    // 把缓存的块全部交还上游。
    void release() noexcept;

    // 诊断：当前缓存的块数与字节数。
    std::size_t cached_blocks() const noexcept;
    std::size_t cached_bytes() const noexcept;

  private:
    struct free_block {
        free_block* next;
    };

    struct size_class {
        std::size_t size;
        std::size_t alignment;
        free_block* head;
        std::size_t count;
    };

    void* do_allocate(std::size_t bytes, std::size_t alignment) override;
    void do_deallocate(void* pointer, std::size_t bytes,
                       std::size_t alignment) noexcept override;
    bool do_is_equal(memory_resource const& other) const noexcept override;

    size_class* find_class(std::size_t bytes, std::size_t alignment) noexcept;

    memory_resource* upstream_;
    config options_;
    mutable std::mutex mutex_;
    std::vector<size_class> classes_;
};

// ---- 带外帧分配器通道 ----

namespace detail {

inline memory_resource*& cached_frame_allocator_slot() noexcept {
    thread_local memory_resource* slot = nullptr;
    return slot;
}

} // namespace detail

inline memory_resource* get_cached_frame_allocator() noexcept {
    return detail::cached_frame_allocator_slot();
}

inline void set_cached_frame_allocator(memory_resource* const resource) noexcept {
    detail::cached_frame_allocator_slot() = resource;
}

// 恢复一个协程句柄，前后保存/恢复线程局部的帧分配器槽位（P4172R1 §8.3）。执行循环
// （执行器的事件循环、strand 的派发循环）必须经由它恢复协程。
inline void safe_resume(coroutine_handle<> const handle) {
    struct restore {
        memory_resource* saved;
        ~restore() { set_cached_frame_allocator(saved); }
    } guard{get_cached_frame_allocator()};
    handle.resume();
}

} // namespace net
