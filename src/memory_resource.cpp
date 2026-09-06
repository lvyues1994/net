#include "net/memory_resource.hpp"

#include <new>

namespace net {

namespace {

struct new_delete_memory_resource final : memory_resource {
    void* do_allocate(std::size_t const bytes, std::size_t const alignment) override {
        if (alignment <= alignof(std::max_align_t)) return ::operator new(bytes);
        // C++14 没有对齐 new：手工过对齐，把原始指针存在返回块前面。
        auto const total = bytes + alignment + sizeof(void*);
        auto* const raw = static_cast<unsigned char*>(::operator new(total));
        auto const base = reinterpret_cast<std::uintptr_t>(raw) + sizeof(void*);
        auto const aligned = (base + alignment - 1U) & ~(alignment - 1U);
        auto* const result = reinterpret_cast<unsigned char*>(aligned);
        *reinterpret_cast<void**>(result - sizeof(void*)) = raw;
        return result;
    }

    void do_deallocate(void* const pointer, std::size_t, std::size_t const alignment) noexcept override {
        if (alignment <= alignof(std::max_align_t)) {
            ::operator delete(pointer);
            return;
        }
        auto* const bytes = static_cast<unsigned char*>(pointer);
        ::operator delete(*reinterpret_cast<void**>(bytes - sizeof(void*)));
    }

    bool do_is_equal(memory_resource const& other) const noexcept override { return &other == this; }
};

} // namespace

memory_resource* new_delete_resource() noexcept {
    static new_delete_memory_resource resource;
    return &resource;
}

// ---- recycling_memory_resource ----

recycling_memory_resource::recycling_memory_resource(memory_resource* const upstream)
    : recycling_memory_resource{upstream, config{}} {}

recycling_memory_resource::recycling_memory_resource(memory_resource* const upstream, config const& options)
    : upstream_{upstream}, options_{options}, classes_{new size_class[class_count]} {}

recycling_memory_resource::~recycling_memory_resource() { release(); }

void recycling_memory_resource::lock(size_class& cls) noexcept {
    while (cls.locked.exchange(true, std::memory_order_acquire)) {
        while (cls.locked.load(std::memory_order_relaxed)) {
        }
    }
}

void recycling_memory_resource::unlock(size_class& cls) noexcept { cls.locked.store(false, std::memory_order_release); }

void* recycling_memory_resource::pop(size_class& cls) noexcept {
    lock(cls);
    auto* const block = cls.head;
    if (block != nullptr) {
        cls.head = block->next;
        --cls.count;
    }
    unlock(cls);
    return block;
}

bool recycling_memory_resource::push(size_class& cls, void* const pointer) noexcept {
    auto* const block = static_cast<free_block*>(pointer);
    lock(cls);
    if (cls.count >= options_.max_blocks_per_class) {
        unlock(cls);
        return false;
    }
    block->next = cls.head;
    cls.head = block;
    ++cls.count;
    unlock(cls);
    return true;
}

void recycling_memory_resource::release() noexcept {
    for (auto index = std::size_t{}; index != class_count; ++index) {
        auto& cls = classes_[index];
        while (auto* const block = pop(cls))
            upstream_->deallocate(block, class_size(index), alignof(std::max_align_t));
    }
}

std::size_t recycling_memory_resource::cached_blocks() const noexcept {
    auto total = std::size_t{};
    for (auto index = std::size_t{}; index != class_count; ++index) {
        lock(classes_[index]);
        total += classes_[index].count;
        unlock(classes_[index]);
    }
    return total;
}

std::size_t recycling_memory_resource::cached_bytes() const noexcept {
    auto total = std::size_t{};
    for (auto index = std::size_t{}; index != class_count; ++index) {
        lock(classes_[index]);
        total += classes_[index].count * class_size(index);
        unlock(classes_[index]);
    }
    return total;
}

void* recycling_memory_resource::do_allocate(std::size_t const bytes, std::size_t const alignment) {
    if (bytes == 0U || bytes > max_cached_size || alignment > alignof(std::max_align_t))
        return upstream_->allocate(bytes, alignment);
    auto const index = class_index(bytes);
    if (auto* const block = pop(classes_[index])) return block;
    return upstream_->allocate(class_size(index), alignof(std::max_align_t));
}

void recycling_memory_resource::do_deallocate(void* const pointer, std::size_t const bytes,
                                              std::size_t const alignment) noexcept {
    if (bytes == 0U || bytes > max_cached_size || alignment > alignof(std::max_align_t)) {
        upstream_->deallocate(pointer, bytes, alignment);
        return;
    }
    auto const index = class_index(bytes);
    if (push(classes_[index], pointer)) return;
    upstream_->deallocate(pointer, class_size(index), alignof(std::max_align_t));
}

bool recycling_memory_resource::do_is_equal(memory_resource const& other) const noexcept { return &other == this; }

} // namespace net
