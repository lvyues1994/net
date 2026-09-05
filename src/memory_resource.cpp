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

    void do_deallocate(void* const pointer, std::size_t,
                       std::size_t const alignment) noexcept override {
        if (alignment <= alignof(std::max_align_t)) {
            ::operator delete(pointer);
            return;
        }
        auto* const bytes = static_cast<unsigned char*>(pointer);
        ::operator delete(*reinterpret_cast<void**>(bytes - sizeof(void*)));
    }

    bool do_is_equal(memory_resource const& other) const noexcept override {
        return &other == this;
    }
};

} // namespace

memory_resource* new_delete_resource() noexcept {
    static new_delete_memory_resource resource;
    return &resource;
}

// ---- recycling_memory_resource ----

recycling_memory_resource::recycling_memory_resource(memory_resource* const upstream)
    : recycling_memory_resource{upstream, config{}} {}

recycling_memory_resource::recycling_memory_resource(memory_resource* const upstream,
                                                     config const& options)
    : upstream_{upstream}, options_{options} {
    classes_.reserve(options_.max_size_classes);
}

recycling_memory_resource::~recycling_memory_resource() { release(); }

void recycling_memory_resource::release() noexcept {
    std::lock_guard<std::mutex> lock{mutex_};
    for (auto& cls : classes_) {
        while (cls.head != nullptr) {
            auto* const block = cls.head;
            cls.head = block->next;
            upstream_->deallocate(block, cls.size, cls.alignment);
        }
        cls.count = 0U;
    }
}

std::size_t recycling_memory_resource::cached_blocks() const noexcept {
    std::lock_guard<std::mutex> lock{mutex_};
    auto total = std::size_t{};
    for (auto const& cls : classes_)
        total += cls.count;
    return total;
}

std::size_t recycling_memory_resource::cached_bytes() const noexcept {
    std::lock_guard<std::mutex> lock{mutex_};
    auto total = std::size_t{};
    for (auto const& cls : classes_)
        total += cls.count * cls.size;
    return total;
}

recycling_memory_resource::size_class*
recycling_memory_resource::find_class(std::size_t const bytes,
                                      std::size_t const alignment) noexcept {
    for (auto& cls : classes_)
        if (cls.size == bytes && cls.alignment == alignment) return &cls;
    return nullptr;
}

void* recycling_memory_resource::do_allocate(std::size_t const bytes,
                                             std::size_t const alignment) {
    {
        std::lock_guard<std::mutex> lock{mutex_};
        auto* const cls = find_class(bytes, alignment);
        if (cls != nullptr && cls->head != nullptr) {
            auto* const block = cls->head;
            cls->head = block->next;
            --cls->count;
            return block;
        }
    }
    return upstream_->allocate(bytes, alignment);
}

void recycling_memory_resource::do_deallocate(void* const pointer, std::size_t const bytes,
                                              std::size_t const alignment) noexcept {
    if (bytes >= sizeof(free_block) && bytes <= options_.max_block_size) {
        std::lock_guard<std::mutex> lock{mutex_};
        auto* cls = find_class(bytes, alignment);
        if (cls == nullptr && classes_.size() < options_.max_size_classes) {
            classes_.push_back(size_class{bytes, alignment, nullptr, 0U});
            cls = &classes_.back();
        }
        if (cls != nullptr && cls->count < options_.max_blocks_per_class) {
            auto* const block = static_cast<free_block*>(pointer);
            block->next = cls->head;
            cls->head = block;
            ++cls->count;
            return;
        }
    }
    upstream_->deallocate(pointer, bytes, alignment);
}

bool recycling_memory_resource::do_is_equal(memory_resource const& other) const noexcept {
    return &other == this;
}

} // namespace net
