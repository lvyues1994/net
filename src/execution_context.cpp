#include "net/execution_context.hpp"

#include <stdexcept>

namespace net {

memory_resource* execution_context::built_in_frame_allocator() noexcept {
#if defined(NET_DEFAULT_FRAME_ALLOCATOR_RECYCLING)
    return default_frame_allocator_.get();
#else
    return new_delete_resource();
#endif
}

execution_context::execution_context()
    : default_frame_allocator_{new recycling_memory_resource{}},
#if defined(NET_DEFAULT_FRAME_ALLOCATOR_RECYCLING)
      frame_allocator_{default_frame_allocator_.get()} {
}
#else
      frame_allocator_{new_delete_resource()} {
}
#endif

execution_context::~execution_context() {
    shutdown();
    destroy();
}

void execution_context::shutdown() noexcept {
    service* head = nullptr;
    {
        std::lock_guard<std::mutex> lock{mutex_};
        if (shut_down_) return;
        shut_down_ = true;
        head = first_service_;
    }
    // 列表最新在前，正向遍历即逆注册顺序。shutdown 期间不再有服务加入。
    for (auto* current = head; current != nullptr; current = current->next_)
        current->shutdown();
}

void execution_context::destroy() noexcept {
    service* head = nullptr;
    {
        std::lock_guard<std::mutex> lock{mutex_};
        head = first_service_;
        first_service_ = nullptr;
    }
    while (head != nullptr) {
        auto* const next = head->next_;
        delete head;
        head = next;
    }
}

execution_context::service*
execution_context::find_service(std::type_index const key) const noexcept {
    std::lock_guard<std::mutex> lock{mutex_};
    for (auto* current = first_service_; current != nullptr; current = current->next_)
        if (current->key_ == key) return current;
    return nullptr;
}

execution_context::service&
execution_context::add_or_replace(std::type_index const key,
                                  std::unique_ptr<service> created) {
    // 服务在锁外构造（它的构造函数可能 use_service 其它服务）；再加锁复查，若期间已有
    // 同键服务注册则丢弃刚构造的这份。
    std::lock_guard<std::mutex> lock{mutex_};
    if (shut_down_) throw std::logic_error{"net: execution_context has been shut down"};
    for (auto* current = first_service_; current != nullptr; current = current->next_)
        if (current->key_ == key) return *current;
    created->key_ = key;
    created->next_ = first_service_;
    first_service_ = created.release();
    return *first_service_;
}

execution_context::service& execution_context::add_unique(std::type_index const key,
                                                          std::unique_ptr<service> created) {
    std::lock_guard<std::mutex> lock{mutex_};
    if (shut_down_) throw std::logic_error{"net: execution_context has been shut down"};
    for (auto* current = first_service_; current != nullptr; current = current->next_)
        if (current->key_ == key)
            throw std::logic_error{"net: service already exists in this execution_context"};
    created->key_ = key;
    created->next_ = first_service_;
    first_service_ = created.release();
    return *first_service_;
}

} // namespace net
