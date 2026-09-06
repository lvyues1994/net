#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <type_traits>
#include <typeindex>
#include <typeinfo>
#include <utility>

#include "net/memory_resource.hpp"

// P4003R3 §4.4：所有执行上下文的基类——服务注册表 + 默认帧分配器。设计取自
// Boost.Asio / Networking TS 的 execution_context：平台反应器作为服务注册在上下文里，
// I/O 对象持有对上下文（而不是执行器）的引用；服务按注册顺序的逆序 shutdown()，避免
// 服务间依赖的 use-after-free。
//
// 服务类型 T 满足：可由 `T(execution_context&)`（use_service）或 `T(execution_context&,
// Args...)`（make_service）构造，派生自 execution_context::service；可选的 `T::key_type`
// 让多个实现共享一个键（例如不同平台的反应器共用 reactor_service 键）。

namespace net {

struct execution_context {
    struct service {
        service(service const&) = delete;
        service& operator=(service const&) = delete;
        virtual ~service() = default;

      protected:
        service() = default;

        // 在拥有它的上下文 shutdown 时被调用（逆注册顺序）。此后不再有新的操作进入。
        virtual void shutdown() = 0;

      private:
        friend struct execution_context;
        service* next_ = nullptr;
        std::type_index key_{typeid(void)};
    };

    execution_context();
    execution_context(execution_context const&) = delete;
    execution_context& operator=(execution_context const&) = delete;

    // 派生类的析构函数应先调用 shutdown() 再 destroy()（与 Networking TS 一致）。
    ~execution_context();

    // 取服务，不存在时以 T(*this) 构造并注册。
    template <class T> T& use_service() {
        auto* const existing = find_service(key_of<T>());
        if (existing != nullptr) return static_cast<T&>(*existing);
        std::unique_ptr<service> created{new T(*this)};
        return static_cast<T&>(add_or_replace(key_of<T>(), std::move(created)));
    }

    // 显式构造服务。同键服务已存在时抛 std::logic_error。
    template <class T, class... Args> T& make_service(Args&&... args) {
        std::unique_ptr<service> created{new T(*this, std::forward<Args>(args)...)};
        return static_cast<T&>(add_unique(key_of<T>(), std::move(created)));
    }

    template <class T> bool has_service() const noexcept {
        return find_service(key_of<T>()) != nullptr;
    }

    // 默认帧分配器：通过本上下文启动的每条协程链都用它。默认由构建选项
    // NET_DEFAULT_FRAME_ALLOCATOR 决定（new_delete_resource，或上下文自带的回收式分配器）；
    // 传空指针恢复默认。
    memory_resource* get_frame_allocator() const noexcept {
        return frame_allocator_.load(std::memory_order_acquire);
    }

    void set_frame_allocator(memory_resource* const resource) noexcept {
        frame_allocator_.store(resource != nullptr ? resource : built_in_frame_allocator(), std::memory_order_release);
    }

    // 上下文自带的回收式分配器（无论默认是哪个都可用）。
    recycling_memory_resource& recycling_frame_allocator() noexcept { return *default_frame_allocator_; }

  protected:
    // 逆注册顺序调用每个服务的 shutdown()。可重复调用。
    void shutdown() noexcept;
    // 逆注册顺序销毁全部服务。之后上下文不再可用。
    void destroy() noexcept;

  private:
    // 在 execution_context.cpp 里定义：取决于构建选项 NET_DEFAULT_FRAME_ALLOCATOR，不能内联在头里
    //（不同 TU 看到不同宏会违反 ODR）。
    memory_resource* built_in_frame_allocator() noexcept;

    template <class T, class = void> struct key_type_of {
        using type = T;
    };
    template <class T> struct key_type_of<T, decltype(void(sizeof(typename T::key_type)))> {
        using type = typename T::key_type;
    };

    template <class T> static std::type_index key_of() noexcept {
        return std::type_index{typeid(typename key_type_of<T>::type)};
    }

    service* find_service(std::type_index key) const noexcept;
    service& add_or_replace(std::type_index key, std::unique_ptr<service> created);
    service& add_unique(std::type_index key, std::unique_ptr<service> created);

    mutable std::mutex mutex_;
    service* first_service_ = nullptr; // 最新注册的在前
    bool shut_down_ = false;
    std::unique_ptr<recycling_memory_resource> default_frame_allocator_;
    std::atomic<memory_resource*> frame_allocator_;
};

} // namespace net
