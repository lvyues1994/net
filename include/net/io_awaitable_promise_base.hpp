#pragma once

#include <cstddef>
#include <cstring>
#include <type_traits>
#include <utility>

#include "co2/detail/awaitable.hpp"

#include "net/coroutine.hpp"
#include "net/io_env.hpp"
#include "net/memory_resource.hpp"
#include "net/this_coro.hpp"

// IoAwaitable 协议在 co2 上的落地（P4003R3 §4.2，P4172R1 附录 C）。
//
// 协议只有一个函数：awaiter 的双参数 await_suspend(coroutine_handle<>, io_env const*)。
// 调用方 promise 的 await_transform 把环境作为第二个参数注入——没有模板参数，没有类型
// 泄漏，task<T> 只需一个模板参数。C++14/co2 的 await_suspend 只有单参数形态，因此
// await_transform 把每个 IoAwaitable 包进 env_awaiter：它是 co2 眼中的 awaiter，转发
// await_ready / await_resume，并把带类型的父句柄擦成 coroutine_handle<> 后连同 io_env
// 指针一起交给内层的双参数 await_suspend。不满足协议的 awaitable 在这里编译失败——
// 这正是协议想要的编译期边界检查。
//
// 每次恢复（await_resume）都把 io_env 里的帧分配器写回线程局部槽位（§8.3 的执行窗口
// 协议）：协程体随后调用的子协程在 operator new 里读到正确的分配器。
//
// io_awaitable_promise_base<Derived> 是框架作者的 mixin：帧分配（operator new /
// delete，memory_resource* 存在帧尾）、续体、环境存储、await_transform（截获 this_coro
// 标签，其余交给 Derived::transform_awaitable）。派生 promise 要加自己的变换时覆盖
// transform_awaitable，不要遮蔽 await_transform。

namespace net {
namespace detail {

template <class A, class = void> struct is_io_awaitable_impl : std::false_type {};

template <class A>
struct is_io_awaitable_impl<
    A, void_t<decltype(std::declval<A&>().await_suspend(std::declval<coroutine_handle<>>(),
                                                        std::declval<io_env const*>()))>>
    : std::true_type {};

// 不挂起、直接交出一个值的 awaiter（this_coro 标签用）。
template <class T> struct immediate_value {
    T value;

    bool await_ready() const noexcept { return true; }
    void await_suspend(coroutine_handle<>) const noexcept {}
    T await_resume() noexcept(std::is_nothrow_move_constructible<T>::value) {
        return std::move(value);
    }
};

template <class Inner> struct env_awaiter {
    static_assert(is_io_awaitable_impl<Inner>::value,
                  "net: the co_await operand does not satisfy IoAwaitable: it must "
                  "provide await_suspend(coroutine_handle<>, io_env const*)");

    template <class A>
    env_awaiter(A&& awaiter, io_env const* const env_)
        : inner(std::forward<A>(awaiter)), env{env_} {}

    env_awaiter(env_awaiter&&) = default;
    env_awaiter(env_awaiter const&) = delete;
    env_awaiter& operator=(env_awaiter const&) = delete;
    env_awaiter& operator=(env_awaiter&&) = delete;

    bool await_ready() { return inner.await_ready(); }

    template <class Promise>
    auto await_suspend(coroutine_handle<Promise> const awaiting)
        -> decltype(std::declval<Inner&>().await_suspend(coroutine_handle<>{},
                                                          std::declval<io_env const*>())) {
        return inner.await_suspend(static_cast<coroutine_handle<>>(awaiting), env);
    }

    auto await_resume() -> decltype(std::declval<Inner&>().await_resume()) {
        set_cached_frame_allocator(env->frame_allocator);
        return inner.await_resume();
    }

    Inner inner;
    io_env const* env;
};

} // namespace detail

// 一个类型是否满足 IoAwaitable：有 await_suspend(coroutine_handle<>, io_env const*)。
template <class A>
struct is_io_awaitable
    : detail::is_io_awaitable_impl<co2::detail::AwaiterOf<typename std::decay<A>::type>> {};

// awaitable 的 await_resume() 结果类型（不经过任何 await_transform）。
template <class A>
using awaitable_result_t =
    decltype(std::declval<co2::detail::AwaiterOf<A>&>().await_resume());

template <class Derived> struct io_awaitable_promise_base {
    // ---- 帧分配：带外通道 + 帧尾存 memory_resource* ----

    static void* operator new(std::size_t const size) {
        auto* resource = get_cached_frame_allocator();
        if (resource == nullptr) resource = new_delete_resource();
        auto const total = size + sizeof(memory_resource*);
        void* const raw = resource->allocate(total, alignof(std::max_align_t));
        std::memcpy(static_cast<unsigned char*>(raw) + size, &resource, sizeof(resource));
        return raw;
    }

    static void operator delete(void* const pointer, std::size_t const size) noexcept {
        memory_resource* resource = nullptr;
        std::memcpy(&resource, static_cast<unsigned char*>(pointer) + size, sizeof(resource));
        resource->deallocate(pointer, size + sizeof(memory_resource*),
                             alignof(std::max_align_t));
    }

    // ---- 续体与环境（IoRunnable） ----

    void set_continuation(coroutine_handle<> const cont) noexcept { cont_ = cont; }

    // 取走续体（之后为 noop）：final_suspend 对称转移到它。
    coroutine_handle<> continuation() noexcept {
        auto const cont = cont_;
        cont_ = noop_coroutine();
        return cont;
    }

    void set_environment(io_env const* const env) noexcept { env_ = env; }

    io_env const* environment() const noexcept { return env_; }

    // 派生类的扩展点：其它 awaitable 先经过它，再包成 env_awaiter。
    template <class A> A&& transform_awaitable(A&& awaitable) noexcept {
        return std::forward<A>(awaitable);
    }

    // ---- await_transform ----

    detail::immediate_value<io_env const*> await_transform(this_coro::environment_tag) noexcept {
        return {env_};
    }

    detail::immediate_value<executor_ref> await_transform(this_coro::executor_tag) noexcept {
        return {env_ != nullptr ? env_->executor : executor_ref{}};
    }

    detail::immediate_value<stop_token> await_transform(this_coro::stop_token_tag) noexcept {
        return {env_ != nullptr ? env_->stop_token : stop_token{}};
    }

    detail::immediate_value<memory_resource*>
    await_transform(this_coro::frame_allocator_tag) noexcept {
        return {env_ != nullptr ? env_->frame_allocator : nullptr};
    }

    // D 只是 Derived 的别名：把对派生类成员的访问变成依赖名，推迟到调用点再解析（基类
    // 实例化时派生类还不完整）。
    template <class A, class D = Derived>
    auto await_transform(A&& awaitable)
        -> detail::env_awaiter<co2::detail::AwaiterOf<decltype(
            std::declval<D&>().transform_awaitable(std::forward<A>(awaitable)))>> {
        using transformed =
            decltype(std::declval<D&>().transform_awaitable(std::forward<A>(awaitable)));
        return detail::env_awaiter<co2::detail::AwaiterOf<transformed>>{
            co2::detail::getAwaiter(
                static_cast<D*>(this)->transform_awaitable(std::forward<A>(awaitable))),
            env_};
    }

  private:
    io_env const* env_ = nullptr;
    coroutine_handle<> cont_ = noop_coroutine();
};

} // namespace net
