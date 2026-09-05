#pragma once

#include <type_traits>
#include <utility>

#include "net/continuation.hpp"
#include "net/coroutine.hpp"
#include "net/execution_context.hpp"

// P4003R3 §4.3：Executor 概念与它的类型擦除视图 executor_ref。
//
// Executor 的七条要求（每条都有具体的失效场景，见 P4172R1 §5.2）：
//   - nothrow 拷贝/移动：挂起点上的异常安全；
//   - context()：启动函数由此找到默认帧分配器与服务；
//   - on_work_started / on_work_finished：run() 在 co_await run(worker)(...) 期间不能提前返回；
//   - dispatch(continuation&) → coroutine_handle<>：调用方已在执行器上下文时直接返回
//     c.h 做对称转移，否则排队并返回 noop_coroutine()；
//   - post(continuation&)：总是延迟，绝不在调用线程上运行；
//   - operator==：为 strand 预留。
//
// C++14 没有 concept，is_executor<E> 以 SFINAE 逐条检查这些要求；executor_ref 的构造
// 函数用它约束。

namespace net {
namespace detail {

template <class...> struct void_t_impl { using type = void; };
template <class... T> using void_t = typename void_t_impl<T...>::type;

template <class E, class = void> struct is_executor_impl : std::false_type {};

template <class E>
struct is_executor_impl<
    E, void_t<decltype(std::declval<E const&>().dispatch(std::declval<continuation&>())),
              decltype(std::declval<E const&>().post(std::declval<continuation&>())),
              decltype(std::declval<E const&>().context()),
              decltype(std::declval<E const&>().on_work_started()),
              decltype(std::declval<E const&>().on_work_finished()),
              decltype(std::declval<E const&>() == std::declval<E const&>())>>
    : std::integral_constant<
          bool,
          std::is_nothrow_copy_constructible<E>::value &&
              std::is_nothrow_move_constructible<E>::value &&
              std::is_same<decltype(std::declval<E const&>().dispatch(
                               std::declval<continuation&>())),
                           coroutine_handle<>>::value &&
              std::is_lvalue_reference<decltype(std::declval<E const&>().context())>::value &&
              std::is_base_of<execution_context,
                              typename std::remove_reference<decltype(
                                  std::declval<E const&>().context())>::type>::value &&
              std::is_convertible<decltype(std::declval<E const&>() ==
                                           std::declval<E const&>()),
                                  bool>::value> {};

struct executor_vtable {
    coroutine_handle<> (*dispatch)(void const* executor, continuation& c);
    void (*post)(void const* executor, continuation& c);
    execution_context& (*context)(void const* executor) noexcept;
    void (*on_work_started)(void const* executor) noexcept;
    void (*on_work_finished)(void const* executor) noexcept;
};

template <class E> struct executor_vtable_for {
    static E const& self(void const* const executor) noexcept {
        return *static_cast<E const*>(executor);
    }

    static coroutine_handle<> dispatch(void const* const executor, continuation& c) {
        return self(executor).dispatch(c);
    }

    static void post(void const* const executor, continuation& c) {
        self(executor).post(c);
    }

    static execution_context& context(void const* const executor) noexcept {
        return self(executor).context();
    }

    static void on_work_started(void const* const executor) noexcept {
        self(executor).on_work_started();
    }

    static void on_work_finished(void const* const executor) noexcept {
        self(executor).on_work_finished();
    }

    static executor_vtable const* get() noexcept {
        static executor_vtable const table{&dispatch, &post, &context, &on_work_started,
                                           &on_work_finished};
        return &table;
    }
};

} // namespace detail

template <class E>
struct is_executor : detail::is_executor_impl<typename std::decay<E>::type> {};

// 两个指针宽的非拥有执行器视图。被引用的执行器对象必须比 executor_ref 活得久——启动
// 函数把执行器按值保存在自己的状态里，io_env 里的 executor_ref 指向那份副本。
struct executor_ref {
    executor_ref() noexcept = default;

    template <class E,
              class = typename std::enable_if<
                  is_executor<E>::value &&
                  not std::is_same<typename std::decay<E>::type, executor_ref>::value>::type>
    executor_ref(E const& executor) noexcept
        : ex_{&executor}, vt_{detail::executor_vtable_for<E>::get()} {}

    coroutine_handle<> dispatch(continuation& c) const { return vt_->dispatch(ex_, c); }

    void post(continuation& c) const { vt_->post(ex_, c); }

    execution_context& context() const noexcept { return vt_->context(ex_); }

    void on_work_started() const noexcept { vt_->on_work_started(ex_); }

    void on_work_finished() const noexcept { vt_->on_work_finished(ex_); }

    explicit operator bool() const noexcept { return ex_ != nullptr; }

    // 被引用的执行器对象地址。
    void const* target() const noexcept { return ex_; }

    friend bool operator==(executor_ref const& left, executor_ref const& right) noexcept {
        return left.ex_ == right.ex_;
    }

    friend bool operator!=(executor_ref const& left, executor_ref const& right) noexcept {
        return not(left == right);
    }

  private:
    void const* ex_ = nullptr;
    detail::executor_vtable const* vt_ = nullptr;
};

} // namespace net
