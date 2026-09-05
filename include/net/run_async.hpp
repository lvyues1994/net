#pragma once

#include <exception>
#include <memory>
#include <type_traits>
#include <utility>

#include "co2/contract.hpp"

#include "net/continuation.hpp"
#include "net/coroutine.hpp"
#include "net/detail/completion_frame.hpp"
#include "net/executor_ref.hpp"
#include "net/io_env.hpp"
#include "net/memory_resource.hpp"
#include "net/task.hpp"

// P4003R3 §4.6 / P4172R1 §7.1：从普通代码启动第一条协程链。
//
//   run_async(ex)(server_main());                       // fire and forget
//   run_async(ex, on_value, on_error)(compute());       // 两个结果都被显式路由
//   run_async(ex, token, frame_allocator)(work());      // 可选的 stop_token 与帧分配器
//
// 两段调用的原因是 operator new 在协程体之前执行：第一段（run_async(ex)）把帧分配器
// 放进带外槽位，第二段（(my_task())）调用协程、它的 operator new 读到槽位。C++17 保证
// 后缀表达式先于实参求值；C++14 未作规定（GCC/Clang 实际上也先求后缀表达式），需要
// 确定性时用工厂形态：run_async(ex)([&] { return my_task(); })。
//
// 没有处理器时结果被丢弃、异常在执行器线程上重抛（从执行器的 run() 抛出）。有处理器时
// on_value(result) / on_error(exception_ptr) 恰好调用其一。启动函数持有执行器的一份
// 工作计数直到链完成：run() 不会提前返回。

namespace net {
namespace detail {

struct discard_result {
    template <class... Args> void operator()(Args&&...) const noexcept {}
};

struct rethrow_error {
    void operator()(std::exception_ptr const error) const { std::rethrow_exception(error); }
};

template <class T, class OnValue> void invoke_on_value(OnValue& on_value, task<T>& t) {
    on_value(task_access::take_result(t));
}

template <class OnValue> void invoke_on_value(OnValue& on_value, task<void>& t) {
    task_access::take_result(t);
    on_value();
}

template <class Ex, class T, class OnValue, class OnError> struct run_async_state {
    run_async_state(Ex executor_, stop_token token, memory_resource* const resource,
                    OnValue on_value_, OnError on_error_, task<T> t_)
        : executor(std::move(executor_)),
          env{executor_ref{executor}, std::move(token), resource}, t(std::move(t_)),
          on_value(std::move(on_value_)), on_error(std::move(on_error_)),
          completion{&on_complete, this} {
        start.h = task_access::arm(t, completion.handle(), &env);
    }

    run_async_state(run_async_state const&) = delete;
    run_async_state& operator=(run_async_state const&) = delete;

    static coroutine_handle<> on_complete(void* const user) {
        std::unique_ptr<run_async_state> self{static_cast<run_async_state*>(user)};
        // 处理器可能抛出（默认的 on_error 就是重抛）：状态释放与工作计数由守卫保证。
        struct finish {
            Ex executor;
            ~finish() { executor.on_work_finished(); }
        } guard{self->executor};
        auto const error = task_access::exception(self->t);
        if (error)
            self->on_error(error);
        else
            invoke_on_value(self->on_value, self->t);
        return nullptr;
    }

    Ex executor;
    io_env env;
    task<T> t;
    OnValue on_value;
    OnError on_error;
    completion_frame completion;
    continuation start;
};

template <class F, class = void> struct is_task_factory : std::false_type {};
template <class F>
struct is_task_factory<F, void_t<typename std::decay<decltype(std::declval<F&>()())>::type::promise_type>>
    : std::true_type {};

template <class F, class = void> struct accepts_exception_ptr : std::false_type {};
template <class F>
struct accepts_exception_ptr<
    F, void_t<decltype(std::declval<F&>()(std::declval<std::exception_ptr>()))>>
    : std::true_type {};

} // namespace detail

template <class Ex, class OnValue = detail::discard_result, class OnError = detail::rethrow_error>
struct run_async_launcher {
    run_async_launcher(Ex executor, stop_token token, memory_resource* resource,
                       OnValue on_value, OnError on_error)
        : executor_(std::move(executor)), token_(std::move(token)),
          resource_{resource != nullptr ? resource : executor_.context().get_frame_allocator()},
          on_value_(std::move(on_value)), on_error_(std::move(on_error)),
          saved_{get_cached_frame_allocator()} {
        set_cached_frame_allocator(resource_);
    }

    // C++14 没有保证的复制消除：允许移动，槽位的恢复责任随之转移。
    run_async_launcher(run_async_launcher&& other) noexcept(
        std::is_nothrow_move_constructible<Ex>::value &&
        std::is_nothrow_move_constructible<OnValue>::value &&
        std::is_nothrow_move_constructible<OnError>::value)
        : executor_(std::move(other.executor_)), token_(std::move(other.token_)),
          resource_{other.resource_}, on_value_(std::move(other.on_value_)),
          on_error_(std::move(other.on_error_)), saved_{other.saved_}, active_{other.active_} {
        other.active_ = false;
    }

    run_async_launcher(run_async_launcher const&) = delete;
    run_async_launcher& operator=(run_async_launcher const&) = delete;
    run_async_launcher& operator=(run_async_launcher&&) = delete;

    ~run_async_launcher() {
        if (active_) set_cached_frame_allocator(saved_);
    }

    // 第二段：接收刚创建的惰性 task，排到执行器上启动。
    template <class T> void operator()(task<T> t) && {
        CO2_CONTRACT_CHECK(t);
        using state_type = detail::run_async_state<Ex, T, OnValue, OnError>;
        auto* const state =
            new state_type{std::move(executor_), std::move(token_), resource_,
                           std::move(on_value_), std::move(on_error_), std::move(t)};
        state->executor.on_work_started();
        state->executor.post(state->start);
    }

    // 工厂形态：在帧分配器就位之后才调用协程（C++14 下求值顺序的确定性保证）。
    template <class F, class = typename std::enable_if<detail::is_task_factory<F>::value>::type>
    void operator()(F&& factory) && {
        std::move(*this)(factory());
    }

  private:
    Ex executor_;
    stop_token token_;
    memory_resource* resource_;
    OnValue on_value_;
    OnError on_error_;
    memory_resource* saved_;
    bool active_ = true;
};

template <class Ex, class = typename std::enable_if<is_executor<Ex>::value>::type>
run_async_launcher<Ex> run_async(Ex executor) {
    return run_async_launcher<Ex>{std::move(executor), stop_token{}, nullptr,
                                  detail::discard_result{}, detail::rethrow_error{}};
}

template <class Ex, class = typename std::enable_if<is_executor<Ex>::value>::type>
run_async_launcher<Ex> run_async(Ex executor, stop_token token,
                                 memory_resource* const resource = nullptr) {
    return run_async_launcher<Ex>{std::move(executor), std::move(token), resource,
                                  detail::discard_result{}, detail::rethrow_error{}};
}

template <class Ex, class OnValue, class OnError,
          class = typename std::enable_if<is_executor<Ex>::value &&
                                          detail::accepts_exception_ptr<OnError>::value>::type>
run_async_launcher<Ex, OnValue, OnError> run_async(Ex executor, OnValue on_value,
                                                   OnError on_error) {
    return run_async_launcher<Ex, OnValue, OnError>{std::move(executor), stop_token{}, nullptr,
                                                    std::move(on_value), std::move(on_error)};
}

template <class Ex, class OnValue, class OnError,
          class = typename std::enable_if<is_executor<Ex>::value &&
                                          detail::accepts_exception_ptr<OnError>::value>::type>
run_async_launcher<Ex, OnValue, OnError> run_async(Ex executor, stop_token token,
                                                   memory_resource* const resource,
                                                   OnValue on_value, OnError on_error) {
    return run_async_launcher<Ex, OnValue, OnError>{std::move(executor), std::move(token),
                                                    resource, std::move(on_value),
                                                    std::move(on_error)};
}

} // namespace net
