#pragma once

#include <exception>
#include <type_traits>
#include <utility>

#include "co2/contract.hpp"

#include "net/coroutine.hpp"
#include "net/detail/storage.hpp"
#include "net/io_awaitable_promise_base.hpp"
#include "net/io_env.hpp"

// task<T>：惰性、单消费者、只可移动的协程返回类型，同时满足 IoAwaitable（可被父协程
// co_await）与 IoRunnable（可被 run_async / run 启动）。一个模板参数——执行器、stop_token
// 与帧分配器全部经 io_env 传播，返回 task<int> 的协程可以定义在 .cpp 里、被任何翻译单元
// 调用（P4172R1 §6.2 / §9）。
//
// 生命周期（标准销毁契约）：task 析构销毁帧；已启动、未完成的 task 挂起在某个操作上，
// 销毁它是契约违规。取消是协作式的：stop_token 请求停止 → 操作提前完成 → 协程走到
// final_suspend → 拥有者销毁。
//
// 等待形态：
//   CO2_AWAIT_SET(v, child());        // 右值：task 本身是 awaiter，帧随 awaiter 槽销毁
//   kept = child(); CO2_AWAIT_SET(v, kept);   // 左值：借用，kept 继续拥有帧
//
// 与 co2::Task 的差别：await_suspend 的第二个参数是 io_env const*（协议要求），子 task
// 从它继承整个环境而不只是 stop_token；帧由 promise 的 operator new 经带外通道从
// io_env::frame_allocator 分配。

namespace net {

template <class T = void> struct task;

namespace detail {

struct task_access;

template <class T> struct task_promise : io_awaitable_promise_base<task_promise<T>> {
    task<T> get_return_object() noexcept;

    // 惰性启动：初始挂起。恢复时把本链的帧分配器写进线程局部槽位——协程体里调用的
    // 子协程在 operator new 里读到它（P4172R1 §8.3）。
    struct initial_awaiter {
        task_promise* promise;

        bool await_ready() const noexcept { return false; }
        void await_suspend(coroutine_handle<>) const noexcept {}
        void await_resume() const noexcept {
            auto const* const env = promise->environment();
            if (env != nullptr) set_cached_frame_allocator(env->frame_allocator);
        }
    };

    initial_awaiter initial_suspend() noexcept { return initial_awaiter{this}; }

    // 完成：对称转移到续体（没有续体时交回 resumer）。
    struct final_awaiter {
        bool await_ready() const noexcept { return false; }

        coroutine_handle<>
        await_suspend(coroutine_handle<task_promise> const self) const noexcept {
            return self.promise().continuation();
        }

        void await_resume() const noexcept {}
    };

    final_awaiter final_suspend() noexcept { return {}; }

    template <class Value, class U = T,
              class = typename std::enable_if<not std::is_void<U>::value>::type>
    void return_value(Value&& value) {
        storage_.emplace(std::forward<Value>(value));
    }

    template <class U = T, class = typename std::enable_if<std::is_void<U>::value>::type>
    void return_void() noexcept {
        storage_.emplace();
    }

    void unhandled_exception() noexcept { error_ = std::current_exception(); }

    std::exception_ptr exception() const noexcept { return error_; }

    // 只可取走一次：重抛异常或移出值。
    T result() {
        if (error_) std::rethrow_exception(error_);
        CO2_CONTRACT_CHECK(storage_.hasValue());
        return storage_.take();
    }

    bool started() const noexcept { return started_; }

    // 启动：设置续体与环境、标记已启动。只能调用一次。
    void start(coroutine_handle<> const cont, io_env const* const env) noexcept {
        CO2_CONTRACT_CHECK(not started_);
        this->set_continuation(cont);
        this->set_environment(env);
        started_ = true;
    }

  private:
    result_storage<T> storage_;
    std::exception_ptr error_;
    bool started_ = false;
};

// 借用一个 task 的 awaiter（左值等待）：不拥有帧。
template <class T> struct task_awaiter {
    coroutine_handle<task_promise<T>> handle;

    bool await_ready() const noexcept { return not handle || handle.done(); }

    coroutine_handle<> await_suspend(coroutine_handle<> const awaiting,
                                     io_env const* const env) noexcept {
        handle.promise().start(awaiting, env);
        return handle;
    }

    T await_resume() {
        CO2_CONTRACT_CHECK(handle);
        return handle.promise().result();
    }
};

} // namespace detail

template <class T> struct task {
    using promise_type = detail::task_promise<T>;
    using value_type = T;

    task() noexcept = default;

    task(task&& other) noexcept : handle_{other.handle_} { other.handle_ = nullptr; }

    task& operator=(task&& other) noexcept {
        if (this == &other) return *this;
        reset();
        handle_ = other.handle_;
        other.handle_ = nullptr;
        return *this;
    }

    task(task const&) = delete;
    task& operator=(task const&) = delete;

    ~task() { reset(); }

    explicit operator bool() const noexcept { return static_cast<bool>(handle_); }

    // ---- IoRunnable ----

    coroutine_handle<promise_type> handle() const noexcept { return handle_; }

    // 交出帧的所有权：调用方负责在完成后 destroy()。
    coroutine_handle<promise_type> release() noexcept {
        auto const handle = handle_;
        handle_ = nullptr;
        return handle;
    }

    // ---- IoAwaitable（右值：task 本身是 awaiter，拥有帧） ----

    bool await_ready() const noexcept { return awaiter().await_ready(); }

    coroutine_handle<> await_suspend(coroutine_handle<> const awaiting,
                                     io_env const* const env) noexcept {
        return awaiter().await_suspend(awaiting, env);
    }

    T await_resume() { return awaiter().await_resume(); }

    // 左值：借用句柄。
    detail::task_awaiter<T> operator_co_await() & noexcept { return awaiter(); }

  private:
    friend struct detail::task_promise<T>;
    friend struct detail::task_access;

    explicit task(coroutine_handle<promise_type> const handle) noexcept : handle_{handle} {}

    detail::task_awaiter<T> awaiter() const noexcept { return detail::task_awaiter<T>{handle_}; }

    void reset() noexcept {
        if (not handle_) return;
        // 已启动却未完成的 task 挂起在某个操作上，销毁它是未定义行为。
        CO2_CONTRACT_CHECK(not handle_.promise().started() || handle_.done());
        handle_.destroy();
        handle_ = nullptr;
    }

    coroutine_handle<promise_type> handle_;
};

namespace detail {

template <class T> task<T> task_promise<T>::get_return_object() noexcept {
    return task<T>{coroutine_handle<task_promise>::from_promise(*this)};
}

// 启动函数（run_async、run、组合子）的入口：设置续体与环境、标记已启动，交出可恢复的
// 句柄。惰性 task 停在初始挂起点，本身就是一个可排队的句柄，不需要跳板帧。
struct task_access {
    template <class T>
    static coroutine_handle<> arm(task<T>& t, coroutine_handle<> const cont,
                                  io_env const* const env) noexcept {
        CO2_CONTRACT_CHECK(t.handle_);
        t.handle_.promise().start(cont, env);
        return t.handle_;
    }

    template <class T> static bool is_done(task<T> const& t) noexcept {
        return t.handle_ && t.handle_.done();
    }

    template <class T> static std::exception_ptr exception(task<T> const& t) noexcept {
        return t.handle_ ? t.handle_.promise().exception() : nullptr;
    }

    template <class T> static T take_result(task<T>& t) {
        CO2_CONTRACT_CHECK(t.handle_ && t.handle_.done());
        return t.handle_.promise().result();
    }
};

} // namespace detail
} // namespace net
