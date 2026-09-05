#pragma once

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

// P4003R3 §4.6 / P4172R1 §7.2：在协程链内为一个子任务切换执行器、stop_token 或帧分配器。
//
//   CO2_AWAIT_SET(v, net::run(worker_ex)(compute()));
//   CO2_AWAIT(net::run(source.get_token())(sensitive_op()));
//   CO2_AWAIT(net::run(pool_resource)(alloc_heavy_op()));
//   CO2_AWAIT_SET(v, net::run(worker_ex, source.get_token(), pool_resource)(compute()));
//
// run 不能分离：父协程在 co_await 处挂起，子任务完成后才恢复——词法边界由语言保证。
// 子任务在新执行器上启动（dispatch：已在该上下文则对称转移，否则排队）；完成时经由
// 边界帧 dispatch 回父协程自己的执行器，恢复到正确的执行上下文。子任务运行期间持有新
// 执行器的一份工作计数，它的 run() 不会提前返回。
//
// 两段调用与 run_async 相同：第一段把子链的帧分配器放进带外槽位。

namespace net {
namespace detail {

template <class Ex, class T> struct run_awaitable {
    run_awaitable(Ex executor_, bool const has_executor_, stop_token token_,
                  bool const has_token_, memory_resource* const resource_, task<T> t_)
        : executor(std::move(executor_)), token(std::move(token_)), resource{resource_},
          t(std::move(t_)), has_executor{has_executor_}, has_token{has_token_} {}

    // 只允许在启动前移动（awaiter 被移进等待者的帧）；边界帧在 await_suspend 里才指向 this。
    run_awaitable(run_awaitable&& other) noexcept(std::is_nothrow_move_constructible<Ex>::value)
        : executor(std::move(other.executor)), token(std::move(other.token)),
          resource{other.resource}, t(std::move(other.t)), has_executor{other.has_executor},
          has_token{other.has_token} {}

    run_awaitable(run_awaitable const&) = delete;
    run_awaitable& operator=(run_awaitable const&) = delete;
    run_awaitable& operator=(run_awaitable&&) = delete;

    bool await_ready() const noexcept { return not t || task_access::is_done(t); }

    coroutine_handle<> await_suspend(coroutine_handle<> const awaiting, io_env const* const env) {
        parent_env = env;
        parent.h = awaiting;
        child_env.executor = has_executor ? executor_ref{executor} : env->executor;
        child_env.stop_token = has_token ? token : env->stop_token;
        child_env.frame_allocator = resource != nullptr ? resource : env->frame_allocator;
        boundary.set(&on_child_done, this);
        child.h = task_access::arm(t, boundary.handle(), &child_env);
        child_env.executor.on_work_started();
        return child_env.executor.dispatch(child);
    }

    T await_resume() { return task_access::take_result(t); }

  private:
    static coroutine_handle<> on_child_done(void* const user) {
        auto* const self = static_cast<run_awaitable*>(user);
        self->child_env.executor.on_work_finished();
        return self->parent_env->executor.dispatch(self->parent);
    }

    Ex executor;
    stop_token token;
    memory_resource* resource;
    task<T> t;
    io_env child_env;
    io_env const* parent_env = nullptr;
    continuation parent;
    continuation child;
    completion_frame boundary;
    bool has_executor;
    bool has_token;
};

// 没有新执行器时占位：run(token) / run(resource) 沿用父协程的执行器。它满足 Executor
// 的语法要求以便实例化，但从不被真正调用。
struct inherit_executor {
    coroutine_handle<> dispatch(continuation&) const noexcept { return noop_coroutine(); }
    void post(continuation&) const noexcept {}
    execution_context& context() const noexcept {
        CO2_CONTRACT_FAIL("net: inherit_executor has no execution_context");
    }
    void on_work_started() const noexcept {}
    void on_work_finished() const noexcept {}
    friend bool operator==(inherit_executor, inherit_executor) noexcept { return true; }
};

} // namespace detail

template <class Ex = detail::inherit_executor> struct run_launcher {
    run_launcher(Ex executor, bool const has_executor, stop_token token, bool const has_token,
                 memory_resource* const resource)
        : executor_(std::move(executor)), token_(std::move(token)), resource_{resource},
          has_executor_{has_executor}, has_token_{has_token},
          saved_{get_cached_frame_allocator()} {
        // 子链的帧分配器：显式给出的 > 新执行器上下文的默认 > 父链当前的。
        auto* effective = resource_;
        if (effective == nullptr && has_executor_) effective = context_allocator(executor_);
        if (effective == nullptr) effective = saved_;
        resource_ = effective;
        set_cached_frame_allocator(effective);
    }

    run_launcher(run_launcher&& other) noexcept(std::is_nothrow_move_constructible<Ex>::value)
        : executor_(std::move(other.executor_)), token_(std::move(other.token_)),
          resource_{other.resource_}, has_executor_{other.has_executor_},
          has_token_{other.has_token_}, saved_{other.saved_}, active_{other.active_} {
        other.active_ = false;
    }

    run_launcher(run_launcher const&) = delete;
    run_launcher& operator=(run_launcher const&) = delete;
    run_launcher& operator=(run_launcher&&) = delete;

    ~run_launcher() {
        if (active_) set_cached_frame_allocator(saved_);
    }

    template <class T> detail::run_awaitable<Ex, T> operator()(task<T> t) && {
        CO2_CONTRACT_CHECK(t);
        return detail::run_awaitable<Ex, T>{std::move(executor_), has_executor_,
                                            std::move(token_), has_token_, resource_,
                                            std::move(t)};
    }

  private:
    template <class E> static memory_resource* context_allocator(E const& executor) noexcept {
        return executor.context().get_frame_allocator();
    }
    static memory_resource* context_allocator(detail::inherit_executor const&) noexcept {
        return nullptr;
    }

    Ex executor_;
    stop_token token_;
    memory_resource* resource_;
    bool has_executor_;
    bool has_token_;
    memory_resource* saved_;
    bool active_ = true;
};

template <class Ex, class = typename std::enable_if<is_executor<Ex>::value>::type>
run_launcher<Ex> run(Ex executor) {
    return run_launcher<Ex>{std::move(executor), true, stop_token{}, false, nullptr};
}

template <class Ex, class = typename std::enable_if<is_executor<Ex>::value>::type>
run_launcher<Ex> run(Ex executor, stop_token token, memory_resource* const resource = nullptr) {
    return run_launcher<Ex>{std::move(executor), true, std::move(token), true, resource};
}

template <class Ex, class = typename std::enable_if<is_executor<Ex>::value>::type>
run_launcher<Ex> run(Ex executor, memory_resource* const resource) {
    return run_launcher<Ex>{std::move(executor), true, stop_token{}, false, resource};
}

inline run_launcher<> run(stop_token token, memory_resource* const resource = nullptr) {
    return run_launcher<>{detail::inherit_executor{}, false, std::move(token), true, resource};
}

inline run_launcher<> run(memory_resource* const resource) {
    return run_launcher<>{detail::inherit_executor{}, false, stop_token{}, false, resource};
}

} // namespace net
