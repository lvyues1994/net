#pragma once

#include <cstddef>
#include <memory>

#include "net/continuation.hpp"
#include "net/coroutine.hpp"
#include "net/execution_context.hpp"

// thread_pool：固定数量工作线程的执行上下文（P4100R1 §8.2 "Coroutine Task" 一篇里的
// thread_pool）。线程在构造时启动；join() 等待未完成的工作全部结束后线程退出；stop()
// 让线程尽快退出。dispatch 在工作线程上返回 c.h（对称转移），否则排队。
//
// 工作计数：池自身持有一份"初始工作"直到 join()/stop()/析构；run_async / run 持有链
// 存续期间的工作。计数是原子的；归零且队列为空时工作线程退出。

namespace net {

struct thread_pool final : execution_context {
    struct executor_type {
        executor_type() noexcept = default;

        thread_pool& context() const noexcept { return *pool_; }

        void on_work_started() const noexcept;
        void on_work_finished() const noexcept;

        coroutine_handle<> dispatch(continuation& c) const;
        void post(continuation& c) const;

        bool running_in_this_thread() const noexcept;

        friend bool operator==(executor_type const& left, executor_type const& right) noexcept {
            return left.pool_ == right.pool_;
        }

        friend bool operator!=(executor_type const& left, executor_type const& right) noexcept {
            return left.pool_ != right.pool_;
        }

      private:
        friend struct thread_pool;
        explicit executor_type(thread_pool* const pool) noexcept : pool_{pool} {}

        thread_pool* pool_ = nullptr;
    };

    static std::size_t default_thread_count() noexcept;

    explicit thread_pool(std::size_t thread_count = default_thread_count());
    ~thread_pool();

    executor_type get_executor() noexcept { return executor_type{this}; }

    std::size_t thread_count() const noexcept;

    // 让工作线程尽快退出（排队中的工作被放弃）。
    void stop();
    // 释放初始工作，等待全部工作完成后线程退出。可重复调用。
    void join();

  private:
    struct impl;
    std::unique_ptr<impl> impl_;
};

} // namespace net
