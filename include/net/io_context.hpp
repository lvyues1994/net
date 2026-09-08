#pragma once

#include <chrono>
#include <cstddef>
#include <memory>
#include <type_traits>

#include "net/backend.hpp"
#include "net/buffers.hpp"
#include "net/continuation.hpp"
#include "net/coroutine.hpp"
#include "net/execution_context.hpp"

// io_context：平台事件循环 + 执行器。形态取自 Networking TS 的 io_context：run() 在调用
// 线程上运行事件循环直到没有未完成的工作或 stop()；多个线程可以同时 run() 同一个上下文；
// executor_type 满足 Executor 概念。
//
// 后端：构造时以标签选择（默认 epoll）。事件等待、套接字与定时器的实现由后端提供，
// io_context 只持有一个抽象的 detail::io_backend；I/O 对象经抽象接口对接，代码与后端无关。
//
// 工作计数：run_async 与 run 在链存续期间持有一份工作；后端为每个排队中的 I/O 操作与
// 定时器持有一份。计数归零且队列为空时 run() 返回。
//
// dispatch：调用线程正在 run() 本上下文时直接返回 c.h（对称转移，零开销恢复），否则排队
// 并返回 noop_coroutine()。post 总是排队。恢复一律经由 safe_resume。
//
// 销毁：析构时未完成的操作被放弃（等待它们的协程不再恢复，其帧不会被销毁）。销毁前应
// 通过 stop_token 请求停止并 run() 到所有链完成。

namespace net {

namespace detail {
struct io_backend;
struct io_context_access;
} // namespace detail

// concurrency_hint 的特殊值（对应 Asio 的 BOOST_ASIO_CONCURRENCY_HINT_UNSAFE）：调用方**承诺**只有
// 一个线程会调用 run() / run_one() / poll() 系列函数，而且始终是同一个线程。后端据此启用单线程
// 优化——io_uring 以 IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN 创建（内核省掉 SQ
// 锁、完成批量交付），第一个调用 run() 的线程成为唯一提交者。违反承诺时 io_uring_enter 以
// EEXIST 失败并抛出。就绪型后端忽略它。
constexpr int single_thread_hint = 0;

struct io_context : execution_context {
    struct executor_type {
        executor_type() noexcept = default;

        io_context& context() const noexcept { return *context_; }

        void on_work_started() const noexcept;
        void on_work_finished() const noexcept;

        coroutine_handle<> dispatch(continuation& c) const;
        void post(continuation& c) const;

        // 调用线程是否正在运行本上下文的事件循环。
        bool running_in_this_thread() const noexcept;

        friend bool operator==(executor_type const& left, executor_type const& right) noexcept {
            return left.context_ == right.context_;
        }

        friend bool operator!=(executor_type const& left, executor_type const& right) noexcept {
            return left.context_ != right.context_;
        }

      private:
        friend struct io_context;
        explicit executor_type(io_context* const context) noexcept : context_{context} {}

        io_context* context_ = nullptr;
    };

    // 默认后端。
    io_context();
    // concurrency_hint：预期同时 run() 的线程数（提示，不是限制）；net::single_thread_hint 是
    // 例外——它是承诺，见其说明。
    explicit io_context(int concurrency_hint);
    // 指定后端：io_context{net::poll} / io_context{net::select, 4} /
    // io_context{net::io_uring, net::single_thread_hint}。
    template <class Backend, class = decltype(Backend::kind)>
    explicit io_context(Backend, int const concurrency_hint = 1)
        : io_context(Backend::kind, concurrency_hint) {}
    io_context(backend_kind backend, int concurrency_hint);

    io_context(io_context const&) = delete;
    io_context& operator=(io_context const&) = delete;
    ~io_context();

    executor_type get_executor() noexcept { return executor_type{this}; }

    backend_kind backend() const noexcept;
    char const* backend_name() const noexcept;

    // 把一块缓冲区登记给后端（io_uring：固定缓冲表）。之后完全落在该区域内的单缓冲 read_some / write_some /
    // 文件读写走 READ_FIXED / WRITE_FIXED，省掉每次操作的页钉扎。这是优化提示：不支持的后端返回
    // not_supported，读写照常工作；区域必须在注销前保持有效。
    std::error_code register_buffer(mutable_buffer region) noexcept;
    void unregister_buffer(mutable_buffer region) noexcept;

    // 运行事件循环直到工作计数归零或 stop()。返回恢复的协程数。
    std::size_t run();
    // 最多恢复一个协程（可能阻塞等待事件）。
    std::size_t run_one();
    // 运行直到无工作、stop() 或超时。
    template <class Rep, class Period>
    std::size_t run_for(std::chrono::duration<Rep, Period> const& timeout) {
        return run_until(std::chrono::steady_clock::now() +
                         std::chrono::duration_cast<std::chrono::steady_clock::duration>(timeout));
    }
    std::size_t run_until(std::chrono::steady_clock::time_point deadline);
    template <class Rep, class Period>
    std::size_t run_one_for(std::chrono::duration<Rep, Period> const& timeout) {
        return run_one_until(std::chrono::steady_clock::now() +
                             std::chrono::duration_cast<std::chrono::steady_clock::duration>(timeout));
    }
    std::size_t run_one_until(std::chrono::steady_clock::time_point deadline);
    // 只处理已就绪的工作，不阻塞。
    std::size_t poll();
    std::size_t poll_one();

    // 让所有 run() 尽快返回；之后 run() 立即返回直到 restart()。
    void stop();
    bool stopped() const noexcept;
    void restart();

  private:
    friend struct detail::io_context_access;

    struct impl;
    std::unique_ptr<impl> impl_;
};

} // namespace net
