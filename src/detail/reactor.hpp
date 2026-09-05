#pragma once

#include <chrono>
#include <cstddef>
#include <system_error>

#include "net/coroutine.hpp"

// 反应器与 I/O 对象之间的私有协议（仅 src/ 内可见）。
//
// 每个 I/O 对象为每个方向保存一个 reactor_op，地址稳定；反应器只在描述符就绪时调用
// perform()（执行非阻塞系统调用），完成后调用一次 complete()（把续体交给执行器）。
// 取消（stop_token、cancel()、close()）经 cancel_op：仍在队列里则摘下并以
// operation_aborted 完成。一个操作恰好完成一次。

namespace net {

struct io_context;

namespace detail {

enum class op_direction : unsigned char { read = 0, write = 1 };

struct reactor_op {
    enum class state_type : unsigned char { idle, queued };

    reactor_op() = default;
    reactor_op(reactor_op const&) = delete;
    reactor_op& operator=(reactor_op const&) = delete;
    virtual ~reactor_op() = default;

    // 描述符就绪时在反应器锁内调用。返回 true 表示完成（ec / bytes 已记录），false 表示
    // 仍需等待下一次就绪（EAGAIN）。
    virtual bool perform() noexcept = 0;

    // 操作完成后在反应器锁外恰好调用一次：把续体交给操作的执行器。
    virtual void complete() noexcept = 0;

    std::error_code ec;
    std::size_t bytes_transferred = 0;
    state_type state = state_type::idle;
    // 为假的操作（信号泵这类常驻监听）排队时不计入 io_context 的未完成工作。
    bool counts_as_work = true;
};

struct descriptor_state {
    int fd = -1;
    reactor_op* ops[2] = {nullptr, nullptr};
    unsigned ready = 0; // 已到达但尚未被消费的就绪事件（位：1 读，2 写）
    bool registered = false;
};

struct timer_op {
    static constexpr std::size_t not_queued = static_cast<std::size_t>(-1);

    timer_op() = default;
    timer_op(timer_op const&) = delete;
    timer_op& operator=(timer_op const&) = delete;
    virtual ~timer_op() = default;

    virtual void complete() noexcept = 0;

    std::chrono::steady_clock::time_point expiry{};
    std::size_t heap_index = not_queued;
    std::error_code ec;
};

struct reactor {
    virtual ~reactor() = default;

    virtual std::error_code register_descriptor(descriptor_state& state, int fd) noexcept = 0;
    // 从 epoll 摘除并取消两个方向的操作（以 operation_aborted 完成）。
    virtual void deregister_descriptor(descriptor_state& state) noexcept = 0;

    // 启动操作。描述符已就绪时先在调用线程上 perform()：完成则返回 true（调用方自己恢复
    // 协程，不会再调用 complete()）；否则排队并返回 false。
    virtual bool start_op(descriptor_state& state, op_direction direction,
                          reactor_op& op) noexcept = 0;
    // 仍排队时摘下并以 operation_aborted 完成，返回 true；否则返回 false。
    virtual bool cancel_op(descriptor_state& state, op_direction direction,
                           reactor_op& op) noexcept = 0;
    virtual void cancel_ops(descriptor_state& state) noexcept = 0;

    virtual void add_timer(timer_op& op) noexcept = 0;
    virtual bool cancel_timer(timer_op& op) noexcept = 0;
};

// io_context 的私有入口：取它的反应器。
struct io_context_access {
    static reactor& get_reactor(io_context& context) noexcept;
};

} // namespace detail
} // namespace net
