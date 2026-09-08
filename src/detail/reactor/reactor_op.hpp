#pragma once

#include <chrono>
#include <cstddef>
#include <system_error>

#include "detail/backend.hpp"
#include "detail/timer_heap.hpp"

// 就绪型后端族（epoll / poll / select）共享的操作与描述符状态。
//
// 反应器只知道"描述符在某个方向上就绪了"；具体的系统调用由操作自己在 perform() 里做。
// 每个 I/O 对象为每个方向保存一个 reactor_op，地址稳定；一个操作恰好完成一次
// （perform 返回 true，或被取消）。

namespace net {
namespace detail {

constexpr unsigned read_ready_bit = 1U;
constexpr unsigned write_ready_bit = 2U;

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
    // 停止请求在"装好 stop_callback 之后、start_op 之前"到达：cancel_op 找不到已登记的操作，
    // 记在这里，start_op 看到就以 operation_aborted 同步完成。发布操作之后不能再碰 env / op
    //（别的线程可能已经完成它、恢复协程、销毁帧），所以这个窗口只能这样关。
    bool cancel_requested = false;
    // 为假的操作（信号泵这类常驻监听）排队时不计入 io_context 的未完成工作。
    bool counts_as_work = true;
};

struct descriptor_state {
    static constexpr std::size_t no_index = static_cast<std::size_t>(-1);

    int fd = -1;
    reactor_op* ops[2] = {nullptr, nullptr};
    unsigned ready = 0;                 // 已到达但尚未被消费的就绪位（边沿触发后端）
    unsigned interest = 0;              // 当前登记的兴趣位（电平触发后端）
    std::size_t demux_index = no_index; // 解复用器私有（poll 数组下标等）
    bool registered = false;

    unsigned wanted() const noexcept {
        return (ops[0] != nullptr ? read_ready_bit : 0U) | (ops[1] != nullptr ? write_ready_bit : 0U);
    }
};


} // namespace detail
} // namespace net
