#pragma once

#include <chrono>
#include <cstddef>
#include <system_error>

#include "net/continuation.hpp"
#include "net/detail/completion_frame.hpp"

#include "detail/backend.hpp"
#include "detail/timer_heap.hpp"

// 就绪型后端族（epoll / poll / select）共享的操作与描述符状态。
//
// 反应器只知道"描述符在某个方向上就绪了"；具体的系统调用由操作自己在 perform() 里做。
// 每个 I/O 对象为每个方向保存一个 reactor_op，地址稳定；一个操作恰好完成一次
// （perform 返回 true，或被取消）。
//
// 就绪后谁来 perform：单线程时反应器线程在锁内就地做；io_context 有空闲线程时反应器只把操作标成
// dispatched 并投进执行队列（dispatch_cont → dispatch_frame），由取到它的线程在锁外做系统调用、再直接
// 恢复协程——否则一批几十个就绪描述符的 recv 全串行在反应器线程上、还一直持着反应器锁。

namespace net {
namespace detail {

constexpr unsigned read_ready_bit = 1U;
constexpr unsigned write_ready_bit = 2U;

struct descriptor_state;
struct reactor_backend;

struct reactor_op {
    // idle：不在反应器里；queued：登记在 descriptor_state::ops 里等就绪；dispatched：已就绪、投进了执行队列，
    // 仍登记在 ops 里（取消 / 注销看得到它，但只能记标记，由执行它的线程收尾）。
    enum class state_type : unsigned char { idle, queued, dispatched };

    reactor_op() = default;
    reactor_op(reactor_op const&) = delete;
    reactor_op& operator=(reactor_op const&) = delete;
    virtual ~reactor_op() = default;

    // 在描述符 fd 上执行系统调用。返回 true 表示完成（ec / bytes 已记录），false 表示仍需等待
    // 下一次就绪（EAGAIN）。就地执行时在反应器锁内调用，派发执行时在锁外调用。
    virtual bool perform(int fd) noexcept = 0;

    // 操作完成后在反应器锁外恰好调用一次：把续体交给操作的执行器。
    virtual void complete() noexcept = 0;

    // 派发执行的线程上完成：经操作的执行器 dispatch——已在它的执行上下文里就返回协程句柄（调用方对称
    // 转移过去），否则排队并返回 noop。之后不再碰 op。
    virtual coroutine_handle<> complete_here() noexcept {
        complete();
        return noop_coroutine();
    }

    std::error_code ec;
    std::size_t bytes_transferred = 0;
    state_type state = state_type::idle;
    // 停止请求在"装好 stop_callback 之后、start_op 之前"到达：cancel_op 找不到已登记的操作，
    // 记在这里，start_op 看到就以 operation_aborted 同步完成。发布操作之后不能再碰 env / op
    //（别的线程可能已经完成它、恢复协程、销毁帧），所以这个窗口只能这样关。dispatched 的操作被取消时
    // 也记在这里，由执行它的线程看到。
    bool cancel_requested = false;
    // 为假的操作（信号泵这类常驻监听）排队时不计入 io_context 的未完成工作，也总是就地执行。
    bool counts_as_work = true;

    // 派发执行用（start_op 填写）。
    reactor_backend* backend = nullptr;
    descriptor_state* descriptor = nullptr;
    unsigned direction_index = 0;
    unsigned generation = 0; // 登记时 descriptor_state::generation 的值：关闭后重开的描述符不会被旧任务误用
    completion_frame dispatch_frame;
    continuation dispatch_cont;
};

struct descriptor_state {
    static constexpr std::size_t no_index = static_cast<std::size_t>(-1);

    int fd = -1;
    reactor_op* ops[2] = {nullptr, nullptr};
    unsigned ready = 0;                 // 已到达但尚未被消费的就绪位（边沿触发后端）
    unsigned interest = 0;              // 当前登记的兴趣位（电平触发后端）
    std::size_t demux_index = no_index; // 解复用器私有（poll 数组下标等）
    bool registered = false;
    unsigned generation = 0; // 每次注册 / 注销加一
    unsigned in_syscall = 0; // 派发执行的线程正在这个描述符上做系统调用：注销要等它归零再交还 fd

    // 需要等待的方向：已派发的操作不算（电平触发的解复用器不该在它被执行之前反复报告同一个就绪）。
    unsigned wanted() const noexcept {
        return (waiting(ops[0]) ? read_ready_bit : 0U) | (waiting(ops[1]) ? write_ready_bit : 0U);
    }

    static bool waiting(reactor_op const* const op) noexcept {
        return op != nullptr && op->state != reactor_op::state_type::dispatched;
    }
};


} // namespace detail
} // namespace net
