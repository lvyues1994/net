#pragma once

#include <cstdint>

#include <linux/io_uring.h>

// 完成型后端的操作：提交一个 SQE，收到一个 CQE。与就绪型的 reactor_op 不同，这里没有
// "就绪后执行"的阶段——参数在 prepare 时全部交给内核，结果在 on_complete 时到达。
//
// 两类操作：
//   - 普通操作（套接字读写、定时器）：一个 SQE 恰好一个 CQE，之后 complete() 一次。
//   - 常驻 / 多发操作（persistent：eventfd 与信号管道的多发 POLL_ADD、多发 accept）：一个 SQE
//     产生一串带 IORING_CQE_F_MORE 的 CQE；终止 CQE（无 F_MORE）之后后端问 rearm()，为真就
//     重新提交同一个操作。它们的 on_complete 在环锁内调用，complete() 永不调用。
//
// 状态位（in_flight / deferred / cancel_requested / retired）由后端的环锁保护。

namespace net {
namespace detail {

struct uring_op {
    uring_op() = default;
    uring_op(uring_op const&) = delete;
    uring_op& operator=(uring_op const&) = delete;
    virtual ~uring_op() = default;

    // 填 SQE（已清零；user_data 由后端设置）。
    virtual void prepare(io_uring_sqe& sqe) noexcept = 0;
    // CQE 到达（在事件循环线程；普通操作在环锁外，常驻操作在环锁内）：记录结果。
    virtual void on_complete(int res, unsigned flags) noexcept = 0;
    // 普通操作结束后恰好调用一次：把续体交给操作的执行器。常驻操作不会被调用。
    virtual void complete() noexcept = 0;
    // 取消本操作用的操作码：一般是 ASYNC_CANCEL，定时器是 TIMEOUT_REMOVE。
    virtual std::uint8_t cancel_opcode() const noexcept { return IORING_OP_ASYNC_CANCEL; }
    // 常驻操作收到终止 CQE 后（环锁内）：是否重新提交。
    virtual bool rearm() noexcept { return false; }

    bool in_flight = false;        // SQE 已提交，CQE 未到（多发：终止 CQE 未到）
    bool deferred = false;         // 环满，等待提交
    bool cancel_requested = false; // 提交前收到取消
    bool persistent = false;       // 常驻 / 多发：见文件头
    bool retired = false;          // 拥有者已放手，由后端持有到终止 CQE 后删除
    bool counts_as_work = true;
    uring_op* next = nullptr;      // 延迟队列链接
};

} // namespace detail
} // namespace net
