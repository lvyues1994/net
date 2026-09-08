#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <vector>

#include <linux/io_uring.h>

#include "net/continuation.hpp"
#include "net/coroutine.hpp"
#include "net/detail/storage.hpp"
#include "net/io_env.hpp"

#include "detail/backend.hpp"
#include "detail/io_uring/uring_op.hpp"

// io_uring 的被调方拥有缓冲区接收：一个常驻的多发 RECV（IORING_RECV_MULTISHOT + IOSQE_BUFFER_SELECT）
// 加一个提供缓冲环（IORING_REGISTER_PBUF_RING）。内核每收到一段就从环里挑一块、填好、投一个带
// IORING_CQE_F_BUFFER 的 CQE（缓冲号在 flags >> IORING_CQE_BUFFER_SHIFT），F_MORE 表示请求仍在武装；
// 环空时以 -ENOBUFS 结束，consume 归还缓冲后重新武装。
//
// 生命周期：缓冲存储与环由操作对象拥有——在飞的接收随时可能写进去，所以它们要活到终止 CQE；
// receive_stream 销毁时把操作退役给后端（owner 置空 + ASYNC_CANCEL），后端在终止 CQE 后删除操作，
// 操作的析构再注销环、释放存储。
//
// 锁序：环锁 → 流锁（on_complete 在环锁内被调用，再拿流锁）；流一侧要提交时先放流锁再拿环锁。

namespace net {
namespace detail {

struct uring_backend;
struct uring_receive_stream;

struct uring_multishot_recv_op final : uring_op {
    uring_multishot_recv_op(uring_backend& backend, int fd, int file_slot, unsigned entries, std::size_t buffer_size,
                            unsigned short group);
    ~uring_multishot_recv_op() override;

    bool valid() const noexcept { return ring_ != nullptr && storage_ != nullptr; }

    void prepare(io_uring_sqe& sqe) noexcept override;
    void on_complete(int res, unsigned flags) noexcept override;
    void complete() noexcept override {}
    bool rearm() noexcept override; // 流一侧在 on_disarmed 里决定：有人在等就让后端立刻重新提交

    // 归还一块缓冲（写进环但不发布）；publish 一次性发布。
    void give_back(unsigned short bid) noexcept;
    void publish() noexcept;
    unsigned char* buffer_at(unsigned short const bid) const noexcept { return storage_ + bid * buffer_size_; }
    std::size_t buffer_size() const noexcept { return buffer_size_; }
    unsigned entries() const noexcept { return entries_; }

    uring_receive_stream* owner = nullptr; // 退役后为空（环锁保护）

  private:
    uring_backend* backend_;
    int fd_;
    int file_slot_;
    unsigned entries_;
    unsigned mask_;
    std::size_t buffer_size_;
    unsigned short group_;
    bool ring_registered_ = false;
    unsigned char* ring_ = nullptr;    // io_uring_buf[entries]，尾指针叠在 bufs[0].resv 上
    unsigned char* storage_ = nullptr; // entries × buffer_size
    unsigned short local_tail_ = 0;
    unsigned pending_adds_ = 0;
};

struct cancel_uring_receive {
    uring_receive_stream* stream;
    void operator()() const noexcept;
};

struct uring_receive_stream final : receive_stream_impl {
    uring_receive_stream(io_context& context, uring_backend& backend, int fd, int file_slot, std::size_t buffer_count,
                         std::size_t buffer_size);
    ~uring_receive_stream() override;

    bool valid() const noexcept { return op_ && op_->valid(); }

    bool pull_ready() noexcept override;
    coroutine_handle<> pull_suspend(coroutine_handle<> h, io_env const* env) noexcept override;
    io_result<span<const_buffer>> pull_finish(span<const_buffer> dest) noexcept override;
    void consume(std::size_t n) noexcept override;
    void cancel() noexcept override;
    bool has_pending() const noexcept override { return waiting_; }

    // 环锁 + 流锁内，由操作调用。
    void on_chunk(unsigned short bid, std::size_t length) noexcept;
    void on_terminal(std::error_code ec, bool eof) noexcept;
    void on_disarmed(bool starved) noexcept;
    bool take_rearm_request() noexcept; // 环锁内：on_disarmed 记下的"请后端重新提交"
    std::mutex& mutex() noexcept { return mutex_; }

  private:
    struct chunk {
        unsigned short bid;
        std::size_t length;
    };

    void wake_locked() noexcept; // 有等待者就交出续体
    void arm() noexcept;          // 流锁外：提交多发接收

    io_context* context_;
    uring_backend* backend_;
    std::unique_ptr<uring_multishot_recv_op> op_;
    std::mutex mutex_;
    std::vector<chunk> chunks_;
    std::size_t head_ = 0;
    std::size_t first_offset_ = 0;
    bool armed_ = false;    // 多发请求在飞，或后端即将按 rearm() 重新提交
    bool rearm_requested_ = false; // on_disarmed 决定让后端重新提交（rearm() 取走）
    bool starved_ = false;  // 以 -ENOBUFS 结束：归还缓冲后再武装
    bool eof_ = false;
    std::error_code error_;
    bool aborted_once_ = false; // 取消：下一次 pull 报一次 operation_aborted
    bool waiting_ = false;
    continuation cont_;
    io_env const* env_ = nullptr;
    late_init<stop_callback<cancel_uring_receive>> stop_cb_;
};

} // namespace detail
} // namespace net
