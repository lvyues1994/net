#pragma once

#include <cstdint>
#include <system_error>

#include <linux/io_uring.h>
#include <linux/time_types.h>

// 最小的 io_uring 环封装（裸系统调用 + mmap，不依赖 liburing）。
//
// 提交队列（SQ）是单生产者：调用方在自己的互斥锁内使用 get_sqe / flush。写 SQE 只是用户态
// 内存写；真正进内核只有 enter()——把待提交的 SQE 与等待完成合并成一次 io_uring_enter。
// 完成队列（CQ）是单消费者：只有运行事件循环的那个线程调用 enter / peek / advance。
//
// single_issuer 模式（Corosio 单线程模式的做法）：IORING_SETUP_SINGLE_ISSUER 让内核省掉 SQ 的
// 内部锁，IORING_SETUP_DEFER_TASKRUN 让完成的 task_work 在 GETEVENTS 边界批量交付而不是用
// 信号打断运行线程。两者要求 io_uring_enter 始终来自同一个线程，所以环以 R_DISABLED 创建，
// 由第一个调用 enable() 的线程（事件循环线程）启用并成为提交者。老内核不认识这些标志时
// 自动退回普通模式。

namespace net {
namespace detail {

struct uring {
    explicit uring(unsigned entries, bool single_issuer = false);
    uring(uring const&) = delete;
    uring& operator=(uring const&) = delete;
    ~uring();

    int fd() const noexcept { return fd_; }
    unsigned features() const noexcept { return features_; }
    unsigned sq_entries() const noexcept { return sq_entries_; }
    // 是否以 SINGLE_ISSUER | DEFER_TASKRUN 创建（决定 poll 模式是否必须进内核取完成）。
    bool defer_taskrun() const noexcept { return defer_taskrun_; }

    // R_DISABLED 创建的环需要先启用；由事件循环线程在第一次 enter 之前调用。幂等。
    int enable() noexcept;
    bool enabled() const noexcept { return enabled_; }

    // ---- SQ（调用方持锁） ----

    // 取一个已清零的 SQE；环满时返回空。
    io_uring_sqe* get_sqe() noexcept;
    // 发布本地尾指针，返回待提交的条目数（不进内核）。
    unsigned flush() noexcept;

    // ---- 进内核（单消费者线程） ----

    // 一次 io_uring_enter(to_submit, min_complete, GETEVENTS[, timeout])：提交已发布的 SQE，
    // 并等待至少 min_complete 个完成（0 = 不等待）。返回 0 或负 errno（-ETIME / -EINTR /
    // CQ 满导致的 -EAGAIN / -EBUSY 都视为正常返回 0）。
    int enter(unsigned to_submit, unsigned min_complete, __kernel_timespec const* timeout) noexcept;

    // ---- CQ（单消费者） ----

    bool peek(io_uring_cqe const*& cqe) noexcept;
    void advance() noexcept;
    unsigned ready() const noexcept;

  private:
    void setup_mapped(io_uring_params const& params);
    void unmap() noexcept;

    int fd_ = -1;
    unsigned features_ = 0;
    bool defer_taskrun_ = false;
    bool enabled_ = true;
    unsigned sq_entries_ = 0;
    unsigned cq_entries_ = 0;

    void* sq_ring_ = nullptr;
    std::size_t sq_ring_size_ = 0;
    void* cq_ring_ = nullptr;
    std::size_t cq_ring_size_ = 0;
    io_uring_sqe* sqes_ = nullptr;
    std::size_t sqes_size_ = 0;

    unsigned* sq_head_ = nullptr;
    unsigned* sq_tail_ = nullptr;
    unsigned* sq_mask_ = nullptr;
    unsigned* sq_flags_ = nullptr;
    unsigned* sq_array_ = nullptr;
    unsigned sq_local_tail_ = 0;

    unsigned* cq_head_ = nullptr;
    unsigned* cq_tail_ = nullptr;
    unsigned* cq_mask_ = nullptr;
    io_uring_cqe* cqes_ = nullptr;
};

// 运行时探测：本进程能否创建 io_uring（内核支持且未被 seccomp / sysctl 禁用）。
bool uring_available() noexcept;
// 内核 ≥ 5.19：IORING_ACCEPT_MULTISHOT 可用（没有 feature 位，按 uname 判断）。
bool multishot_accept_supported() noexcept;

} // namespace detail
} // namespace net
