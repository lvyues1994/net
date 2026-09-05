#pragma once

#include <cstdint>
#include <system_error>

#include <linux/io_uring.h>
#include <linux/time_types.h>

// 最小的 io_uring 环封装（裸系统调用 + mmap，不依赖 liburing）。
//
// 提交队列（SQ）是单生产者：调用方在自己的互斥锁内使用 get_sqe / flush / submit。
// 完成队列（CQ）是单消费者：只有运行事件循环的那个线程调用 wait / peek / advance。
// 两侧可以并发：一个线程阻塞在 wait 里时，其它线程可以提交（内核允许并发 io_uring_enter）。

namespace net {
namespace detail {

struct uring {
    explicit uring(unsigned entries);
    uring(uring const&) = delete;
    uring& operator=(uring const&) = delete;
    ~uring();

    int fd() const noexcept { return fd_; }
    unsigned features() const noexcept { return features_; }
    unsigned sq_entries() const noexcept { return sq_entries_; }

    // ---- SQ（调用方持锁） ----

    // 取一个已清零的 SQE；环满时返回空。
    io_uring_sqe* get_sqe() noexcept;
    // 发布本地尾指针，返回待提交的条目数。
    unsigned flush() noexcept;
    // io_uring_enter(to_submit)。返回内核接受的条目数，失败返回负 errno。
    int submit(unsigned to_submit) noexcept;

    // ---- CQ（单消费者） ----

    // 等待至少一个完成；timeout 为空表示不限时；wait_for_any 为假时只做一次非阻塞进入。
    // 返回 0 或负 errno（-ETIME 超时、-EINTR 被打断都视为正常返回 0）。
    int wait(__kernel_timespec const* timeout, bool block) noexcept;
    bool peek(io_uring_cqe const*& cqe) noexcept;
    void advance() noexcept;
    unsigned ready() const noexcept;

  private:
    void unmap() noexcept;

    int fd_ = -1;
    unsigned features_ = 0;
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

} // namespace detail
} // namespace net
