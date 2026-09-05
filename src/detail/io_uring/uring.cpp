#include "detail/io_uring/uring.hpp"

#include <algorithm>
#include <cerrno>
#include <cstring>

#include <sys/mman.h>
#include <sys/syscall.h>
#include <unistd.h>

namespace net {
namespace detail {

namespace {

int sys_io_uring_setup(unsigned const entries, io_uring_params* const params) noexcept {
    return static_cast<int>(::syscall(__NR_io_uring_setup, entries, params));
}

int sys_io_uring_enter(int const fd, unsigned const to_submit, unsigned const min_complete, unsigned const flags,
                       void const* const arg, std::size_t const arg_size) noexcept {
    return static_cast<int>(::syscall(__NR_io_uring_enter, fd, to_submit, min_complete, flags, arg, arg_size));
}

unsigned* ring_field(void* const base, unsigned const offset) noexcept {
    return reinterpret_cast<unsigned*>(static_cast<unsigned char*>(base) + offset);
}

unsigned load_acquire(unsigned const* const p) noexcept { return __atomic_load_n(p, __ATOMIC_ACQUIRE); }
void store_release(unsigned* const p, unsigned const v) noexcept { __atomic_store_n(p, v, __ATOMIC_RELEASE); }

} // namespace

uring::uring(unsigned const entries) {
    io_uring_params params{};
    params.flags = IORING_SETUP_CLAMP;
    fd_ = sys_io_uring_setup(entries, &params);
    if (fd_ < 0) throw std::system_error{errno, std::system_category(), "io_uring_setup"};
    features_ = params.features;
    sq_entries_ = params.sq_entries;
    cq_entries_ = params.cq_entries;

    sq_ring_size_ = params.sq_off.array + params.sq_entries * sizeof(unsigned);
    cq_ring_size_ = params.cq_off.cqes + params.cq_entries * sizeof(io_uring_cqe);
    auto const single_mmap = (features_ & IORING_FEAT_SINGLE_MMAP) != 0U;
    if (single_mmap) sq_ring_size_ = cq_ring_size_ = std::max(sq_ring_size_, cq_ring_size_);

    sq_ring_ = ::mmap(nullptr, sq_ring_size_, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, fd_,
                      static_cast<off_t>(IORING_OFF_SQ_RING));
    if (sq_ring_ == MAP_FAILED) {
        auto const saved = errno;
        sq_ring_ = nullptr;
        unmap();
        throw std::system_error{saved, std::system_category(), "mmap(sq ring)"};
    }
    if (single_mmap) {
        cq_ring_ = sq_ring_;
    } else {
        cq_ring_ = ::mmap(nullptr, cq_ring_size_, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, fd_,
                          static_cast<off_t>(IORING_OFF_CQ_RING));
        if (cq_ring_ == MAP_FAILED) {
            auto const saved = errno;
            cq_ring_ = nullptr;
            unmap();
            throw std::system_error{saved, std::system_category(), "mmap(cq ring)"};
        }
    }
    sqes_size_ = params.sq_entries * sizeof(io_uring_sqe);
    auto* const sqes = ::mmap(nullptr, sqes_size_, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, fd_,
                              static_cast<off_t>(IORING_OFF_SQES));
    if (sqes == MAP_FAILED) {
        auto const saved = errno;
        unmap();
        throw std::system_error{saved, std::system_category(), "mmap(sqes)"};
    }
    sqes_ = static_cast<io_uring_sqe*>(sqes);

    sq_head_ = ring_field(sq_ring_, params.sq_off.head);
    sq_tail_ = ring_field(sq_ring_, params.sq_off.tail);
    sq_mask_ = ring_field(sq_ring_, params.sq_off.ring_mask);
    sq_flags_ = ring_field(sq_ring_, params.sq_off.flags);
    sq_array_ = ring_field(sq_ring_, params.sq_off.array);
    sq_local_tail_ = *sq_tail_;

    cq_head_ = ring_field(cq_ring_, params.cq_off.head);
    cq_tail_ = ring_field(cq_ring_, params.cq_off.tail);
    cq_mask_ = ring_field(cq_ring_, params.cq_off.ring_mask);
    cqes_ = reinterpret_cast<io_uring_cqe*>(static_cast<unsigned char*>(cq_ring_) + params.cq_off.cqes);
}

uring::~uring() { unmap(); }

void uring::unmap() noexcept {
    if (sqes_ != nullptr) ::munmap(sqes_, sqes_size_);
    if (cq_ring_ != nullptr && cq_ring_ != sq_ring_) ::munmap(cq_ring_, cq_ring_size_);
    if (sq_ring_ != nullptr) ::munmap(sq_ring_, sq_ring_size_);
    sqes_ = nullptr;
    cq_ring_ = nullptr;
    sq_ring_ = nullptr;
    if (fd_ >= 0) ::close(fd_);
    fd_ = -1;
}

// ---- SQ ----

io_uring_sqe* uring::get_sqe() noexcept {
    auto const head = load_acquire(sq_head_);
    if (sq_local_tail_ - head >= sq_entries_) return nullptr;
    auto const index = sq_local_tail_ & *sq_mask_;
    ++sq_local_tail_;
    auto* const sqe = &sqes_[index];
    std::memset(sqe, 0, sizeof(*sqe));
    sq_array_[index] = index;
    return sqe;
}

unsigned uring::flush() noexcept {
    auto const tail = *sq_tail_;
    if (tail != sq_local_tail_) store_release(sq_tail_, sq_local_tail_);
    return sq_local_tail_ - load_acquire(sq_head_);
}

int uring::submit(unsigned const to_submit) noexcept {
    if (to_submit == 0U) return 0;
    for (;;) {
        auto const submitted = sys_io_uring_enter(fd_, to_submit, 0U, 0U, nullptr, 0U);
        if (submitted >= 0) return submitted;
        if (errno == EINTR) continue;
        return -errno;
    }
}

// ---- CQ ----

int uring::wait(__kernel_timespec const* const timeout, bool const block) noexcept {
    if (not block) return 0; // 提交已由 submit 完成；只读已到达的 CQE
    if (ready() != 0U) return 0;
    for (;;) {
        int result = 0;
        if (timeout != nullptr) {
            io_uring_getevents_arg arg{};
            arg.ts = reinterpret_cast<std::uint64_t>(timeout);
            result = sys_io_uring_enter(fd_, 0U, 1U, IORING_ENTER_GETEVENTS | IORING_ENTER_EXT_ARG, &arg, sizeof(arg));
        } else {
            result = sys_io_uring_enter(fd_, 0U, 1U, IORING_ENTER_GETEVENTS, nullptr, 0U);
        }
        if (result >= 0) return 0;
        if (errno == EINTR || errno == ETIME) return 0;
        if (errno == EAGAIN || errno == EBUSY) return 0; // CQ 满 / 需要先消费
        return -errno;
    }
}

bool uring::peek(io_uring_cqe const*& cqe) noexcept {
    auto const head = *cq_head_;
    if (head == load_acquire(cq_tail_)) return false;
    cqe = &cqes_[head & *cq_mask_];
    return true;
}

void uring::advance() noexcept { store_release(cq_head_, *cq_head_ + 1U); }

unsigned uring::ready() const noexcept { return load_acquire(cq_tail_) - *cq_head_; }

bool uring_available() noexcept {
    io_uring_params params{};
    auto const fd = sys_io_uring_setup(2U, &params);
    if (fd < 0) return false;
    ::close(fd);
    return (params.features & IORING_FEAT_NODROP) != 0U; // 需要 5.5+ 的语义（ACCEPT / CONNECT / ASYNC_CANCEL）
}

} // namespace detail
} // namespace net
