#include "detail/io_uring/uring.hpp"

#include <algorithm>
#include <cerrno>
#include <cstring>

#include <cstdio>

#include <sys/mman.h>
#include <sys/uio.h>
#include <sys/syscall.h>
#include <sys/utsname.h>
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

int sys_io_uring_register(int const fd, unsigned const opcode, void const* const arg, unsigned const nr_args) noexcept {
    return static_cast<int>(::syscall(__NR_io_uring_register, fd, opcode, arg, nr_args));
}

#ifndef IORING_SETUP_SINGLE_ISSUER
#define IORING_SETUP_SINGLE_ISSUER (1U << 12)
#endif
#ifndef IORING_SETUP_DEFER_TASKRUN
#define IORING_SETUP_DEFER_TASKRUN (1U << 13)
#endif
#ifndef IORING_SETUP_R_DISABLED
#define IORING_SETUP_R_DISABLED (1U << 6)
#endif
#ifndef IORING_REGISTER_FILES2
#define IORING_REGISTER_FILES2 13
#endif
#ifndef IORING_REGISTER_FILES_UPDATE2
#define IORING_REGISTER_FILES_UPDATE2 14
#endif
#ifndef IORING_REGISTER_BUFFERS2
#define IORING_REGISTER_BUFFERS2 15
#endif
#ifndef IORING_REGISTER_BUFFERS_UPDATE
#define IORING_REGISTER_BUFFERS_UPDATE 16
#endif
#ifndef IORING_REGISTER_PBUF_RING
#define IORING_REGISTER_PBUF_RING 22
#endif
#ifndef IORING_UNREGISTER_PBUF_RING
#define IORING_UNREGISTER_PBUF_RING 23
#endif
#ifndef IORING_RSRC_REGISTER_SPARSE
#define IORING_RSRC_REGISTER_SPARSE (1U << 0)
#endif
#ifndef IORING_REGISTER_ENABLE_RINGS
#define IORING_REGISTER_ENABLE_RINGS 12
#endif

unsigned* ring_field(void* const base, unsigned const offset) noexcept {
    return reinterpret_cast<unsigned*>(static_cast<unsigned char*>(base) + offset);
}

unsigned load_acquire(unsigned const* const p) noexcept { return __atomic_load_n(p, __ATOMIC_ACQUIRE); }
void store_release(unsigned* const p, unsigned const v) noexcept { __atomic_store_n(p, v, __ATOMIC_RELEASE); }

} // namespace

uring::uring(unsigned const entries, bool const single_issuer) {
    if (single_issuer) {
        // 6.1+ 才有 DEFER_TASKRUN；老内核返回 EINVAL，退回普通模式。
        io_uring_params params{};
        params.flags = IORING_SETUP_CLAMP | IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN |
                       IORING_SETUP_R_DISABLED;
        fd_ = sys_io_uring_setup(entries, &params);
        if (fd_ >= 0) {
            defer_taskrun_ = true;
            enabled_ = false;
            setup_mapped(params);
            return;
        }
        if (errno != EINVAL) throw std::system_error{errno, std::system_category(), "io_uring_setup"};
    }
    io_uring_params params{};
    params.flags = IORING_SETUP_CLAMP;
    fd_ = sys_io_uring_setup(entries, &params);
    if (fd_ < 0) throw std::system_error{errno, std::system_category(), "io_uring_setup"};
    setup_mapped(params);
}

void uring::setup_mapped(io_uring_params const& params) {
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

int uring::enable() noexcept {
    if (enabled_) return 0;
    if (sys_io_uring_register(fd_, IORING_REGISTER_ENABLE_RINGS, nullptr, 0U) < 0) return -errno;
    enabled_ = true;
    return 0;
}

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

int uring::enter(unsigned const to_submit, unsigned const min_complete, __kernel_timespec const* const timeout) noexcept {
    for (;;) {
        int result = 0;
        if (timeout != nullptr && min_complete != 0U) {
            io_uring_getevents_arg arg{};
            arg.ts = reinterpret_cast<std::uint64_t>(timeout);
            result = sys_io_uring_enter(fd_, to_submit, min_complete, IORING_ENTER_GETEVENTS | IORING_ENTER_EXT_ARG, &arg,
                                        sizeof(arg));
        } else {
            result = sys_io_uring_enter(fd_, to_submit, min_complete, IORING_ENTER_GETEVENTS, nullptr, 0U);
        }
        if (result >= 0) return 0;
        if (errno == EINTR) {
            if (to_submit == 0U) return 0;
            continue; // 提交被打断：重试，内核只接受尚未消费的条目
        }
        if (errno == ETIME) return 0;
        if (errno == EAGAIN || errno == EBUSY) return 0; // CQ 满 / 需要先消费
        return -errno;
    }
}

// ---- CQ ----

bool uring::peek(io_uring_cqe const*& cqe) noexcept {
    auto const head = *cq_head_;
    if (head == load_acquire(cq_tail_)) return false;
    cqe = &cqes_[head & *cq_mask_];
    return true;
}

void uring::advance() noexcept { store_release(cq_head_, *cq_head_ + 1U); }

unsigned uring::ready() const noexcept { return load_acquire(cq_tail_) - *cq_head_; }

bool kernel_at_least(int const want_major, int const want_minor) noexcept {
    static int const version = [] {
        utsname info{};
        if (::uname(&info) != 0) return 0;
        auto major = 0;
        auto minor = 0;
        if (std::sscanf(info.release, "%d.%d", &major, &minor) != 2) return 0;
        return major * 1000 + minor;
    }();
    return version >= want_major * 1000 + want_minor;
}

bool multishot_accept_supported() noexcept { return kernel_at_least(5, 19); }

bool multishot_recv_supported() noexcept { return kernel_at_least(6, 0); }

// ---- 注册资源 ----

int uring::register_sparse_files(unsigned const count) noexcept {
    io_uring_rsrc_register reg{};
    reg.nr = count;
    reg.flags = IORING_RSRC_REGISTER_SPARSE;
    if (sys_io_uring_register(fd_, IORING_REGISTER_FILES2, &reg, sizeof(reg)) < 0) return -errno;
    return 0;
}

int uring::update_file(unsigned const slot, int const fd) noexcept {
    auto value = fd;
    io_uring_rsrc_update2 update{};
    update.offset = slot;
    update.data = reinterpret_cast<std::uintptr_t>(&value);
    update.nr = 1U;
    if (sys_io_uring_register(fd_, IORING_REGISTER_FILES_UPDATE2, &update, sizeof(update)) < 0) return -errno;
    return 0;
}

int uring::register_sparse_buffers(unsigned const count) noexcept {
    io_uring_rsrc_register reg{};
    reg.nr = count;
    reg.flags = IORING_RSRC_REGISTER_SPARSE;
    if (sys_io_uring_register(fd_, IORING_REGISTER_BUFFERS2, &reg, sizeof(reg)) < 0) return -errno;
    return 0;
}

int uring::update_buffer(unsigned const slot, void* const data, std::size_t const size) noexcept {
    iovec vec{};
    vec.iov_base = data;
    vec.iov_len = data != nullptr ? size : 0U;
    io_uring_rsrc_update2 update{};
    update.offset = slot;
    update.data = reinterpret_cast<std::uintptr_t>(&vec);
    update.nr = 1U;
    if (sys_io_uring_register(fd_, IORING_REGISTER_BUFFERS_UPDATE, &update, sizeof(update)) < 0) return -errno;
    return 0;
}

int uring::register_buffer_ring(void* const ring, unsigned const entries, unsigned short const group) noexcept {
    io_uring_buf_reg reg{};
    reg.ring_addr = reinterpret_cast<std::uintptr_t>(ring);
    reg.ring_entries = entries;
    reg.bgid = group;
    if (sys_io_uring_register(fd_, IORING_REGISTER_PBUF_RING, &reg, 1U) < 0) return -errno;
    return 0;
}

int uring::unregister_buffer_ring(unsigned short const group) noexcept {
    io_uring_buf_reg reg{};
    reg.bgid = group;
    if (sys_io_uring_register(fd_, IORING_UNREGISTER_PBUF_RING, &reg, 1U) < 0) return -errno;
    return 0;
}

bool uring_available() noexcept {
    io_uring_params params{};
    auto const fd = sys_io_uring_setup(2U, &params);
    if (fd < 0) return false;
    ::close(fd);
    // 5.5+ 的语义（ACCEPT / CONNECT / ASYNC_CANCEL）与 5.11+ 的 EXT_ARG（带超时的等待用它）。
    constexpr auto required = static_cast<unsigned>(IORING_FEAT_NODROP | IORING_FEAT_EXT_ARG);
    return (params.features & required) == required;
}

} // namespace detail
} // namespace net
