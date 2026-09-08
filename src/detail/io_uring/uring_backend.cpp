#include "detail/io_uring/uring_backend.hpp"

#include <cerrno>
#include <cstdint>

#include <poll.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include "co2/contract.hpp"

#include "net/error.hpp"
#include "net/io_context.hpp"

#include "detail/io_uring/uring_socket.hpp"
#include "detail/io_uring/uring_file.hpp"
#include "detail/io_uring/uring_timer.hpp"

#ifndef IORING_POLL_ADD_MULTI
#define IORING_POLL_ADD_MULTI (1U << 0)
#endif
#ifndef IORING_CQE_F_MORE
#define IORING_CQE_F_MORE (1U << 1)
#endif

namespace net {
namespace detail {

namespace {

// 取消请求自己的 CQE 用这个 user_data 标记，结果忽略。
constexpr std::uint64_t cancel_marker = 1U;

std::uint64_t user_data_of(uring_op const* const op) noexcept {
    return static_cast<std::uint64_t>(reinterpret_cast<std::uintptr_t>(op));
}

uring_op* op_of(std::uint64_t const user_data) noexcept {
    return reinterpret_cast<uring_op*>(static_cast<std::uintptr_t>(user_data));
}

} // namespace

// ---- poller ----

void uring_backend::poller::prepare(io_uring_sqe& sqe) noexcept {
    sqe.opcode = IORING_OP_POLL_ADD;
    sqe.fd = fd;
    sqe.poll32_events = POLLIN;
    if (multishot) sqe.len = IORING_POLL_ADD_MULTI;
}

void uring_backend::poller::on_complete(int const res, unsigned) noexcept {
    if (res == -EINVAL && multishot) multishot = false; // 内核不支持多发：退回一次性
    if (res >= 0 && on_readable != nullptr) on_readable(*this);
}

void uring_backend::drain_eventfd(poller& self) noexcept {
    std::uint64_t value = 0;
    while (::read(self.fd, &value, sizeof(value)) > 0) {}
}

void uring_backend::drain_signal_pipe(poller& self) noexcept {
    for (;;) {
        int value = 0;
        auto const n = ::read(self.fd, &value, sizeof(value));
        if (n == static_cast<ssize_t>(sizeof(value))) {
            if (self.deliver != nullptr) self.deliver(value);
            continue;
        }
        if (n < 0 && errno == EINTR) continue;
        return;
    }
}

// ---- 构造 / 析构 ----

uring_backend::uring_backend(execution_context& context, bool const single_issuer, unsigned const entries)
    : context_{static_cast<io_context*>(&context)}, ring_{entries, single_issuer} {
    event_fd_ = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    if (event_fd_ < 0) throw std::system_error{errno, std::system_category(), "eventfd"};
    completed_.reserve(128U);
    reaped_.reserve(128U);
    interrupt_poller_.fd = event_fd_;
    interrupt_poller_.on_readable = &drain_eventfd;
    interrupt_poller_.persistent = true;
    interrupt_poller_.counts_as_work = false;
    signal_poller_.on_readable = &drain_signal_pipe;
    signal_poller_.persistent = true;
    signal_poller_.counts_as_work = false;
    // 稀疏文件表 / 缓冲表：内核不支持（< 5.19 / 5.13）或资源限制时静默退回裸 fd / 普通缓冲。
    if (ring_.register_sparse_files(file_table_size) == 0) {
        file_table_ = true;
        free_file_slots_.reserve(file_table_size);
        for (auto slot = static_cast<int>(file_table_size); slot-- > 0;) free_file_slots_.push_back(slot);
    }
    if (ring_.register_sparse_buffers(buffer_table_size) == 0) {
        buffer_table_ = true;
        free_buffer_slots_.reserve(buffer_table_size);
        for (auto slot = static_cast<int>(buffer_table_size); slot-- > 0;) free_buffer_slots_.push_back(slot);
    }
    {
        std::lock_guard<std::mutex> lock{mutex_};
        if (not try_submit_locked(interrupt_poller_)) push_deferred_locked(interrupt_poller_);
        ring_.flush();
    }
}

uring_backend::~uring_backend() {
    if (event_fd_ >= 0) ::close(event_fd_);
}

void uring_backend::shutdown() {
    // 未完成的操作被放弃：环随后销毁，CQE 不再被读取。退役的操作此时可以安全删除。
    std::lock_guard<std::mutex> lock{mutex_};
    shut_down_ = true;
    deferred_head_ = nullptr;
    deferred_tail_ = nullptr;
    pending_cancels_.clear();
    retired_.clear();
}

// ---- 注册资源 ----

int uring_backend::register_file(int const fd) noexcept {
    std::lock_guard<std::mutex> lock{mutex_};
    if (not file_table_ || free_file_slots_.empty() || shut_down_) return -1;
    auto const slot = free_file_slots_.back();
    if (ring_.update_file(static_cast<unsigned>(slot), fd) != 0) return -1;
    free_file_slots_.pop_back();
    return slot;
}

void uring_backend::unregister_file(int const slot) noexcept {
    if (slot < 0) return;
    std::lock_guard<std::mutex> lock{mutex_};
    if (shut_down_) return;
    ring_.update_file(static_cast<unsigned>(slot), -1); // 在飞的请求持有自己的文件引用，不受影响
    free_file_slots_.push_back(slot);
}

std::error_code uring_backend::register_buffer(void* const data, std::size_t const size) noexcept {
    if (data == nullptr || size == 0U) return std::make_error_code(std::errc::invalid_argument);
    std::lock_guard<std::mutex> lock{mutex_};
    if (not buffer_table_) return std::make_error_code(std::errc::not_supported);
    if (free_buffer_slots_.empty()) return std::make_error_code(std::errc::no_buffer_space);
    auto const slot = free_buffer_slots_.back();
    auto const rc = ring_.update_buffer(static_cast<unsigned>(slot), data, size);
    if (rc != 0) return std::error_code{-rc, std::system_category()}; // 常见：RLIMIT_MEMLOCK 不够（-ENOMEM）
    free_buffer_slots_.pop_back();
    buffer_regions_.push_back(buffer_region{static_cast<char const*>(data), size, slot});
    return {};
}

void uring_backend::unregister_buffer(void* const data) noexcept {
    std::lock_guard<std::mutex> lock{mutex_};
    for (auto it = buffer_regions_.begin(); it != buffer_regions_.end(); ++it) {
        if (it->base != data) continue;
        if (not shut_down_) ring_.update_buffer(static_cast<unsigned>(it->slot), nullptr, 0U);
        free_buffer_slots_.push_back(it->slot);
        buffer_regions_.erase(it);
        return;
    }
}

int uring_backend::fixed_buffer_slot_locked(void const* const data, std::size_t const size) const noexcept {
    auto const* const begin = static_cast<char const*>(data);
    for (auto const& region : buffer_regions_)
        if (begin >= region.base && begin + size <= region.base + region.size) return region.slot;
    return -1;
}

int uring_backend::allocate_buffer_group() noexcept {
    std::lock_guard<std::mutex> lock{mutex_};
    if (not free_buffer_groups_.empty()) {
        auto const group = free_buffer_groups_.back();
        free_buffer_groups_.pop_back();
        return group;
    }
    if (next_buffer_group_ > 0xFFFF) return -1;
    return next_buffer_group_++;
}

void uring_backend::release_buffer_group(int const group) noexcept {
    std::lock_guard<std::mutex> lock{mutex_};
    free_buffer_groups_.push_back(group);
}

// ---- 工厂 ----

std::unique_ptr<socket_impl> uring_backend::create_socket(io_context& context) {
    return std::unique_ptr<socket_impl>{new uring_socket{context, *this}};
}

std::unique_ptr<timer_impl> uring_backend::create_timer(io_context& context) {
    return std::unique_ptr<timer_impl>{new uring_timer{context, *this}};
}

std::unique_ptr<file_impl> uring_backend::create_file(io_context& context) {
    return std::unique_ptr<file_impl>{new uring_file{context, *this}};
}

std::error_code uring_backend::register_signal_reader(int const read_fd, void (*const deliver)(int)) noexcept {
    auto wake = false;
    {
        std::lock_guard<std::mutex> lock{mutex_};
        if (signal_poller_.fd >= 0) return {};
        signal_poller_.fd = read_fd;
        signal_poller_.deliver = deliver;
        if (not try_submit_locked(signal_poller_)) push_deferred_locked(signal_poller_);
        ring_.flush();
        wake = waiting_;
    }
    if (wake) interrupt();
    return {};
}

// ---- 提交 / 取消 ----

bool uring_backend::try_submit_locked(uring_op& op) noexcept {
    auto* const sqe = ring_.get_sqe();
    if (sqe == nullptr) return false;
    op.prepare(*sqe);
    sqe->user_data = user_data_of(&op);
    op.in_flight = true;
    if (not op.persistent) ++inflight_;
    return true;
}

void uring_backend::push_deferred_locked(uring_op& op) noexcept {
    op.deferred = true;
    op.next = nullptr;
    if (deferred_tail_ != nullptr)
        deferred_tail_->next = &op;
    else
        deferred_head_ = &op;
    deferred_tail_ = &op;
}

bool uring_backend::remove_deferred_locked(uring_op& op) noexcept {
    uring_op* previous = nullptr;
    for (auto* current = deferred_head_; current != nullptr; previous = current, current = current->next) {
        if (current != &op) continue;
        if (previous != nullptr)
            previous->next = current->next;
        else
            deferred_head_ = current->next;
        if (deferred_tail_ == current) deferred_tail_ = previous;
        current->next = nullptr;
        current->deferred = false;
        return true;
    }
    return false;
}

void uring_backend::drain_deferred_locked() noexcept {
    while (deferred_head_ != nullptr) {
        auto* const op = deferred_head_;
        if (not try_submit_locked(*op)) return; // 仍然满
        deferred_head_ = op->next;
        if (deferred_head_ == nullptr) deferred_tail_ = nullptr;
        op->next = nullptr;
        op->deferred = false;
    }
}

bool uring_backend::submit(uring_op& op) noexcept {
    auto wake = false;
    {
        std::lock_guard<std::mutex> lock{mutex_};
        if (shut_down_) return false;
        if (op.cancel_requested) {
            op.cancel_requested = false;
            return false;
        }
        // 工作计数在发布之前、锁内加：run() 完成它时先取环锁，因此 on_work_finished 必然在这
        // 之后；锁外加会与"提交后立刻完成"竞争，把 outstanding_work 打到 0。
        if (op.counts_as_work) context_->get_executor().on_work_started();
        if (not try_submit_locked(op)) push_deferred_locked(op);
        ring_.flush(); // 只发布尾指针；进内核推迟到 run()
        wake = waiting_;
    }
    // 另一个线程正阻塞在 enter 里：叫醒它，让它把这条 SQE 提交进内核。单线程时永远不会走到。
    if (wake) interrupt();
    return true;
}

void uring_backend::submit_locked(uring_op& op) noexcept {
    if (shut_down_) return;
    if (op.counts_as_work) context_->get_executor().on_work_started(); // 发布之前
    if (not try_submit_locked(op)) push_deferred_locked(op);
    ring_.flush();
}

void uring_backend::retire(std::unique_ptr<uring_op> op) noexcept {
    std::lock_guard<std::mutex> lock{mutex_};
    op->retired = true;
    if (op->in_flight) submit_cancel_locked(*op);
    if (op->deferred) remove_deferred_locked(*op);
    if (not shut_down_) retired_.push_back(std::move(op));
    // shut_down_：环不再读 CQE，直接删。
}

void uring_backend::submit_cancel_locked(uring_op& op) noexcept {
    auto* const sqe = ring_.get_sqe();
    if (sqe == nullptr) {
        // 环满：记下，run() 补发。丢掉取消会让一个对端永不发数据的读挂到永远，或让退役的多发
        // accept 在已关闭的描述符上继续接受。
        if (not op.cancel_pending) {
            op.cancel_pending = true;
            pending_cancels_.push_back(&op);
        }
        return;
    }
    op.cancel_pending = false;
    sqe->opcode = op.cancel_opcode();
    sqe->fd = -1;
    sqe->addr = user_data_of(&op);
    sqe->user_data = cancel_marker;
    ring_.flush();
}

void uring_backend::forget_pending_cancel_locked(uring_op& op) noexcept {
    op.cancel_pending = false;
    for (auto it = pending_cancels_.begin(); it != pending_cancels_.end(); ++it) {
        if (*it != &op) continue;
        pending_cancels_.erase(it);
        return;
    }
}

void uring_backend::flush_pending_cancels_locked() noexcept {
    while (not pending_cancels_.empty()) {
        auto* const op = pending_cancels_.back(); // 仍在飞：完成时已经被 forget_pending_cancel_locked 摘掉
        auto* const sqe = ring_.get_sqe();
        if (sqe == nullptr) return; // 仍然满
        pending_cancels_.pop_back();
        op->cancel_pending = false;
        sqe->opcode = op->cancel_opcode();
        sqe->fd = -1;
        sqe->addr = user_data_of(op);
        sqe->user_data = cancel_marker;
    }
    ring_.flush();
}

void uring_backend::cancel(uring_op& op) noexcept {
    auto complete_now = false;
    auto wake = false;
    {
        std::lock_guard<std::mutex> lock{mutex_};
        if (op.in_flight) {
            submit_cancel_locked(op);
            wake = waiting_;
        } else if (op.deferred) {
            remove_deferred_locked(op);
            complete_now = true;
        } else {
            op.cancel_requested = true;
        }
    }
    if (wake) interrupt();
    if (complete_now) {
        // complete() 之后不再触碰 op：续体恢复后套接字（连同 op）可能已被销毁。
        auto const counts = op.counts_as_work;
        op.on_complete(-ECANCELED, 0U);
        op.complete();
        if (counts) context_->get_executor().on_work_finished();
    }
}

// ---- 事件循环 ----

void uring_backend::interrupt() noexcept {
    std::uint64_t const one = 1U;
    static_cast<void>(::write(event_fd_, &one, sizeof(one)));
}

void uring_backend::run(long const timeout_ms) {
    auto const block = timeout_ms != 0;
    auto to_submit = 0U;
    auto inflight = std::size_t{};
    {
        std::lock_guard<std::mutex> lock{mutex_};
        if (not ring_.enabled()) {
            // R_DISABLED 的环由第一个运行事件循环的线程启用——它成为唯一的提交者。
            auto const rc = ring_.enable();
            if (rc < 0) throw std::system_error{-rc, std::system_category(), "io_uring_register(ENABLE_RINGS)"};
        }
        drain_deferred_locked();
        flush_pending_cancels_locked();
        to_submit = ring_.flush();
        inflight = inflight_;
        if (block) waiting_ = true;
    }

    __kernel_timespec ts{};
    if (timeout_ms > 0) {
        ts.tv_sec = timeout_ms / 1000;
        ts.tv_nsec = (timeout_ms % 1000) * 1000000L;
    }

    // 一次进内核：提交 + 等待。已有完成且无待提交时连这一次也省掉。
    auto rc = 0;
    if (block) {
        auto const nothing_ready = ring_.ready() == 0U;
        if (to_submit != 0U || nothing_ready)
            rc = ring_.enter(to_submit, nothing_ready ? 1U : 0U, timeout_ms > 0 ? &ts : nullptr);
    } else if (to_submit != 0U || (ring_.defer_taskrun() && inflight != 0U)) {
        // DEFER_TASKRUN 下完成只在 GETEVENTS 边界交付：poll 模式有在飞操作时也要进一次。
        rc = ring_.enter(to_submit, 0U, nullptr);
    }
    if (block) {
        std::lock_guard<std::mutex> lock{mutex_};
        waiting_ = false;
    }
    if (rc < 0) throw std::system_error{-rc, std::system_category(), "io_uring_enter"};

    completed_.clear();
    rearm_.clear();
    reaped_.clear();
    io_uring_cqe const* cqe = nullptr;
    while (ring_.peek(cqe)) {
        if (cqe->user_data != cancel_marker) reaped_.push_back(reaped_cqe{op_of(cqe->user_data), cqe->res, cqe->flags});
        ring_.advance();
    }

    // 先取一次环锁再读操作：提交者在环锁内写完操作参数后 unlock，这里的 lock 与之建立
    // happens-before（内核发布 CQE 那一跳对 C++ 内存模型不可见）。常驻 / 多发操作的 on_complete
    // 与 rearm() 直接在锁内进行：它们的拥有者可能正在另一个线程上退役它们（retire 也在锁内）。
    {
        std::lock_guard<std::mutex> lock{mutex_};
        for (auto const& entry : reaped_) {
            auto* const op = entry.op;
            auto const more = (entry.flags & IORING_CQE_F_MORE) != 0U;
            if (op->cancel_pending && not more) forget_pending_cancel_locked(*op); // 自己完成了：取消不必再发，且 op 随后可能被销毁
            if (not op->persistent) {
                op->in_flight = false;
                --inflight_;
                completed_.push_back(entry);
                continue;
            }
            op->on_complete(entry.res, entry.flags);
            if (more) continue; // 仍在武装
            op->in_flight = false;
            if (op->retired) continue; // 下面统一删除
            if (not shut_down_ && op->rearm()) rearm_.push_back(op);
        }
        for (auto* const op : rearm_)
            if (not try_submit_locked(*op)) push_deferred_locked(*op);
        rearm_.clear();
        drain_deferred_locked();
        flush_pending_cancels_locked();
        ring_.flush(); // 下一次 run() 一并提交
        // 退役操作：终止 CQE 已到（不在飞、不在延迟队列）就删除。
        for (auto it = retired_.begin(); it != retired_.end();) {
            if (not (*it)->in_flight && not (*it)->deferred)
                it = retired_.erase(it);
            else
                ++it;
        }
    }
    // 普通操作：锁外记录结果并交出续体。
    for (auto const& entry : completed_)
        entry.op->on_complete(entry.res, entry.flags);
    auto const executor = context_->get_executor();
    for (auto const& entry : completed_) {
        auto const counts = entry.op->counts_as_work; // complete() 之后不再触碰 op
        entry.op->complete();
        if (counts) executor.on_work_finished();
    }
    completed_.clear();
}

} // namespace detail
} // namespace net
