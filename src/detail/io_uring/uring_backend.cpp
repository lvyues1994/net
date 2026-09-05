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
#include "detail/io_uring/uring_timer.hpp"

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
}

void uring_backend::poller::on_complete(int const res, unsigned) noexcept {
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

uring_backend::uring_backend(execution_context& context, unsigned const entries)
    : context_{static_cast<io_context*>(&context)}, ring_{entries} {
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
    {
        std::lock_guard<std::mutex> lock{mutex_};
        if (not try_submit_locked(interrupt_poller_)) push_deferred_locked(interrupt_poller_);
        flush_locked();
    }
}

uring_backend::~uring_backend() {
    if (event_fd_ >= 0) ::close(event_fd_);
}

void uring_backend::shutdown() {
    // 未完成的操作被放弃：环随后销毁，CQE 不再被读取。
    std::lock_guard<std::mutex> lock{mutex_};
    shut_down_ = true;
    deferred_head_ = nullptr;
    deferred_tail_ = nullptr;
}

// ---- 工厂 ----

std::unique_ptr<socket_impl> uring_backend::create_socket(io_context& context) {
    return std::unique_ptr<socket_impl>{new uring_socket{context, *this}};
}

std::unique_ptr<timer_impl> uring_backend::create_timer(io_context& context) {
    return std::unique_ptr<timer_impl>{new uring_timer{context, *this}};
}

std::error_code uring_backend::register_signal_reader(int const read_fd, void (*const deliver)(int)) noexcept {
    std::lock_guard<std::mutex> lock{mutex_};
    if (signal_poller_.fd >= 0) return {};
    signal_poller_.fd = read_fd;
    signal_poller_.deliver = deliver;
    if (not try_submit_locked(signal_poller_)) push_deferred_locked(signal_poller_);
    flush_locked();
    return {};
}

// ---- 提交 / 取消 ----

bool uring_backend::try_submit_locked(uring_op& op) noexcept {
    auto* const sqe = ring_.get_sqe();
    if (sqe == nullptr) return false;
    op.prepare(*sqe);
    sqe->user_data = user_data_of(&op);
    op.in_flight = true;
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

void uring_backend::flush_locked() noexcept {
    auto const pending = ring_.flush();
    if (pending != 0U) ring_.submit(pending);
}

bool uring_backend::submit(uring_op& op) noexcept {
    {
        std::lock_guard<std::mutex> lock{mutex_};
        if (shut_down_) return false;
        if (op.cancel_requested) {
            op.cancel_requested = false;
            return false;
        }
        if (not try_submit_locked(op)) push_deferred_locked(op);
        flush_locked();
    }
    if (op.counts_as_work) context_->get_executor().on_work_started();
    return true;
}

void uring_backend::submit_cancel_locked(uring_op& op) noexcept {
    auto* const sqe = ring_.get_sqe();
    if (sqe == nullptr) return; // 环满：放弃这次取消（操作会自然完成）
    sqe->opcode = op.cancel_opcode();
    sqe->fd = -1;
    sqe->addr = user_data_of(&op);
    sqe->user_data = cancel_marker;
    flush_locked();
}

void uring_backend::cancel(uring_op& op) noexcept {
    auto complete_now = false;
    {
        std::lock_guard<std::mutex> lock{mutex_};
        if (op.in_flight) {
            submit_cancel_locked(op);
            return;
        }
        if (op.deferred) {
            remove_deferred_locked(op);
            complete_now = true;
        } else {
            op.cancel_requested = true;
        }
    }
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
    {
        std::lock_guard<std::mutex> lock{mutex_};
        drain_deferred_locked();
        flush_locked();
    }

    __kernel_timespec ts{};
    if (timeout_ms > 0) {
        ts.tv_sec = timeout_ms / 1000;
        ts.tv_nsec = (timeout_ms % 1000) * 1000000L;
    }
    auto const wait_error = ring_.wait(timeout_ms > 0 ? &ts : nullptr, timeout_ms != 0);
    if (wait_error < 0) throw std::system_error{-wait_error, std::system_category(), "io_uring_enter"};

    completed_.clear();
    rearm_.clear();
    reaped_.clear();
    io_uring_cqe const* cqe = nullptr;
    while (ring_.peek(cqe)) {
        if (cqe->user_data != cancel_marker) reaped_.push_back(reaped_cqe{op_of(cqe->user_data), cqe->res, cqe->flags});
        ring_.advance();
    }

    // 先取一次环锁再读操作：提交者在环锁内写完操作参数后 unlock，这里的 lock 与之建立
    // happens-before（内核发布 CQE 那一跳对 C++ 内存模型不可见）。
    {
        std::lock_guard<std::mutex> lock{mutex_};
        for (auto const& entry : reaped_)
            entry.op->in_flight = false;
    }
    for (auto const& entry : reaped_) {
        entry.op->on_complete(entry.res, entry.flags);
        if (entry.op->persistent)
            rearm_.push_back(entry.op);
        else
            completed_.push_back(entry.op);
    }

    {
        std::lock_guard<std::mutex> lock{mutex_};
        for (auto* const op : rearm_)
            if (not shut_down_ && not try_submit_locked(*op)) push_deferred_locked(*op);
        drain_deferred_locked();
        flush_locked();
    }

    auto const executor = context_->get_executor();
    for (auto* const op : completed_) {
        auto const counts = op->counts_as_work; // complete() 之后不再触碰 op
        op->complete();
        if (counts) executor.on_work_finished();
    }
    completed_.clear();
    rearm_.clear();
}

} // namespace detail
} // namespace net
