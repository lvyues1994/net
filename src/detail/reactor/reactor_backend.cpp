#include "detail/reactor/reactor_backend.hpp"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdint>

#include <sys/timerfd.h>
#include <unistd.h>

#include "co2/contract.hpp"

#include "net/error.hpp"
#include "net/io_context.hpp"

#include "detail/reactor/reactor_socket.hpp"
#include "detail/reactor/reactor_file.hpp"
#include "detail/reactor/reactor_timer.hpp"

namespace net {
namespace detail {

reactor_backend::reactor_backend(execution_context& context, std::unique_ptr<demultiplexer> demux)
    : context_{static_cast<io_context*>(&context)}, demux_{std::move(demux)} {
    CO2_CONTRACT_CHECK(demux_ != nullptr);
    events_.reserve(128U);
    completed_.reserve(128U);
    expired_.reserve(32U);
    timer_fd_ = ::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
    if (timer_fd_ < 0) throw std::system_error{errno, std::system_category(), "timerfd_create"};
    timer_state_.fd = timer_fd_;
    timer_state_.registered = true;
    if (auto const ec = demux_->add(timer_state_, read_ready_bit)) {
        ::close(timer_fd_);
        throw std::system_error{ec, "register timerfd"};
    }
}

reactor_backend::~reactor_backend() {
    if (timer_fd_ >= 0) ::close(timer_fd_);
}

void reactor_backend::shutdown() {
    // 未完成的操作被放弃：不再调用 complete()（等待它们的协程不会恢复）。
    std::lock_guard<std::mutex> lock{mutex_};
    shut_down_ = true;
    for (auto* const state : registered_) {
        state->ops[0] = nullptr;
        state->ops[1] = nullptr;
        state->registered = false;
        demux_->remove(*state);
    }
    registered_.clear();
    for (auto* const timer : timers_)
        timer->heap_index = timer_op::not_queued;
    timers_.clear();
    demux_->remove(timer_state_);
}

// ---- 工厂 ----

std::unique_ptr<socket_impl> reactor_backend::create_socket(io_context& context) {
    return std::unique_ptr<socket_impl>{new reactor_socket{context, *this}};
}

std::unique_ptr<timer_impl> reactor_backend::create_timer(io_context& context) {
    return std::unique_ptr<timer_impl>{new reactor_timer{context, *this}};
}

std::unique_ptr<file_impl> reactor_backend::create_file(io_context& context) {
    return std::unique_ptr<file_impl>{new reactor_file{context}};
}

// ---- 信号泵 ----

bool reactor_backend::signal_pump::perform() noexcept {
    for (;;) {
        int value = 0;
        auto const n = ::read(fd, &value, sizeof(value));
        if (n == static_cast<ssize_t>(sizeof(value))) {
            deliver(value);
            continue;
        }
        if (n < 0 && errno == EINTR) continue;
        return false; // EAGAIN：保持排队等待下一次可读
    }
}

std::error_code reactor_backend::register_signal_reader(int const read_fd,
                                                        void (*const deliver)(int)) noexcept {
    if (signal_state_.registered) return {};
    signal_pump_.fd = read_fd;
    signal_pump_.deliver = deliver;
    signal_pump_.counts_as_work = false;
    auto const ec = register_descriptor(signal_state_, read_fd);
    if (ec) return ec;
    start_op(signal_state_, op_direction::read, signal_pump_);
    return {};
}

// ---- 描述符 ----

std::error_code reactor_backend::register_descriptor(descriptor_state& state, int const fd) noexcept {
    std::lock_guard<std::mutex> lock{mutex_};
    if (shut_down_) return make_error_code(error::operation_aborted);
    CO2_CONTRACT_CHECK(not state.registered);
    state.fd = fd;
    state.ops[0] = nullptr;
    state.ops[1] = nullptr;
    state.ready = 0U;
    state.interest = 0U;
    state.demux_index = descriptor_state::no_index;
    // 边沿触发：一次登记全部兴趣；电平触发：从无兴趣开始，排队时再加。
    auto const initial = demux_->edge_triggered() ? (read_ready_bit | write_ready_bit) : 0U;
    auto const ec = demux_->add(state, initial);
    if (ec) return ec;
    state.interest = initial;
    state.registered = true;
    registered_.insert(&state);
    return {};
}

void reactor_backend::detach_ops(descriptor_state& state, reactor_op* (&cancelled)[2]) noexcept {
    // 锁内。
    for (auto direction = 0; direction != 2; ++direction) {
        auto* const op = state.ops[direction];
        cancelled[direction] = op;
        if (op == nullptr) continue;
        state.ops[direction] = nullptr;
        op->state = reactor_op::state_type::idle;
        op->ec = make_error_code(error::operation_aborted);
    }
}

void reactor_backend::finish_cancelled(reactor_op* const (&cancelled)[2]) noexcept {
    // 锁外。
    auto const executor = context_->get_executor();
    for (auto* const op : cancelled) {
        if (op == nullptr) continue;
        auto const counts = op->counts_as_work; // complete() 之后不再触碰 op
        op->complete();
        if (counts) executor.on_work_finished();
    }
}

void reactor_backend::deregister_descriptor(descriptor_state& state) noexcept {
    reactor_op* cancelled[2] = {nullptr, nullptr};
    {
        std::lock_guard<std::mutex> lock{mutex_};
        if (not state.registered) return;
        demux_->remove(state);
        registered_.erase(&state);
        state.registered = false;
        detach_ops(state, cancelled);
        state.ready = 0U;
        state.interest = 0U;
    }
    finish_cancelled(cancelled);
}

void reactor_backend::refresh_interest(descriptor_state& state) noexcept {
    // 锁内。边沿触发的后端不需要。
    if (demux_->edge_triggered()) return;
    auto const wanted = state.wanted();
    if (wanted == state.interest) return;
    state.interest = wanted;
    demux_->update(state, wanted);
}

bool reactor_backend::start_op(descriptor_state& state, op_direction const direction,
                               reactor_op& op) noexcept {
    auto const index = static_cast<unsigned>(direction);
    auto const bit = direction == op_direction::read ? read_ready_bit : write_ready_bit;
    {
        std::lock_guard<std::mutex> lock{mutex_};
        CO2_CONTRACT_CHECK(state.registered);
        CO2_CONTRACT_CHECK(state.ops[index] == nullptr);
        if (op.cancel_requested) {
            // 停止请求先到了：不排队，同步以 aborted 完成。
            op.cancel_requested = false;
            op.ec = make_error_code(error::operation_aborted);
            op.bytes_transferred = 0U;
            return true;
        }
        if (state.ready & bit) {
            // 上一次就绪已经到达且尚未被消费：先试一次。
            state.ready &= ~bit;
            if (op.perform()) return true;
        }
        // 工作计数在发布之前、锁内加：发布之后别的线程可能立刻完成它并 on_work_finished——
        // 计数先减后加会把 outstanding_work 打到 0（run() 提前返回 / 契约违规），而且那时 op
        // 可能已随套接字销毁。
        if (op.counts_as_work) context_->get_executor().on_work_started();
        op.state = reactor_op::state_type::queued;
        state.ops[index] = &op;
        refresh_interest(state);
    }
    return false;
}

void reactor_backend::clear_ready(descriptor_state& state, unsigned const bits) noexcept {
    std::lock_guard<std::mutex> lock{mutex_};
    state.ready &= ~bits;
}

bool reactor_backend::cancel_op(descriptor_state& state, op_direction const direction,
                                reactor_op& op) noexcept {
    auto const index = static_cast<unsigned>(direction);
    {
        std::lock_guard<std::mutex> lock{mutex_};
        if (state.ops[index] != &op) {
            // 尚未 start_op（或已完成、协程尚未恢复——finish 会清掉这个过期标记）。
            op.cancel_requested = true;
            return false;
        }
        state.ops[index] = nullptr;
        op.state = reactor_op::state_type::idle;
        op.ec = make_error_code(error::operation_aborted);
        refresh_interest(state);
    }
    auto const counts = op.counts_as_work; // complete() 之后不再触碰 op
    op.complete();
    if (counts) context_->get_executor().on_work_finished();
    return true;
}

void reactor_backend::cancel_ops(descriptor_state& state) noexcept {
    reactor_op* cancelled[2] = {nullptr, nullptr};
    {
        std::lock_guard<std::mutex> lock{mutex_};
        detach_ops(state, cancelled);
        if (state.registered) refresh_interest(state);
    }
    finish_cancelled(cancelled);
}

// ---- 事件 ----

void reactor_backend::on_ready(descriptor_state& state, unsigned const ready_bits) noexcept {
    events_.push_back(pending_event{&state, ready_bits});
}

void reactor_backend::process_event(descriptor_state& state, unsigned const ready,
                                    std::vector<completed_op>& completed) noexcept {
    // 锁内。就绪的方向：有排队操作就执行，完成则摘下交给完成列表；没有操作就记下就绪位。
    unsigned const bits[2] = {read_ready_bit, write_ready_bit};
    auto changed = false;
    for (auto direction = 0U; direction != 2U; ++direction) {
        if ((ready & bits[direction]) == 0U) continue;
        auto* const op = state.ops[direction];
        if (op == nullptr) {
            state.ready |= bits[direction];
            continue;
        }
        if (not op->perform()) continue; // 仍是 EAGAIN：保持排队
        state.ops[direction] = nullptr;
        op->state = reactor_op::state_type::idle;
        completed.push_back(completed_op{op, op->counts_as_work});
        changed = true;
    }
    if (changed) refresh_interest(state);
}

// ---- 定时器 ----

bool reactor_backend::add_timer(timer_op& op) noexcept {
    auto became_earliest = false;
    {
        std::lock_guard<std::mutex> lock{mutex_};
        CO2_CONTRACT_CHECK(op.heap_index == timer_op::not_queued);
        if (op.cancel_requested) {
            op.cancel_requested = false;
            op.ec = make_error_code(error::operation_aborted);
            return true;
        }
        context_->get_executor().on_work_started(); // 发布之前（见 start_op）
        heap_push(op);
        // 成为最早到期且有线程正阻塞在解复用器里：重新武装 timerfd，它会在新到期时刻叫醒那个
        // 线程（timerfd 在解复用器的集合里，跨线程 settime 立即生效）。没有线程在等时留给 run()
        // 进入等待前统一武装，省一次系统调用。
        became_earliest = waiting_ && timers_.front() == &op;
        if (became_earliest) arm_timer_fd_locked();
    }
    return false;
}

bool reactor_backend::cancel_timer(timer_op& op, bool const from_stop_token) noexcept {
    {
        std::lock_guard<std::mutex> lock{mutex_};
        if (op.heap_index == timer_op::not_queued) {
            // 只有 stop_token 路径会在"装好回调、尚未 add_timer"的窗口里到达；用户的 cancel()
            // 对没在等的定时器不能留下标记，否则下一次 wait() 会被误中止。
            if (from_stop_token) op.cancel_requested = true;
            return false;
        }
        heap_remove(op.heap_index);
        op.ec = make_error_code(error::operation_aborted);
    }
    op.complete();
    context_->get_executor().on_work_finished();
    return true;
}

long long reactor_backend::wait_timeout_ns(long const limit_ms) const noexcept {
    // 锁内。定时器到期由 timerfd 负责；这里只有 io_context 给的上限（run_for 的截止），以及
    //"已有定时器到期"时的 0（不必进内核等待）。
    if (not timers_.empty() && timers_.front()->expiry <= std::chrono::steady_clock::now()) return 0;
    return limit_ms < 0 ? -1LL : static_cast<long long>(limit_ms) * 1000000LL;
}

void reactor_backend::arm_timer_fd_locked() noexcept {
    // 锁内。
    if (timers_.empty()) {
        if (armed_) {
            itimerspec const disarm{};
            ::timerfd_settime(timer_fd_, 0, &disarm, nullptr);
            armed_ = false;
        }
        return;
    }
    auto const expiry = timers_.front()->expiry;
    if (armed_ && expiry == armed_expiry_) return;
    // steady_clock 在 Linux/libstdc++ 上就是 CLOCK_MONOTONIC：直接用绝对时间。
    auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(expiry.time_since_epoch()).count();
    if (ns <= 0) ns = 1; // it_value 全零表示解除武装；过去的时刻立刻到期
    itimerspec spec{};
    spec.it_value.tv_sec = static_cast<time_t>(ns / 1000000000LL);
    spec.it_value.tv_nsec = static_cast<long>(ns % 1000000000LL);
    ::timerfd_settime(timer_fd_, TFD_TIMER_ABSTIME, &spec, nullptr);
    armed_ = true;
    armed_expiry_ = expiry;
}

void reactor_backend::pop_expired_timers(std::vector<timer_op*>& expired) noexcept {
    // 锁内。
    if (timers_.empty()) return;
    auto const now = std::chrono::steady_clock::now();
    while (not timers_.empty() && timers_.front()->expiry <= now) {
        auto* const op = timers_.front();
        heap_remove(0U);
        op->ec = std::error_code{};
        expired.push_back(op);
    }
}

void reactor_backend::heap_push(timer_op& op) noexcept {
    op.heap_index = timers_.size();
    timers_.push_back(&op);
    heap_up(op.heap_index);
}

void reactor_backend::heap_remove(std::size_t const index) noexcept {
    auto* const removed = timers_[index];
    auto const last = timers_.size() - 1U;
    if (index != last) {
        heap_swap(index, last);
        timers_.pop_back();
        if (index > 0U && timers_[index]->expiry < timers_[(index - 1U) / 2U]->expiry)
            heap_up(index);
        else
            heap_down(index);
    } else {
        timers_.pop_back();
    }
    removed->heap_index = timer_op::not_queued;
}

void reactor_backend::heap_up(std::size_t index) noexcept {
    while (index > 0U) {
        auto const parent = (index - 1U) / 2U;
        if (not(timers_[index]->expiry < timers_[parent]->expiry)) break;
        heap_swap(index, parent);
        index = parent;
    }
}

void reactor_backend::heap_down(std::size_t index) noexcept {
    auto const count = timers_.size();
    for (;;) {
        auto const left = 2U * index + 1U;
        auto const right = left + 1U;
        auto smallest = index;
        if (left < count && timers_[left]->expiry < timers_[smallest]->expiry) smallest = left;
        if (right < count && timers_[right]->expiry < timers_[smallest]->expiry) smallest = right;
        if (smallest == index) break;
        heap_swap(index, smallest);
        index = smallest;
    }
}

void reactor_backend::heap_swap(std::size_t const a, std::size_t const b) noexcept {
    std::swap(timers_[a], timers_[b]);
    timers_[a]->heap_index = a;
    timers_[b]->heap_index = b;
}

// ---- 事件循环 ----

void reactor_backend::interrupt() noexcept { demux_->interrupt(); }

void reactor_backend::run(long const timeout_ms) {
    auto timeout = 0LL;
    {
        std::lock_guard<std::mutex> lock{mutex_};
        arm_timer_fd_locked();
        timeout = wait_timeout_ns(timeout_ms);
        waiting_ = timeout != 0; // 与武装在同一把锁内：之后加入的更早定时器会重新武装 timerfd
    }
    events_.clear();
    auto const wait_error = demux_->wait(timeout, *this);
    {
        std::lock_guard<std::mutex> lock{mutex_};
        waiting_ = false;
    }
    if (wait_error) throw std::system_error{wait_error, demux_->name()};

    completed_.clear();
    expired_.clear();
    {
        std::lock_guard<std::mutex> lock{mutex_};
        for (auto const& event : events_) {
            if (event.state == &timer_state_) {
                std::uint64_t expirations = 0;
                static_cast<void>(::read(timer_fd_, &expirations, sizeof(expirations)));
                armed_ = false; // 已触发：下一轮按堆顶重新武装
                continue;
            }
            if (registered_.count(event.state) == 0U) continue; // 已注销：迟到的事件
            process_event(*event.state, event.bits, completed_);
        }
        pop_expired_timers(expired_);
    }
    events_.clear();

    auto const executor = context_->get_executor();
    for (auto const& entry : completed_) {
        entry.op->complete(); // complete() 之后不再触碰 op
        if (entry.counts_as_work) executor.on_work_finished();
    }
    for (auto* const op : expired_) {
        op->complete();
        executor.on_work_finished();
    }
    completed_.clear();
    expired_.clear();
}

} // namespace detail
} // namespace net
