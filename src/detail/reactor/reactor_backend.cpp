#include "detail/reactor/reactor_backend.hpp"

#include <algorithm>
#include <cerrno>
#include <chrono>

#include <unistd.h>

#include "co2/contract.hpp"

#include "net/error.hpp"
#include "net/io_context.hpp"

#include "detail/reactor/reactor_socket.hpp"
#include "detail/reactor/reactor_timer.hpp"

namespace net {
namespace detail {

reactor_backend::reactor_backend(execution_context& context, std::unique_ptr<demultiplexer> demux)
    : context_{static_cast<io_context*>(&context)}, demux_{std::move(demux)} {
    CO2_CONTRACT_CHECK(demux_ != nullptr);
    events_.reserve(128U);
}

reactor_backend::~reactor_backend() = default;

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
}

// ---- 工厂 ----

std::unique_ptr<socket_impl> reactor_backend::create_socket(io_context& context) {
    return std::unique_ptr<socket_impl>{new reactor_socket{context, *this}};
}

std::unique_ptr<timer_impl> reactor_backend::create_timer(io_context& context) {
    return std::unique_ptr<timer_impl>{new reactor_timer{context, *this}};
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
        if (state.ready & bit) {
            // 上一次就绪已经到达且尚未被消费：先试一次。
            state.ready &= ~bit;
            if (op.perform()) return true;
        }
        op.state = reactor_op::state_type::queued;
        state.ops[index] = &op;
        refresh_interest(state);
    }
    if (op.counts_as_work) context_->get_executor().on_work_started();
    return false;
}

bool reactor_backend::cancel_op(descriptor_state& state, op_direction const direction,
                                reactor_op& op) noexcept {
    auto const index = static_cast<unsigned>(direction);
    {
        std::lock_guard<std::mutex> lock{mutex_};
        if (state.ops[index] != &op) return false;
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

void reactor_backend::add_timer(timer_op& op) noexcept {
    auto became_earliest = false;
    {
        std::lock_guard<std::mutex> lock{mutex_};
        CO2_CONTRACT_CHECK(op.heap_index == timer_op::not_queued);
        heap_push(op);
        became_earliest = timers_.front() == &op;
    }
    context_->get_executor().on_work_started();
    if (became_earliest) interrupt();
}

bool reactor_backend::cancel_timer(timer_op& op) noexcept {
    {
        std::lock_guard<std::mutex> lock{mutex_};
        if (op.heap_index == timer_op::not_queued) return false;
        heap_remove(op.heap_index);
        op.ec = make_error_code(error::operation_aborted);
    }
    op.complete();
    context_->get_executor().on_work_finished();
    return true;
}

long reactor_backend::timer_timeout_ms(long const limit) const noexcept {
    // 锁内。
    if (timers_.empty()) return limit;
    auto const now = std::chrono::steady_clock::now();
    auto const earliest = timers_.front()->expiry;
    if (earliest <= now) return 0;
    auto const remaining =
        std::chrono::duration_cast<std::chrono::milliseconds>(earliest - now).count() + 1;
    auto const clamped =
        remaining > static_cast<long long>(1L << 30) ? (1L << 30) : static_cast<long>(remaining);
    if (limit < 0) return clamped;
    return std::min(limit, clamped);
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
    long timeout = 0;
    {
        std::lock_guard<std::mutex> lock{mutex_};
        timeout = timer_timeout_ms(timeout_ms);
    }
    events_.clear();
    auto const wait_error = demux_->wait(timeout, *this);
    if (wait_error) throw std::system_error{wait_error, demux_->name()};

    std::vector<completed_op> completed;
    std::vector<timer_op*> expired;
    {
        std::lock_guard<std::mutex> lock{mutex_};
        for (auto const& event : events_) {
            if (registered_.count(event.state) == 0U) continue; // 已注销：迟到的事件
            process_event(*event.state, event.bits, completed);
        }
        pop_expired_timers(expired);
    }
    events_.clear();

    auto const executor = context_->get_executor();
    for (auto const& entry : completed) {
        entry.op->complete();
        if (entry.counts_as_work) executor.on_work_finished();
    }
    for (auto* const op : expired) {
        op->complete();
        executor.on_work_finished();
    }
}

} // namespace detail
} // namespace net
