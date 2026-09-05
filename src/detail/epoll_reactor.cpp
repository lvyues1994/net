#include "detail/epoll_reactor.hpp"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdint>

#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include "co2/contract.hpp"

#include "net/error.hpp"

namespace net {
namespace detail {

namespace {

constexpr int max_events = 128;

unsigned ready_bits_of(std::uint32_t const events) noexcept {
    auto bits = 0U;
    if (events & (EPOLLIN | EPOLLRDHUP | EPOLLERR | EPOLLHUP)) bits |= 1U;
    if (events & (EPOLLOUT | EPOLLERR | EPOLLHUP)) bits |= 2U;
    return bits;
}

} // namespace

epoll_reactor::epoll_reactor(execution_context& context)
    : context_{static_cast<io_context*>(&context)} {
    epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ < 0) throw std::system_error{errno, std::system_category(), "epoll_create1"};
    event_fd_ = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    if (event_fd_ < 0) {
        auto const saved = errno;
        ::close(epoll_fd_);
        throw std::system_error{saved, std::system_category(), "eventfd"};
    }
    epoll_event event{};
    event.events = EPOLLIN | EPOLLET;
    event.data.ptr = nullptr; // 空指针标记中断用的 eventfd
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, event_fd_, &event) != 0) {
        auto const saved = errno;
        ::close(event_fd_);
        ::close(epoll_fd_);
        throw std::system_error{saved, std::system_category(), "epoll_ctl"};
    }
}

epoll_reactor::~epoll_reactor() {
    if (event_fd_ >= 0) ::close(event_fd_);
    if (epoll_fd_ >= 0) ::close(epoll_fd_);
}

void epoll_reactor::shutdown() {
    // 未完成的操作被放弃：不再调用 complete()（等待它们的协程不会恢复）。
    std::lock_guard<std::mutex> lock{mutex_};
    shut_down_ = true;
    for (auto* const state : registered_) {
        state->ops[0] = nullptr;
        state->ops[1] = nullptr;
        state->registered = false;
    }
    registered_.clear();
    for (auto* const timer : timers_)
        timer->heap_index = timer_op::not_queued;
    timers_.clear();
}

// ---- 描述符 ----

std::error_code epoll_reactor::register_descriptor(descriptor_state& state,
                                                   int const fd) noexcept {
    std::lock_guard<std::mutex> lock{mutex_};
    if (shut_down_) return make_error_code(error::operation_aborted);
    CO2_CONTRACT_CHECK(not state.registered);
    epoll_event event{};
    event.events = EPOLLIN | EPOLLOUT | EPOLLRDHUP | EPOLLET;
    event.data.ptr = &state;
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &event) != 0)
        return std::error_code{errno, std::system_category()};
    state.fd = fd;
    state.ops[0] = nullptr;
    state.ops[1] = nullptr;
    state.ready = 0U;
    state.registered = true;
    registered_.insert(&state);
    return {};
}

void epoll_reactor::deregister_descriptor(descriptor_state& state) noexcept {
    reactor_op* cancelled[2] = {nullptr, nullptr};
    {
        std::lock_guard<std::mutex> lock{mutex_};
        if (not state.registered) return;
        epoll_event event{};
        ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, state.fd, &event);
        registered_.erase(&state);
        state.registered = false;
        for (auto direction = 0; direction != 2; ++direction) {
            auto* const op = state.ops[direction];
            if (op == nullptr) continue;
            state.ops[direction] = nullptr;
            op->state = reactor_op::state_type::idle;
            op->ec = make_error_code(error::operation_aborted);
            cancelled[direction] = op;
        }
        state.ready = 0U;
    }
    auto const executor = context_->get_executor();
    for (auto* const op : cancelled) {
        if (op == nullptr) continue;
        op->complete();
        if (op->counts_as_work) executor.on_work_finished();
    }
}

bool epoll_reactor::start_op(descriptor_state& state, op_direction const direction,
                             reactor_op& op) noexcept {
    auto const index = static_cast<unsigned>(direction);
    auto const bit = direction == op_direction::read ? read_ready : write_ready;
    {
        std::lock_guard<std::mutex> lock{mutex_};
        CO2_CONTRACT_CHECK(state.registered);
        CO2_CONTRACT_CHECK(state.ops[index] == nullptr);
        if (state.ready & bit) {
            // 上一次边沿已经到达且尚未被消费：先试一次。
            state.ready &= ~bit;
            if (op.perform()) return true;
        }
        op.state = reactor_op::state_type::queued;
        state.ops[index] = &op;
    }
    if (op.counts_as_work) context_->get_executor().on_work_started();
    return false;
}

bool epoll_reactor::cancel_op(descriptor_state& state, op_direction const direction,
                              reactor_op& op) noexcept {
    auto const index = static_cast<unsigned>(direction);
    {
        std::lock_guard<std::mutex> lock{mutex_};
        if (state.ops[index] != &op) return false;
        state.ops[index] = nullptr;
        op.state = reactor_op::state_type::idle;
        op.ec = make_error_code(error::operation_aborted);
    }
    op.complete();
    if (op.counts_as_work) context_->get_executor().on_work_finished();
    return true;
}

void epoll_reactor::cancel_ops(descriptor_state& state) noexcept {
    reactor_op* cancelled[2] = {nullptr, nullptr};
    {
        std::lock_guard<std::mutex> lock{mutex_};
        for (auto direction = 0; direction != 2; ++direction) {
            auto* const op = state.ops[direction];
            if (op == nullptr) continue;
            state.ops[direction] = nullptr;
            op->state = reactor_op::state_type::idle;
            op->ec = make_error_code(error::operation_aborted);
            cancelled[direction] = op;
        }
    }
    auto const executor = context_->get_executor();
    for (auto* const op : cancelled) {
        if (op == nullptr) continue;
        op->complete();
        if (op->counts_as_work) executor.on_work_finished();
    }
}

void epoll_reactor::process_descriptor(descriptor_state& state, unsigned const ready,
                                       std::vector<completed_op>& completed) noexcept {
    // 锁内。就绪的方向：有排队操作就执行，完成则摘下交给完成列表；没有操作就记下就绪位。
    unsigned const bits[2] = {read_ready, write_ready};
    for (auto direction = 0U; direction != 2U; ++direction) {
        if ((ready & bits[direction]) == 0U) continue;
        auto* const op = state.ops[direction];
        if (op == nullptr) {
            state.ready |= bits[direction];
            continue;
        }
        if (not op->perform()) continue; // 仍是 EAGAIN：保持排队，等待下一个边沿
        state.ops[direction] = nullptr;
        op->state = reactor_op::state_type::idle;
        completed.push_back(completed_op{op, op->counts_as_work});
    }
}

// ---- 定时器 ----

void epoll_reactor::add_timer(timer_op& op) noexcept {
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

bool epoll_reactor::cancel_timer(timer_op& op) noexcept {
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

long epoll_reactor::timer_timeout_ms(long const limit) const noexcept {
    // 锁内。
    if (timers_.empty()) return limit;
    auto const now = std::chrono::steady_clock::now();
    auto const earliest = timers_.front()->expiry;
    if (earliest <= now) return 0;
    auto const remaining =
        std::chrono::duration_cast<std::chrono::milliseconds>(earliest - now).count() + 1;
    auto const clamped = remaining > static_cast<long long>(1L << 30) ? (1L << 30)
                                                                       : static_cast<long>(remaining);
    if (limit < 0) return clamped;
    return std::min(limit, clamped);
}

void epoll_reactor::pop_expired_timers(std::vector<timer_op*>& expired) noexcept {
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

void epoll_reactor::heap_push(timer_op& op) noexcept {
    op.heap_index = timers_.size();
    timers_.push_back(&op);
    heap_up(op.heap_index);
}

void epoll_reactor::heap_remove(std::size_t const index) noexcept {
    auto* const removed = timers_[index];
    auto const last = timers_.size() - 1U;
    if (index != last) {
        heap_swap(index, last);
        timers_.pop_back();
        // 被换上来的元素可能需要上浮或下沉。
        if (index > 0U && timers_[index]->expiry < timers_[(index - 1U) / 2U]->expiry)
            heap_up(index);
        else
            heap_down(index);
    } else {
        timers_.pop_back();
    }
    removed->heap_index = timer_op::not_queued;
}

void epoll_reactor::heap_up(std::size_t index) noexcept {
    while (index > 0U) {
        auto const parent = (index - 1U) / 2U;
        if (not(timers_[index]->expiry < timers_[parent]->expiry)) break;
        heap_swap(index, parent);
        index = parent;
    }
}

void epoll_reactor::heap_down(std::size_t index) noexcept {
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

void epoll_reactor::heap_swap(std::size_t const a, std::size_t const b) noexcept {
    std::swap(timers_[a], timers_[b]);
    timers_[a]->heap_index = a;
    timers_[b]->heap_index = b;
}

// ---- 事件循环 ----

void epoll_reactor::interrupt() noexcept {
    std::uint64_t const one = 1U;
    // 非阻塞 eventfd：计数器满时 write 返回 EAGAIN，此时已有待处理的中断，无需重试。
    static_cast<void>(::write(event_fd_, &one, sizeof(one)));
}

void epoll_reactor::run(long const timeout_ms) {
    epoll_event events[max_events];
    long timeout = 0;
    {
        std::lock_guard<std::mutex> lock{mutex_};
        timeout = timer_timeout_ms(timeout_ms);
    }
    auto const count = ::epoll_wait(epoll_fd_, events, max_events,
                                    timeout < 0 ? -1 : static_cast<int>(timeout));
    if (count < 0 && errno != EINTR)
        throw std::system_error{errno, std::system_category(), "epoll_wait"};

    std::vector<completed_op> completed;
    std::vector<timer_op*> expired;
    {
        std::lock_guard<std::mutex> lock{mutex_};
        for (auto index = 0; index < count; ++index) {
            auto* const state = static_cast<descriptor_state*>(events[index].data.ptr);
            if (state == nullptr) {
                std::uint64_t drained = 0;
                while (::read(event_fd_, &drained, sizeof(drained)) > 0) {}
                continue;
            }
            if (registered_.count(state) == 0U) continue; // 已注销：迟到的事件
            process_descriptor(*state, ready_bits_of(events[index].events), completed);
        }
        pop_expired_timers(expired);
    }

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
