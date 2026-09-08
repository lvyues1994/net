#include "detail/timer_heap.hpp"

#include <utility>

namespace net {
namespace detail {

void timer_heap::push(timer_op& op) noexcept {
    op.heap_index = timers_.size();
    timers_.push_back(&op);
    up(op.heap_index);
}

void timer_heap::remove(std::size_t const index) noexcept {
    auto* const removed = timers_[index];
    auto const last = timers_.size() - 1U;
    if (index != last) {
        swap_at(index, last);
        timers_.pop_back();
        if (index > 0U && timers_[index]->expiry < timers_[(index - 1U) / 2U]->expiry)
            up(index);
        else
            down(index);
    } else {
        timers_.pop_back();
    }
    removed->heap_index = timer_op::not_queued;
}

void timer_heap::clear() noexcept {
    for (auto* const op : timers_)
        op->heap_index = timer_op::not_queued;
    timers_.clear();
}

void timer_heap::pop_expired(std::chrono::steady_clock::time_point const now, std::vector<timer_op*>& expired) noexcept {
    while (not timers_.empty() && timers_.front()->expiry <= now) {
        auto* const op = timers_.front();
        remove(0U);
        op->ec = std::error_code{};
        expired.push_back(op);
    }
}

void timer_heap::up(std::size_t index) noexcept {
    while (index > 0U) {
        auto const parent = (index - 1U) / 2U;
        if (not(timers_[index]->expiry < timers_[parent]->expiry)) break;
        swap_at(index, parent);
        index = parent;
    }
}

void timer_heap::down(std::size_t index) noexcept {
    auto const count = timers_.size();
    for (;;) {
        auto const left = 2U * index + 1U;
        auto const right = left + 1U;
        auto smallest = index;
        if (left < count && timers_[left]->expiry < timers_[smallest]->expiry) smallest = left;
        if (right < count && timers_[right]->expiry < timers_[smallest]->expiry) smallest = right;
        if (smallest == index) break;
        swap_at(index, smallest);
        index = smallest;
    }
}

void timer_heap::swap_at(std::size_t const a, std::size_t const b) noexcept {
    std::swap(timers_[a], timers_[b]);
    timers_[a]->heap_index = a;
    timers_[b]->heap_index = b;
}

} // namespace detail
} // namespace net
