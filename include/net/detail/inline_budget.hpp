#pragma once

#include "net/config.hpp"

// 内联完成预算（Corosio 的 try_consume_inline_budget 同义）。推测执行让已就绪的传输在 await_ready 里
// 直接完成，协程不经调度器就继续——快，但一条永远就绪的连接可以让同一上下文的其它协程饿着。预算是
// 线程局部计数：safe_resume 恢复一个协程时重置为 NET_INLINE_COMPLETION_BUDGET，每次同步完成消耗一份；
// 用完后 awaiter 把本已完成的操作改为经执行器 post 恢复（结果不变，只多一次调度器往返），其它续体得以
// 插进来。单连接 ping-pong 每次恢复只消耗一两份，永远用不完，不付代价。

namespace net {
namespace detail {

constexpr unsigned inline_completion_budget = NET_INLINE_COMPLETION_BUDGET;

inline unsigned& inline_budget_slot() noexcept {
    static thread_local unsigned budget = inline_completion_budget;
    return budget;
}

inline void reset_inline_budget() noexcept { inline_budget_slot() = inline_completion_budget; }

// 消耗一份预算；返回 false 表示用完（预算为 0 时表示关闭，恒为 true）。
inline bool try_consume_inline_budget() noexcept {
    if (inline_completion_budget == 0U) return true;
    auto& budget = inline_budget_slot();
    if (budget == 0U) return false;
    --budget;
    return true;
}

} // namespace detail
} // namespace net
