#include "detail/heap_timer.hpp"

#include "co2/contract.hpp"

#include "net/error.hpp"
#include "net/io_context.hpp"

namespace net {
namespace detail {

void cancel_heap_timer::operator()() const noexcept { impl->scheduler().cancel_timer(*impl, true); }

heap_timer::heap_timer(io_context& context, timer_scheduler& scheduler) noexcept
    : context_{&context}, scheduler_{&scheduler} {
    timer_op::expiry = std::chrono::steady_clock::now();
}

heap_timer::~heap_timer() { CO2_CONTRACT_CHECK(not pending_); }

std::size_t heap_timer::expires_at(time_point const expiry) noexcept {
    auto const cancelled = cancel();
    timer_op::expiry = expiry;
    return cancelled;
}

std::size_t heap_timer::cancel() noexcept { return scheduler_->cancel_timer(*this, false) ? 1U : 0U; }

bool heap_timer::ready() noexcept {
    if (timer_op::expiry <= std::chrono::steady_clock::now()) {
        ec.clear();
        return true;
    }
    return false;
}

coroutine_handle<> heap_timer::suspend(coroutine_handle<> const h, io_env const* const env) noexcept {
    cont_.h = h;
    env_ = env;
    pending_ = true;
    if (env->stop_token.stop_requested()) {
        ec = make_error_code(error::operation_aborted);
        return h;
    }
    if (env->stop_token.stop_possible()) stop_cb_.emplace(env->stop_token, cancel_heap_timer{this});
    if (scheduler_->add_timer(*this)) return h; // 停止请求先到：已同步中止
    return noop_coroutine();                    // add_timer 之后不再碰 env / this
}

io_result<> heap_timer::finish() noexcept {
    stop_cb_.reset();
    cancel_requested = false;
    pending_ = false;
    env_ = nullptr;
    return io_result<>{ec};
}

} // namespace detail
} // namespace net
