#include "net/timer.hpp"

#include "co2/contract.hpp"

#include "net/detail/timeout_state.hpp"
#include "net/error.hpp"
#include "net/io_context.hpp"

#include "detail/backend.hpp"

// 具体层：steady_timer 把一切转发给后端创建的 timer_impl。

namespace net {

// ---- awaiter ----

bool timer_wait_awaitable::await_ready() noexcept { return impl->ready(); }

bool timer_wait_awaitable::await_ready(io_env const* const env) noexcept {
    if (env->stop_token.stop_requested()) {
        stopped = true;
        return true;
    }
    return await_ready();
}

coroutine_handle<> timer_wait_awaitable::await_suspend(coroutine_handle<> const h,
                                                       io_env const* const env) noexcept {
    return impl->suspend(h, env);
}

io_result<> timer_wait_awaitable::await_resume() noexcept {
    if (stopped) return io_result<>{make_error_code(error::operation_aborted)};
    return impl->finish();
}

// ---- steady_timer ----

steady_timer::steady_timer(io_context& context)
    : impl_{detail::io_context_access::backend(context).create_timer(context)} {}

steady_timer::steady_timer(io_context& context, time_point const& expiry) : steady_timer{context} {
    impl_->expires_at(expiry);
}

steady_timer::steady_timer(io_context& context, duration const& expiry_from_now) : steady_timer{context} {
    impl_->expires_at(clock_type::now() + expiry_from_now);
}

steady_timer::steady_timer(steady_timer&&) noexcept = default;
steady_timer& steady_timer::operator=(steady_timer&&) noexcept = default;
steady_timer::~steady_timer() = default;

io_context& steady_timer::context() const noexcept { return impl_->context(); }

steady_timer::time_point steady_timer::expiry() const noexcept { return impl_->expiry(); }

std::size_t steady_timer::expires_at(time_point const& expiry) noexcept { return impl_->expires_at(expiry); }

std::size_t steady_timer::expires_after(duration const& expiry_from_now) noexcept {
    return impl_->expires_at(clock_type::now() + expiry_from_now);
}

std::size_t steady_timer::cancel() noexcept { return impl_->cancel(); }

timer_wait_awaitable steady_timer::wait() noexcept {
    CO2_CONTRACT_CHECK(not impl_->has_pending());
    return timer_wait_awaitable{impl_.get()};
}

// ---- timeout() 的内部入口（net/detail/timeout_state.hpp） ----

void detail::request_timer_cancel(timer_wait_awaitable const& wait) noexcept { wait.impl->request_cancel(); }

std::error_code detail::timer_completion_error(timer_wait_awaitable const& wait) noexcept {
    if (wait.stopped) return make_error_code(error::operation_aborted);
    return wait.impl->completion_error();
}

} // namespace net
