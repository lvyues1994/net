#include "detail/reactor/reactor_timer.hpp"

#include "co2/contract.hpp"

#include "net/error.hpp"
#include "net/io_context.hpp"

#include "detail/reactor/reactor_backend.hpp"

namespace net {
namespace detail {

void cancel_reactor_timer::operator()() const noexcept { impl->backend().cancel_timer(*impl); }

reactor_timer::reactor_timer(io_context& context, reactor_backend& backend) noexcept
    : context_{&context}, backend_{&backend} {
    timer_op::expiry = std::chrono::steady_clock::now();
}

reactor_timer::~reactor_timer() { CO2_CONTRACT_CHECK(not pending_); }

std::size_t reactor_timer::expires_at(time_point const expiry) noexcept {
    auto const cancelled = cancel();
    timer_op::expiry = expiry;
    return cancelled;
}

std::size_t reactor_timer::cancel() noexcept { return backend_->cancel_timer(*this) ? 1U : 0U; }

bool reactor_timer::ready() noexcept {
    if (timer_op::expiry <= std::chrono::steady_clock::now()) {
        ec.clear();
        return true;
    }
    return false;
}

coroutine_handle<> reactor_timer::suspend(coroutine_handle<> const h, io_env const* const env) noexcept {
    cont_.h = h;
    env_ = env;
    pending_ = true;
    if (env->stop_token.stop_requested()) {
        ec = make_error_code(error::operation_aborted);
        return h;
    }
    if (env->stop_token.stop_possible()) stop_cb_.emplace(env->stop_token, cancel_reactor_timer{this});
    backend_->add_timer(*this);
    if (env->stop_token.stop_requested()) backend_->cancel_timer(*this);
    return noop_coroutine();
}

io_result<> reactor_timer::finish() noexcept {
    stop_cb_.reset();
    pending_ = false;
    env_ = nullptr;
    return io_result<>{ec};
}

} // namespace detail
} // namespace net
