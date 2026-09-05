#include "net/timer.hpp"

#include "co2/contract.hpp"

#include "net/continuation.hpp"
#include "net/detail/storage.hpp"
#include "net/error.hpp"
#include "net/io_context.hpp"

#include "detail/reactor.hpp"

namespace net {
namespace detail {

struct timer_impl;

struct cancel_timer_wait {
    timer_impl* impl;
    void operator()() const noexcept;
};

struct timer_impl final : timer_op {
    explicit timer_impl(io_context& context_) noexcept
        : context{&context_}, reactor_{&io_context_access::get_reactor(context_)} {}

    ~timer_impl() override { CO2_CONTRACT_CHECK(not pending); }

    void complete() noexcept override { env->executor.post(cont); }

    std::size_t cancel_wait() noexcept { return reactor_->cancel_timer(*this) ? 1U : 0U; }

    io_context* context;
    reactor* reactor_;
    continuation cont;
    io_env const* env = nullptr;
    bool pending = false;
    late_init<stop_callback<cancel_timer_wait>> stop_cb;
};

void cancel_timer_wait::operator()() const noexcept { impl->reactor_->cancel_timer(*impl); }

} // namespace detail

// ---- awaiter ----

bool timer_wait_awaitable::await_ready() noexcept {
    if (impl->expiry <= steady_timer::clock_type::now()) {
        impl->ec.clear();
        return true;
    }
    return false;
}

coroutine_handle<> timer_wait_awaitable::await_suspend(coroutine_handle<> const h,
                                                       io_env const* const env) noexcept {
    impl->cont.h = h;
    impl->env = env;
    impl->pending = true;
    if (env->stop_token.stop_requested()) {
        impl->ec = make_error_code(error::operation_aborted);
        return h;
    }
    if (env->stop_token.stop_possible())
        impl->stop_cb.emplace(env->stop_token, detail::cancel_timer_wait{impl});
    impl->reactor_->add_timer(*impl);
    if (env->stop_token.stop_requested()) impl->reactor_->cancel_timer(*impl);
    return noop_coroutine();
}

io_result<> timer_wait_awaitable::await_resume() noexcept {
    impl->stop_cb.reset();
    impl->pending = false;
    impl->env = nullptr;
    return io_result<>{impl->ec};
}

// ---- steady_timer ----

steady_timer::steady_timer(io_context& context) : impl_{new detail::timer_impl{context}} {
    impl_->expiry = clock_type::now();
}

steady_timer::steady_timer(io_context& context, time_point const& expiry)
    : impl_{new detail::timer_impl{context}} {
    impl_->expiry = expiry;
}

steady_timer::steady_timer(io_context& context, duration const& expiry_from_now)
    : impl_{new detail::timer_impl{context}} {
    impl_->expiry = clock_type::now() + expiry_from_now;
}

steady_timer::steady_timer(steady_timer&&) noexcept = default;
steady_timer& steady_timer::operator=(steady_timer&&) noexcept = default;
steady_timer::~steady_timer() = default;

io_context& steady_timer::context() const noexcept { return *impl_->context; }

steady_timer::time_point steady_timer::expiry() const noexcept { return impl_->expiry; }

std::size_t steady_timer::expires_at(time_point const& expiry) noexcept {
    auto const cancelled = impl_->cancel_wait();
    impl_->expiry = expiry;
    return cancelled;
}

std::size_t steady_timer::expires_after(duration const& expiry_from_now) noexcept {
    return expires_at(clock_type::now() + expiry_from_now);
}

std::size_t steady_timer::cancel() noexcept { return impl_->cancel_wait(); }

timer_wait_awaitable steady_timer::wait() noexcept {
    CO2_CONTRACT_CHECK(not impl_->pending);
    return timer_wait_awaitable{impl_.get()};
}

} // namespace net
