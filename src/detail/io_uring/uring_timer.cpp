#include "detail/io_uring/uring_timer.hpp"

#include <cerrno>
#include <chrono>

#include "co2/contract.hpp"

#include "net/error.hpp"
#include "net/io_context.hpp"

#include "detail/io_uring/uring_backend.hpp"

namespace net {
namespace detail {

void cancel_uring_timer::operator()() const noexcept { impl->backend().cancel(*impl); }

uring_timer::uring_timer(io_context& context, uring_backend& backend) noexcept
    : context_{&context}, backend_{&backend}, expiry_{std::chrono::steady_clock::now()} {}

uring_timer::~uring_timer() { CO2_CONTRACT_CHECK(not pending_); }

std::size_t uring_timer::expires_at(time_point const expiry) noexcept {
    auto const cancelled = cancel();
    expiry_ = expiry;
    return cancelled;
}

std::size_t uring_timer::cancel() noexcept {
    // 取消是异步的：这里只能报告"有一个挂起的 wait 被请求取消"。
    if (not pending_) return 0U;
    backend_->cancel(*this);
    return 1U;
}

bool uring_timer::ready() noexcept {
    if (expiry_ <= std::chrono::steady_clock::now()) {
        ec_.clear();
        return true;
    }
    return false;
}

coroutine_handle<> uring_timer::suspend(coroutine_handle<> const h, io_env const* const env) noexcept {
    cont_.h = h;
    env_ = env;
    pending_ = true;
    if (env->stop_token.stop_requested()) {
        ec_ = make_error_code(error::operation_aborted);
        return h;
    }
    if (env->stop_token.stop_possible()) stop_cb_.emplace(env->stop_token, cancel_uring_timer{this});
    if (not backend_->submit(*this)) {
        ec_ = make_error_code(error::operation_aborted);
        return h;
    }
    if (env->stop_token.stop_requested()) backend_->cancel(*this);
    return noop_coroutine();
}

io_result<> uring_timer::finish() noexcept {
    stop_cb_.reset();
    cancel_requested = false;
    pending_ = false;
    env_ = nullptr;
    return io_result<>{ec_};
}

void uring_timer::prepare(io_uring_sqe& sqe) noexcept {
    // steady_clock 是 CLOCK_MONOTONIC：绝对到期时间直接换算。
    auto const since_epoch = expiry_.time_since_epoch();
    auto const seconds = std::chrono::duration_cast<std::chrono::seconds>(since_epoch);
    auto const nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(since_epoch - seconds);
    ts_.tv_sec = static_cast<long long>(seconds.count());
    ts_.tv_nsec = static_cast<long long>(nanos.count());
    sqe.opcode = IORING_OP_TIMEOUT;
    sqe.fd = -1;
    sqe.addr = reinterpret_cast<std::uintptr_t>(&ts_);
    sqe.len = 1U;
    sqe.off = 0U;
    sqe.timeout_flags = IORING_TIMEOUT_ABS;
}

void uring_timer::on_complete(int const res, unsigned) noexcept {
    if (res == -ETIME || res == 0) {
        ec_.clear();
        return;
    }
    if (res == -ECANCELED) {
        ec_ = make_error_code(error::operation_aborted);
        return;
    }
    ec_ = std::error_code{-res, std::system_category()};
}

} // namespace detail
} // namespace net
