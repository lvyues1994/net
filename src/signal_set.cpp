#include "net/signal_set.hpp"

#include <algorithm>
#include <cerrno>
#include <csignal>
#include <deque>
#include <mutex>
#include <vector>

#include <fcntl.h>
#include <unistd.h>

#include "co2/contract.hpp"

#include "net/continuation.hpp"
#include "net/detail/storage.hpp"
#include "net/error.hpp"
#include "net/execution_context.hpp"
#include "net/io_context.hpp"

#include "detail/reactor.hpp"

namespace net {
namespace detail {

struct signal_set_impl;

struct cancel_signal_wait {
    signal_set_impl* impl;
    void operator()() const noexcept;
};

// 进程唯一：自管道 + 每个信号注册了哪些 signal_set。
struct signal_state {
    static signal_state& instance() {
        static signal_state state;
        return state;
    }

    static void handler(int const signal_number) {
        auto const fd = instance().pipe_write_;
        if (fd < 0) return;
        auto const value = signal_number;
        // 异步信号安全：只做一次 write。
        static_cast<void>(::write(fd, &value, sizeof(value)));
    }

    std::error_code ensure_pipe() noexcept {
        if (pipe_read_ >= 0) return {};
        int fds[2];
        if (::pipe2(fds, O_NONBLOCK | O_CLOEXEC) != 0)
            return std::error_code{errno, std::system_category()};
        pipe_read_ = fds[0];
        pipe_write_ = fds[1];
        return {};
    }

    int pipe_read() const noexcept { return pipe_read_; }

    std::error_code add(signal_set_impl& set, int const signal_number) noexcept;
    std::error_code remove(signal_set_impl& set, int const signal_number) noexcept;
    void deliver(int signal_number) noexcept;

    std::mutex mutex;

  private:
    signal_state() : registrations_(NSIG), old_actions_(NSIG), installed_(NSIG, false) {}

    int pipe_read_ = -1;
    int pipe_write_ = -1;
    std::vector<std::vector<signal_set_impl*>> registrations_;
    std::vector<struct sigaction> old_actions_;
    std::vector<bool> installed_;
};

struct signal_set_impl {
    explicit signal_set_impl(io_context& context_);
    ~signal_set_impl();

    // 锁内（signal_state::mutex）。
    void on_signal(int const signal_number) noexcept {
        if (waiting) {
            waiting = false;
            ec.clear();
            delivered = signal_number;
            env->executor.post(cont);
            context->get_executor().on_work_finished();
            return;
        }
        queued.push_back(signal_number);
    }

    // 锁内。
    bool cancel_locked() noexcept {
        if (not waiting) return false;
        waiting = false;
        ec = make_error_code(error::operation_aborted);
        env->executor.post(cont);
        context->get_executor().on_work_finished();
        return true;
    }

    io_context* context;
    std::vector<int> signals;
    std::deque<int> queued;
    continuation cont;
    io_env const* env = nullptr;
    bool pending = false; // wait() 已发出且尚未 await_resume
    bool waiting = false; // 已挂起等待信号（锁内读写）
    int delivered = 0;
    std::error_code ec;
    late_init<stop_callback<cancel_signal_wait>> stop_cb;
};

// 每个 io_context 一个：把管道读端注册进反应器，用一个常驻读操作分发信号。
struct signal_service final : execution_context::service {
    explicit signal_service(execution_context& context)
        : context_{static_cast<io_context*>(&context)},
          reactor_{&io_context_access::get_reactor(*context_)} {
        auto& state = signal_state::instance();
        auto fd = -1;
        {
            // 锁序：反应器锁 → 信号状态锁（泵在反应器锁内分发）。这里不能反过来持锁注册。
            std::lock_guard<std::mutex> lock{state.mutex};
            auto const ec = state.ensure_pipe();
            if (ec) throw std::system_error{ec, "signal_set pipe"};
            fd = state.pipe_read();
        }
        pump_.fd_ = fd;
        pump_.counts_as_work = false;
        auto const registered = reactor_->register_descriptor(descriptor_, fd);
        if (registered) throw std::system_error{registered, "signal_set register"};
        reactor_->start_op(descriptor_, op_direction::read, pump_);
    }

    ~signal_service() override { deregister(); }

    void shutdown() override { deregister(); }

  private:
    struct pump_op final : reactor_op {
        bool perform() noexcept override {
            for (;;) {
                int value = 0;
                auto const n = ::read(fd_, &value, sizeof(value));
                if (n == static_cast<ssize_t>(sizeof(value))) {
                    signal_state::instance().deliver(value);
                    continue;
                }
                if (n < 0 && errno == EINTR) continue;
                return false; // EAGAIN 或异常：保持排队等待下一个边沿
            }
        }

        void complete() noexcept override {}

        int fd_ = -1;
    };

    void deregister() noexcept {
        if (descriptor_.registered) reactor_->deregister_descriptor(descriptor_);
    }

    io_context* context_;
    reactor* reactor_;
    descriptor_state descriptor_;
    pump_op pump_;
};

// ---- signal_state ----

std::error_code signal_state::add(signal_set_impl& set, int const signal_number) noexcept {
    if (signal_number < 1 || signal_number >= NSIG) return std::make_error_code(std::errc::invalid_argument);
    auto& list = registrations_[static_cast<std::size_t>(signal_number)];
    if (std::find(list.begin(), list.end(), &set) != list.end()) return {};
    if (list.empty()) {
        struct sigaction action{};
        action.sa_handler = &signal_state::handler;
        action.sa_flags = SA_RESTART;
        sigfillset(&action.sa_mask);
        if (::sigaction(signal_number, &action, &old_actions_[static_cast<std::size_t>(signal_number)]) != 0)
            return std::error_code{errno, std::system_category()};
        installed_[static_cast<std::size_t>(signal_number)] = true;
    }
    list.push_back(&set);
    return {};
}

std::error_code signal_state::remove(signal_set_impl& set, int const signal_number) noexcept {
    if (signal_number < 1 || signal_number >= NSIG) return std::make_error_code(std::errc::invalid_argument);
    auto& list = registrations_[static_cast<std::size_t>(signal_number)];
    auto const position = std::find(list.begin(), list.end(), &set);
    if (position == list.end()) return {};
    list.erase(position);
    if (list.empty() && installed_[static_cast<std::size_t>(signal_number)]) {
        ::sigaction(signal_number, &old_actions_[static_cast<std::size_t>(signal_number)], nullptr);
        installed_[static_cast<std::size_t>(signal_number)] = false;
    }
    return {};
}

void signal_state::deliver(int const signal_number) noexcept {
    if (signal_number < 1 || signal_number >= NSIG) return;
    std::lock_guard<std::mutex> lock{mutex};
    for (auto* const set : registrations_[static_cast<std::size_t>(signal_number)])
        set->on_signal(signal_number);
}

// ---- signal_set_impl ----

signal_set_impl::signal_set_impl(io_context& context_) : context{&context_} {
    context_.use_service<signal_service>();
}

signal_set_impl::~signal_set_impl() { CO2_CONTRACT_CHECK(not pending); }

void cancel_signal_wait::operator()() const noexcept {
    auto& state = signal_state::instance();
    std::lock_guard<std::mutex> lock{state.mutex};
    impl->cancel_locked();
}

} // namespace detail

// ---- awaiter ----

bool signal_wait_awaitable::await_ready() noexcept {
    impl->pending = true;
    auto& state = detail::signal_state::instance();
    std::lock_guard<std::mutex> lock{state.mutex};
    if (impl->queued.empty()) return false;
    impl->delivered = impl->queued.front();
    impl->queued.pop_front();
    impl->ec.clear();
    return true;
}

coroutine_handle<> signal_wait_awaitable::await_suspend(coroutine_handle<> const h,
                                                        io_env const* const env) noexcept {
    impl->cont.h = h;
    impl->env = env;
    if (env->stop_token.stop_requested()) {
        impl->ec = make_error_code(error::operation_aborted);
        return h;
    }
    auto& state = detail::signal_state::instance();
    {
        std::lock_guard<std::mutex> lock{state.mutex};
        // 在 await_ready 与这里之间可能已有信号到达。
        if (not impl->queued.empty()) {
            impl->delivered = impl->queued.front();
            impl->queued.pop_front();
            impl->ec.clear();
            return h;
        }
        impl->waiting = true;
        impl->context->get_executor().on_work_started();
    }
    if (env->stop_token.stop_possible())
        impl->stop_cb.emplace(env->stop_token, detail::cancel_signal_wait{impl});
    return noop_coroutine();
}

io_result<int> signal_wait_awaitable::await_resume() noexcept {
    impl->stop_cb.reset();
    impl->pending = false;
    impl->env = nullptr;
    return io_result<int>{impl->ec, impl->delivered};
}

// ---- signal_set ----

signal_set::signal_set(io_context& context) : impl_{new detail::signal_set_impl{context}} {}

signal_set::signal_set(io_context& context, int const signal_number_1) : signal_set{context} {
    auto const ec = add(signal_number_1);
    if (ec) throw std::system_error{ec, "signal_set::add"};
}

signal_set::signal_set(io_context& context, int const signal_number_1, int const signal_number_2)
    : signal_set{context, signal_number_1} {
    auto const ec = add(signal_number_2);
    if (ec) throw std::system_error{ec, "signal_set::add"};
}

signal_set::signal_set(io_context& context, int const signal_number_1, int const signal_number_2,
                       int const signal_number_3)
    : signal_set{context, signal_number_1, signal_number_2} {
    auto const ec = add(signal_number_3);
    if (ec) throw std::system_error{ec, "signal_set::add"};
}

signal_set::signal_set(signal_set&&) noexcept = default;
signal_set& signal_set::operator=(signal_set&&) noexcept = default;

signal_set::~signal_set() {
    if (impl_ != nullptr) clear();
}

io_context& signal_set::context() const noexcept { return *impl_->context; }

std::error_code signal_set::add(int const signal_number) noexcept {
    auto& state = detail::signal_state::instance();
    std::lock_guard<std::mutex> lock{state.mutex};
    auto const ec = state.add(*impl_, signal_number);
    if (not ec &&
        std::find(impl_->signals.begin(), impl_->signals.end(), signal_number) == impl_->signals.end())
        impl_->signals.push_back(signal_number);
    return ec;
}

std::error_code signal_set::remove(int const signal_number) noexcept {
    auto& state = detail::signal_state::instance();
    std::lock_guard<std::mutex> lock{state.mutex};
    auto const ec = state.remove(*impl_, signal_number);
    auto const position = std::find(impl_->signals.begin(), impl_->signals.end(), signal_number);
    if (position != impl_->signals.end()) impl_->signals.erase(position);
    return ec;
}

std::error_code signal_set::clear() noexcept {
    auto& state = detail::signal_state::instance();
    std::lock_guard<std::mutex> lock{state.mutex};
    auto result = std::error_code{};
    for (auto const signal_number : impl_->signals) {
        auto const ec = state.remove(*impl_, signal_number);
        if (ec && not result) result = ec;
    }
    impl_->signals.clear();
    impl_->queued.clear();
    return result;
}

std::size_t signal_set::cancel() noexcept {
    auto& state = detail::signal_state::instance();
    std::lock_guard<std::mutex> lock{state.mutex};
    return impl_->cancel_locked() ? 1U : 0U;
}

signal_wait_awaitable signal_set::wait() noexcept {
    CO2_CONTRACT_CHECK(not impl_->pending);
    return signal_wait_awaitable{impl_.get()};
}

} // namespace net
