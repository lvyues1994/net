#include "net/signal_set.hpp"

#include <algorithm>
#include <cerrno>
#include <csignal>
#include <deque>
#include <mutex>
#include <vector>

#if NET_PLATFORM_WINDOWS
#include "net/detail/socket_types.hpp"
#else
#include <fcntl.h>
#include <unistd.h>
#endif

#include "co2/contract.hpp"

#include "net/continuation.hpp"
#include "net/detail/storage.hpp"
#include "net/error.hpp"
#include "net/execution_context.hpp"
#include "net/io_context.hpp"

#include "detail/backend.hpp"

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

#if NET_PLATFORM_WINDOWS
    // Windows 的信号处理函数在别的线程上跑（SIGINT 来自控制台控制线程），没有异步信号安全的顾虑；
    // "自管道"是一对回环 TCP 套接字：写端 send，读端交给 IOCP 后端做重叠接收。一个套接字只能挂到
    // 一个完成端口，所以每个 io_context 一对（写端登记在 writers_ 里，处理函数向全部写端发送）。
    static void handler(int const signal_number) {
        auto const value = signal_number;
        {
            auto& self = instance();
            std::lock_guard<std::mutex> lock{self.writers_mutex_};
            for (auto const s : self.writers_)
                static_cast<void>(::send(s, reinterpret_cast<char const*>(&value), sizeof(value), 0));
        }
        ::signal(signal_number, &handler); // CRT 在投递后把处理函数复位为 SIG_DFL
    }

    // 新建一对；读端交给调用方（后端拥有并关闭），写端登记。
    std::error_code create_pipe(native_socket_type& read_end, native_socket_type& write_end) noexcept {
        ensure_networking_initialized();
        auto const listener = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        if (not socket_is_valid(listener)) return last_socket_error();
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = 0;
        auto length = static_cast<int>(sizeof(address));
        auto writer = invalid_socket;
        auto reader = invalid_socket;
        auto ec = std::error_code{};
        if (::bind(listener, reinterpret_cast<sockaddr*>(&address), length) != 0 || ::listen(listener, 1) != 0 ||
            ::getsockname(listener, reinterpret_cast<sockaddr*>(&address), &length) != 0) {
            ec = last_socket_error();
        } else {
            writer = ::WSASocketW(AF_INET, SOCK_STREAM, IPPROTO_TCP, nullptr, 0, 0); // 写端不重叠：同步 send
            if (not socket_is_valid(writer) || ::connect(writer, reinterpret_cast<sockaddr*>(&address), length) != 0) {
                ec = last_socket_error();
            } else {
                reader = ::accept(listener, nullptr, nullptr); // 继承监听套接字的重叠属性
                if (not socket_is_valid(reader)) ec = last_socket_error();
            }
        }
        ::closesocket(listener);
        if (ec) {
            if (socket_is_valid(writer)) ::closesocket(writer);
            if (socket_is_valid(reader)) ::closesocket(reader);
            return ec;
        }
        {
            std::lock_guard<std::mutex> lock{writers_mutex_};
            writers_.push_back(writer);
        }
        read_end = reader;
        write_end = writer;
        return {};
    }

    void destroy_pipe(native_socket_type const write_end) noexcept {
        {
            std::lock_guard<std::mutex> lock{writers_mutex_};
            writers_.erase(std::remove(writers_.begin(), writers_.end(), write_end), writers_.end());
        }
        ::closesocket(write_end);
    }
#else
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
    native_socket_type pipe_read() const noexcept { return pipe_read_; }
#endif

    std::error_code add(signal_set_impl& set, int const signal_number) noexcept;
    std::error_code remove(signal_set_impl& set, int const signal_number) noexcept;
    void deliver(int signal_number) noexcept;

    std::mutex mutex;

  private:
    signal_state() : registrations_(NSIG), old_actions_(NSIG), installed_(NSIG, false) {}

#if NET_PLATFORM_WINDOWS
    std::mutex writers_mutex_;
    std::vector<native_socket_type> writers_;
#else
    native_socket_type pipe_read_ = invalid_socket;
    native_socket_type pipe_write_ = invalid_socket;
#endif
    std::vector<std::vector<signal_set_impl*>> registrations_;
#if NET_PLATFORM_WINDOWS
    using old_action = void (*)(int);
#else
    using old_action = struct sigaction;
#endif
    std::vector<old_action> old_actions_;
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

    // 锁内。from_stop_token：停止请求在"回调装好、尚未挂起"的窗口里到达时记为 cancel_requested，
    // await_suspend 看到即中止；用户的 cancel() 对没在等的 set 不能留下标记，否则下一次 wait()
    // 会被误中止。
    bool cancel_locked(bool const from_stop_token) noexcept {
        if (not waiting) {
            if (from_stop_token) cancel_requested = true;
            return false;
        }
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
    bool cancel_requested = false; // 停止请求在挂起之前到达（锁内读写）
    int delivered = 0;
    std::error_code ec;
    late_init<stop_callback<cancel_signal_wait>> stop_cb;
};

// 每个 io_context 一个：请求后端监视信号管道的读端。可读时后端排空它并对每个信号号
// 调用 deliver（就绪型后端用一个常驻读操作；完成型后端可用 poll-add / 等待对象）。
struct signal_service final : execution_context::service {
    explicit signal_service(execution_context& context) {
        auto& state = signal_state::instance();
        auto fd = invalid_socket;
#if NET_PLATFORM_WINDOWS
        {
            auto const ec = state.create_pipe(fd, write_end_);
            if (ec) throw std::system_error{ec, "signal_set pipe"};
        }
#else
        {
            std::lock_guard<std::mutex> lock{state.mutex};
            auto const ec = state.ensure_pipe();
            if (ec) throw std::system_error{ec, "signal_set pipe"};
            fd = state.pipe_read();
        }
#endif
        auto const registered = io_context_access::backend(static_cast<io_context&>(context))
                                    .register_signal_reader(fd, &signal_service::deliver);
        if (registered) {
#if NET_PLATFORM_WINDOWS
            state.destroy_pipe(write_end_);
            write_end_ = invalid_socket;
            ::closesocket(fd);
#endif
            throw std::system_error{registered, "signal_set register"};
        }
    }

#if NET_PLATFORM_WINDOWS
    ~signal_service() override {
        if (socket_is_valid(write_end_)) signal_state::instance().destroy_pipe(write_end_);
    }
#endif

    void shutdown() override {}

  private:
    static void deliver(int const signal_number) noexcept {
        signal_state::instance().deliver(signal_number);
    }
#if NET_PLATFORM_WINDOWS
    native_socket_type write_end_ = invalid_socket;
#endif
};

// ---- signal_state ----

std::error_code signal_state::add(signal_set_impl& set, int const signal_number) noexcept {
    if (signal_number < 1 || signal_number >= NSIG) return std::make_error_code(std::errc::invalid_argument);
    auto& list = registrations_[static_cast<std::size_t>(signal_number)];
    if (std::find(list.begin(), list.end(), &set) != list.end()) return {};
    if (list.empty()) {
#if NET_PLATFORM_WINDOWS
        auto const previous = ::signal(signal_number, &signal_state::handler);
        if (previous == SIG_ERR) return std::make_error_code(std::errc::invalid_argument);
        old_actions_[static_cast<std::size_t>(signal_number)] = previous;
#else
        struct sigaction action{};
        action.sa_handler = &signal_state::handler;
        action.sa_flags = SA_RESTART;
        sigfillset(&action.sa_mask);
        if (::sigaction(signal_number, &action, &old_actions_[static_cast<std::size_t>(signal_number)]) != 0)
            return std::error_code{errno, std::system_category()};
#endif
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
#if NET_PLATFORM_WINDOWS
        ::signal(signal_number, old_actions_[static_cast<std::size_t>(signal_number)]);
#else
        ::sigaction(signal_number, &old_actions_[static_cast<std::size_t>(signal_number)], nullptr);
#endif
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
    impl->cancel_locked(true);
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
    // 回调装在发布（waiting = true）之前：发布之后别的线程可能立刻交付信号、恢复并结束协程，
    // env 与 impl->stop_cb 都不能再碰。发布前到达的停止请求由 cancel_locked 记为 cancel_requested。
    if (env->stop_token.stop_possible())
        impl->stop_cb.emplace(env->stop_token, detail::cancel_signal_wait{impl});
    auto& state = detail::signal_state::instance();
    std::lock_guard<std::mutex> lock{state.mutex};
    if (impl->cancel_requested) {
        impl->cancel_requested = false;
        impl->ec = make_error_code(error::operation_aborted);
        return h;
    }
    // 在 await_ready 与这里之间可能已有信号到达。
    if (not impl->queued.empty()) {
        impl->delivered = impl->queued.front();
        impl->queued.pop_front();
        impl->ec.clear();
        return h;
    }
    impl->waiting = true;
    impl->context->get_executor().on_work_started();
    return noop_coroutine();
}

io_result<int> signal_wait_awaitable::await_resume() noexcept {
    impl->stop_cb.reset(); // 之后不再有取消回调
    impl->cancel_requested = false; // 交付后、恢复前到达的取消留下的过期标记
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
    return impl_->cancel_locked(false) ? 1U : 0U;
}

signal_wait_awaitable signal_set::wait() noexcept {
    CO2_CONTRACT_CHECK(not impl_->pending);
    return signal_wait_awaitable{impl_.get()};
}

} // namespace net
