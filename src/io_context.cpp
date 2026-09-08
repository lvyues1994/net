#include "net/io_context.hpp"

#include <condition_variable>
#include <mutex>
#include <thread>

#include "co2/contract.hpp"

#include "net/memory_resource.hpp"

#include "detail/backend.hpp"
#if NET_PLATFORM_WINDOWS
#include "detail/iocp/iocp_backend.hpp"
#else
#include "detail/io_uring/uring.hpp"
#include "detail/io_uring/uring_backend.hpp"
#include "detail/reactor/demultiplexer.hpp"
#include "detail/reactor/reactor_backend.hpp"
#endif

namespace net {

namespace {

// 线程正在运行哪些 io_context（run() 可以嵌套：一个上下文的协程里 run() 另一个）。
struct thread_context_entry {
    io_context const* context;
    thread_context_entry* next;
};

thread_context_entry*& thread_context_top() noexcept {
    thread_local thread_context_entry* top = nullptr;
    return top;
}

struct thread_context_guard {
    explicit thread_context_guard(io_context const* const context) noexcept
        : entry{context, thread_context_top()} {
        thread_context_top() = &entry;
    }
    ~thread_context_guard() { thread_context_top() = entry.next; }
    thread_context_guard(thread_context_guard const&) = delete;
    thread_context_guard& operator=(thread_context_guard const&) = delete;

    thread_context_entry entry;
};

bool thread_runs(io_context const* const context) noexcept {
    for (auto* entry = thread_context_top(); entry != nullptr; entry = entry->next)
        if (entry->context == context) return true;
    return false;
}

} // namespace

namespace {

#if NET_PLATFORM_WINDOWS

// Windows 只有一个后端：IOCP。其它标签构造时报 not_supported。
detail::io_backend& make_backend(io_context& owner, backend_kind const kind, int) {
    if (kind != backend_kind::iocp)
        throw std::system_error{std::make_error_code(std::errc::not_supported), "io_context backend"};
    return owner.make_service<detail::iocp_backend>();
}

#else

std::unique_ptr<detail::demultiplexer> make_demultiplexer(backend_kind const kind) {
    switch (kind) {
    case backend_kind::epoll: return detail::make_epoll_demultiplexer();
    case backend_kind::poll: return detail::make_poll_demultiplexer();
    case backend_kind::select: return detail::make_select_demultiplexer();
    case backend_kind::io_uring:
    case backend_kind::iocp: break;
    }
    return detail::make_epoll_demultiplexer();
}

// 就绪型后端族：一个共享的 reactor_backend + 按标签选择的解复用器；完成型后端各自是一个
// io_backend 服务。
detail::io_backend& make_backend(io_context& owner, backend_kind const kind, int const concurrency_hint) {
    // single_thread_hint 是调用方的承诺：io_uring 以单提交者模式创建（SINGLE_ISSUER |
    // DEFER_TASKRUN）。其它提示值（包括 1）只是提示，用普通模式。
    if (kind == backend_kind::iocp)
        throw std::system_error{std::make_error_code(std::errc::not_supported), "io_context backend"};
    if (kind == backend_kind::io_uring)
        return owner.make_service<detail::uring_backend>(concurrency_hint == single_thread_hint);
    return owner.make_service<detail::reactor_backend>(make_demultiplexer(kind));
}

#endif

} // namespace

struct io_context::impl {
    impl(io_context& owner, backend_kind const kind_, int const concurrency_hint)
        : kind{kind_}, backend{make_backend(owner, kind_, concurrency_hint)}, concurrent{backend.concurrent_run()} {}

    // 并发模式：当前线程是否在本上下文的 backend.run() 里（后端在 run() 里 post 续体时不必叫醒自己）。
    static impl*& backend_of_this_thread() noexcept {
        static thread_local impl* current = nullptr;
        return current;
    }

    void push(continuation& c) noexcept {
        c.next = nullptr;
        if (tail != nullptr)
            tail->next = &c;
        else
            head = &c;
        tail = &c;
    }

    continuation* pop() noexcept {
        auto* const c = head;
        head = c->next;
        if (head == nullptr) tail = nullptr;
        return c;
    }

    void post(continuation& c) {
        std::lock_guard<std::mutex> lock{mutex};
        push(c);
        wake_one_locked();
    }

    void work_started() noexcept {
        std::lock_guard<std::mutex> lock{mutex};
        ++outstanding_work;
    }

    void work_finished() noexcept {
        std::lock_guard<std::mutex> lock{mutex};
        CO2_CONTRACT_CHECK(outstanding_work > 0);
        if (--outstanding_work == 0) wake_all_locked();
    }

    void stop() {
        std::lock_guard<std::mutex> lock{mutex};
        stopped = true;
        wake_all_locked();
    }

    // 锁内：叫醒一个能处理新队列元素的线程。有空闲线程就叫它；否则唯一在跑的线程可能
    // 阻塞在 epoll_wait / io_uring_enter 里，打断它——除非提交者就是那个线程自己（后端在
    // run() 里完成操作时 post 续体的情形）：它不在等待，回到循环就会看到队列，打断只是浪费
    // 一次 eventfd 写、两次读和一个多余的完成事件。
    void wake_one_locked() noexcept {
        if (concurrent) {
            // 后端里的线程都阻塞在完成端口上；除自己以外还有就叫醒一个。
            if (threads_in_backend > (backend_of_this_thread() == this ? 1U : 0U)) backend.interrupt();
            return;
        }
        if (idle_threads > 0U)
            cv.notify_one();
        else if (reactor_busy && reactor_thread != std::this_thread::get_id())
            backend.interrupt();
    }

    void wake_all_locked() noexcept {
        if (concurrent) {
            auto const others = threads_in_backend - (backend_of_this_thread() == this ? 1U : 0U);
            for (auto i = 0U; i != others; ++i) backend.interrupt(); // 一个包叫醒一个线程
            return;
        }
        cv.notify_all();
        if (reactor_busy && reactor_thread != std::this_thread::get_id()) backend.interrupt();
    }

    static long milliseconds_until(std::chrono::steady_clock::time_point const deadline) noexcept {
        auto const now = std::chrono::steady_clock::now();
        if (deadline <= now) return 0;
        auto const ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now).count() + 1;
        return ms > static_cast<long long>(1L << 30) ? (1L << 30) : static_cast<long>(ms);
    }

    // 恢复最多一个协程。block 为假时不等待事件（poll）。返回恢复的协程数（0 或 1）。
    std::size_t do_one(bool const block, std::chrono::steady_clock::time_point const* deadline) {
        std::unique_lock<std::mutex> lock{mutex};
        for (;;) {
            if (stopped) return 0U;
            if (head != nullptr) {
                auto const handle = pop()->h;
                lock.unlock();
                safe_resume(handle);
                return 1U;
            }
            if (outstanding_work <= 0) return 0U;

            if (concurrent) {
                // 所有线程都直接进后端等待；后端自己保证并发安全。
                struct in_backend_guard {
                    impl* self;
                    std::unique_lock<std::mutex>* lock;
                    ~in_backend_guard() {
                        backend_of_this_thread() = nullptr;
                        lock->lock();
                        --self->threads_in_backend;
                    }
                };
                ++threads_in_backend;
                lock.unlock();
                {
                    in_backend_guard guard{this, &lock};
                    backend_of_this_thread() = this;
                    auto timeout = block ? -1L : 0L;
                    if (block && deadline != nullptr) timeout = milliseconds_until(*deadline);
                    backend.run(timeout);
                }
                if (head != nullptr) continue;
                if (not block) return 0U;
                if (deadline != nullptr && std::chrono::steady_clock::now() >= *deadline) return 0U;
                continue;
            }

            if (not reactor_busy) {
                struct busy_guard {
                    impl* self;
                    std::unique_lock<std::mutex>* lock;
                    ~busy_guard() {
                        lock->lock();
                        self->reactor_busy = false;
                        if (self->head != nullptr && self->idle_threads > 0U) self->cv.notify_one();
                    }
                };
                reactor_busy = true;
                reactor_thread = std::this_thread::get_id();
                lock.unlock();
                {
                    busy_guard guard{this, &lock};
                    auto timeout = block ? -1L : 0L;
                    if (block && deadline != nullptr) timeout = milliseconds_until(*deadline);
                    backend.run(timeout);
                }
                if (head != nullptr) continue;
                if (not block) return 0U;
                if (deadline != nullptr && std::chrono::steady_clock::now() >= *deadline)
                    return 0U;
                continue;
            }

            // 另一个线程在跑反应器：等它交出工作。
            if (not block) return 0U;
            ++idle_threads;
            if (deadline != nullptr) {
                cv.wait_until(lock, *deadline);
                --idle_threads;
                if (head == nullptr && std::chrono::steady_clock::now() >= *deadline) return 0U;
            } else {
                cv.wait(lock);
                --idle_threads;
            }
        }
    }

    backend_kind kind;
    detail::io_backend& backend;
    std::mutex mutex;
    std::condition_variable cv;
    continuation* head = nullptr;
    continuation* tail = nullptr;
    long outstanding_work = 0;
    unsigned idle_threads = 0U;
    bool reactor_busy = false;
    std::thread::id reactor_thread; // reactor_busy 为真时：正在 backend.run() 里的线程
    bool const concurrent;          // backend.concurrent_run()
    unsigned threads_in_backend = 0U; // 并发模式：在 backend.run() 里的线程数
    bool stopped = false;
};

// ---- io_context ----

io_context::io_context() : io_context(default_backend_t::kind, 1) {}

io_context::io_context(int const concurrency_hint) : io_context(default_backend_t::kind, concurrency_hint) {}

io_context::io_context(backend_kind const backend, int const concurrency_hint)
    : impl_{new impl{*this, backend, concurrency_hint}} {}

io_context::~io_context() {
    shutdown();
    destroy();
}

backend_kind io_context::backend() const noexcept { return impl_->kind; }

bool backend_available(backend_kind const kind) noexcept {
    switch (kind) {
#if NET_PLATFORM_WINDOWS
    case backend_kind::iocp: return true;
    case backend_kind::epoll:
    case backend_kind::poll:
    case backend_kind::select:
    case backend_kind::io_uring: return false;
#else
    case backend_kind::epoll:
    case backend_kind::poll:
    case backend_kind::select: return true;
    case backend_kind::io_uring: return detail::uring_available();
    case backend_kind::iocp: return false;
#endif
    }
    return false;
}

char const* io_context::backend_name() const noexcept { return impl_->backend.name(); }

std::error_code io_context::register_buffer(mutable_buffer const region) noexcept {
    return impl_->backend.register_buffer(region.data(), region.size());
}

void io_context::unregister_buffer(mutable_buffer const region) noexcept { impl_->backend.unregister_buffer(region.data()); }

std::size_t io_context::run() {
    thread_context_guard guard{this};
    auto count = std::size_t{};
    while (impl_->do_one(true, nullptr) != 0U)
        ++count;
    return count;
}

std::size_t io_context::run_one() {
    thread_context_guard guard{this};
    return impl_->do_one(true, nullptr);
}

std::size_t io_context::run_until(std::chrono::steady_clock::time_point const deadline) {
    thread_context_guard guard{this};
    auto count = std::size_t{};
    while (impl_->do_one(true, &deadline) != 0U) {
        ++count;
        if (std::chrono::steady_clock::now() >= deadline) break;
    }
    return count;
}

std::size_t io_context::run_one_until(std::chrono::steady_clock::time_point const deadline) {
    thread_context_guard guard{this};
    return impl_->do_one(true, &deadline);
}

std::size_t io_context::poll() {
    thread_context_guard guard{this};
    auto count = std::size_t{};
    while (impl_->do_one(false, nullptr) != 0U)
        ++count;
    return count;
}

std::size_t io_context::poll_one() {
    thread_context_guard guard{this};
    return impl_->do_one(false, nullptr);
}

void io_context::stop() { impl_->stop(); }

bool io_context::stopped() const noexcept {
    std::lock_guard<std::mutex> lock{impl_->mutex};
    return impl_->stopped;
}

void io_context::restart() {
    std::lock_guard<std::mutex> lock{impl_->mutex};
    impl_->stopped = false;
}

// ---- executor_type ----

void io_context::executor_type::on_work_started() const noexcept { context_->impl_->work_started(); }

void io_context::executor_type::on_work_finished() const noexcept {
    context_->impl_->work_finished();
}

coroutine_handle<> io_context::executor_type::dispatch(continuation& c) const {
    if (running_in_this_thread()) return c.h;
    context_->impl_->post(c);
    return noop_coroutine();
}

void io_context::executor_type::post(continuation& c) const { context_->impl_->post(c); }

bool io_context::executor_type::running_in_this_thread() const noexcept {
    return thread_runs(context_);
}

// ---- 私有入口 ----

detail::io_backend& detail::io_context_access::backend(io_context& context) noexcept {
    return context.impl_->backend;
}

} // namespace net
