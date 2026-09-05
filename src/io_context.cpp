#include "net/io_context.hpp"

#include <condition_variable>
#include <mutex>

#include "co2/contract.hpp"

#include "net/memory_resource.hpp"

#include "detail/backend.hpp"
#include "detail/io_uring/uring.hpp"
#include "detail/io_uring/uring_backend.hpp"
#include "detail/reactor/demultiplexer.hpp"
#include "detail/reactor/reactor_backend.hpp"

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

std::unique_ptr<detail::demultiplexer> make_demultiplexer(backend_kind const kind) {
    switch (kind) {
    case backend_kind::epoll: return detail::make_epoll_demultiplexer();
    case backend_kind::poll: return detail::make_poll_demultiplexer();
    case backend_kind::select: return detail::make_select_demultiplexer();
    case backend_kind::io_uring: break;
    }
    return detail::make_epoll_demultiplexer();
}

// 就绪型后端族：一个共享的 reactor_backend + 按标签选择的解复用器；完成型后端各自是一个
// io_backend 服务。
detail::io_backend& make_backend(io_context& owner, backend_kind const kind) {
    if (kind == backend_kind::io_uring) return owner.make_service<detail::uring_backend>();
    return owner.make_service<detail::reactor_backend>(make_demultiplexer(kind));
}

} // namespace

struct io_context::impl {
    impl(io_context& owner, backend_kind const kind_) : kind{kind_}, backend{make_backend(owner, kind_)} {}

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
    // 阻塞在 epoll_wait 里，打断它。
    void wake_one_locked() noexcept {
        if (idle_threads > 0U)
            cv.notify_one();
        else if (reactor_busy)
            backend.interrupt();
    }

    void wake_all_locked() noexcept {
        cv.notify_all();
        if (reactor_busy) backend.interrupt();
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
    bool stopped = false;
};

// ---- io_context ----

io_context::io_context() : io_context(default_backend_t::kind, 1) {}

io_context::io_context(int const concurrency_hint) : io_context(default_backend_t::kind, concurrency_hint) {}

io_context::io_context(backend_kind const backend, int) : impl_{new impl{*this, backend}} {}

io_context::~io_context() {
    shutdown();
    destroy();
}

backend_kind io_context::backend() const noexcept { return impl_->kind; }

bool backend_available(backend_kind const kind) noexcept {
    switch (kind) {
    case backend_kind::epoll:
    case backend_kind::poll:
    case backend_kind::select: return true;
    case backend_kind::io_uring: return detail::uring_available();
    }
    return false;
}

char const* io_context::backend_name() const noexcept { return impl_->backend.name(); }

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
