#include "net/io_context.hpp"

#include <atomic>
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

    // 线程在本上下文的 backend.run() 里时的私有状态：后端在 run() 里完成操作、post 续体，续体先进
    // 这个线程私有队列——不加锁，也不叫醒任何人（本线程回到 do_one 就会看到）。run() 返回后 do_one 在
    // 已持有的锁内一次把整批接到全局队列。Asio 的 private_op_queue 是同一做法：每次完成少两次调度器锁。
    // 实测对单线程回环往返是中性的（无争用的锁不到 100 ns），价值在多个线程 run() 同一上下文时少争用。
    struct backend_run_state {
        impl* context = nullptr; // 正在哪个上下文的 backend.run() 里
        continuation* head = nullptr;
        continuation* tail = nullptr;
        long count = 0;

        void push(continuation& c) noexcept {
            c.next = nullptr;
            if (tail != nullptr)
                tail->next = &c;
            else
                head = &c;
            tail = &c;
            ++count;
        }
    };

    static backend_run_state& this_thread_backend_run() noexcept {
        static thread_local backend_run_state state;
        return state;
    }

    bool this_thread_in_backend() const noexcept { return this_thread_backend_run().context == this; }

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
        auto& run_state = this_thread_backend_run();
        if (run_state.context == this) {
            // 私有队列里的续体在接到全局队列之前也算未完成的工作：否则别的线程在 do_one 里看到
            // 空队列、工作计数为零就返回了，而续体还停在这个线程手里。
            outstanding_work.fetch_add(1, std::memory_order_relaxed);
            run_state.push(c);
            return;
        }
        std::lock_guard<std::mutex> lock{mutex};
        push(c);
        wake_one_locked();
    }

    // 锁内：把本线程私有队列整批接到全局队列尾；返回接入个数。
    long adopt_private_locked(backend_run_state& run_state) noexcept {
        if (run_state.head == nullptr) return 0;
        if (tail != nullptr)
            tail->next = run_state.head;
        else
            head = run_state.head;
        tail = run_state.tail;
        auto const n = run_state.count;
        run_state.head = nullptr;
        run_state.tail = nullptr;
        run_state.count = 0;
        // 续体已在全局队列里可见：归还 post 时预借的工作计数。到零只可能发生在这里（别处的
        // work_finished 看到的计数至少还包含这 n 份），所以由这里叫醒等待者。
        if (outstanding_work.fetch_sub(n, std::memory_order_acq_rel) == n) wake_all_locked();
        return n;
    }

    // 工作计数是原子的：on_work_started / on_work_finished 在每个操作的发布与完成路径上各来一次，
    // 不值得为它们拿调度器锁。只有归零那一次需要叫醒 run() 里的等待者，才进锁。
    void work_started() noexcept { outstanding_work.fetch_add(1, std::memory_order_relaxed); }

    void work_finished() noexcept {
        auto const previous = outstanding_work.fetch_sub(1, std::memory_order_acq_rel);
        CO2_CONTRACT_CHECK(previous > 0);
        if (previous == 1) {
            std::lock_guard<std::mutex> lock{mutex};
            wake_all_locked();
        }
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
            wake_in_backend_locked(1U);
            return;
        }
        if (idle_threads > 0U)
            cv.notify_one();
        else if (reactor_busy && reactor_thread != std::this_thread::get_id())
            backend.interrupt();
    }

    // 锁内：并发模式，叫醒最多 n 个（除自己以外）阻塞在后端里的线程；一个包叫醒一个线程。
    void wake_in_backend_locked(unsigned n) noexcept {
        auto const others = threads_in_backend - (this_thread_in_backend() ? 1U : 0U);
        if (n > others) n = others;
        for (auto i = 0U; i != n; ++i) backend.interrupt();
    }

    // 锁内：非并发模式，叫醒最多 n 个在条件变量上等待的空闲线程。
    void wake_idle_locked(unsigned n) noexcept {
        if (n > idle_threads) n = idle_threads;
        for (auto i = 0U; i != n; ++i) cv.notify_one();
    }

    void wake_all_locked() noexcept {
        if (concurrent) {
            wake_in_backend_locked(threads_in_backend);
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
            if (outstanding_work.load(std::memory_order_acquire) <= 0) return 0U;

            if (concurrent) {
                // 所有线程都直接进后端等待；后端自己保证并发安全。
                struct in_backend_guard {
                    impl* self;
                    std::unique_lock<std::mutex>* lock;
                    backend_run_state* run_state;
                    ~in_backend_guard() {
                        run_state->context = nullptr;
                        lock->lock();
                        --self->threads_in_backend;
                        // 私有队列接到全局队列：自己接着取一个，其余的叫醒还在后端里等的线程。
                        auto const adopted = self->adopt_private_locked(*run_state);
                        if (adopted > 1) self->wake_in_backend_locked(static_cast<unsigned>(adopted - 1));
                    }
                };
                auto& run_state = this_thread_backend_run();
                ++threads_in_backend;
                lock.unlock();
                {
                    in_backend_guard guard{this, &lock, &run_state};
                    run_state.context = this;
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
                    backend_run_state* run_state;
                    ~busy_guard() {
                        run_state->context = nullptr;
                        lock->lock();
                        self->reactor_busy = false;
                        // 私有队列接到全局队列：自己接着取一个，其余的叫醒空闲线程。别的线程在此期间
                        // post 的续体已经各自叫醒过一个空闲线程。
                        auto const adopted = self->adopt_private_locked(*run_state);
                        if (adopted > 1)
                            self->wake_idle_locked(static_cast<unsigned>(adopted - 1));
                        else if (self->head != nullptr)
                            self->wake_idle_locked(1U);
                    }
                };
                auto& run_state = this_thread_backend_run();
                reactor_busy = true;
                reactor_thread = std::this_thread::get_id();
                lock.unlock();
                {
                    busy_guard guard{this, &lock, &run_state};
                    run_state.context = this;
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
    std::atomic<long> outstanding_work{0};
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
