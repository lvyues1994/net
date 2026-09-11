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
    // 线程正在运行哪些 io_context（run() 可以嵌套：一个上下文的协程里 run() 另一个）。
    // 每个栈帧带一条本线程、本上下文的私有 continuation 队列：本线程 post 不加锁、不唤醒。
    struct thread_entry {
        io_context const* context;
        thread_entry* next;
        continuation* private_head = nullptr;
        continuation* private_tail = nullptr;
    };

    static thread_entry*& thread_top() noexcept {
        thread_local thread_entry* top = nullptr;
        return top;
    }

    static thread_entry* find_entry(io_context const* const context) noexcept {
        for (auto* entry = thread_top(); entry != nullptr; entry = entry->next)
            if (entry->context == context) return entry;
        return nullptr;
    }

    static bool thread_runs(io_context const* const context) noexcept { return find_entry(context) != nullptr; }

    static void push_private(thread_entry& entry, continuation& c) noexcept {
        c.next = nullptr;
        if (entry.private_tail != nullptr)
            entry.private_tail->next = &c;
        else
            entry.private_head = &c;
        entry.private_tail = &c;
    }

    static continuation* pop_private(thread_entry& entry) noexcept {
        auto* const c = entry.private_head;
        entry.private_head = c->next;
        if (entry.private_head == nullptr) entry.private_tail = nullptr;
        return c;
    }

    struct run_guard {
        run_guard(io_context const* const context, impl& i)
            : impl_{&i}, entry{context, thread_top()} {
            // 嵌套 poll / run 同一上下文：把外层私有队列拼到全局，否则内层 do_one 看不到那些项。
            if (auto* const outer = find_entry(context))
                i.flush_private_queue(outer->private_head, outer->private_tail);
            thread_top() = &entry;
        }
        ~run_guard() {
            thread_top() = entry.next;
            impl_->flush_private_queue(entry.private_head, entry.private_tail);
        }
        run_guard(run_guard const&) = delete;
        run_guard& operator=(run_guard const&) = delete;

        impl* impl_;
        thread_entry entry;
    };

    impl(io_context& owner_, backend_kind const kind_, int const concurrency_hint)
        : owner{&owner_}, kind{kind_}, backend{make_backend(owner_, kind_, concurrency_hint)},
          concurrent{backend.concurrent_run()} {}

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

    // 其它线程：全局队列 + 叫醒。本线程路径见 executor_type::post（私有队列）。
    void post(continuation& c) {
        std::lock_guard<std::mutex> lock{mutex};
        push(c);
        wake_one_locked();
    }

    // 把一条私有队列拼到全局队尾。离开 run/poll 或嵌套 poll 切入时用，避免析构丢掉续体。
    void flush_private_queue(continuation*& phead, continuation*& ptail) {
        if (phead == nullptr) return;
        std::lock_guard<std::mutex> lock{mutex};
        if (tail != nullptr)
            tail->next = phead;
        else
            head = phead;
        tail = ptail;
        phead = nullptr;
        ptail = nullptr;
        wake_one_locked();
    }

    void work_started() noexcept { outstanding_work.fetch_add(1, std::memory_order_relaxed); }

    void work_finished() noexcept {
        // fetch_sub 返回减之前的值；仅 1→0 叫醒等待中的 run()。
        auto const prev = outstanding_work.fetch_sub(1, std::memory_order_acq_rel);
        CO2_CONTRACT_CHECK(prev > 0);
        if (prev == 1) {
            std::lock_guard<std::mutex> lock{mutex};
            wake_all_locked();
        }
    }

    void stop() {
        stopped.store(true, std::memory_order_release);
        std::lock_guard<std::mutex> lock{mutex};
        wake_all_locked();
    }

    // 锁内：叫醒一个能处理新队列元素的线程。有空闲线程就叫它；否则唯一在跑的线程可能
    // 阻塞在 epoll_wait / io_uring_enter 里，打断它——除非提交者就是那个线程自己（后端在
    // run() 里完成操作时 post 续体的情形）：它不在等待，回到循环就会看到队列，打断只是浪费
    // 一次 eventfd 写、两次读和一个多余的完成事件。本线程 post 已走私有队列，不会进这里。
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
    // 先排空本线程私有队列，再取全局 head。工作计数用 acquire 读，与 work_finished 的
    // acq_rel 配对；stop() 后立即返回，残留私有项由 run_guard 刷回全局。
    std::size_t do_one(bool const block, std::chrono::steady_clock::time_point const* deadline) {
        auto* const self = find_entry(owner);
        CO2_CONTRACT_CHECK(self != nullptr);

        for (;;) {
            if (stopped.load(std::memory_order_acquire)) return 0U;
            if (self->private_head != nullptr) {
                auto const handle = pop_private(*self)->h;
                safe_resume(handle);
                return 1U;
            }

            std::unique_lock<std::mutex> lock{mutex};
            if (stopped.load(std::memory_order_acquire)) return 0U;
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
                // complete() 的同线程 post 在私有队列上；必须先看到它们再决定 poll 返回。
                if (self->private_head != nullptr || head != nullptr) {
                    lock.unlock();
                    continue;
                }
                if (not block) return 0U;
                if (deadline != nullptr && std::chrono::steady_clock::now() >= *deadline) return 0U;
                lock.unlock();
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
                if (self->private_head != nullptr || head != nullptr) {
                    lock.unlock();
                    continue;
                }
                if (not block) return 0U;
                if (deadline != nullptr && std::chrono::steady_clock::now() >= *deadline)
                    return 0U;
                lock.unlock();
                continue;
            }

            // 另一个线程在跑反应器：等它交出工作。
            if (not block) return 0U;
            ++idle_threads;
            if (deadline != nullptr) {
                cv.wait_until(lock, *deadline);
                --idle_threads;
                if (self->private_head == nullptr && head == nullptr &&
                    std::chrono::steady_clock::now() >= *deadline)
                    return 0U;
            } else {
                cv.wait(lock);
                --idle_threads;
            }
            lock.unlock();
        }
    }

    io_context* owner;
    backend_kind kind;
    detail::io_backend& backend;
    std::mutex mutex;
    std::condition_variable cv;
    continuation* head = nullptr;
    continuation* tail = nullptr;
    // 原子：on_work_* / stop 不必为计数或标志本身持有 mutex。mutex 只保护全局队列与唤醒
    // 状态（idle_threads / reactor_busy / threads_in_backend）。1→0 与 stop() 仍在锁内
    // wake_all，以便 interrupt() 正在 backend.run() 的线程。
    std::atomic<long> outstanding_work{0};
    unsigned idle_threads = 0U;
    bool reactor_busy = false;
    std::thread::id reactor_thread; // reactor_busy 为真时：正在 backend.run() 里的线程
    bool const concurrent;          // backend.concurrent_run()
    unsigned threads_in_backend = 0U; // 并发模式：在 backend.run() 里的线程数
    std::atomic<bool> stopped{false};
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
    impl::run_guard guard{this, *impl_};
    auto count = std::size_t{};
    while (impl_->do_one(true, nullptr) != 0U)
        ++count;
    return count;
}

std::size_t io_context::run_one() {
    impl::run_guard guard{this, *impl_};
    return impl_->do_one(true, nullptr);
}

std::size_t io_context::run_until(std::chrono::steady_clock::time_point const deadline) {
    impl::run_guard guard{this, *impl_};
    auto count = std::size_t{};
    while (impl_->do_one(true, &deadline) != 0U) {
        ++count;
        if (std::chrono::steady_clock::now() >= deadline) break;
    }
    return count;
}

std::size_t io_context::run_one_until(std::chrono::steady_clock::time_point const deadline) {
    impl::run_guard guard{this, *impl_};
    return impl_->do_one(true, &deadline);
}

std::size_t io_context::poll() {
    impl::run_guard guard{this, *impl_};
    auto count = std::size_t{};
    while (impl_->do_one(false, nullptr) != 0U)
        ++count;
    return count;
}

std::size_t io_context::poll_one() {
    impl::run_guard guard{this, *impl_};
    return impl_->do_one(false, nullptr);
}

void io_context::stop() { impl_->stop(); }

bool io_context::stopped() const noexcept { return impl_->stopped.load(std::memory_order_acquire); }

void io_context::restart() { impl_->stopped.store(false, std::memory_order_release); }

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

void io_context::executor_type::post(continuation& c) const {
    if (auto* const entry = io_context::impl::find_entry(context_)) {
        io_context::impl::push_private(*entry, c);
        return;
    }
    context_->impl_->post(c);
}

bool io_context::executor_type::running_in_this_thread() const noexcept {
    return io_context::impl::thread_runs(context_);
}

// ---- 私有入口 ----

detail::io_backend& detail::io_context_access::backend(io_context& context) noexcept {
    return context.impl_->backend;
}

} // namespace net
