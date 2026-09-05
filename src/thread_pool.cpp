#include "net/thread_pool.hpp"

#include <condition_variable>
#include <mutex>
#include <thread>
#include <vector>

#include "co2/contract.hpp"

#include "net/memory_resource.hpp"

namespace net {

namespace {

thread_pool const*& current_pool_slot() noexcept {
    thread_local thread_pool const* current = nullptr;
    return current;
}

} // namespace

struct thread_pool::impl {
    explicit impl(thread_pool& owner_, std::size_t const count) : owner{&owner_} {
        CO2_CONTRACT_CHECK(count != 0U);
        threads.reserve(count);
        try {
            for (auto index = std::size_t{}; index != count; ++index)
                threads.emplace_back([this] { worker_main(); });
        } catch (...) {
            request_stop();
            join_threads();
            throw;
        }
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
        cv.notify_one();
    }

    void work_started() noexcept {
        std::lock_guard<std::mutex> lock{mutex};
        ++outstanding_work;
    }

    void work_finished() noexcept {
        std::lock_guard<std::mutex> lock{mutex};
        CO2_CONTRACT_CHECK(outstanding_work > 0);
        if (--outstanding_work == 0) cv.notify_all();
    }

    void request_stop() {
        std::lock_guard<std::mutex> lock{mutex};
        stopped = true;
        cv.notify_all();
    }

    void release_initial_work() {
        std::lock_guard<std::mutex> lock{mutex};
        if (not initial_work_held) return;
        initial_work_held = false;
        if (--outstanding_work == 0) cv.notify_all();
    }

    void join_threads() {
        for (auto& thread : threads)
            if (thread.joinable()) thread.join();
    }

    void worker_main() {
        current_pool_slot() = owner;
        std::unique_lock<std::mutex> lock{mutex};
        for (;;) {
            if (stopped) break;
            if (head != nullptr) {
                auto const handle = pop()->h;
                lock.unlock();
                safe_resume(handle);
                lock.lock();
                continue;
            }
            if (outstanding_work <= 0) break;
            cv.wait(lock);
        }
        current_pool_slot() = nullptr;
    }

    thread_pool* owner;
    std::mutex mutex;
    std::condition_variable cv;
    continuation* head = nullptr;
    continuation* tail = nullptr;
    long outstanding_work = 1; // 初始工作，join() 释放
    bool initial_work_held = true;
    bool stopped = false;
    std::vector<std::thread> threads;
};

std::size_t thread_pool::default_thread_count() noexcept {
    auto const detected = std::thread::hardware_concurrency();
    return detected == 0U ? 1U : detected;
}

thread_pool::thread_pool(std::size_t const thread_count) : impl_{new impl{*this, thread_count}} {}

thread_pool::~thread_pool() {
    impl_->request_stop();
    impl_->join_threads();
    shutdown();
    destroy();
}

std::size_t thread_pool::thread_count() const noexcept { return impl_->threads.size(); }

void thread_pool::stop() { impl_->request_stop(); }

void thread_pool::join() {
    impl_->release_initial_work();
    impl_->join_threads();
}

void thread_pool::executor_type::on_work_started() const noexcept { pool_->impl_->work_started(); }

void thread_pool::executor_type::on_work_finished() const noexcept {
    pool_->impl_->work_finished();
}

coroutine_handle<> thread_pool::executor_type::dispatch(continuation& c) const {
    if (running_in_this_thread()) return c.h;
    pool_->impl_->post(c);
    return noop_coroutine();
}

void thread_pool::executor_type::post(continuation& c) const { pool_->impl_->post(c); }

bool thread_pool::executor_type::running_in_this_thread() const noexcept {
    return current_pool_slot() == pool_;
}

} // namespace net
