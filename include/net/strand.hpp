#pragma once

#include <memory>
#include <mutex>
#include <type_traits>
#include <utility>

#include "net/continuation.hpp"
#include "net/coroutine.hpp"
#include "net/detail/completion_frame.hpp"
#include "net/execution_context.hpp"
#include "net/executor_ref.hpp"
#include "net/memory_resource.hpp"

// strand<Ex>：在任意 Executor 之上串行化协程恢复（P4172R1 附录 A.7）。经同一 strand 派发
// 的续体绝不并发运行，也不需要锁；不同 strand 可以在不同线程上同时推进。
//
// 实现：一个互斥锁保护的侵入式队列 + 一个"派发帧"（手写帧，见 completion_frame）。第一
// 个到达的续体把派发帧 post 到内层执行器；派发帧运行时取走整批续体逐个 safe_resume，
// 期间到达的续体排入下一批，批处理结束后重新 post 派发帧（让内层执行器上的其它工作有
// 机会运行）。dispatch 在调用线程已经处于本 strand 内时直接返回 c.h（对称转移）。
//
// strand 是可拷贝的轻量句柄，共享同一个实现；实现在派发帧排队期间自我保活。

namespace net {
namespace detail {

struct strand_call_stack {
    struct entry {
        void const* strand;
        entry* next;
    };

    static entry*& top() noexcept {
        thread_local entry* current = nullptr;
        return current;
    }

    static bool contains(void const* const strand) noexcept {
        for (auto* e = top(); e != nullptr; e = e->next)
            if (e->strand == strand) return true;
        return false;
    }

    struct scope {
        explicit scope(void const* const strand) noexcept : e{strand, top()} { top() = &e; }
        ~scope() { top() = e.next; }
        scope(scope const&) = delete;
        scope& operator=(scope const&) = delete;
        entry e;
    };
};

template <class Ex> struct strand_impl : std::enable_shared_from_this<strand_impl<Ex>> {
    explicit strand_impl(Ex executor_) : executor(std::move(executor_)) {
        drain.set(&on_drain, this);
        drain_cont.h = drain.handle();
    }

    strand_impl(strand_impl const&) = delete;
    strand_impl& operator=(strand_impl const&) = delete;

    void enqueue(continuation& c) {
        std::unique_lock<std::mutex> lock{mutex};
        c.next = nullptr;
        if (tail != nullptr)
            tail->next = &c;
        else
            head = &c;
        tail = &c;
        if (locked) return;
        locked = true;
        keepalive = this->shared_from_this();
        lock.unlock();
        executor.post(drain_cont);
    }

    static coroutine_handle<> on_drain(void* const user) {
        auto* const self = static_cast<strand_impl*>(user);
        std::shared_ptr<strand_impl> keep;
        continuation* batch = nullptr;
        {
            std::lock_guard<std::mutex> lock{self->mutex};
            keep = std::move(self->keepalive);
            batch = self->head;
            self->head = nullptr;
            self->tail = nullptr;
        }
        try {
            strand_call_stack::scope scope{self};
            while (batch != nullptr) {
                auto* const c = batch;
                batch = c->next;
                safe_resume(c->h);
            }
        } catch (...) {
            // 某个续体抛出（例如 run_async 默认的 rethrow_error）：不能带着 locked == true 和一批未
            // 恢复的续体离开——那会让这个 strand 之后吞掉所有投递。把剩余批次放回队首、重新排队
            // 排空帧，再把异常交给执行器的 run()。
            std::unique_lock<std::mutex> lock{self->mutex};
            if (batch != nullptr) {
                auto* last = batch;
                while (last->next != nullptr)
                    last = last->next;
                last->next = self->head;
                self->head = batch;
                if (self->tail == nullptr) self->tail = last;
            }
            if (self->head == nullptr) {
                self->locked = false;
                lock.unlock();
                keep.reset(); // 可能销毁 self，此后不再触碰
                throw;
            }
            self->keepalive = std::move(keep);
            lock.unlock();
            self->executor.post(self->drain_cont);
            throw;
        }
        std::unique_lock<std::mutex> lock{self->mutex};
        if (self->head == nullptr) {
            self->locked = false;
            return nullptr; // keep 随之析构：可能销毁 self，此后不再触碰
        }
        self->keepalive = std::move(keep);
        lock.unlock();
        self->executor.post(self->drain_cont);
        return nullptr;
    }

    Ex executor;
    std::mutex mutex;
    continuation* head = nullptr;
    continuation* tail = nullptr;
    bool locked = false;
    std::shared_ptr<strand_impl> keepalive;
    completion_frame drain;
    continuation drain_cont;
};

} // namespace detail

template <class Ex> struct strand {
    static_assert(is_executor<Ex>::value, "strand requires an Executor");

    using inner_executor_type = Ex;

    strand() noexcept = default;

    explicit strand(Ex executor)
        : impl_{std::make_shared<detail::strand_impl<Ex>>(std::move(executor))} {}

    strand(strand const&) noexcept = default;
    strand(strand&&) noexcept = default;
    strand& operator=(strand const&) noexcept = default;
    strand& operator=(strand&&) noexcept = default;

    Ex get_inner_executor() const noexcept { return impl_->executor; }

    execution_context& context() const noexcept { return impl_->executor.context(); }

    void on_work_started() const noexcept { impl_->executor.on_work_started(); }
    void on_work_finished() const noexcept { impl_->executor.on_work_finished(); }

    coroutine_handle<> dispatch(continuation& c) const {
        if (running_in_this_thread()) return c.h;
        impl_->enqueue(c);
        return noop_coroutine();
    }

    void post(continuation& c) const { impl_->enqueue(c); }

    bool running_in_this_thread() const noexcept {
        return detail::strand_call_stack::contains(impl_.get());
    }

    friend bool operator==(strand const& left, strand const& right) noexcept {
        return left.impl_ == right.impl_;
    }

    friend bool operator!=(strand const& left, strand const& right) noexcept {
        return left.impl_ != right.impl_;
    }

  private:
    std::shared_ptr<detail::strand_impl<Ex>> impl_;
};

template <class Ex> strand<Ex> make_strand(Ex executor) { return strand<Ex>{std::move(executor)}; }

} // namespace net
