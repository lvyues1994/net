#pragma once

#include <memory>
#include <type_traits>
#include <typeinfo>
#include <utility>

#include "net/continuation.hpp"
#include "net/coroutine.hpp"
#include "net/execution_context.hpp"
#include "net/executor_ref.hpp"

// any_executor：拥有型的类型擦除执行器（executor_ref 是非拥有的两指针视图）。满足
// Executor 概念，因此可以像具体执行器一样交给 run_async / run / strand。相等性：同一个
// 底层对象，或类型相同且底层执行器相等。

namespace net {

struct any_executor {
    any_executor() noexcept = default;

    template <class E,
              class = typename std::enable_if<
                  is_executor<E>::value &&
                  not std::is_same<typename std::decay<E>::type, any_executor>::value>::type>
    any_executor(E executor) : impl_{std::make_shared<model<E>>(std::move(executor))} {}

    any_executor(any_executor const&) noexcept = default;
    any_executor(any_executor&&) noexcept = default;
    any_executor& operator=(any_executor const&) noexcept = default;
    any_executor& operator=(any_executor&&) noexcept = default;

    execution_context& context() const noexcept { return impl_->context(); }
    void on_work_started() const noexcept { impl_->on_work_started(); }
    void on_work_finished() const noexcept { impl_->on_work_finished(); }
    coroutine_handle<> dispatch(continuation& c) const { return impl_->dispatch(c); }
    void post(continuation& c) const { impl_->post(c); }

    explicit operator bool() const noexcept { return impl_ != nullptr; }

    std::type_info const& target_type() const noexcept {
        return impl_ ? impl_->target_type() : typeid(void);
    }

    template <class E> E const* target() const noexcept {
        if (not impl_ || impl_->target_type() != typeid(E)) return nullptr;
        return static_cast<E const*>(impl_->target());
    }

    friend bool operator==(any_executor const& left, any_executor const& right) noexcept {
        if (left.impl_ == right.impl_) return true;
        if (not left.impl_ || not right.impl_) return false;
        return left.impl_->equals(*right.impl_);
    }

    friend bool operator!=(any_executor const& left, any_executor const& right) noexcept {
        return not(left == right);
    }

  private:
    struct concept_type {
        virtual ~concept_type() = default;
        virtual execution_context& context() const noexcept = 0;
        virtual void on_work_started() const noexcept = 0;
        virtual void on_work_finished() const noexcept = 0;
        virtual coroutine_handle<> dispatch(continuation& c) const = 0;
        virtual void post(continuation& c) const = 0;
        virtual std::type_info const& target_type() const noexcept = 0;
        virtual void const* target() const noexcept = 0;
        virtual bool equals(concept_type const& other) const noexcept = 0;
    };

    template <class E> struct model final : concept_type {
        explicit model(E executor_) : executor(std::move(executor_)) {}

        execution_context& context() const noexcept override { return executor.context(); }
        void on_work_started() const noexcept override { executor.on_work_started(); }
        void on_work_finished() const noexcept override { executor.on_work_finished(); }
        coroutine_handle<> dispatch(continuation& c) const override { return executor.dispatch(c); }
        void post(continuation& c) const override { executor.post(c); }
        std::type_info const& target_type() const noexcept override { return typeid(E); }
        void const* target() const noexcept override { return &executor; }
        bool equals(concept_type const& other) const noexcept override {
            return other.target_type() == typeid(E) &&
                   executor == *static_cast<E const*>(other.target());
        }

        E executor;
    };

    std::shared_ptr<concept_type const> impl_;
};

} // namespace net
