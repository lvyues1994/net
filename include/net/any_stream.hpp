#pragma once

#include <cstddef>
#include <memory>
#include <type_traits>
#include <utility>

#include "co2/contract.hpp"
#include "co2/detail/awaitable.hpp"

#include "net/buffers.hpp"
#include "net/coroutine.hpp"
#include "net/io_env.hpp"
#include "net/io_result.hpp"
#include "net/memory_resource.hpp"
#include "net/stream.hpp"

// 类型擦除的流（P4172R1 附录 B）：一个指针、一个 vtable、零每操作分配。
//
// vtable 经双参数 await_suspend(coroutine_handle<>, io_env const*) 派发，跨越类型擦除边界
// 仍遵守 IoAwaitable 协议。具体流的 awaitable 在构造 any_read_stream 时按 vtable 里的
// 尺寸/对齐预分配一块存储，每次 read_some 在其中就地构造、await_resume 后销毁——稳态读
// 操作零分配。缓冲区序列在 read_some() 里展平进 any_read_stream 自己的数组，交给协程帧
// 的 awaiter 只有一个指针。
//
// 接受 any_stream& 的函数编译一次，与任何传输（TCP、TLS、内存流、测试替身）一起工作；
// vtable 布局固定，库可以以二进制形式分发。
//
// 契约：同一 any_read_stream 同一时刻只能有一个未完成的 read_some；有未完成操作时不能
// 移动或销毁。

namespace net {
namespace detail {

template <class Buffer, class IoResult> struct any_stream_vtable {
    void (*construct_awaitable)(void* stream, void* storage, span<Buffer const> buffers);
    bool (*await_ready)(void* awaitable);
    coroutine_handle<> (*await_suspend)(void* awaitable, coroutine_handle<> h, io_env const* env);
    IoResult (*await_resume)(void* awaitable);
    void (*destroy_awaitable)(void* awaitable) noexcept;
    std::size_t awaitable_size;
    std::size_t awaitable_align;
    void (*destroy_stream)(void* stream) noexcept;
};

template <class Buffer, class Direction> struct any_stream_base;

struct read_direction {
    template <class S>
    static auto start(S& stream, span<mutable_buffer const> const buffers)
        -> decltype(stream.read_some(buffers)) {
        return stream.read_some(buffers);
    }
};

struct write_direction {
    template <class S>
    static auto start(S& stream, span<const_buffer const> const buffers)
        -> decltype(stream.write_some(buffers)) {
        return stream.write_some(buffers);
    }
};

template <class Buffer, class Direction> struct any_stream_base {
    using vtable_type = any_stream_vtable<Buffer, io_result<std::size_t>>;

    any_stream_base() noexcept = default;

    any_stream_base(any_stream_base&& other) noexcept
        : stream_{other.stream_}, vt_{other.vt_}, awaitable_storage_{other.awaitable_storage_},
          owns_stream_{other.owns_stream_} {
        CO2_CONTRACT_CHECK(not other.awaitable_active_);
        other.stream_ = nullptr;
        other.vt_ = nullptr;
        other.awaitable_storage_ = nullptr;
        other.owns_stream_ = false;
    }

    any_stream_base& operator=(any_stream_base&& other) noexcept {
        if (this == &other) return *this;
        reset();
        stream_ = other.stream_;
        vt_ = other.vt_;
        awaitable_storage_ = other.awaitable_storage_;
        owns_stream_ = other.owns_stream_;
        CO2_CONTRACT_CHECK(not other.awaitable_active_);
        other.stream_ = nullptr;
        other.vt_ = nullptr;
        other.awaitable_storage_ = nullptr;
        other.owns_stream_ = false;
        return *this;
    }

    any_stream_base(any_stream_base const&) = delete;
    any_stream_base& operator=(any_stream_base const&) = delete;

    ~any_stream_base() { reset(); }

    bool has_value() const noexcept { return stream_ != nullptr; }
    explicit operator bool() const noexcept { return has_value(); }

    struct awaitable {
        any_stream_base* self;

        bool await_ready() {
            CO2_CONTRACT_CHECK(not self->awaitable_active_);
            self->vt_->construct_awaitable(self->stream_, self->awaitable_storage_,
                                           self->pending_.to_span());
            self->awaitable_active_ = true;
            struct disarm_on_throw {
                any_stream_base* s;
                bool armed;
                ~disarm_on_throw() {
                    if (armed) {
                        s->vt_->destroy_awaitable(s->awaitable_storage_);
                        s->awaitable_active_ = false;
                    }
                }
            } guard{self, true};
            auto const ready = self->vt_->await_ready(self->awaitable_storage_);
            guard.armed = false;
            return ready;
        }

        coroutine_handle<> await_suspend(coroutine_handle<> const h, io_env const* const env) {
            return self->vt_->await_suspend(self->awaitable_storage_, h, env);
        }

        io_result<std::size_t> await_resume() {
            struct guard {
                any_stream_base* s;
                ~guard() {
                    s->vt_->destroy_awaitable(s->awaitable_storage_);
                    s->awaitable_active_ = false;
                }
            } g{self};
            return self->vt_->await_resume(self->awaitable_storage_);
        }
    };

  protected:
    template <class S> void adopt(S* const stream, bool const owns) {
        reset();
        vt_ = vtable_for<S>::get();
        awaitable_storage_ =
            new_delete_resource()->allocate(vt_->awaitable_size, vt_->awaitable_align);
        stream_ = stream;
        owns_stream_ = owns;
    }

    template <class Sequence> awaitable start(Sequence const& buffers) noexcept {
        CO2_CONTRACT_CHECK(stream_ != nullptr);
        CO2_CONTRACT_CHECK(not awaitable_active_); // 同一方向同时只能有一个操作：第二个会覆盖第一个的缓冲区
        pending_ = buffer_array<Buffer, max_iovec>{buffers};
        return awaitable{this};
    }

    void* stream_pointer() const noexcept { return stream_; }

  private:
    template <class S> struct vtable_for {
        using awaitable_type = co2::detail::AwaiterOf<decltype(
            Direction::start(std::declval<S&>(), std::declval<span<Buffer const>>()))>;

        static void construct(void* const stream, void* const storage,
                              span<Buffer const> const buffers) {
            auto& s = *static_cast<S*>(stream);
            ::new (storage) awaitable_type(co2::detail::getAwaiter(Direction::start(s, buffers)));
        }
        static bool ready(void* const p) { return static_cast<awaitable_type*>(p)->await_ready(); }
        static coroutine_handle<> suspend(void* const p, coroutine_handle<> const h,
                                          io_env const* const env) {
            return static_cast<awaitable_type*>(p)->await_suspend(h, env);
        }
        static io_result<std::size_t> resume(void* const p) {
            return static_cast<awaitable_type*>(p)->await_resume();
        }
        static void destroy_awaitable(void* const p) noexcept {
            static_cast<awaitable_type*>(p)->~awaitable_type();
        }
        static void destroy_stream(void* const p) noexcept { delete static_cast<S*>(p); }

        static vtable_type const* get() noexcept {
            static vtable_type const table{&construct,        &ready,
                                           &suspend,          &resume,
                                           &destroy_awaitable, sizeof(awaitable_type),
                                           alignof(awaitable_type), &destroy_stream};
            return &table;
        }
    };

    void reset() noexcept {
        CO2_CONTRACT_CHECK(not awaitable_active_);
        if (vt_ != nullptr && awaitable_storage_ != nullptr)
            new_delete_resource()->deallocate(awaitable_storage_, vt_->awaitable_size,
                                              vt_->awaitable_align);
        if (owns_stream_ && stream_ != nullptr) vt_->destroy_stream(stream_);
        stream_ = nullptr;
        vt_ = nullptr;
        awaitable_storage_ = nullptr;
        owns_stream_ = false;
    }

    void* stream_ = nullptr;
    vtable_type const* vt_ = nullptr;
    void* awaitable_storage_ = nullptr;
    buffer_array<Buffer, max_iovec> pending_;
    bool awaitable_active_ = false;
    bool owns_stream_ = false;
};

} // namespace detail

struct any_read_stream : detail::any_stream_base<mutable_buffer, detail::read_direction> {
    any_read_stream() noexcept = default;
    any_read_stream(any_read_stream&&) noexcept = default;
    any_read_stream& operator=(any_read_stream&&) noexcept = default;

    // 拥有：流被移进堆上的副本。
    template <class S, class = typename std::enable_if<
                           is_read_stream<S>::value &&
                           not std::is_same<typename std::decay<S>::type, any_read_stream>::value>::type>
    explicit any_read_stream(S stream) {
        std::unique_ptr<S> owned{new S(std::move(stream))};
        this->adopt(owned.get(), true); // adopt 可能抛出（awaiter 存储分配）：那时 owned 仍负责释放
        owned.release();
    }

    // 引用：调用方保证流比本对象活得久。
    template <class S, class = typename std::enable_if<is_read_stream<S>::value>::type>
    explicit any_read_stream(S* const stream) {
        this->adopt(stream, false);
    }

    template <class MutableBufferSequence>
    awaitable read_some(MutableBufferSequence const& buffers) noexcept {
        static_assert(is_mutable_buffer_sequence<MutableBufferSequence>::value,
                      "read_some requires a MutableBufferSequence");
        return this->start(buffers);
    }
};

struct any_write_stream : detail::any_stream_base<const_buffer, detail::write_direction> {
    any_write_stream() noexcept = default;
    any_write_stream(any_write_stream&&) noexcept = default;
    any_write_stream& operator=(any_write_stream&&) noexcept = default;

    template <class S, class = typename std::enable_if<
                           is_write_stream<S>::value &&
                           not std::is_same<typename std::decay<S>::type, any_write_stream>::value>::type>
    explicit any_write_stream(S stream) {
        std::unique_ptr<S> owned{new S(std::move(stream))};
        this->adopt(owned.get(), true);
        owned.release();
    }

    template <class S, class = typename std::enable_if<is_write_stream<S>::value>::type>
    explicit any_write_stream(S* const stream) {
        this->adopt(stream, false);
    }

    template <class ConstBufferSequence>
    awaitable write_some(ConstBufferSequence const& buffers) noexcept {
        static_assert(is_const_buffer_sequence<ConstBufferSequence>::value,
                      "write_some requires a ConstBufferSequence");
        return this->start(buffers);
    }
};

// 读写双向：一个流对象，两张 vtable。
struct any_stream : any_read_stream, any_write_stream {
    any_stream() noexcept = default;

    any_stream(any_stream&& other) noexcept
        : any_read_stream(std::move(static_cast<any_read_stream&>(other))),
          any_write_stream(std::move(static_cast<any_write_stream&>(other))),
          owned_{other.owned_}, destroy_owned_{other.destroy_owned_} {
        other.owned_ = nullptr;
        other.destroy_owned_ = nullptr;
    }

    any_stream& operator=(any_stream&& other) noexcept {
        if (this == &other) return *this;
        release_owned();
        any_read_stream::operator=(std::move(static_cast<any_read_stream&>(other)));
        any_write_stream::operator=(std::move(static_cast<any_write_stream&>(other)));
        owned_ = other.owned_;
        destroy_owned_ = other.destroy_owned_;
        other.owned_ = nullptr;
        other.destroy_owned_ = nullptr;
        return *this;
    }

    ~any_stream() { release_owned(); }

    template <class S, class = typename std::enable_if<
                           is_stream<S>::value &&
                           not std::is_same<typename std::decay<S>::type, any_stream>::value>::type>
    explicit any_stream(S stream) : any_stream(std::unique_ptr<S>{new S(std::move(stream))}) {}

    template <class S, class = typename std::enable_if<is_stream<S>::value>::type>
    explicit any_stream(S* const stream) : any_read_stream(stream), any_write_stream(stream) {}

    bool has_value() const noexcept { return any_read_stream::has_value(); }
    explicit operator bool() const noexcept { return has_value(); }

  private:
    template <class S>
    explicit any_stream(std::unique_ptr<S> stream)
        : any_read_stream(stream.get()), any_write_stream(stream.get()), owned_{stream.get()},
          destroy_owned_{&destroy_as<S>} {
        stream.release();
    }

    template <class S> static void destroy_as(void* const p) noexcept { delete static_cast<S*>(p); }

    void release_owned() noexcept {
        if (owned_ != nullptr) destroy_owned_(owned_);
        owned_ = nullptr;
        destroy_owned_ = nullptr;
    }

    void* owned_ = nullptr;
    void (*destroy_owned_)(void*) noexcept = nullptr;
};

} // namespace net
