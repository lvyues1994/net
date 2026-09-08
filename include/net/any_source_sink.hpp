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
#include "net/source_sink.hpp"
#include "net/stream.hpp"
#include "net/task.hpp"

// 第二族流概念的类型擦除（P4100R1 §8.6：any_read_source / any_write_sink / any_buffer_source /
// any_buffer_sink；形态取自 Capy）。与 any_stream 相同的原则：一个指针、一个 vtable、每次操作
// 零分配——被包装类型的 awaiter 在构造时按 vtable 里的尺寸预分配一块槽位，每次操作在其中就地
// 构造、await_resume 后销毁。一个对象有多种操作（write_some / write / write_eof…），它们共用一个
// 槽位（尺寸取最大值），同一时刻只能有一个在飞。
//
// 涉及"整个序列"的操作（read / write / write_eof(buffers)）按至多 max_iovec 个缓冲区一窗地穿过
// 类型擦除边界，任意长的序列都覆盖；代价是一个组合协程帧（与 net::read / net::write 相同）。
// any_buffer_source 另外提供 read_some / read，any_buffer_sink 另外提供 write_some / write / write_eof：
// 被包装类型自己满足对应概念就转发，否则用 pull/consume 或 prepare/commit 合成（一次拷贝）。

namespace net {
namespace detail {

// ---- 就地构造的类型擦除 awaiter ----

template <class Result> struct erased_ops {
    bool (*ready)(void* awaiter);
    coroutine_handle<> (*suspend)(void* awaiter, coroutine_handle<> h, io_env const* env);
    Result (*resume)(void* awaiter);
    void (*destroy)(void* awaiter) noexcept;
};

template <class Awaiter, class Result> struct erased_ops_for {
    static bool ready(void* const p) { return static_cast<Awaiter*>(p)->await_ready(); }
    static coroutine_handle<> suspend(void* const p, coroutine_handle<> const h, io_env const* const env) {
        return static_cast<Awaiter*>(p)->await_suspend(h, env);
    }
    static Result resume(void* const p) { return static_cast<Awaiter*>(p)->await_resume(); }
    static void destroy(void* const p) noexcept { static_cast<Awaiter*>(p)->~Awaiter(); }
    static erased_ops<Result> const* get() noexcept {
        static erased_ops<Result> const table{&ready, &suspend, &resume, &destroy};
        return &table;
    }
};

// 槽位：拥有者按该类型全部 awaiter 的最大尺寸分配一块。
struct erased_slot {
    erased_slot() noexcept = default;
    erased_slot(erased_slot const&) = delete;
    erased_slot& operator=(erased_slot const&) = delete;
    ~erased_slot() { release(); }

    void allocate(std::size_t const size_, std::size_t const align_) {
        release();
        storage = new_delete_resource()->allocate(size_, align_);
        size = size_;
        align = align_;
    }

    void release() noexcept {
        CO2_CONTRACT_CHECK(not active);
        if (storage != nullptr) new_delete_resource()->deallocate(storage, size, align);
        storage = nullptr;
    }

    void steal(erased_slot& other) noexcept {
        CO2_CONTRACT_CHECK(not other.active);
        release();
        storage = other.storage;
        size = other.size;
        align = other.align;
        other.storage = nullptr;
    }

    void* storage = nullptr;
    std::size_t size = 0;
    std::size_t align = 0;
    bool active = false;
};

// 交给协程的 awaitable：await_ready 时才用暂存的参数就地构造具体 awaiter（构造出来却没 co_await 的
// awaitable 不花任何东西）。construct 返回该 awaiter 的 ops 表。
template <class Result> struct erased_awaitable {
    using construct_fn = erased_ops<Result> const* (*)(void* object, void const* args, void* storage);

    void* object;
    void const* args;
    erased_slot* slot;
    construct_fn construct;
    erased_ops<Result> const* ops = nullptr;

    bool await_ready() {
        CO2_CONTRACT_CHECK(construct != nullptr);
        CO2_CONTRACT_CHECK(not slot->active); // 同一对象同时只能有一个操作
        ops = construct(object, args, slot->storage);
        slot->active = true;
        struct disarm_on_throw {
            erased_awaitable* a;
            bool armed;
            ~disarm_on_throw() {
                if (armed) {
                    a->ops->destroy(a->slot->storage);
                    a->slot->active = false;
                }
            }
        } guard{this, true};
        auto const ready = ops->ready(slot->storage);
        guard.armed = false;
        return ready;
    }

    coroutine_handle<> await_suspend(coroutine_handle<> const h, io_env const* const env) {
        return ops->suspend(slot->storage, h, env);
    }

    Result await_resume() {
        struct guard {
            erased_awaitable* a;
            ~guard() {
                a->ops->destroy(a->slot->storage);
                a->slot->active = false;
            }
        } g{this};
        return ops->resume(slot->storage);
    }
};

constexpr std::size_t max_of(std::size_t const a, std::size_t const b) noexcept { return a > b ? a : b; }

// 某个操作在被包装类型上存在时的 awaiter 类型与就地构造函数；不存在时尺寸为 0、构造函数为空。
struct absent_awaiter {};

// expr 用 s() / args() 表达：类作用域里它们是只在 decltype 里出现的静态函数声明，construct 里是
// 返回真实引用的局部 lambda——同一个表达式两处都成立。
#define NET_DETAIL_ERASED_OP(name, Present, Result, ArgsType, expr)                                          \
    template <class S, bool = Present> struct name {                                                        \
        static S& s() noexcept;                                                                             \
        static ArgsType const& args() noexcept;                                                             \
        using awaiter = co2::detail::AwaiterOf<decltype(expr)>;                                             \
        static erased_ops<Result> const* construct(void* const object, void const* const args_, void* const storage) { \
            auto* const object_ptr = static_cast<S*>(object);                                               \
            auto const* const args_ptr = static_cast<ArgsType const*>(args_);                               \
            auto s = [object_ptr]() noexcept -> S& { return *object_ptr; };                                 \
            auto args = [args_ptr]() noexcept -> ArgsType const& { return *args_ptr; };                     \
            static_cast<void>(args);                                                                        \
            ::new (storage) awaiter(co2::detail::getAwaiter(expr));                                         \
            return erased_ops_for<awaiter, Result>::get();                                                  \
        }                                                                                                   \
        static constexpr std::size_t size = sizeof(awaiter);                                                \
        static constexpr std::size_t align = alignof(awaiter);                                              \
        static constexpr typename erased_awaitable<Result>::construct_fn fn = &construct;                   \
    };                                                                                                      \
    template <class S> struct name<S, false> {                                                              \
        static constexpr std::size_t size = 0;                                                              \
        static constexpr std::size_t align = 1;                                                             \
        static constexpr typename erased_awaitable<Result>::construct_fn fn = nullptr;                      \
    };

// 操作：参数形状分三种——缓冲区序列（暂存为 buffer_array）、目标 span（pull）、字节数（commit）。
NET_DETAIL_ERASED_OP(op_read_some, is_read_stream<S>::value, io_result<std::size_t>, mutable_buffer_array<>,
                     s().read_some(args().to_span()))
NET_DETAIL_ERASED_OP(op_read, is_read_source<S>::value, io_result<std::size_t>, mutable_buffer_array<>,
                     s().read(args().to_span()))
NET_DETAIL_ERASED_OP(op_write_some, is_write_stream<S>::value, io_result<std::size_t>, const_buffer_array<>,
                     s().write_some(args().to_span()))
NET_DETAIL_ERASED_OP(op_write, is_write_sink<S>::value, io_result<std::size_t>, const_buffer_array<>,
                     s().write(args().to_span()))
NET_DETAIL_ERASED_OP(op_write_eof_buffers, is_write_sink<S>::value, io_result<std::size_t>, const_buffer_array<>,
                     s().write_eof(args().to_span()))
NET_DETAIL_ERASED_OP(op_write_eof, is_write_sink<S>::value, io_result<>, int, s().write_eof())
NET_DETAIL_ERASED_OP(op_pull, is_buffer_source<S>::value, io_result<const_buffer_span>, const_buffer_span, s().pull(args()))
NET_DETAIL_ERASED_OP(op_commit, is_buffer_sink<S>::value, io_result<>, std::size_t, s().commit(args()))
NET_DETAIL_ERASED_OP(op_commit_eof, is_buffer_sink<S>::value, io_result<>, std::size_t, s().commit_eof(args()))

#undef NET_DETAIL_ERASED_OP

template <class S> void destroy_object(void* const p) noexcept { delete static_cast<S*>(p); }

// 拥有 / 引用被包装对象的公共部分。
template <class VTable> struct erased_object_base {
    erased_object_base() noexcept = default;
    erased_object_base(erased_object_base&& other) noexcept
        : object_{other.object_}, vt_{other.vt_}, owns_{other.owns_} {
        slot_.steal(other.slot_);
        other.object_ = nullptr;
        other.vt_ = nullptr;
        other.owns_ = false;
    }
    erased_object_base& operator=(erased_object_base&& other) noexcept {
        if (this == &other) return *this;
        reset();
        object_ = other.object_;
        vt_ = other.vt_;
        owns_ = other.owns_;
        slot_.steal(other.slot_);
        other.object_ = nullptr;
        other.vt_ = nullptr;
        other.owns_ = false;
        return *this;
    }
    erased_object_base(erased_object_base const&) = delete;
    erased_object_base& operator=(erased_object_base const&) = delete;
    ~erased_object_base() { reset(); }

    bool has_value() const noexcept { return object_ != nullptr; }
    explicit operator bool() const noexcept { return has_value(); }

  protected:
    template <class S> void adopt(S* const object, bool const owns, VTable const* const vt) {
        std::unique_ptr<S> guard{owns ? object : nullptr};
        reset();
        slot_.allocate(vt->awaiter_size, vt->awaiter_align);
        object_ = object;
        vt_ = vt;
        owns_ = owns;
        guard.release();
    }

    void reset() noexcept {
        slot_.release();
        if (owns_ && object_ != nullptr) vt_->destroy_object(object_);
        object_ = nullptr;
        vt_ = nullptr;
        owns_ = false;
    }

    template <class Result> erased_awaitable<Result> make(typename erased_awaitable<Result>::construct_fn const fn,
                                                          void const* const args) noexcept {
        CO2_CONTRACT_CHECK(object_ != nullptr);
        CO2_CONTRACT_CHECK(not slot_.active); // 同一对象同时只能有一个操作
        return erased_awaitable<Result>{object_, args, &slot_, fn};
    }

    void* object_ = nullptr;
    VTable const* vt_ = nullptr;
    erased_slot slot_;
    bool owns_ = false;
};

} // namespace detail

// ---------------------------------------------------------------------------
// any_read_source

namespace detail {
struct any_read_source_vtable {
    erased_awaitable<io_result<std::size_t>>::construct_fn read_some;
    erased_awaitable<io_result<std::size_t>>::construct_fn read;
    void (*destroy_object)(void*) noexcept;
    std::size_t awaiter_size;
    std::size_t awaiter_align;

    template <class S> static any_read_source_vtable const* get() noexcept {
        static any_read_source_vtable const table{
            op_read_some<S>::fn, op_read<S>::fn, &detail::destroy_object<S>,
            max_of(op_read_some<S>::size, op_read<S>::size), max_of(op_read_some<S>::align, op_read<S>::align)};
        return &table;
    }
};
} // namespace detail

struct any_read_source : detail::erased_object_base<detail::any_read_source_vtable> {
    any_read_source() noexcept = default;
    any_read_source(any_read_source&&) noexcept = default;
    any_read_source& operator=(any_read_source&&) noexcept = default;

    template <class S, class = typename std::enable_if<is_read_source<S>::value &&
                                                       not std::is_same<typename std::decay<S>::type, any_read_source>::value>::type>
    explicit any_read_source(S source) {
        std::unique_ptr<S> owned{new S(std::move(source))};
        this->adopt(owned.get(), true, detail::any_read_source_vtable::get<S>());
        owned.release();
    }

    template <class S, class = typename std::enable_if<is_read_source<S>::value>::type>
    explicit any_read_source(S* const source) {
        this->adopt(source, false, detail::any_read_source_vtable::get<S>());
    }

    template <class MutableBufferSequence>
    detail::erased_awaitable<io_result<std::size_t>> read_some(MutableBufferSequence const& buffers) noexcept {
        pending_ = mutable_buffer_array<>{buffers};
        return make<io_result<std::size_t>>(vt_->read_some, &pending_);
    }

    // 读满整个序列：按窗口穿过类型擦除边界。
    template <class MutableBufferSequence> task<io_result<std::size_t>> read(MutableBufferSequence buffers) {
        auto const goal = buffer_size(buffers); // 先算：实参求值顺序未定，不能与 std::move 同列
        return read_windows(this, std::move(buffers), goal);
    }

  private:
    template <class MutableBufferSequence>
    static auto read_windows(any_read_source* self, MutableBufferSequence buffers, std::size_t goal)
        CO2_BEG((task<io_result<std::size_t>>), (self, buffers, goal), std::size_t total{}; io_result<std::size_t> r;) {
        while (total < goal) {
            self->pending_ = mutable_buffer_array<>{buffers, total};
            CO2_AWAIT_SET(r, self->make<io_result<std::size_t>>(self->vt_->read, &self->pending_));
            total += r.value;
            if (r.ec) CO2_RETURN((io_result<std::size_t>{r.ec, total}));
        }
        CO2_RETURN((io_result<std::size_t>{std::error_code{}, total}));
    }
    CO2_END

    mutable_buffer_array<> pending_;
};

// ---------------------------------------------------------------------------
// any_write_sink

namespace detail {
struct any_write_sink_vtable {
    erased_awaitable<io_result<std::size_t>>::construct_fn write_some;
    erased_awaitable<io_result<std::size_t>>::construct_fn write;
    erased_awaitable<io_result<std::size_t>>::construct_fn write_eof_buffers;
    erased_awaitable<io_result<>>::construct_fn write_eof;
    void (*destroy_object)(void*) noexcept;
    std::size_t awaiter_size;
    std::size_t awaiter_align;

    template <class S> static any_write_sink_vtable const* get() noexcept {
        static any_write_sink_vtable const table{
            op_write_some<S>::fn, op_write<S>::fn, op_write_eof_buffers<S>::fn, op_write_eof<S>::fn, &detail::destroy_object<S>,
            max_of(max_of(op_write_some<S>::size, op_write<S>::size), max_of(op_write_eof_buffers<S>::size, op_write_eof<S>::size)),
            max_of(max_of(op_write_some<S>::align, op_write<S>::align),
                   max_of(op_write_eof_buffers<S>::align, op_write_eof<S>::align))};
        return &table;
    }
};
} // namespace detail

struct any_write_sink : detail::erased_object_base<detail::any_write_sink_vtable> {
    any_write_sink() noexcept = default;
    any_write_sink(any_write_sink&&) noexcept = default;
    any_write_sink& operator=(any_write_sink&&) noexcept = default;

    template <class S, class = typename std::enable_if<is_write_sink<S>::value &&
                                                       not std::is_same<typename std::decay<S>::type, any_write_sink>::value>::type>
    explicit any_write_sink(S sink) {
        std::unique_ptr<S> owned{new S(std::move(sink))};
        this->adopt(owned.get(), true, detail::any_write_sink_vtable::get<S>());
        owned.release();
    }

    template <class S, class = typename std::enable_if<is_write_sink<S>::value>::type>
    explicit any_write_sink(S* const sink) {
        this->adopt(sink, false, detail::any_write_sink_vtable::get<S>());
    }

    template <class ConstBufferSequence>
    detail::erased_awaitable<io_result<std::size_t>> write_some(ConstBufferSequence const& buffers) noexcept {
        pending_ = const_buffer_array<>{buffers};
        return make<io_result<std::size_t>>(vt_->write_some, &pending_);
    }

    template <class ConstBufferSequence> task<io_result<std::size_t>> write(ConstBufferSequence buffers) {

        auto const goal = buffer_size(buffers); // 先算：实参求值顺序未定，不能与 std::move 同列

        return write_windows(this, std::move(buffers), goal, false);

    }

    template <class ConstBufferSequence> task<io_result<std::size_t>> write_eof(ConstBufferSequence buffers) {

        auto const goal = buffer_size(buffers); // 先算：实参求值顺序未定，不能与 std::move 同列

        return write_windows(this, std::move(buffers), goal, true);

    }

    detail::erased_awaitable<io_result<>> write_eof() noexcept { return make<io_result<>>(vt_->write_eof, &pending_eof_); }

  private:
    // 逐窗写；eof 为真时最后一窗用 write_eof(buffers)（空序列则 write_eof()）。
    template <class ConstBufferSequence>
    static auto write_windows(any_write_sink* self, ConstBufferSequence buffers, std::size_t goal, bool eof)
        CO2_BEG((task<io_result<std::size_t>>), (self, buffers, goal, eof), std::size_t total{}; io_result<std::size_t> r;
                io_result<> e; bool last{};) {
        while (total < goal) {
            self->pending_ = const_buffer_array<>{buffers, total};
            last = eof && total + self->pending_.total_size() >= goal;
            CO2_AWAIT_SET(r, self->make<io_result<std::size_t>>(last ? self->vt_->write_eof_buffers : self->vt_->write,
                                                                   &self->pending_));
            total += r.value;
            if (r.ec) CO2_RETURN((io_result<std::size_t>{r.ec, total}));
            if (last) CO2_RETURN((io_result<std::size_t>{std::error_code{}, total}));
        }
        if (eof) { // 空序列：只发 EOF
            CO2_AWAIT_SET(e, self->write_eof());
            CO2_RETURN((io_result<std::size_t>{e.ec, total}));
        }
        CO2_RETURN((io_result<std::size_t>{std::error_code{}, total}));
    }
    CO2_END

    const_buffer_array<> pending_;
    int pending_eof_ = 0;
};

// ---------------------------------------------------------------------------
// any_buffer_source

namespace detail {
struct any_buffer_source_vtable {
    erased_awaitable<io_result<const_buffer_span>>::construct_fn pull;
    void (*consume)(void* object, std::size_t n) noexcept;
    erased_awaitable<io_result<std::size_t>>::construct_fn read_some; // 被包装类型是 ReadStream 时非空
    erased_awaitable<io_result<std::size_t>>::construct_fn read;      // 被包装类型是 ReadSource 时非空
    void (*destroy_object)(void*) noexcept;
    std::size_t awaiter_size;
    std::size_t awaiter_align;

    template <class S> static void consume_impl(void* const object, std::size_t const n) noexcept {
        static_cast<S*>(object)->consume(n);
    }

    template <class S> static any_buffer_source_vtable const* get() noexcept {
        static any_buffer_source_vtable const table{
            op_pull<S>::fn, &consume_impl<S>, op_read_some<S>::fn, op_read<S>::fn, &detail::destroy_object<S>,
            max_of(op_pull<S>::size, max_of(op_read_some<S>::size, op_read<S>::size)),
            max_of(op_pull<S>::align, max_of(op_read_some<S>::align, op_read<S>::align))};
        return &table;
    }
};
} // namespace detail

struct any_buffer_source : detail::erased_object_base<detail::any_buffer_source_vtable> {
    any_buffer_source() noexcept = default;
    any_buffer_source(any_buffer_source&&) noexcept = default;
    any_buffer_source& operator=(any_buffer_source&&) noexcept = default;

    template <class S, class = typename std::enable_if<is_buffer_source<S>::value &&
                                                       not std::is_same<typename std::decay<S>::type, any_buffer_source>::value>::type>
    explicit any_buffer_source(S source) {
        std::unique_ptr<S> owned{new S(std::move(source))};
        this->adopt(owned.get(), true, detail::any_buffer_source_vtable::get<S>());
        owned.release();
    }

    template <class S, class = typename std::enable_if<is_buffer_source<S>::value>::type>
    explicit any_buffer_source(S* const source) {
        this->adopt(source, false, detail::any_buffer_source_vtable::get<S>());
    }

    detail::erased_awaitable<io_result<const_buffer_span>> pull(const_buffer_span const dest) noexcept {
        pending_dest_ = dest;
        return make<io_result<const_buffer_span>>(vt_->pull, &pending_dest_);
    }

    void consume(std::size_t const n) noexcept { vt_->consume(object_, n); }

    // 调用方拥有缓冲区的读：被包装类型是 ReadStream 就转发，否则 pull → 拷贝 → consume 合成。
    template <class MutableBufferSequence> task<io_result<std::size_t>> read_some(MutableBufferSequence buffers) {
        return read_some_impl(this, mutable_buffer_array<>{buffers});
    }

    template <class MutableBufferSequence> task<io_result<std::size_t>> read(MutableBufferSequence buffers) {

        auto const goal = buffer_size(buffers); // 先算：实参求值顺序未定，不能与 std::move 同列

        return read_impl(this, std::move(buffers), goal);

    }

  private:
    static auto read_some_impl(any_buffer_source* self, mutable_buffer_array<> buffers)
        CO2_BEG((task<io_result<std::size_t>>), (self, buffers), io_result<std::size_t> r; const_buffer scratch[max_iovec];
                io_result<const_buffer_span> pulled; std::size_t n{};) {
        if (self->vt_->read_some != nullptr) {
            self->pending_ = buffers;
            CO2_AWAIT_SET(r, self->make<io_result<std::size_t>>(self->vt_->read_some, &self->pending_));
            CO2_RETURN(r);
        }
        if (buffers.total_size() == 0U) CO2_RETURN((io_result<std::size_t>{std::error_code{}, 0U}));
        CO2_AWAIT_SET(pulled, self->pull(const_buffer_span{scratch, max_iovec}));
        if (pulled.ec) CO2_RETURN((io_result<std::size_t>{pulled.ec, 0U}));
        n = buffer_copy(buffers, pulled.value);
        self->consume(n);
        CO2_RETURN((io_result<std::size_t>{std::error_code{}, n}));
    }
    CO2_END

    template <class MutableBufferSequence>
    static auto read_impl(any_buffer_source* self, MutableBufferSequence buffers, std::size_t goal)
        CO2_BEG((task<io_result<std::size_t>>), (self, buffers, goal), std::size_t total{}; io_result<std::size_t> r;) {
        while (total < goal) {
            CO2_AWAIT_SET(r, read_some_impl(self, mutable_buffer_array<>{buffers, total}));
            total += r.value;
            if (r.ec) CO2_RETURN((io_result<std::size_t>{r.ec, total}));
        }
        CO2_RETURN((io_result<std::size_t>{std::error_code{}, total}));
    }
    CO2_END

    const_buffer_span pending_dest_;
    mutable_buffer_array<> pending_;
};

// ---------------------------------------------------------------------------
// any_buffer_sink

namespace detail {
struct any_buffer_sink_vtable {
    mutable_buffer_span (*prepare)(void* object, mutable_buffer_span dest);
    erased_awaitable<io_result<>>::construct_fn commit;
    erased_awaitable<io_result<>>::construct_fn commit_eof;
    erased_awaitable<io_result<std::size_t>>::construct_fn write_some;        // WriteStream 时非空
    erased_awaitable<io_result<std::size_t>>::construct_fn write;             // WriteSink 时非空
    erased_awaitable<io_result<std::size_t>>::construct_fn write_eof_buffers; // WriteSink 时非空
    erased_awaitable<io_result<>>::construct_fn write_eof;                    // WriteSink 时非空
    void (*destroy_object)(void*) noexcept;
    std::size_t awaiter_size;
    std::size_t awaiter_align;

    template <class S> static mutable_buffer_span prepare_impl(void* const object, mutable_buffer_span const dest) {
        return static_cast<S*>(object)->prepare(dest);
    }

    template <class S> static any_buffer_sink_vtable const* get() noexcept {
        static any_buffer_sink_vtable const table{
            &prepare_impl<S>, op_commit<S>::fn, op_commit_eof<S>::fn, op_write_some<S>::fn, op_write<S>::fn,
            op_write_eof_buffers<S>::fn, op_write_eof<S>::fn, &detail::destroy_object<S>,
            max_of(max_of(op_commit<S>::size, op_commit_eof<S>::size),
                   max_of(max_of(op_write_some<S>::size, op_write<S>::size),
                          max_of(op_write_eof_buffers<S>::size, op_write_eof<S>::size))),
            max_of(max_of(op_commit<S>::align, op_commit_eof<S>::align),
                   max_of(max_of(op_write_some<S>::align, op_write<S>::align),
                          max_of(op_write_eof_buffers<S>::align, op_write_eof<S>::align)))};
        return &table;
    }
};
} // namespace detail

struct any_buffer_sink : detail::erased_object_base<detail::any_buffer_sink_vtable> {
    any_buffer_sink() noexcept = default;
    any_buffer_sink(any_buffer_sink&&) noexcept = default;
    any_buffer_sink& operator=(any_buffer_sink&&) noexcept = default;

    template <class S, class = typename std::enable_if<is_buffer_sink<S>::value &&
                                                       not std::is_same<typename std::decay<S>::type, any_buffer_sink>::value>::type>
    explicit any_buffer_sink(S sink) {
        std::unique_ptr<S> owned{new S(std::move(sink))};
        this->adopt(owned.get(), true, detail::any_buffer_sink_vtable::get<S>());
        owned.release();
    }

    template <class S, class = typename std::enable_if<is_buffer_sink<S>::value>::type>
    explicit any_buffer_sink(S* const sink) {
        this->adopt(sink, false, detail::any_buffer_sink_vtable::get<S>());
    }

    mutable_buffer_span prepare(mutable_buffer_span const dest) { return vt_->prepare(object_, dest); }

    detail::erased_awaitable<io_result<>> commit(std::size_t const n) noexcept {
        pending_n_ = n;
        return make<io_result<>>(vt_->commit, &pending_n_);
    }

    detail::erased_awaitable<io_result<>> commit_eof(std::size_t const n) noexcept {
        pending_n_ = n;
        return make<io_result<>>(vt_->commit_eof, &pending_n_);
    }

    // 调用方拥有缓冲区的写：被包装类型是 WriteStream / WriteSink 就转发，否则 prepare → 拷贝 → commit 合成。
    template <class ConstBufferSequence> task<io_result<std::size_t>> write_some(ConstBufferSequence buffers) {
        return write_some_impl(this, const_buffer_array<>{buffers}, false);
    }

    template <class ConstBufferSequence> task<io_result<std::size_t>> write(ConstBufferSequence buffers) {

        auto const goal = buffer_size(buffers); // 先算：实参求值顺序未定，不能与 std::move 同列

        return write_impl(this, std::move(buffers), goal, false);

    }

    template <class ConstBufferSequence> task<io_result<std::size_t>> write_eof(ConstBufferSequence buffers) {

        auto const goal = buffer_size(buffers); // 先算：实参求值顺序未定，不能与 std::move 同列

        return write_impl(this, std::move(buffers), goal, true);

    }

    task<io_result<>> write_eof() { return write_eof_impl(this); }

  private:
    // 一窗。forward_eof：这是最后一窗且被包装类型是 WriteSink，用它的 write_eof(buffers) 原子地写完并发
    // EOF。否则转发 write_some（WriteStream）或用 prepare → 拷贝 → commit 合成；合成路径一窗可能写不完
    //（prepare 给的空间小于窗），所以 EOF 不在这里发，由 write_impl 在全部写完后用 write_eof() 发。
    static auto write_some_impl(any_buffer_sink* self, const_buffer_array<> buffers, bool forward_eof)
        CO2_BEG((task<io_result<std::size_t>>), (self, buffers, forward_eof), io_result<std::size_t> r;
                mutable_buffer room[max_iovec]; mutable_buffer_span prepared; std::size_t n{}; io_result<> c;) {
        if (forward_eof) {
            self->pending_ = buffers;
            CO2_AWAIT_SET(r, self->make<io_result<std::size_t>>(self->vt_->write_eof_buffers, &self->pending_));
            CO2_RETURN(r);
        }
        if (self->vt_->write_some != nullptr) {
            self->pending_ = buffers;
            CO2_AWAIT_SET(r, self->make<io_result<std::size_t>>(self->vt_->write_some, &self->pending_));
            CO2_RETURN(r);
        }
        prepared = self->prepare(mutable_buffer_span{room, max_iovec});
        if (prepared.empty() && buffers.total_size() != 0U)
            CO2_RETURN((io_result<std::size_t>{std::make_error_code(std::errc::no_buffer_space), 0U}));
        n = buffer_copy(prepared, buffers);
        CO2_AWAIT_SET(c, self->commit(n));
        CO2_RETURN((io_result<std::size_t>{c.ec, c.ec ? 0U : n}));
    }
    CO2_END

    template <class ConstBufferSequence>
    static auto write_impl(any_buffer_sink* self, ConstBufferSequence buffers, std::size_t goal, bool eof)
        CO2_BEG((task<io_result<std::size_t>>), (self, buffers, goal, eof), std::size_t total{}; io_result<std::size_t> r;
                io_result<> e; const_buffer_array<> window; bool last_forward{};) {
        while (total < goal) {
            window = const_buffer_array<>{buffers, total};
            last_forward = eof && self->vt_->write_eof_buffers != nullptr && total + window.total_size() >= goal;
            CO2_AWAIT_SET(r, write_some_impl(self, window, last_forward));
            total += r.value;
            if (r.ec) CO2_RETURN((io_result<std::size_t>{r.ec, total}));
            if (last_forward) CO2_RETURN((io_result<std::size_t>{std::error_code{}, total}));
        }
        if (eof) {
            CO2_AWAIT_SET(e, self->write_eof());
            CO2_RETURN((io_result<std::size_t>{e.ec, total}));
        }
        CO2_RETURN((io_result<std::size_t>{std::error_code{}, total}));
    }
    CO2_END

    static auto write_eof_impl(any_buffer_sink* self) CO2_BEG((task<io_result<>>), (self), io_result<> r;) {
        if (self->vt_->write_eof != nullptr) {
            CO2_AWAIT_SET(r, self->make<io_result<>>(self->vt_->write_eof, &self->pending_n_));
            CO2_RETURN(r);
        }
        CO2_AWAIT_SET(r, self->commit_eof(0U));
        CO2_RETURN(r);
    }
    CO2_END

    std::size_t pending_n_ = 0;
    const_buffer_array<> pending_;
};

} // namespace net
