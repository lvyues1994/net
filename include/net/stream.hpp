#pragma once

#include <cstddef>
#include <cstring>
#include <exception>
#include <string>
#include <system_error>
#include <type_traits>
#include <utility>

#include "co2/contract.hpp"
#include "co2/detail/awaitable.hpp"

#include "net/buffers.hpp"
#include "net/detail/completion_frame.hpp"
#include "net/detail/storage.hpp"
#include "net/error.hpp"
#include "net/io_awaitable_promise_base.hpp"
#include "net/io_env.hpp"
#include "net/io_result.hpp"
#include "net/task.hpp"

// 流概念（P4100R1 §8.6）：调用方拥有缓冲区的字节流。
//
//   ReadStream:  stream.read_some(MutableBufferSequence)  → IoAwaitable → io_result<size_t>
//   WriteStream: stream.write_some(ConstBufferSequence)   → IoAwaitable → io_result<size_t>
//   Stream:      两者都满足
//
// read_some / write_some 传输至少一个字节或以错误完成（EOF 是 error::eof）。零长度缓冲区
// 立即以 {ec = {}, n = 0} 完成。同一流同一方向同一时刻只能有一个未完成的操作。
//
// 组合算法 read / write（无帧的 awaiter）与 read_until（协程）基于这两个原语，对任何满足概念的流都
// 适用；它们与 io_context 无关，可以定义在 .cpp 里、对类型擦除的 any_stream& 编译一次。

namespace net {
namespace detail {

template <class S, class = void> struct is_read_stream_impl : std::false_type {};

template <class S>
struct is_read_stream_impl<
    S, void_t<decltype(std::declval<S&>().read_some(std::declval<mutable_buffer>()))>>
    : std::integral_constant<
          bool,
          is_io_awaitable<decltype(std::declval<S&>().read_some(std::declval<mutable_buffer>()))>::value &&
              std::is_same<awaitable_result_t<decltype(
                               std::declval<S&>().read_some(std::declval<mutable_buffer>()))>,
                           io_result<std::size_t>>::value> {};

template <class S, class = void> struct is_write_stream_impl : std::false_type {};

template <class S>
struct is_write_stream_impl<
    S, void_t<decltype(std::declval<S&>().write_some(std::declval<const_buffer>()))>>
    : std::integral_constant<
          bool,
          is_io_awaitable<decltype(std::declval<S&>().write_some(std::declval<const_buffer>()))>::value &&
              std::is_same<awaitable_result_t<decltype(
                               std::declval<S&>().write_some(std::declval<const_buffer>()))>,
                           io_result<std::size_t>>::value> {};

} // namespace detail

template <class S> struct is_read_stream : detail::is_read_stream_impl<typename std::decay<S>::type> {};
template <class S> struct is_write_stream : detail::is_write_stream_impl<typename std::decay<S>::type> {};
template <class S>
struct is_stream : std::integral_constant<bool, is_read_stream<S>::value && is_write_stream<S>::value> {};

// ---- 组合算法 ----

namespace detail {

// read / write 不是协程而是一个 awaiter：没有自己的帧。它反复发起 *_some，就绪就在 await_ready 里
// 直接继续（推测成功的写、数据已到的读：整个操作零挂起、零分配）；要等待时把一个手写的
// completion_frame 交给 *_some 当续体，完成后在 on_step 里取结果、发起下一段或恢复父协程
// （对称转移）。一趟 64 B 回环往返因此从 4 次帧分配降到 0，用户态 CPU 少约 200 ns。
//
// awaiter 住在父协程帧的 awaiter 槽里（co2 的 AwaitSlot；net 把槽放大到 NET_AWAIT_STORAGE_SIZE），
// 所以对单个缓冲区它整体内联；缓冲区序列类型很大时 co2 退回一次堆分配，语义不变。
// 契约同 task 版本：awaiter 在 await_ready 之后不再移动（co2 保证）。

struct read_transfer {
    using buffer_type = mutable_buffer;
    template <class S, class B> static auto start(S& s, B const& b) -> decltype(s.read_some(b)) { return s.read_some(b); }
};

struct write_transfer {
    using buffer_type = const_buffer;
    template <class S, class B> static auto start(S& s, B const& b) -> decltype(s.write_some(b)) {
        return s.write_some(b);
    }
};

// 从 skip 字节起的剩余窗口：单个缓冲区直接偏移，序列展平成至多 max_iovec 个（下一段从上次消费
// 到的位置重新展平，任意长的序列都覆盖）。
template <class Buffer, class Sequence>
using is_single_buffer = std::integral_constant<bool, std::is_convertible<Sequence const&, Buffer>::value>;

template <class Buffer, class Sequence>
Buffer window_of(Sequence const& buffers, std::size_t const skip, std::true_type) noexcept {
    return Buffer(buffers) + skip;
}

template <class Buffer, class Sequence>
buffer_array<Buffer, max_iovec> window_of(Sequence const& buffers, std::size_t const skip, std::false_type) noexcept {
    return buffer_array<Buffer, max_iovec>{buffers, skip};
}

template <class Stream, class Sequence, class Direction> struct transfer_awaitable {
    using buffer_type = typename Direction::buffer_type;
    using single = is_single_buffer<buffer_type, Sequence>;
    using window_type =
        typename std::conditional<single::value, buffer_type, buffer_array<buffer_type, max_iovec>>::type;
    using inner_type =
        co2::detail::AwaiterOf<decltype(Direction::start(std::declval<Stream&>(), std::declval<window_type const&>()))>;

    transfer_awaitable(Stream& stream_, Sequence buffers_) : stream{&stream_}, buffers(std::move(buffers_)) {}

    // 只在启动前移动（进父帧的 awaiter 槽）：那时 inner 尚未构造、frame 尚未交出去。
    transfer_awaitable(transfer_awaitable&& other) noexcept(std::is_nothrow_move_constructible<Sequence>::value)
        : stream{other.stream}, buffers(std::move(other.buffers)), total{other.total}, ec{other.ec} {
        CO2_CONTRACT_CHECK(not other.inner.hasValue());
    }

    transfer_awaitable(transfer_awaitable const&) = delete;
    transfer_awaitable& operator=(transfer_awaitable const&) = delete;
    transfer_awaitable& operator=(transfer_awaitable&&) = delete;

    // 同步推进到"完成"（真）或"这一段需要等待"（假；inner 已构造并已发起）。带环境的版本把 env 转给每一段，
    // 停止已请求时下一段以 operation_aborted 结束，整个操作返回 {aborted, 已传输的字节}。
    bool await_ready() { return advance(); }
    bool await_ready(io_env const* const e) {
        env = e;
        return advance();
    }

    coroutine_handle<> await_suspend(coroutine_handle<> const h, io_env const* const e) {
        parent = h;
        env = e;
        frame.set(&on_step, this);
        return inner.get().await_suspend(frame.handle(), e);
    }

    io_result<std::size_t> await_resume() {
        if (exception) std::rethrow_exception(exception);
        return io_result<std::size_t>{ec, total};
    }

  private:
    bool advance() {
        for (;;) {
            if (total >= buffer_size(buffers)) return true;
            inner.emplace(co2::detail::getAwaiter(
                Direction::start(*stream, window_of<buffer_type>(buffers, total, single{}))));
            if (not await_ready_with(inner.get(), env)) return false;
            auto const partial = inner.get().await_resume();
            inner.reset();
            total += partial.value;
            if (partial.ec) {
                ec = partial.ec;
                return true;
            }
        }
    }

    // 一段 *_some 在执行器上完成后：取结果，继续下一段或回到父协程。异常（流的 await_resume /
    // 发起抛出）记下来在 await_resume 重抛——与协程版本一致。
    static coroutine_handle<> on_step(void* const user) {
        auto* const self = static_cast<transfer_awaitable*>(user);
        try {
            auto const partial = self->inner.get().await_resume();
            self->inner.reset();
            self->total += partial.value;
            if (partial.ec) {
                self->ec = partial.ec;
                return self->parent;
            }
            if (self->advance()) return self->parent;
            return self->inner.get().await_suspend(self->frame.handle(), self->env);
        } catch (...) {
            self->inner.reset();
            self->exception = std::current_exception();
            return self->parent;
        }
    }

    Stream* stream;
    Sequence buffers;
    std::size_t total = 0U;
    std::error_code ec;
    late_init<inner_type> inner;
    completion_frame frame;
    coroutine_handle<> parent;
    io_env const* env = nullptr;
    std::exception_ptr exception;
};

} // namespace detail

// 读满整个缓冲区序列（或直到错误）。返回 {ec, 已读字节数}。序列可以有任意多个缓冲区。
template <class Stream, class MutableBufferSequence>
detail::transfer_awaitable<Stream, MutableBufferSequence, detail::read_transfer> read(Stream& stream,
                                                                                       MutableBufferSequence buffers) {
    static_assert(is_read_stream<Stream>::value, "net::read requires a ReadStream");
    static_assert(is_mutable_buffer_sequence<MutableBufferSequence>::value, "net::read requires a MutableBufferSequence");
    return detail::transfer_awaitable<Stream, MutableBufferSequence, detail::read_transfer>{stream, std::move(buffers)};
}

// 写出整个缓冲区序列（或直到错误）。返回 {ec, 已写字节数}。
template <class Stream, class ConstBufferSequence>
detail::transfer_awaitable<Stream, ConstBufferSequence, detail::write_transfer> write(Stream& stream,
                                                                                       ConstBufferSequence buffers) {
    static_assert(is_write_stream<Stream>::value, "net::write requires a WriteStream");
    static_assert(is_const_buffer_sequence<ConstBufferSequence>::value, "net::write requires a ConstBufferSequence");
    return detail::transfer_awaitable<Stream, ConstBufferSequence, detail::write_transfer>{stream, std::move(buffers)};
}

namespace detail {

// 在缓冲区序列（按字节看成一段）里从 from 起查找 delimiter，匹配可以跨段（环形缓冲的可读区绕过末尾时
// 是两段）；找到返回分隔符结束位置，否则返回 npos。位置用（段号，段内偏移）游标表示，逐字节推进。
inline std::size_t find_delimiter(const_buffer_array<> const& parts, std::size_t const from,
                                  std::string const& delimiter) noexcept {
    constexpr auto npos = static_cast<std::size_t>(-1);
    auto const total = parts.total_size();
    auto const length = delimiter.size();
    if (length == 0U) return from <= total ? from : npos; // 空分隔符立即匹配（与 Asio 一致）
    if (total < length || from + length > total) return npos;
    struct cursor {
        std::size_t part;
        std::size_t offset;
        void step(const_buffer_array<> const& in) noexcept { // 前进一字节（buffer_array 里没有空段）
            if (++offset == in[part].size()) {
                ++part;
                offset = 0U;
            }
        }
        char at(const_buffer_array<> const& in) const noexcept { return static_cast<char const*>(in[part].data())[offset]; }
    };
    cursor start{0U, from};
    while (start.offset >= parts[start.part].size()) {
        start.offset -= parts[start.part].size();
        ++start.part;
    }
    for (auto position = from; position + length <= total; ++position) {
        auto probe = start;
        auto matched = std::size_t{};
        while (matched != length && probe.at(parts) == delimiter[matched]) {
            ++matched;
            probe.step(parts);
        }
        if (matched == length) return position + length;
        start.step(parts);
    }
    return npos;
}

} // namespace detail

// 读到动态缓冲里出现 delimiter 为止。成功时 value 是包含分隔符在内、到分隔符结束的字节数
// （缓冲区里可能还有分隔符之后的数据）；缓冲区满而未找到时 ec 是 std::errc::no_buffer_space。
template <class Stream, class DynamicBuffer>
auto read_until(Stream& stream, DynamicBuffer& buffer, std::string delimiter)
    CO2_BEG((task<io_result<std::size_t>>), (stream, buffer, delimiter), std::size_t search_from{};
            std::size_t found{}; std::size_t chunk{}; io_result<std::size_t> partial;) {
    static_assert(is_read_stream<Stream>::value, "net::read_until requires a ReadStream");
    for (;;) {
        {
            auto const readable = const_buffer_array<>{buffer.data()}; // 平面缓冲一段，环形缓冲最多两段
            found = detail::find_delimiter(readable, search_from, delimiter);
            if (found != static_cast<std::size_t>(-1))
                CO2_RETURN((io_result<std::size_t>{std::error_code{}, found}));
            // 下次只需从"可能跨越新旧边界"的位置开始找。
            search_from = readable.total_size() >= delimiter.size() ? readable.total_size() - delimiter.size() + 1U : 0U;
            if (buffer.size() >= buffer.max_size())
                CO2_RETURN((io_result<std::size_t>{make_error_code(std::errc::no_buffer_space), 0U}));
            chunk = buffer.max_size() - buffer.size();
            if (chunk > 4096U) chunk = 4096U;
        }
        CO2_AWAIT_SET(partial, stream.read_some(buffer.prepare(chunk)));
        buffer.commit(partial.value);
        if (partial.ec) CO2_RETURN((io_result<std::size_t>{partial.ec, 0U}));
    }
}
CO2_END

} // namespace net
