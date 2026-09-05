#pragma once

#include <cstddef>
#include <cstring>
#include <string>
#include <system_error>
#include <type_traits>
#include <utility>

#include "net/buffers.hpp"
#include "net/error.hpp"
#include "net/io_awaitable_promise_base.hpp"
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
// 组合算法 read / write / read_until 是基于这两个原语的协程，对任何满足概念的流都适用；
// 它们与 io_context 无关，可以定义在 .cpp 里、对类型擦除的 any_stream& 编译一次。

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

// 读满整个缓冲区序列（或直到错误）。返回 {ec, 已读字节数}。
template <class Stream, class MutableBufferSequence>
auto read(Stream& stream, MutableBufferSequence buffers)
    CO2_BEG((task<io_result<std::size_t>>), (stream, buffers), mutable_buffer_array<> remaining;
            std::size_t total{}; io_result<std::size_t> partial;) {
    static_assert(is_read_stream<Stream>::value, "net::read requires a ReadStream");
    static_assert(is_mutable_buffer_sequence<MutableBufferSequence>::value,
                  "net::read requires a MutableBufferSequence");
    remaining = mutable_buffer_array<>{buffers};
    while (not remaining.empty()) {
        CO2_AWAIT_SET(partial, stream.read_some(remaining));
        total += partial.value;
        if (partial.ec) CO2_RETURN((io_result<std::size_t>{partial.ec, total}));
        remaining.consume(partial.value);
    }
    CO2_RETURN((io_result<std::size_t>{std::error_code{}, total}));
}
CO2_END

// 写出整个缓冲区序列（或直到错误）。返回 {ec, 已写字节数}。
template <class Stream, class ConstBufferSequence>
auto write(Stream& stream, ConstBufferSequence buffers)
    CO2_BEG((task<io_result<std::size_t>>), (stream, buffers), const_buffer_array<> remaining;
            std::size_t total{}; io_result<std::size_t> partial;) {
    static_assert(is_write_stream<Stream>::value, "net::write requires a WriteStream");
    static_assert(is_const_buffer_sequence<ConstBufferSequence>::value,
                  "net::write requires a ConstBufferSequence");
    remaining = const_buffer_array<>{buffers};
    while (not remaining.empty()) {
        CO2_AWAIT_SET(partial, stream.write_some(remaining));
        total += partial.value;
        if (partial.ec) CO2_RETURN((io_result<std::size_t>{partial.ec, total}));
        remaining.consume(partial.value);
    }
    CO2_RETURN((io_result<std::size_t>{std::error_code{}, total}));
}
CO2_END

namespace detail {

// 在 [data, data + size) 中从 from 起查找 delimiter；找到返回分隔符结束位置，否则返回 npos。
inline std::size_t find_delimiter(char const* const data, std::size_t const size,
                                  std::size_t const from, std::string const& delimiter) noexcept {
    if (delimiter.empty() || size < delimiter.size()) return static_cast<std::size_t>(-1);
    auto const last = size - delimiter.size();
    for (auto index = from; index <= last; ++index)
        if (std::memcmp(data + index, delimiter.data(), delimiter.size()) == 0)
            return index + delimiter.size();
    return static_cast<std::size_t>(-1);
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
            auto const readable = buffer.data();
            found = detail::find_delimiter(static_cast<char const*>(readable.data()),
                                           readable.size(), search_from, delimiter);
            if (found != static_cast<std::size_t>(-1))
                CO2_RETURN((io_result<std::size_t>{std::error_code{}, found}));
            // 下次只需从"可能跨越新旧边界"的位置开始找。
            search_from = readable.size() >= delimiter.size() ? readable.size() - delimiter.size() + 1U : 0U;
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
