#pragma once

#include <cstddef>
#include <cstring>
#include <system_error>
#include <type_traits>
#include <utility>
#include <vector>

#include "net/buffers.hpp"
#include "net/error.hpp"
#include "net/immediate.hpp"
#include "net/io_result.hpp"
#include "net/span.hpp"
#include "net/stream.hpp"
#include "net/task.hpp"

// 流概念的第二族（P4100R1 §8.6 Paper 6；形态取自 Capy 的 read_source / write_sink /
// buffer_source / buffer_sink）。
//
// 调用方拥有缓冲区的精化：
//   ReadSource  = ReadStream  + read(buffers)                     读满整个序列：(ec, n)
//   WriteSink   = WriteStream + write(buffers) + write_eof(buffers) + write_eof()
//                                                                 写完整个序列；write_eof 发出流结束
// 被调方拥有缓冲区（零拷贝路径）：
//   BufferSource: pull(span<const_buffer> dest) → (ec, span<const_buffer>)，consume(n)
//                 重复 pull 不 consume 返回同样的数据；耗尽时 ec == eof 且返回空 span
//   BufferSink:   prepare(span<mutable_buffer> dest) → span<mutable_buffer>（同步，指向 sink 自己的存储）
//                 commit(n) → (ec)，commit_eof(n) → (ec)
//
// 这里给出：概念判定 is_read_source / is_write_sink / is_buffer_source / is_buffer_sink；几个
// 具体模型（memory_source、dynamic_buffer_source / dynamic_buffer_sink）；把任意 Stream 提升为
// ReadSource / WriteSink / BufferSource / BufferSink 的适配器；以及 transfer 算法。类型擦除的
// any_read_source / any_write_sink / any_buffer_source / any_buffer_sink 在 any_source_sink.hpp。

namespace net {

using const_buffer_span = span<const_buffer>;
using mutable_buffer_span = span<mutable_buffer>;

namespace detail {

template <class S, class = void> struct is_read_source_impl : std::false_type {};
template <class S>
struct is_read_source_impl<S, void_t<decltype(std::declval<S&>().read(std::declval<mutable_buffer>()))>>
    : std::integral_constant<
          bool, is_read_stream_impl<S>::value &&
                    is_io_awaitable<decltype(std::declval<S&>().read(std::declval<mutable_buffer>()))>::value &&
                    std::is_same<awaitable_result_t<decltype(std::declval<S&>().read(std::declval<mutable_buffer>()))>,
                                 io_result<std::size_t>>::value> {};

template <class S, class = void> struct is_write_sink_impl : std::false_type {};
template <class S>
struct is_write_sink_impl<S, void_t<decltype(std::declval<S&>().write(std::declval<const_buffer>())),
                                    decltype(std::declval<S&>().write_eof(std::declval<const_buffer>())),
                                    decltype(std::declval<S&>().write_eof())>>
    : std::integral_constant<
          bool, is_write_stream_impl<S>::value &&
                    std::is_same<awaitable_result_t<decltype(std::declval<S&>().write(std::declval<const_buffer>()))>,
                                 io_result<std::size_t>>::value &&
                    std::is_same<awaitable_result_t<decltype(std::declval<S&>().write_eof(std::declval<const_buffer>()))>,
                                 io_result<std::size_t>>::value &&
                    std::is_same<awaitable_result_t<decltype(std::declval<S&>().write_eof())>, io_result<>>::value> {};

template <class S, class = void> struct is_buffer_source_impl : std::false_type {};
template <class S>
struct is_buffer_source_impl<S, void_t<decltype(std::declval<S&>().pull(std::declval<const_buffer_span>())),
                                       decltype(std::declval<S&>().consume(std::declval<std::size_t>()))>>
    : std::integral_constant<
          bool, is_io_awaitable<decltype(std::declval<S&>().pull(std::declval<const_buffer_span>()))>::value &&
                    std::is_same<awaitable_result_t<decltype(std::declval<S&>().pull(std::declval<const_buffer_span>()))>,
                                 io_result<const_buffer_span>>::value> {};

template <class S, class = void> struct is_buffer_sink_impl : std::false_type {};
template <class S>
struct is_buffer_sink_impl<S, void_t<decltype(std::declval<S&>().prepare(std::declval<mutable_buffer_span>())),
                                     decltype(std::declval<S&>().commit(std::declval<std::size_t>())),
                                     decltype(std::declval<S&>().commit_eof(std::declval<std::size_t>()))>>
    : std::integral_constant<
          bool, std::is_same<decltype(std::declval<S&>().prepare(std::declval<mutable_buffer_span>())),
                             mutable_buffer_span>::value &&
                    std::is_same<awaitable_result_t<decltype(std::declval<S&>().commit(std::declval<std::size_t>()))>,
                                 io_result<>>::value &&
                    std::is_same<awaitable_result_t<decltype(std::declval<S&>().commit_eof(std::declval<std::size_t>()))>,
                                 io_result<>>::value> {};

} // namespace detail

template <class S> struct is_read_source : detail::is_read_source_impl<typename std::decay<S>::type> {};
template <class S> struct is_write_sink : detail::is_write_sink_impl<typename std::decay<S>::type> {};
template <class S> struct is_buffer_source : detail::is_buffer_source_impl<typename std::decay<S>::type> {};
template <class S> struct is_buffer_sink : detail::is_buffer_sink_impl<typename std::decay<S>::type> {};

// ---------------------------------------------------------------------------
// 具体模型

// BufferSource：一段只读字节区域（任意 ConstBufferSequence，最多 max_iovec 个非空缓冲区）。
// pull 返回尚未消费的部分（最多 dest.size() 个描述符），consume 前进。
struct memory_source {
    memory_source() = default;
    template <class ConstBufferSequence, class = typename std::enable_if<
                                             is_const_buffer_sequence<ConstBufferSequence>::value>::type>
    explicit memory_source(ConstBufferSequence const& buffers) : buffers_{buffers} {}

    immediate<io_result<const_buffer_span>> pull(const_buffer_span const dest) noexcept {
        auto const remaining = buffers_.to_span();
        if (remaining.empty()) return {{make_error_code(error::eof), const_buffer_span{}}};
        auto const count = remaining.size() < dest.size() ? remaining.size() : dest.size();
        for (auto i = std::size_t{}; i != count; ++i)
            dest[i] = remaining[i];
        return {{std::error_code{}, const_buffer_span{dest.data(), count}}};
    }

    void consume(std::size_t const n) noexcept { buffers_.consume(n); }

    std::size_t remaining() const noexcept { return buffers_.total_size(); }

  private:
    const_buffer_array<> buffers_;
};

// BufferSource over 一个 DynamicBuffer 的可读区：pull 给出 data()，consume 转发。
template <class DynamicBuffer> struct dynamic_buffer_source {
    explicit dynamic_buffer_source(DynamicBuffer& buffer) noexcept : buffer_{&buffer} {}

    immediate<io_result<const_buffer_span>> pull(const_buffer_span const dest) noexcept {
        auto const readable = const_buffer_array<>{buffer_->data()};
        if (readable.empty()) return {{make_error_code(error::eof), const_buffer_span{}}};
        auto const count = readable.size() < dest.size() ? readable.size() : dest.size();
        for (auto i = std::size_t{}; i != count; ++i)
            dest[i] = readable[i];
        return {{std::error_code{}, const_buffer_span{dest.data(), count}}};
    }

    void consume(std::size_t const n) noexcept { buffer_->consume(n); }

  private:
    DynamicBuffer* buffer_;
};

// BufferSink over 一个 DynamicBuffer：prepare 从缓冲区拿可写空间（默认一次 4 KiB，可调），commit 转发；
// commit_eof 之后不再接受写入（finished()）。
template <class DynamicBuffer> struct dynamic_buffer_sink {
    explicit dynamic_buffer_sink(DynamicBuffer& buffer, std::size_t const chunk = 4096U) noexcept
        : buffer_{&buffer}, chunk_{chunk} {}

    mutable_buffer_span prepare(mutable_buffer_span const dest) {
        if (dest.empty() || finished_) return mutable_buffer_span{};
        auto const room = buffer_->max_size() - buffer_->size();
        auto const n = room < chunk_ ? room : chunk_;
        if (n == 0U) return mutable_buffer_span{};
        auto const writable = mutable_buffer_array<>{buffer_->prepare(n)};
        auto const count = writable.size() < dest.size() ? writable.size() : dest.size();
        for (auto i = std::size_t{}; i != count; ++i)
            dest[i] = writable[i];
        prepared_ = true;
        return mutable_buffer_span{dest.data(), count};
    }

    immediate<io_result<>> commit(std::size_t const n) noexcept {
        if (finished_) return {{std::make_error_code(std::errc::operation_not_permitted)}};
        if (prepared_) buffer_->commit(n);
        prepared_ = false;
        return {{std::error_code{}}};
    }

    immediate<io_result<>> commit_eof(std::size_t const n) noexcept {
        auto r = commit(n);
        if (not r.value.ec) finished_ = true;
        return r;
    }

    bool finished() const noexcept { return finished_; }

  private:
    DynamicBuffer* buffer_;
    std::size_t chunk_;
    bool prepared_ = false;
    bool finished_ = false;
};

template <class DynamicBuffer> dynamic_buffer_source<DynamicBuffer> make_source(DynamicBuffer& buffer) noexcept {
    return dynamic_buffer_source<DynamicBuffer>{buffer};
}
template <class DynamicBuffer> dynamic_buffer_sink<DynamicBuffer> make_sink(DynamicBuffer& buffer) noexcept {
    return dynamic_buffer_sink<DynamicBuffer>{buffer};
}

// ---------------------------------------------------------------------------
// 把 Stream 提升为精化概念的适配器

// 流结束的发送方式是一个定制点：signal_stream_eof(stream, detail::eof_preferred{}) 经 ADL 找到流自己
// 的重载（tcp.hpp：shutdown(send)；tls/stream.hpp：可等待的 shutdown()）；没有重载的流没有可表达
// 的 EOF，落到下面的默认版本，什么也不做。
namespace detail {
struct eof_fallback {};
struct eof_preferred : eof_fallback {};
} // namespace detail

template <class Stream> auto signal_stream_eof(Stream&, detail::eof_fallback) CO2_BEG((task<io_result<>>), ()) {
    CO2_RETURN((io_result<>{}));
}
CO2_END

namespace detail {

template <class Stream> task<io_result<>> stream_eof(Stream& stream) {
    return signal_stream_eof(stream, eof_preferred{});
}

template <class Stream, class ConstBufferSequence>
auto write_then_eof(Stream& stream, ConstBufferSequence buffers)
    CO2_BEG((task<io_result<std::size_t>>), (stream, buffers), io_result<std::size_t> w; io_result<> e;) {
    CO2_AWAIT_SET(w, net::write(stream, buffers));
    if (w.ec) CO2_RETURN(w);
    CO2_AWAIT_SET(e, stream_eof(stream));
    CO2_RETURN((io_result<std::size_t>{e.ec, w.value}));
}
CO2_END

} // namespace detail

// ReadSource over 任意 ReadStream：read 是组合算法 net::read。
template <class Stream> struct stream_read_source {
    static_assert(is_read_stream<Stream>::value, "stream_read_source requires a ReadStream");

    explicit stream_read_source(Stream& stream) noexcept : stream_{&stream} {}

    template <class MutableBufferSequence> auto read_some(MutableBufferSequence const& buffers)
        -> decltype(std::declval<Stream&>().read_some(buffers)) {
        return stream_->read_some(buffers);
    }

    template <class MutableBufferSequence> task<io_result<std::size_t>> read(MutableBufferSequence const& buffers) {
        return net::read(*stream_, buffers); // 序列按值进入 net::read 的帧：任意多个缓冲区都覆盖
    }

    Stream& next_layer() noexcept { return *stream_; }

  private:
    Stream* stream_;
};

// WriteSink over 任意 WriteStream：write 是 net::write；write_eof 写完后发流结束（见 detail::stream_eof）。
template <class Stream> struct stream_write_sink {
    static_assert(is_write_stream<Stream>::value, "stream_write_sink requires a WriteStream");

    explicit stream_write_sink(Stream& stream) noexcept : stream_{&stream} {}

    template <class ConstBufferSequence> auto write_some(ConstBufferSequence const& buffers)
        -> decltype(std::declval<Stream&>().write_some(buffers)) {
        return stream_->write_some(buffers);
    }

    template <class ConstBufferSequence> task<io_result<std::size_t>> write(ConstBufferSequence const& buffers) {
        return net::write(*stream_, buffers);
    }

    template <class ConstBufferSequence> task<io_result<std::size_t>> write_eof(ConstBufferSequence const& buffers) {
        return detail::write_then_eof(*stream_, buffers);
    }

    task<io_result<>> write_eof() { return detail::stream_eof(*stream_); }

    Stream& next_layer() noexcept { return *stream_; }

  private:
    Stream* stream_;
};

// BufferSource over 任意 ReadStream：pull 在内部缓冲为空时从流里读一批，之后返回未消费的部分。
// 存储属于适配器（被调方拥有缓冲区），默认 16 KiB。
template <class Stream> struct stream_buffer_source {
    static_assert(is_read_stream<Stream>::value, "stream_buffer_source requires a ReadStream");

    explicit stream_buffer_source(Stream& stream, std::size_t const capacity = 16U * 1024U)
        : stream_{&stream}, storage_(capacity) {}

    task<io_result<const_buffer_span>> pull(const_buffer_span const dest) { return pull_impl(this, dest); }

    void consume(std::size_t const n) noexcept {
        auto const available = end_ - begin_;
        begin_ += n < available ? n : available;
        if (begin_ == end_) begin_ = end_ = 0U;
    }

    Stream& next_layer() noexcept { return *stream_; }

  private:
    static auto pull_impl(stream_buffer_source* self, const_buffer_span dest)
        CO2_BEG((task<io_result<const_buffer_span>>), (self, dest), io_result<std::size_t> r;) {
        if (self->begin_ == self->end_) {
            if (self->eof_) CO2_RETURN((io_result<const_buffer_span>{make_error_code(error::eof), const_buffer_span{}}));
            CO2_AWAIT_SET(r, self->stream_->read_some(net::buffer(self->storage_.data(), self->storage_.size())));
            if (r.ec == error::eof) {
                self->eof_ = true;
                CO2_RETURN((io_result<const_buffer_span>{r.ec, const_buffer_span{}}));
            }
            if (r.ec) CO2_RETURN((io_result<const_buffer_span>{r.ec, const_buffer_span{}}));
            self->begin_ = 0U;
            self->end_ = r.value;
        }
        if (dest.empty()) CO2_RETURN((io_result<const_buffer_span>{std::error_code{}, const_buffer_span{}}));
        dest[0] = const_buffer{self->storage_.data() + self->begin_, self->end_ - self->begin_};
        CO2_RETURN((io_result<const_buffer_span>{std::error_code{}, const_buffer_span{dest.data(), 1U}}));
    }
    CO2_END

    Stream* stream_;
    std::vector<unsigned char> storage_;
    std::size_t begin_ = 0;
    std::size_t end_ = 0;
    bool eof_ = false;
};

// BufferSink over 任意 WriteStream：prepare 给出内部存储，commit 把已写字节写到流里，commit_eof 再发流结束。
template <class Stream> struct stream_buffer_sink {
    static_assert(is_write_stream<Stream>::value, "stream_buffer_sink requires a WriteStream");

    explicit stream_buffer_sink(Stream& stream, std::size_t const capacity = 16U * 1024U)
        : stream_{&stream}, storage_(capacity) {}

    mutable_buffer_span prepare(mutable_buffer_span const dest) noexcept {
        if (dest.empty() || finished_) return mutable_buffer_span{};
        dest[0] = mutable_buffer{storage_.data(), storage_.size()};
        return mutable_buffer_span{dest.data(), 1U};
    }

    task<io_result<>> commit(std::size_t const n) { return commit_impl(this, n < storage_.size() ? n : storage_.size(), false); }
    task<io_result<>> commit_eof(std::size_t const n) { return commit_impl(this, n < storage_.size() ? n : storage_.size(), true); }

    bool finished() const noexcept { return finished_; }
    Stream& next_layer() noexcept { return *stream_; }

  private:
    static auto commit_impl(stream_buffer_sink* self, std::size_t n, bool eof)
        CO2_BEG((task<io_result<>>), (self, n, eof), io_result<std::size_t> w; io_result<> e;) {
        if (self->finished_) CO2_RETURN((io_result<>{std::make_error_code(std::errc::operation_not_permitted)}));
        if (n != 0U) {
            CO2_AWAIT_SET(w, net::write(*self->stream_, net::buffer(self->storage_.data(), n)));
            if (w.ec) CO2_RETURN((io_result<>{w.ec}));
        }
        if (eof) {
            self->finished_ = true;
            CO2_AWAIT_SET(e, detail::stream_eof(*self->stream_));
            CO2_RETURN(e);
        }
        CO2_RETURN((io_result<>{}));
    }
    CO2_END

    Stream* stream_;
    std::vector<unsigned char> storage_;
    bool finished_ = false;
};

template <class Stream> stream_read_source<Stream> as_read_source(Stream& stream) noexcept { return stream_read_source<Stream>{stream}; }
template <class Stream> stream_write_sink<Stream> as_write_sink(Stream& stream) noexcept { return stream_write_sink<Stream>{stream}; }
template <class Stream> stream_buffer_source<Stream> as_buffer_source(Stream& stream, std::size_t const capacity = 16U * 1024U) {
    return stream_buffer_source<Stream>{stream, capacity};
}
template <class Stream> stream_buffer_sink<Stream> as_buffer_sink(Stream& stream, std::size_t const capacity = 16U * 1024U) {
    return stream_buffer_sink<Stream>{stream, capacity};
}

// ---------------------------------------------------------------------------
// transfer 算法：把 BufferSource 的全部数据搬到 WriteStream / BufferSink（Paper 6 的两个示例）。
// 返回 {ec, 搬运的字节数}；源耗尽（eof）视为成功。搬到 BufferSink 时最后 commit_eof。

template <class Source, class Stream>
auto transfer_to_stream(Source& source, Stream& stream)
    CO2_BEG((task<io_result<std::size_t>>), (source, stream), const_buffer scratch[max_iovec]; io_result<const_buffer_span> pulled;
            io_result<std::size_t> w; std::size_t total{};) {
    static_assert(is_buffer_source<Source>::value, "transfer_to_stream requires a BufferSource");
    static_assert(is_write_stream<Stream>::value, "transfer_to_stream requires a WriteStream");
    for (;;) {
        CO2_AWAIT_SET(pulled, source.pull(const_buffer_span{scratch, max_iovec}));
        if (pulled.ec == error::eof) CO2_RETURN((io_result<std::size_t>{std::error_code{}, total}));
        if (pulled.ec) CO2_RETURN((io_result<std::size_t>{pulled.ec, total}));
        CO2_AWAIT_SET(w, stream.write_some(pulled.value));
        if (w.ec) CO2_RETURN((io_result<std::size_t>{w.ec, total}));
        source.consume(w.value);
        total += w.value;
    }
}
CO2_END

template <class Source, class Sink>
auto transfer_to_sink(Source& source, Sink& sink)
    CO2_BEG((task<io_result<std::size_t>>), (source, sink), const_buffer scratch[max_iovec]; mutable_buffer room[max_iovec];
            io_result<const_buffer_span> pulled; mutable_buffer_span prepared; std::size_t copied{}; io_result<> c;
            std::size_t total{};) {
    static_assert(is_buffer_source<Source>::value, "transfer_to_sink requires a BufferSource");
    static_assert(is_buffer_sink<Sink>::value, "transfer_to_sink requires a BufferSink");
    for (;;) {
        CO2_AWAIT_SET(pulled, source.pull(const_buffer_span{scratch, max_iovec}));
        if (pulled.ec == error::eof) {
            CO2_AWAIT_SET(c, sink.commit_eof(0U));
            CO2_RETURN((io_result<std::size_t>{c.ec, total}));
        }
        if (pulled.ec) CO2_RETURN((io_result<std::size_t>{pulled.ec, total}));
        prepared = sink.prepare(mutable_buffer_span{room, max_iovec});
        if (prepared.empty()) CO2_RETURN((io_result<std::size_t>{std::make_error_code(std::errc::no_buffer_space), total}));
        copied = buffer_copy(prepared, pulled.value);
        CO2_AWAIT_SET(c, sink.commit(copied));
        if (c.ec) CO2_RETURN((io_result<std::size_t>{c.ec, total}));
        source.consume(copied);
        total += copied;
    }
}
CO2_END

} // namespace net
