// 流概念第二族（Paper 6）：ReadSource / WriteSink / BufferSource / BufferSink 的概念判定、具体模型
//（memory_source、dynamic_buffer_source / sink）、Stream 适配器、transfer 算法，以及四个类型擦除包装
//（含零每操作分配、>16 缓冲区的窗口化、转发 / 合成两条路径）。TCP / TLS 上的端到端见 tcp_tests /
// tls_tests。

#include <array>
#include <atomic>
#include <cstdlib>
#include <cstring>
#include <new>
#include <string>
#include <vector>

#include "net/any_source_sink.hpp"
#include "net/any_stream.hpp"
#include "net/buffers.hpp"
#include "net/dynamic_buffer.hpp"
#include "net/error.hpp"
#include "net/immediate.hpp"
#include "net/io_context.hpp"
#include "net/run_async.hpp"
#include "net/source_sink.hpp"
#include "net/stream.hpp"
#include "net/task.hpp"

#include "check.hpp"

namespace {

std::atomic<long> allocation_count{0};

// 内存流：每次最多传输 chunk 字节。
struct memory_stream {
    std::string input;
    std::string output;
    std::size_t chunk = 3;

    template <class MB> net::immediate<net::io_result<std::size_t>> read_some(MB const& buffers) {
        if (net::buffer_size(buffers) == 0U) return {{std::error_code{}, 0U}};
        if (input.empty()) return {{make_error_code(net::error::eof), 0U}};
        auto const n = net::buffer_copy(buffers, net::buffer(input), chunk);
        input.erase(0, n);
        return {{std::error_code{}, n}};
    }

    template <class CB> net::immediate<net::io_result<std::size_t>> write_some(CB const& buffers) {
        auto const total = net::buffer_size(buffers);
        auto const n = total < chunk ? total : chunk;
        auto const before = output.size();
        output.resize(before + n);
        net::buffer_copy(net::buffer(output) + before, buffers, n);
        return {{std::error_code{}, n}};
    }
};

// ---- 概念判定 ----
static_assert(net::is_buffer_source<net::memory_source>::value, "");
static_assert(not net::is_read_stream<net::memory_source>::value, "");
static_assert(net::is_buffer_source<net::dynamic_buffer_source<net::flat_dynamic_buffer>>::value, "");
static_assert(net::is_buffer_sink<net::dynamic_buffer_sink<net::flat_dynamic_buffer>>::value, "");
static_assert(net::is_read_source<net::stream_read_source<memory_stream>>::value, "");
static_assert(net::is_read_stream<net::stream_read_source<memory_stream>>::value, ""); // 精化
static_assert(net::is_write_sink<net::stream_write_sink<memory_stream>>::value, "");
static_assert(net::is_buffer_source<net::stream_buffer_source<memory_stream>>::value, "");
static_assert(net::is_buffer_sink<net::stream_buffer_sink<memory_stream>>::value, "");
static_assert(not net::is_read_source<memory_stream>::value, ""); // 只有 read_some
static_assert(not net::is_write_sink<memory_stream>::value, "");
static_assert(not net::is_buffer_sink<memory_stream>::value, "");
static_assert(net::is_read_source<net::any_read_source>::value, "");
static_assert(net::is_write_sink<net::any_write_sink>::value, "");
static_assert(net::is_buffer_source<net::any_buffer_source>::value, "");
static_assert(net::is_read_source<net::any_buffer_source>::value, ""); // 合成的 read_some / read
static_assert(net::is_buffer_sink<net::any_buffer_sink>::value, "");
static_assert(net::is_write_sink<net::any_buffer_sink>::value, ""); // 合成的 write / write_eof

template <class T> T run_task(net::task<T> t) {
    net::io_context ctx;
    T result{};
    net::run_async(ctx.get_executor(), [&](T v) { result = std::move(v); }, [](std::exception_ptr) { CHECK(false); })(std::move(t));
    ctx.run();
    return result;
}

std::string to_string(net::const_buffer_span const buffers) {
    std::string out;
    for (auto const& b : buffers) out.append(static_cast<char const*>(b.data()), b.size());
    return out;
}

// ---- memory_source ----

auto pull_all(net::memory_source& source, std::string* out)
    CO2_BEG((net::task<std::error_code>), (source, out), net::const_buffer scratch[2]; net::io_result<net::const_buffer_span> r;
            net::io_result<net::const_buffer_span> again;) {
    for (;;) {
        CO2_AWAIT_SET(r, source.pull(net::const_buffer_span{scratch, 2}));
        if (r.ec) CO2_RETURN(r.ec);
        // 不 consume 再 pull：同样的数据
        CO2_AWAIT_SET(again, source.pull(net::const_buffer_span{scratch, 2}));
        CHECK(to_string(again.value) == to_string(r.value));
        CHECK(r.value.size() <= 2U); // dest 只有两个槽位
        *out += to_string(r.value).substr(0, 1);
        source.consume(1); // 一次一字节：跨缓冲区边界的 consume
    }
}
CO2_END

void memory_source_pull_and_consume() {
    std::string const a = "ab";
    std::string const b = "cd";
    std::string const c = "e";
    std::array<net::const_buffer, 3> pieces{{net::buffer(a), net::buffer(b), net::buffer(c)}};
    net::memory_source source{pieces};
    CHECK_EQ(source.remaining(), 5U);
    std::string out;
    auto const ec = run_task(pull_all(source, &out));
    CHECK(ec == net::error::eof);
    CHECK_EQ(out, "abcde");
    CHECK_EQ(source.remaining(), 0U);
}

// ---- dynamic_buffer_source / dynamic_buffer_sink + transfer ----

template <class Source, class Sink>
auto transfer_sink(Source& source, Sink& sink) CO2_BEG((net::task<net::io_result<std::size_t>>), (source, sink),
                                                       net::io_result<std::size_t> r;) {
    CO2_AWAIT_SET(r, net::transfer_to_sink(source, sink));
    CO2_RETURN(r);
}
CO2_END

template <class Source, class Stream>
auto transfer_stream(Source& source, Stream& stream) CO2_BEG((net::task<net::io_result<std::size_t>>), (source, stream),
                                                           net::io_result<std::size_t> r;) {
    CO2_AWAIT_SET(r, net::transfer_to_stream(source, stream));
    CO2_RETURN(r);
}
CO2_END

void transfer_between_dynamic_buffers() {
    std::string in_storage = "the quick brown fox jumps over the lazy dog";
    auto in = net::dynamic_buffer(in_storage); // 已有内容都算可读
    std::string out_storage;
    auto out = net::dynamic_buffer(out_storage);
    auto source = net::make_source(in);
    auto sink = net::dynamic_buffer_sink<decltype(out)>{out, 7}; // 每次只给 7 字节：多轮 prepare/commit
    auto const r = run_task(transfer_sink(source, sink));
    CHECK(not r.ec);
    CHECK_EQ(r.value, 43U);
    CHECK_EQ(out_storage, "the quick brown fox jumps over the lazy dog");
    CHECK_EQ(in.size(), 0U);
    CHECK(sink.finished()); // commit_eof 之后
    // 结束后不再接受写入
    auto const after = sink.commit(0);
    CHECK(after.value.ec == std::errc::operation_not_permitted);
}

void transfer_source_to_stream() {
    std::string const payload(1000, 'q');
    net::memory_source source{net::buffer(payload)};
    memory_stream stream{"", "", 64}; // 每次最多写 64
    auto const r = run_task(transfer_stream(source, stream));
    CHECK(not r.ec);
    CHECK_EQ(r.value, 1000U);
    CHECK_EQ(stream.output, payload);
}

// ---- Stream 适配器 ----

auto source_read(net::stream_read_source<memory_stream>& source, char* out, std::size_t n)
    CO2_BEG((net::task<net::io_result<std::size_t>>), (source, out, n), net::io_result<std::size_t> r;) {
    CO2_AWAIT_SET(r, source.read(net::buffer(out, n)));
    CO2_RETURN(r);
}
CO2_END

void stream_read_source_reads_completely() {
    memory_stream s{"0123456789", "", 4};
    auto source = net::as_read_source(s);
    char out[10] = {};
    auto const r = run_task(source_read(source, out, sizeof(out)));
    CHECK(not r.ec);
    CHECK_EQ(r.value, 10U);
    CHECK_EQ(std::string(out, 10), "0123456789");
    // 读不满：eof + 已读字节数
    memory_stream short_stream{"abc", "", 2};
    auto short_source = net::as_read_source(short_stream);
    char more[8] = {};
    auto const partial = run_task(source_read(short_source, more, sizeof(more)));
    CHECK(partial.ec == net::error::eof);
    CHECK_EQ(partial.value, 3U);
}

auto sink_write_eof(net::stream_write_sink<memory_stream>& sink, std::string payload)
    CO2_BEG((net::task<net::io_result<std::size_t>>), (sink, payload), net::io_result<std::size_t> r; net::io_result<> e;) {
    CO2_AWAIT_SET(r, sink.write_eof(net::buffer(payload)));
    CO2_AWAIT_SET(e, sink.write_eof()); // 内存流没有可表达的 EOF：无操作、成功
    CHECK(not e.ec);
    CO2_RETURN(r);
}
CO2_END

void stream_write_sink_writes_completely() {
    memory_stream s{"", "", 5};
    auto sink = net::as_write_sink(s);
    auto const r = run_task(sink_write_eof(sink, "hello, sink"));
    CHECK(not r.ec);
    CHECK_EQ(r.value, 11U);
    CHECK_EQ(s.output, "hello, sink");
}

auto buffer_source_drain(net::stream_buffer_source<memory_stream>& source, std::string* out)
    CO2_BEG((net::task<std::error_code>), (source, out), net::const_buffer scratch[4]; net::io_result<net::const_buffer_span> r;) {
    for (;;) {
        CO2_AWAIT_SET(r, source.pull(net::const_buffer_span{scratch, 4}));
        if (r.ec) CO2_RETURN(r.ec);
        CHECK(not r.value.empty());
        *out += to_string(r.value).substr(0, 2); // 每次只消费 2 字节
        source.consume(2);
    }
}
CO2_END

void stream_buffer_source_pulls_from_a_stream() {
    memory_stream s{"abcdefghij", "", 3}; // 流每次给 3，适配器每次消费 2：内部残留跨过多次 pull
    auto source = net::as_buffer_source(s, 8);
    std::string out;
    auto const ec = run_task(buffer_source_drain(source, &out));
    CHECK(ec == net::error::eof);
    CHECK_EQ(out, "abcdefghij");
}

auto buffer_sink_fill(net::stream_buffer_sink<memory_stream>& sink, std::string payload)
    CO2_BEG((net::task<std::error_code>), (sink, payload), net::mutable_buffer room[2]; net::mutable_buffer_span prepared;
            std::size_t offset{}; std::size_t n{}; net::io_result<> c;) {
    while (offset < payload.size()) {
        prepared = sink.prepare(net::mutable_buffer_span{room, 2});
        CHECK(not prepared.empty());
        n = net::buffer_copy(prepared, net::buffer(payload) + offset);
        offset += n;
        CO2_AWAIT_SET(c, offset == payload.size() ? sink.commit_eof(n) : sink.commit(n));
        if (c.ec) CO2_RETURN(c.ec);
    }
    CO2_RETURN(std::error_code{});
}
CO2_END

void stream_buffer_sink_commits_to_a_stream() {
    memory_stream s{"", "", 5};
    auto sink = net::as_buffer_sink(s, 6); // 6 字节内部存储：多轮 prepare/commit
    auto const ec = run_task(buffer_sink_fill(sink, "twenty-two characters!"));
    CHECK(not ec);
    CHECK_EQ(s.output, "twenty-two characters!");
    CHECK(sink.finished());
}

// ---- 类型擦除 ----

auto erased_read_some_loop(net::any_read_source& source, std::string* out)
    CO2_BEG((net::task<std::error_code>), (source, out), char buf[4]; net::io_result<std::size_t> r; long before{};) {
    for (;;) {
        before = allocation_count.load();
        CO2_AWAIT_SET(r, source.read_some(net::buffer(buf)));
        CHECK_EQ(allocation_count.load(), before); // 类型擦除的 read_some：零分配
        if (r.ec) CO2_RETURN(r.ec);
        out->append(buf, r.value);
    }
}
CO2_END

auto erased_read_many(net::any_read_source& source, std::vector<net::mutable_buffer> buffers)
    CO2_BEG((net::task<net::io_result<std::size_t>>), (source, buffers), net::io_result<std::size_t> r;) {
    CO2_AWAIT_SET(r, source.read(buffers));
    CO2_RETURN(r);
}
CO2_END

void any_read_source_forwards_and_windows() {
    memory_stream s{"0123456789", "", 3};
    net::any_read_source erased{net::as_read_source(s)}; // 拥有适配器
    std::string out;
    auto const ec = run_task(erased_read_some_loop(erased, &out));
    CHECK(ec == net::error::eof);
    CHECK_EQ(out, "0123456789");

    // read 覆盖 40 个缓冲区（> max_iovec）：按窗口穿过类型擦除边界
    std::string source_text;
    for (auto i = 0; i != 40; ++i) source_text += static_cast<char>('a' + i % 26);
    memory_stream many{source_text, "", 7};
    net::any_read_source erased_many{net::as_read_source(many)};
    std::vector<std::array<char, 1>> cells(40);
    std::vector<net::mutable_buffer> buffers;
    for (auto& cell : cells) buffers.push_back(net::buffer(cell));
    auto const r = run_task(erased_read_many(erased_many, buffers));
    CHECK(not r.ec);
    CHECK_EQ(r.value, 40U);
    std::string got;
    for (auto const& cell : cells) got += cell[0];
    CHECK_EQ(got, source_text);
}

auto erased_write_eof(net::any_write_sink& sink, std::vector<net::const_buffer> pieces)
    CO2_BEG((net::task<net::io_result<std::size_t>>), (sink, pieces), net::io_result<std::size_t> r; net::io_result<> e; long before{};
            net::io_result<std::size_t> w; std::string const probe{"!"};) {
    before = allocation_count.load();
    CO2_AWAIT_SET(w, sink.write_some(net::buffer(probe)));
    CHECK_EQ(allocation_count.load(), before); // 擦除的 write_some 转发到 immediate：零分配
    CHECK_EQ(w.value, 1U);
    CO2_AWAIT_SET(r, sink.write_eof(pieces));
    CO2_AWAIT_SET(e, sink.write_eof()); // 适配器的 write_eof 本身是协程（一帧）；内存流没有可表达的 EOF
    CHECK(not e.ec);
    CO2_RETURN(r);
}
CO2_END

void any_write_sink_windows_write_eof() {
    memory_stream s{"", "", 5};
    auto adapter = net::as_write_sink(s);
    net::any_write_sink erased{&adapter}; // 引用
    std::string text;
    for (auto i = 0; i != 40; ++i) text += static_cast<char>('A' + i % 26);
    std::vector<net::const_buffer> pieces;
    for (auto i = 0U; i != 40U; ++i) pieces.push_back(net::buffer(&text[i], 1));
    auto const r = run_task(erased_write_eof(erased, pieces));
    CHECK(not r.ec);
    CHECK_EQ(r.value, 40U);
    CHECK_EQ(s.output, "!" + text);
}

auto erased_pull_and_synth_read(net::any_buffer_source& source, std::string* out)
    CO2_BEG((net::task<std::error_code>), (source, out), net::const_buffer scratch[4]; net::io_result<net::const_buffer_span> p;
            char buf[3]; net::io_result<std::size_t> r; long before{};) {
    before = allocation_count.load();
    CO2_AWAIT_SET(p, source.pull(net::const_buffer_span{scratch, 4}));
    CHECK_EQ(allocation_count.load(), before); // 擦除的 pull：零分配
    CHECK(not p.ec);
    CHECK_EQ(to_string(p.value), "0123456789");
    source.consume(2);
    // 合成的 read_some（memory_source 不是 ReadStream）：pull → 拷贝 → consume
    for (;;) {
        CO2_AWAIT_SET(r, source.read_some(net::buffer(buf)));
        if (r.ec) CO2_RETURN(r.ec);
        out->append(buf, r.value);
    }
}
CO2_END

auto erased_forwarded_read(net::any_buffer_source& outer, char* out, std::size_t n)
    CO2_BEG((net::task<net::io_result<std::size_t>>), (outer, out, n), net::io_result<std::size_t> r;) {
    CO2_AWAIT_SET(r, outer.read(net::buffer(out, n)));
    CO2_RETURN(r);
}
CO2_END

void any_buffer_source_pull_synthesized_and_forwarded_reads() {
    std::string const text = "0123456789";
    net::any_buffer_source erased{net::memory_source{net::buffer(text)}};
    std::string out;
    auto const ec = run_task(erased_pull_and_synth_read(erased, &out));
    CHECK(ec == net::error::eof);
    CHECK_EQ(out, "23456789");

    // 转发路径：any_buffer_source 自己就是 ReadSource，再包一层就走转发
    net::any_buffer_source inner{net::memory_source{net::buffer(text)}};
    net::any_buffer_source outer{&inner};
    char buf[10] = {};
    auto const r = run_task(erased_forwarded_read(outer, buf, sizeof(buf)));
    CHECK(not r.ec);
    CHECK_EQ(r.value, 10U);
    CHECK_EQ(std::string(buf, 10), text);
}

auto erased_sink_prepare_commit(net::any_buffer_sink& sink, std::string payload)
    CO2_BEG((net::task<std::error_code>), (sink, payload), net::mutable_buffer room[4]; net::mutable_buffer_span prepared;
            std::size_t n{}; net::io_result<> c; long before{}; net::io_result<std::size_t> w;) {
    prepared = sink.prepare(net::mutable_buffer_span{room, 4});
    CHECK(not prepared.empty());
    n = net::buffer_copy(prepared, net::buffer(payload));
    before = allocation_count.load();
    CO2_AWAIT_SET(c, sink.commit(n));
    CHECK_EQ(allocation_count.load(), before); // 擦除的 commit：零分配
    if (c.ec) CO2_RETURN(c.ec);
    // 合成的 write / write_eof（dynamic_buffer_sink 不是 WriteStream）
    CO2_AWAIT_SET(w, sink.write(net::buffer(payload) + n));
    if (w.ec) CO2_RETURN(w.ec);
    CO2_AWAIT_SET(w, sink.write_eof(net::buffer(" end", 4)));
    CO2_RETURN(w.ec);
}
CO2_END

void any_buffer_sink_prepare_commit_and_synthesized_writes() {
    std::string storage;
    auto db = net::dynamic_buffer(storage);
    auto sink_impl = net::dynamic_buffer_sink<decltype(db)>{db, 8}; // 每次 8 字节
    net::any_buffer_sink erased{&sink_impl};
    auto const ec = run_task(erased_sink_prepare_commit(erased, "hello type-erased sink"));
    CHECK(not ec);
    CHECK_EQ(storage, "hello type-erased sink end");
    CHECK(sink_impl.finished());
}

// 把 any_buffer_sink 再包一层：被包装类型是 WriteSink，write / write_eof 走转发。
auto erased_forwarded_write(net::any_buffer_sink& outer, std::string payload)
    CO2_BEG((net::task<net::io_result<std::size_t>>), (outer, payload), net::io_result<std::size_t> r;) {
    CO2_AWAIT_SET(r, outer.write_eof(net::buffer(payload)));
    CO2_RETURN(r);
}
CO2_END

void any_buffer_sink_forwards_when_wrapping_a_write_sink() {
    std::string storage;
    auto db = net::dynamic_buffer(storage);
    auto sink_impl = net::make_sink(db);
    net::any_buffer_sink inner{&sink_impl};
    net::any_buffer_sink outer{&inner};
    auto const r = run_task(erased_forwarded_write(outer, "forwarded"));
    CHECK(not r.ec);
    CHECK_EQ(r.value, 9U);
    CHECK_EQ(storage, "forwarded");
    CHECK(sink_impl.finished());
}

} // namespace

void* operator new(std::size_t const size) {
    allocation_count.fetch_add(1, std::memory_order_relaxed);
    if (auto* const p = std::malloc(size == 0U ? 1U : size)) return p;
    throw std::bad_alloc{};
}

void operator delete(void* const p) noexcept { std::free(p); }
void operator delete(void* const p, std::size_t) noexcept { std::free(p); }

int main() {
    memory_source_pull_and_consume();
    transfer_between_dynamic_buffers();
    transfer_source_to_stream();
    stream_read_source_reads_completely();
    stream_write_sink_writes_completely();
    stream_buffer_source_pulls_from_a_stream();
    stream_buffer_sink_commits_to_a_stream();
    any_read_source_forwards_and_windows();
    any_write_sink_windows_write_eof();
    any_buffer_source_pull_synthesized_and_forwarded_reads();
    any_buffer_sink_prepare_commit_and_synthesized_writes();
    any_buffer_sink_forwards_when_wrapping_a_write_sink();
    std::cout << "source/sink tests passed\n";
    return 0;
}
