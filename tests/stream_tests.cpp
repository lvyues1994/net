// 流概念、组合算法（read / write / read_until）、类型擦除的 any_stream。

#include <atomic>
#include <cstddef>
#include <cstdlib>
#include <new>
#include <array>
#include <string>
#include <vector>

#include "net/any_stream.hpp"
#include "net/dynamic_buffer.hpp"
#include "net/immediate.hpp"
#include "net/io_context.hpp"
#include "net/run_async.hpp"
#include "net/stream.hpp"
#include "net/task.hpp"
#include "net/test/bufgrind.hpp"
#include "net/test/fuse.hpp"
#include "net/test/memory_stream.hpp"
#include "net/test/run_blocking.hpp"

#include "check.hpp"

namespace {

// 内存流：每次最多传输 chunk 字节，模拟部分读写。
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

struct read_only_stream {
    template <class MB> net::immediate<net::io_result<std::size_t>> read_some(MB const&) {
        return {{make_error_code(net::error::eof), 0U}};
    }
};

static_assert(net::is_read_stream<memory_stream>::value, "");
static_assert(net::is_write_stream<memory_stream>::value, "");
static_assert(net::is_stream<memory_stream>::value, "");
static_assert(net::is_read_stream<read_only_stream>::value, "");
static_assert(not net::is_write_stream<read_only_stream>::value, "");
static_assert(net::is_stream<net::any_stream>::value, "");
static_assert(net::is_read_stream<net::any_read_stream>::value, "");
static_assert(not net::is_write_stream<net::any_read_stream>::value, "");
static_assert(not net::is_read_stream<int>::value, "");

// 缓冲区序列概念只认缓冲区类型本身与"元素可转换为缓冲区"的范围：一个自身可转换为缓冲区的用户类型
// 不算序列（单元素遍历返回对象地址，对转换出的临时对象会悬空）。
struct convertible_to_buffer {
    char data[4];
    operator net::const_buffer() const noexcept { return net::const_buffer{data, sizeof(data)}; }
};
static_assert(not net::is_const_buffer_sequence<convertible_to_buffer>::value, "");
static_assert(net::is_const_buffer_sequence<net::const_buffer>::value, "");
static_assert(net::is_const_buffer_sequence<net::mutable_buffer>::value, "");
static_assert(net::is_const_buffer_sequence<std::vector<convertible_to_buffer>>::value, "");
static_assert(not net::is_mutable_buffer_sequence<net::const_buffer>::value, "");

template <class T> T run_task(net::task<T> t) {
    net::io_context ctx;
    T result{};
    net::run_async(ctx.get_executor(), [&](T v) { result = std::move(v); },
                   [](std::exception_ptr) { CHECK(false); })(std::move(t));
    ctx.run();
    return result;
}

// ---------------------------------------------------------------------------

auto read_all(memory_stream& s, char* out, std::size_t n)
    CO2_BEG((net::task<net::io_result<std::size_t>>), (s, out, n), net::io_result<std::size_t> r;) {
    CO2_AWAIT_SET(r, net::read(s, net::buffer(out, n)));
    CO2_RETURN(r);
}
CO2_END

void read_fills_the_whole_buffer_across_partial_reads() {
    memory_stream s{"abcdefghij", "", 3};
    char out[10] = {};
    auto const r = run_task(read_all(s, out, sizeof(out)));
    CHECK(not r.ec);
    CHECK_EQ(r.value, 10U);
    CHECK_EQ(std::string(out, 10), "abcdefghij");
}

// 超过 max_iovec（16）个缓冲区的序列：read / write 必须覆盖全部，而不是只处理前 16 个。
auto read_vec(memory_stream& s, std::vector<net::mutable_buffer> buffers)
    CO2_BEG((net::task<net::io_result<std::size_t>>), (s, buffers), net::io_result<std::size_t> r;) {
    CO2_AWAIT_SET(r, net::read(s, buffers));
    CO2_RETURN(r);
}
CO2_END

auto write_vec(memory_stream& s, std::vector<net::const_buffer> buffers)
    CO2_BEG((net::task<net::io_result<std::size_t>>), (s, buffers), net::io_result<std::size_t> r;) {
    CO2_AWAIT_SET(r, net::write(s, buffers));
    CO2_RETURN(r);
}
CO2_END

void read_and_write_cover_sequences_longer_than_max_iovec() {
    constexpr auto count = 40U; // > 16
    std::string source;
    for (auto i = 0U; i != count; ++i) source += static_cast<char>('a' + i % 26U);
    memory_stream s{source, "", 7};
    std::vector<std::array<char, 1>> cells(count);
    std::vector<net::mutable_buffer> buffers;
    for (auto& cell : cells) buffers.push_back(net::buffer(cell));
    auto const r = run_task(read_vec(s, buffers));
    CHECK(not r.ec);
    CHECK_EQ(r.value, static_cast<std::size_t>(count));
    std::string got;
    for (auto const& cell : cells) got += cell[0];
    CHECK_EQ(got, source);

    memory_stream w{"", "", 5};
    std::vector<net::const_buffer> pieces;
    for (auto i = 0U; i != count; ++i) pieces.push_back(net::buffer(&source[i], 1));
    auto const wr = run_task(write_vec(w, pieces));
    CHECK(not wr.ec);
    CHECK_EQ(wr.value, static_cast<std::size_t>(count));
    CHECK_EQ(w.output, source);
}

void read_reports_eof_with_partial_count() {
    memory_stream s{"abc", "", 2};
    char out[10] = {};
    auto const r = run_task(read_all(s, out, sizeof(out)));
    CHECK(r.ec == net::error::eof);
    CHECK_EQ(r.value, 3U);
}

auto write_all(memory_stream& s, std::string text)
    CO2_BEG((net::task<net::io_result<std::size_t>>), (s, text), net::io_result<std::size_t> r;) {
    CO2_AWAIT_SET(r, net::write(s, net::buffer(text)));
    CO2_RETURN(r);
}
CO2_END

void write_pushes_everything_across_partial_writes() {
    memory_stream s{"", "", 4};
    auto const r = run_task(write_all(s, "hello, world"));
    CHECK(not r.ec);
    CHECK_EQ(r.value, 12U);
    CHECK_EQ(s.output, "hello, world");
}

auto read_line(memory_stream& s, std::string* line)
    CO2_BEG((net::task<net::io_result<std::size_t>>), (s, line), net::io_result<std::size_t> r;
            net::dynamic_container_buffer<std::string> buffer{*line};) {
    CO2_AWAIT_SET(r, net::read_until(s, buffer, "\r\n"));
    CO2_RETURN(r);
}
CO2_END

void read_until_finds_the_delimiter() {
    memory_stream s{"GET / HTTP/1.1\r\nHost: x\r\n", "", 5};
    std::string line;
    auto const r = run_task(read_line(s, &line));
    CHECK(not r.ec);
    CHECK_EQ(r.value, 16U);
    CHECK_EQ(line.substr(0, r.value), "GET / HTTP/1.1\r\n");
    CHECK(line.size() >= r.value); // 分隔符之后的数据可能也已读入
}

// 空分隔符立即匹配（与 Asio 一致），不消费输入。
auto read_line_with(memory_stream& s, std::string* line, std::string delimiter)
    CO2_BEG((net::task<net::io_result<std::size_t>>), (s, line, delimiter), net::dynamic_container_buffer<std::string> buf{*line};
            net::io_result<std::size_t> r;) {
    CO2_AWAIT_SET(r, net::read_until(s, buf, delimiter));
    CO2_RETURN(r);
}
CO2_END

void read_until_empty_delimiter_matches_immediately() {
    memory_stream s{"payload", "", 5};
    std::string line;
    auto const r = run_task(read_line_with(s, &line, ""));
    CHECK(not r.ec);
    CHECK_EQ(r.value, 0U);
    CHECK_EQ(s.input, "payload");
}

void read_until_reports_eof() {
    memory_stream s{"no newline", "", 5};
    std::string line;
    auto const r = run_task(read_line(s, &line));
    CHECK(r.ec == net::error::eof);
    CHECK_EQ(line, "no newline");
}

// ---------------------------------------------------------------------------

std::atomic<long> allocation_count{0};

auto copy_stream(net::any_stream& in, net::any_stream& out)
    CO2_BEG(net::task<std::size_t>, (in, out), char buf[4]; net::io_result<std::size_t> r;
            net::io_result<std::size_t> w; std::size_t total{}; long before{};) {
    for (;;) {
        before = allocation_count.load();
        CO2_AWAIT_SET(r, in.read_some(net::buffer(buf)));
        CHECK_EQ(allocation_count.load(), before); // 类型擦除的读：零分配
        if (r.ec) break;
        CO2_AWAIT_SET(w, out.write_some(net::buffer(buf, r.value)));
        if (w.ec) break;
        total += w.value;
    }
    CO2_RETURN(total);
}
CO2_END

void any_stream_erases_without_per_operation_allocation() {
    memory_stream src{"0123456789", "", 3};
    memory_stream dst{"", "", 100};
    net::any_stream in{&src};      // 引用
    net::any_stream out{memory_stream{}}; // 拥有一个副本（输出丢弃）
    CHECK(in.has_value() && out.has_value());
    auto const total = run_task(copy_stream(in, out));
    CHECK_EQ(total, 10U);
    // 拥有形态
    net::any_read_stream owned{memory_stream{"xy", "", 1}};
    net::any_read_stream moved{std::move(owned)};
    CHECK(moved.has_value());
    CHECK(not owned.has_value());
}

// 组合算法对类型擦除的流一样工作（编译一次，任意传输）。
auto read_erased(net::any_read_stream& s, char* out, std::size_t n)
    CO2_BEG((net::task<net::io_result<std::size_t>>), (s, out, n), net::io_result<std::size_t> r;) {
    CO2_AWAIT_SET(r, net::read(s, net::buffer(out, n)));
    CO2_RETURN(r);
}
CO2_END

void algorithms_work_on_any_read_stream() {
    memory_stream src{"abcdef", "", 2};
    net::any_read_stream erased{&src};
    char out[6] = {};
    auto const r = run_task(read_erased(erased, out, sizeof(out)));
    CHECK(not r.ec);
    CHECK_EQ(std::string(out, 6), "abcdef");
}

// ---- 公开的测试替身（net/test/） ----

// fuse：同一段代码跑到每条错误路径都被走一遍。write 三段（max_write_size = 2）→ 三个失效点 + 一遍全过。
auto write_all(net::test::memory_stream& s, std::string const* data)
    CO2_BEG((net::task<net::io_result<std::size_t>>), (s, data), net::io_result<std::size_t> r;) {
    CO2_AWAIT_SET(r, net::write(s, net::buffer(*data)));
    CO2_RETURN(r);
}
CO2_END

void fuse_walks_every_failure_point() {
    std::string const payload = "abcdef";
    auto passes = 0U;
    auto failures = 0U;
    net::io_context ctx;
    for (net::test::fuse f; f.next();) {
        net::test::memory_stream s{"", &f};
        s.max_write_size = 2;
        auto const r = net::test::run_blocking(ctx, write_all(s, &payload));
        ++passes;
        if (f.triggered()) {
            ++failures;
            CHECK(r.ec == f.error());
            CHECK_EQ(r.value, f.point() * 2U); // 第 k 个点失败：前 k 段已写
            CHECK_EQ(s.output.size(), f.point() * 2U);
        } else {
            CHECK(not r.ec);
            CHECK_EQ(r.value, 6U);
            CHECK_EQ(s.output, payload);
        }
    }
    CHECK_EQ(failures, 3U);
    CHECK_EQ(passes, 4U);
}

// bufgrind：net::read 对序列的任何切分都读到同样的字节。
auto read_two_halves(net::test::memory_stream& s, std::vector<net::mutable_buffer> const* halves)
    CO2_BEG((net::task<net::io_result<std::size_t>>), (s, halves), net::io_result<std::size_t> r;) {
    CO2_AWAIT_SET(r, net::read(s, *halves));
    CO2_RETURN(r);
}
CO2_END

void bufgrind_splits_do_not_change_what_read_delivers() {
    char out[9];
    net::mutable_buffer const whole{out, sizeof(out)};
    net::io_context ctx;
    for (net::test::bufgrind<net::mutable_buffer> grinder{whole}; grinder.has_next();) {
        auto const split = grinder.next();
        std::vector<net::mutable_buffer> const halves{split.head, split.tail};
        std::memset(out, 0, sizeof(out));
        net::test::memory_stream s{"123456789"};
        s.max_read_size = 4;
        auto const r = net::test::run_blocking(ctx, read_two_halves(s, &halves));
        CHECK(not r.ec);
        CHECK_EQ(r.value, 9U);
        CHECK_EQ(std::string(out, 9), "123456789");
    }
}

// read_until 在环形缓冲上：分隔符跨过存储末尾也能找到。
auto read_line_ring(net::test::memory_stream& s, net::circular_dynamic_buffer* ring)
    CO2_BEG((net::task<net::io_result<std::size_t>>), (s, ring), net::io_result<std::size_t> r;) {
    CO2_AWAIT_SET(r, net::read_until(s, *ring, "\r\n"));
    CO2_RETURN(r);
}
CO2_END

void read_until_spans_the_ring_wrap() {
    unsigned char storage[8];
    net::circular_dynamic_buffer ring{storage};
    ring.commit(net::buffer_size(ring.prepare(6))); // 读指针推到 6：接下来的数据必然绕过末尾
    ring.consume(6);
    net::test::memory_stream s{"ab\r\ncd\r\n"};
    s.max_read_size = 5; // 第一次读进 "ab\r\nc"：分隔符正好跨段
    net::io_context ctx;
    auto r = net::test::run_blocking(ctx, read_line_ring(s, &ring));
    CHECK(not r.ec);
    CHECK_EQ(r.value, 4U);
    {
        std::string line;
        for (auto const& b : ring.data()) line.append(static_cast<char const*>(b.data()), b.size());
        CHECK_EQ(line.substr(0, 4), "ab\r\n");
    }
    ring.consume(4);
    r = net::test::run_blocking(ctx, read_line_ring(s, &ring));
    CHECK(not r.ec);
    CHECK_EQ(r.value, 4U);
}

} // namespace

void* operator new(std::size_t const size) {
    allocation_count.fetch_add(1, std::memory_order_relaxed);
    if (auto* const p = std::malloc(size)) return p;
    throw std::bad_alloc{};
}

void operator delete(void* const p) noexcept { std::free(p); }
void operator delete(void* const p, std::size_t) noexcept { std::free(p); }

int main() {
    read_fills_the_whole_buffer_across_partial_reads();
    read_and_write_cover_sequences_longer_than_max_iovec();
    read_until_empty_delimiter_matches_immediately();
    read_reports_eof_with_partial_count();
    write_pushes_everything_across_partial_writes();
    read_until_finds_the_delimiter();
    read_until_reports_eof();
    any_stream_erases_without_per_operation_allocation();
    algorithms_work_on_any_read_stream();
    fuse_walks_every_failure_point();
    bufgrind_splits_do_not_change_what_read_delivers();
    read_until_spans_the_ring_wrap();
    std::cout << "stream tests passed\n";
    return 0;
}
