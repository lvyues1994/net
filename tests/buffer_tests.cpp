// 缓冲区词汇：mutable_buffer / const_buffer、序列特征、buffer_size / buffer_copy、
// buffer_array、动态缓冲、span。

#include <array>
#include <cstring>
#include <list>
#include <string>
#include <vector>

#include "net/buffer_slice.hpp"
#include "net/buffers.hpp"
#include "net/dynamic_buffer.hpp"
#include "net/span.hpp"
#include "net/test/bufgrind.hpp"

#include "check.hpp"

namespace {

std::string to_string(net::const_buffer_array<> const& parts) {
    std::string s;
    for (auto const& b : parts) s.append(static_cast<char const*>(b.data()), b.size());
    return s;
}
template <class Sequence> std::string to_string(Sequence const& sequence) { return to_string(net::const_buffer_array<>{sequence}); }

static_assert(net::is_const_buffer_sequence<net::detail::slice_of<std::vector<net::const_buffer>>>::value, "");
static_assert(net::is_mutable_buffer_sequence<net::detail::slice_of<std::array<net::mutable_buffer, 3>>>::value, "");
static_assert(not net::is_mutable_buffer_sequence<net::detail::slice_of<std::vector<net::const_buffer>>>::value, "");
static_assert(std::is_same<net::slice_type<net::mutable_buffer>, net::mutable_buffer>::value, "");
static_assert(std::is_same<net::slice_type<net::const_buffer>, net::const_buffer>::value, "");
static_assert(net::is_const_buffer_sequence<net::const_buffer_pair>::value, "");
static_assert(net::is_mutable_buffer_sequence<net::mutable_buffer_pair>::value, "");

static_assert(net::is_mutable_buffer_sequence<net::mutable_buffer>::value, "");
static_assert(net::is_const_buffer_sequence<net::mutable_buffer>::value, "");
static_assert(net::is_const_buffer_sequence<net::const_buffer>::value, "");
static_assert(not net::is_mutable_buffer_sequence<net::const_buffer>::value, "");
static_assert(net::is_mutable_buffer_sequence<std::vector<net::mutable_buffer>>::value, "");
static_assert(net::is_const_buffer_sequence<std::vector<net::mutable_buffer>>::value, "");
static_assert(net::is_const_buffer_sequence<std::list<net::const_buffer>>::value, "");
static_assert(net::is_const_buffer_sequence<net::span<net::const_buffer const>>::value, "");
static_assert(net::is_mutable_buffer_sequence<net::mutable_buffer_array<>>::value, "");
static_assert(not net::is_const_buffer_sequence<std::string>::value, "");
static_assert(not net::is_mutable_buffer_sequence<int>::value, "");

void buffer_factories() {
    char raw[8];
    std::string text{"hello"};
    std::vector<int> ints{1, 2, 3};
    std::array<unsigned char, 4> bytes{};
    std::string const const_text{"x"};

    CHECK_EQ(net::buffer(raw).size(), 8U);
    CHECK_EQ(net::buffer(text).size(), 5U);
    CHECK_EQ(net::buffer(ints).size(), 3U * sizeof(int));
    CHECK_EQ(net::buffer(bytes).size(), 4U);
    CHECK_EQ(net::buffer(const_text).size(), 1U);
    CHECK_EQ(net::buffer(raw, 3).size(), 3U);
    CHECK_EQ(net::buffer(net::buffer(raw), 100).size(), 8U);
    CHECK((net::buffer(raw) + 5).size() == 3U);
    CHECK((net::buffer(raw) + 100).size() == 0U);
    CHECK(net::buffer_size(net::buffer(text)) == 5U);
    CHECK(net::buffer_empty(net::mutable_buffer{}));
}

void buffer_copy_spans_element_boundaries() {
    std::string source{"0123456789"};
    char a[3];
    char b[4];
    char c[10];
    std::vector<net::mutable_buffer> target{net::buffer(a), net::buffer(b), net::buffer(c)};
    CHECK_EQ(net::buffer_size(target), 17U);
    CHECK_EQ(net::buffer_copy(target, net::buffer(source)), 10U);
    CHECK(std::memcmp(a, "012", 3) == 0);
    CHECK(std::memcmp(b, "3456", 4) == 0);
    CHECK(std::memcmp(c, "789", 3) == 0);

    std::vector<net::const_buffer> pieces{net::buffer(source.data(), 2), net::buffer(source.data() + 2, 3)};
    char d[16] = {};
    CHECK_EQ(net::buffer_copy(net::buffer(d), pieces, 4), 4U);
    CHECK(std::string(d, 4) == "0123");
}

void buffer_array_flattens_and_consumes() {
    char a[3];
    char b[0 + 5];
    std::vector<net::mutable_buffer> seq{net::buffer(a), net::mutable_buffer{}, net::buffer(b)};
    net::mutable_buffer_array<> array{seq};
    CHECK_EQ(array.size(), 2U); // 空缓冲区被跳过
    CHECK_EQ(array.total_size(), 8U);
    array.consume(4);
    CHECK_EQ(array.size(), 1U);
    CHECK_EQ(array.total_size(), 4U);
    CHECK(array[0].data() == b + 1);
    array.consume(100);
    CHECK(array.empty());

    net::const_buffer_array<2> truncated{std::vector<net::const_buffer>{net::buffer(a), net::buffer(b), net::buffer(a)}};
    CHECK_EQ(truncated.size(), 2U);
    CHECK_EQ(truncated.to_span().size(), 2U);
}

void flat_dynamic_buffer_two_phase() {
    char storage[16];
    net::flat_dynamic_buffer buffer{storage};
    CHECK_EQ(buffer.size(), 0U);
    CHECK_EQ(buffer.capacity(), 16U);
    auto out = buffer.prepare(10);
    CHECK_EQ(out.size(), 10U);
    std::memcpy(out.data(), "abcdefghij", 10);
    buffer.commit(10);
    CHECK_EQ(buffer.size(), 10U);
    CHECK(std::memcmp(buffer.data().data(), "abcdefghij", 10) == 0);
    buffer.consume(4);
    CHECK_EQ(buffer.size(), 6U);
    CHECK(std::memcmp(buffer.data().data(), "efghij", 6) == 0);
    // 剩余 6 字节可读，需要 10 字节可写：压缩后可用。
    out = buffer.prepare(10);
    CHECK_EQ(out.size(), 10U);
    CHECK(std::memcmp(buffer.data().data(), "efghij", 6) == 0);
    auto threw = false;
    try {
        buffer.prepare(11);
    } catch (std::length_error const&) {
        threw = true;
    }
    CHECK(threw);
}

void container_dynamic_buffer_grows() {
    std::string text;
    auto buffer = net::dynamic_buffer(text, 32U);
    auto out = buffer.prepare(5);
    std::memcpy(out.data(), "hello", 5);
    buffer.commit(5);
    CHECK_EQ(text, "hello");
    buffer.consume(2);
    CHECK_EQ(text, "llo");
    CHECK_EQ(buffer.size(), 3U);
    CHECK_EQ(buffer.max_size(), 32U);

    std::vector<char> bytes;
    auto vb = net::dynamic_buffer(bytes);
    out = vb.prepare(3);
    std::memcpy(out.data(), "xyz", 3);
    vb.commit(2); // 只提交 2 个
    CHECK_EQ(bytes.size(), 2U);
    CHECK(vb.data().size() == 2U);
}

void span_basics() {
    int values[] = {1, 2, 3, 4};
    net::span<int> all{values};
    CHECK_EQ(all.size(), 4U);
    CHECK_EQ(all.first(2).size(), 2U);
    CHECK_EQ(all.last(1)[0], 4);
    CHECK_EQ(all.subspan(1, 2)[1], 3);
    net::span<int const> view = all;
    CHECK_EQ(view.size_bytes(), 4U * sizeof(int));
    std::vector<int> v{5, 6};
    net::span<int const> from_vector{v};
    CHECK_EQ(from_vector[1], 6);
}

// 字节粒度切片：跨元素边界，首尾两段被裁，超出范围为空；单个缓冲区切出来还是缓冲区。
void buffer_slice_cuts_at_byte_granularity() {
    char a[] = "hello", b[] = " ", c[] = "world";
    std::vector<net::const_buffer> const parts{net::const_buffer{a, 5}, net::const_buffer{b, 1}, net::const_buffer{c, 5}};
    CHECK_EQ(to_string(net::buffer_slice(parts)), "hello world");
    CHECK_EQ(to_string(net::buffer_slice(parts, 3)), "lo world");
    CHECK_EQ(to_string(net::buffer_slice(parts, 3, 5)), "lo wo");
    CHECK_EQ(to_string(net::buffer_slice(parts, 5, 1)), " ");
    CHECK_EQ(to_string(net::buffer_slice(parts, 6)), "world");
    CHECK_EQ(to_string(net::buffer_slice(parts, 11)), "");
    CHECK_EQ(to_string(net::buffer_slice(parts, 99)), "");
    CHECK_EQ(to_string(net::buffer_slice(parts, 0, 0)), "");
    CHECK_EQ(net::buffer_size(net::buffer_slice(parts, 2, 100)), 9U);
    // 双向迭代器：从后往前也一致
    {
        auto const slice = net::buffer_slice(parts, 1, 9); // "ello worl"
        auto it = slice.end();
        --it;
        CHECK_EQ(to_string(net::const_buffer(*it)), "worl");
        --it;
        CHECK_EQ(to_string(net::const_buffer(*it)), " ");
        --it;
        CHECK_EQ(to_string(net::const_buffer(*it)), "ello");
        CHECK(it == slice.begin());
    }
    // 空元素被跳过，不影响字节位置
    std::vector<net::const_buffer> const with_empty{net::const_buffer{}, net::const_buffer{a, 5}, net::const_buffer{}, net::const_buffer{c, 5}};
    CHECK_EQ(to_string(net::buffer_slice(with_empty, 4, 3)), "owo");
    CHECK(net::buffer_front(with_empty).data() == a);
    CHECK_EQ(net::buffer_front(std::vector<net::const_buffer>{}).size(), 0U);
    // 单个缓冲区：值语义，临时的也能切
    auto const single = net::buffer_slice(net::buffer(c), 1, 3);
    CHECK_EQ(single.size(), 3U);
    CHECK(std::memcmp(single.data(), "orl", 3) == 0);
    CHECK_EQ(net::buffer_slice(net::buffer(c), 9).size(), 0U);
    // 切出来的视图可以再切
    auto const outer = net::buffer_slice(parts, 2);
    CHECK_EQ(to_string(net::buffer_slice(outer, 3, 4)), " wor");
}

void consuming_buffers_advances_in_place() {
    char a[] = "hello", b[] = " ", c[] = "world";
    std::vector<net::const_buffer> const parts{net::const_buffer{a, 5}, net::const_buffer{b, 1}, net::const_buffer{c, 5}};
    auto cursor = net::make_consuming_buffers(parts);
    CHECK_EQ(to_string(cursor.data()), "hello world");
    cursor.consume(4);
    CHECK_EQ(to_string(cursor.data()), "o world");
    cursor.consume(3);
    CHECK_EQ(to_string(cursor.data()), "orld");
    cursor.consume(100);
    CHECK_EQ(net::buffer_size(cursor.data()), 0U);
    CHECK_EQ(cursor.consumed(), 107U);
    // 单个缓冲区上的游标
    net::const_buffer const one{a, 5};
    net::consuming_buffers<net::const_buffer> single{one};
    single.consume(2);
    CHECK_EQ(to_string(single.data()), "llo");
}

// 切分器把序列在每个字节位置切成两半：两半拼回去总是原序列；step 缩短枚举。
void bufgrind_enumerates_every_split() {
    char a[] = "abc", b[] = "de";
    std::vector<net::const_buffer> const parts{net::const_buffer{a, 3}, net::const_buffer{b, 2}};
    auto splits = 0U;
    for (net::test::bufgrind<std::vector<net::const_buffer>> grinder{parts}; grinder.has_next();) {
        auto const halves = grinder.next();
        CHECK_EQ(halves.position, splits);
        CHECK_EQ(net::buffer_size(halves.head), splits);
        CHECK_EQ(to_string(halves.head) + to_string(halves.tail), "abcde");
        ++splits;
    }
    CHECK_EQ(splits, 6U); // 0..5 含两端
    auto stepped = 0U;
    for (auto grinder = net::test::make_bufgrind(parts, 4U); grinder.has_next(); grinder.next()) ++stepped;
    CHECK_EQ(stepped, 3U); // 0, 4, 5
}

// 环形缓冲：绕过末尾时 data() / prepare() 是两段；consume 不搬数据。
void circular_dynamic_buffer_wraps() {
    unsigned char storage[8];
    net::circular_dynamic_buffer ring{storage};
    CHECK_EQ(ring.capacity(), 8U);
    CHECK_EQ(ring.max_size(), 8U);
    CHECK_EQ(ring.size(), 0U);
    CHECK_EQ(ring.data().size(), 1U); // 空：一段空缓冲
    auto writable = ring.prepare(6);
    CHECK_EQ(writable.size(), 1U);
    CHECK_EQ(net::buffer_size(writable), 6U);
    net::buffer_copy(writable, net::buffer("abcdef", 6));
    ring.commit(6);
    CHECK_EQ(to_string(ring.data()), "abcdef");
    ring.consume(4);
    CHECK_EQ(to_string(ring.data()), "ef");
    writable = ring.prepare(5); // [6,8) + [0,3)
    CHECK_EQ(writable.size(), 2U);
    CHECK_EQ(writable[0].size(), 2U);
    CHECK_EQ(writable[1].size(), 3U);
    CHECK(writable[1].data() == storage);
    net::buffer_copy(writable, net::buffer("ghijk", 5));
    ring.commit(5);
    CHECK_EQ(ring.size(), 7U);
    auto const readable = ring.data();
    CHECK_EQ(readable.size(), 2U);
    CHECK_EQ(to_string(readable), "efghijk");
    ring.consume(3); // 读指针到 7："h" 在末尾，"ijk" 绕到开头
    CHECK_EQ(to_string(ring.data()), "hijk");
    CHECK_EQ(ring.data().size(), 2U);
    ring.consume(1);
    CHECK_EQ(to_string(ring.data()), "ijk");
    CHECK_EQ(ring.data().size(), 1U); // 读指针绕回 0 之后只有一段
    ring.consume(3);
    CHECK_EQ(ring.size(), 0U);
    // commit 多于 prepare 的部分被截掉；空间不足抛出
    ring.prepare(2);
    ring.commit(5);
    CHECK_EQ(ring.size(), 2U);
    auto threw = false;
    try {
        ring.prepare(7);
    } catch (std::length_error const&) {
        threw = true;
    }
    CHECK(threw);
    // 零容量的默认对象不做除法
    net::circular_dynamic_buffer empty;
    CHECK_EQ(empty.capacity(), 0U);
    CHECK_EQ(net::buffer_size(empty.prepare(0)), 0U);
    empty.consume(0);
}

} // namespace

int main() {
    buffer_factories();
    buffer_copy_spans_element_boundaries();
    buffer_array_flattens_and_consumes();
    flat_dynamic_buffer_two_phase();
    container_dynamic_buffer_grows();
    span_basics();
    buffer_slice_cuts_at_byte_granularity();
    consuming_buffers_advances_in_place();
    bufgrind_enumerates_every_split();
    circular_dynamic_buffer_wraps();
    std::cout << "buffer tests passed\n";
    return 0;
}
