// 缓冲区词汇：mutable_buffer / const_buffer、序列特征、buffer_size / buffer_copy、
// buffer_array、动态缓冲、span。

#include <array>
#include <cstring>
#include <list>
#include <string>
#include <vector>

#include "net/buffers.hpp"
#include "net/dynamic_buffer.hpp"
#include "net/span.hpp"

#include "check.hpp"

namespace {

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

} // namespace

int main() {
    buffer_factories();
    buffer_copy_spans_element_boundaries();
    buffer_array_flattens_and_consumes();
    flat_dynamic_buffer_two_phase();
    container_dynamic_buffer_grows();
    span_basics();
    std::cout << "buffer tests passed\n";
    return 0;
}
