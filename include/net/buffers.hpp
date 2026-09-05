#pragma once

#include <array>
#include <cstddef>
#include <cstring>
#include <iterator>
#include <string>
#include <type_traits>
#include <vector>

#include "net/config.hpp"
#include "net/span.hpp"

// I/O 缓冲区词汇（P4100R1 §8.4 "I/O Buffer Ranges"）：字节区域描述符 mutable_buffer /
// const_buffer 与 scatter/gather 的序列概念。POSIX iovec、WSABUF、Asio const_buffer、
// libuv uv_buf_t 都是同一个形状；单个缓冲区不是 range（它是一段字节，不是可迭代序列），
// 所以序列概念是"可转换为单个缓冲区，或元素可转换为缓冲区的双向 range"。
//
// C++14 没有 concept：is_mutable_buffer_sequence<T> / is_const_buffer_sequence<T> 是对应
// 的特征；buffer_sequence_begin/end 统一遍历单个缓冲区与序列；buffer_size 按字节求和，
// buffer_copy 跨元素边界按字节拷贝。

namespace net {

struct mutable_buffer {
    mutable_buffer() noexcept = default;

    mutable_buffer(void* const data, std::size_t const size) noexcept : data_{data}, size_{size} {}

    void* data() const noexcept { return data_; }
    std::size_t size() const noexcept { return size_; }

    // 跳过前 n 个字节（不超过大小）。
    mutable_buffer& operator+=(std::size_t const n) noexcept {
        auto const offset = n < size_ ? n : size_;
        data_ = static_cast<unsigned char*>(data_) + offset;
        size_ -= offset;
        return *this;
    }

  private:
    void* data_ = nullptr;
    std::size_t size_ = 0U;
};

struct const_buffer {
    const_buffer() noexcept = default;

    const_buffer(void const* const data, std::size_t const size) noexcept
        : data_{data}, size_{size} {}

    const_buffer(mutable_buffer const& other) noexcept : data_{other.data()}, size_{other.size()} {}

    void const* data() const noexcept { return data_; }
    std::size_t size() const noexcept { return size_; }

    const_buffer& operator+=(std::size_t const n) noexcept {
        auto const offset = n < size_ ? n : size_;
        data_ = static_cast<unsigned char const*>(data_) + offset;
        size_ -= offset;
        return *this;
    }

  private:
    void const* data_ = nullptr;
    std::size_t size_ = 0U;
};

inline mutable_buffer operator+(mutable_buffer const& b, std::size_t const n) noexcept {
    auto result = b;
    result += n;
    return result;
}

inline mutable_buffer operator+(std::size_t const n, mutable_buffer const& b) noexcept {
    return b + n;
}

inline const_buffer operator+(const_buffer const& b, std::size_t const n) noexcept {
    auto result = b;
    result += n;
    return result;
}

inline const_buffer operator+(std::size_t const n, const_buffer const& b) noexcept { return b + n; }

// ---- 序列特征 ----

namespace detail {

template <class...> struct buffers_void { using type = void; };
template <class... T> using buffers_void_t = typename buffers_void<T...>::type;

template <class T, class = void> struct range_value : std::false_type {};

template <class T>
struct range_value<T, buffers_void_t<decltype(std::begin(std::declval<T const&>())),
                                      decltype(std::end(std::declval<T const&>()))>>
    : std::true_type {
    using type = typename std::iterator_traits<decltype(std::begin(
        std::declval<T const&>()))>::value_type;
};

template <class T, class Buffer, bool IsRange = range_value<T>::value>
struct is_buffer_range : std::false_type {};

template <class T, class Buffer>
struct is_buffer_range<T, Buffer, true>
    : std::is_convertible<typename range_value<T>::type, Buffer> {};

} // namespace detail

template <class T>
struct is_mutable_buffer_sequence
    : std::integral_constant<
          bool, std::is_convertible<typename std::decay<T>::type, mutable_buffer>::value ||
                    detail::is_buffer_range<typename std::decay<T>::type, mutable_buffer>::value> {};

template <class T>
struct is_const_buffer_sequence
    : std::integral_constant<
          bool, std::is_convertible<typename std::decay<T>::type, const_buffer>::value ||
                    detail::is_buffer_range<typename std::decay<T>::type, const_buffer>::value> {};

// ---- 统一遍历 ----

inline mutable_buffer const* buffer_sequence_begin(mutable_buffer const& b) noexcept { return &b; }
inline mutable_buffer const* buffer_sequence_end(mutable_buffer const& b) noexcept { return &b + 1; }
inline const_buffer const* buffer_sequence_begin(const_buffer const& b) noexcept { return &b; }
inline const_buffer const* buffer_sequence_end(const_buffer const& b) noexcept { return &b + 1; }

template <class Sequence,
          class = typename std::enable_if<
              not std::is_convertible<Sequence const&, const_buffer>::value &&
              not std::is_convertible<Sequence const&, mutable_buffer>::value>::type>
auto buffer_sequence_begin(Sequence const& sequence) noexcept -> decltype(std::begin(sequence)) {
    return std::begin(sequence);
}

template <class Sequence,
          class = typename std::enable_if<
              not std::is_convertible<Sequence const&, const_buffer>::value &&
              not std::is_convertible<Sequence const&, mutable_buffer>::value>::type>
auto buffer_sequence_end(Sequence const& sequence) noexcept -> decltype(std::end(sequence)) {
    return std::end(sequence);
}

// ---- 算法 ----

template <class Sequence> std::size_t buffer_size(Sequence const& sequence) noexcept {
    auto total = std::size_t{};
    auto const last = buffer_sequence_end(sequence);
    for (auto it = buffer_sequence_begin(sequence); it != last; ++it)
        total += const_buffer(*it).size();
    return total;
}

template <class Sequence> bool buffer_empty(Sequence const& sequence) noexcept {
    return buffer_size(sequence) == 0U;
}

// 从 source 拷贝到 target，最多 max_bytes 字节（默认不限），跨元素边界；返回拷贝的字节数。
template <class MutableSequence, class ConstSequence>
std::size_t buffer_copy(MutableSequence const& target, ConstSequence const& source,
                        std::size_t const max_bytes = static_cast<std::size_t>(-1)) noexcept {
    static_assert(is_mutable_buffer_sequence<MutableSequence>::value,
                  "buffer_copy target must be a MutableBufferSequence");
    static_assert(is_const_buffer_sequence<ConstSequence>::value,
                  "buffer_copy source must be a ConstBufferSequence");
    auto copied = std::size_t{};
    auto target_it = buffer_sequence_begin(target);
    auto const target_end = buffer_sequence_end(target);
    auto source_it = buffer_sequence_begin(source);
    auto const source_end = buffer_sequence_end(source);
    auto target_offset = std::size_t{};
    auto source_offset = std::size_t{};
    while (copied < max_bytes && target_it != target_end && source_it != source_end) {
        auto const t = mutable_buffer(*target_it) + target_offset;
        auto const s = const_buffer(*source_it) + source_offset;
        auto n = t.size() < s.size() ? t.size() : s.size();
        if (n > max_bytes - copied) n = max_bytes - copied;
        if (n != 0U) std::memcpy(t.data(), s.data(), n);
        copied += n;
        target_offset += n;
        source_offset += n;
        if (target_offset == mutable_buffer(*target_it).size()) {
            ++target_it;
            target_offset = 0U;
        }
        if (source_offset == const_buffer(*source_it).size()) {
            ++source_it;
            source_offset = 0U;
        }
    }
    return copied;
}

// ---- 构造 ----

inline mutable_buffer buffer(void* const data, std::size_t const size) noexcept {
    return mutable_buffer{data, size};
}

inline const_buffer buffer(void const* const data, std::size_t const size) noexcept {
    return const_buffer{data, size};
}

inline mutable_buffer buffer(mutable_buffer const& b) noexcept { return b; }
inline const_buffer buffer(const_buffer const& b) noexcept { return b; }

inline mutable_buffer buffer(mutable_buffer const& b, std::size_t const max_size) noexcept {
    return mutable_buffer{b.data(), b.size() < max_size ? b.size() : max_size};
}

inline const_buffer buffer(const_buffer const& b, std::size_t const max_size) noexcept {
    return const_buffer{b.data(), b.size() < max_size ? b.size() : max_size};
}

template <class T, std::size_t N,
          class = typename std::enable_if<std::is_trivially_copyable<T>::value>::type>
mutable_buffer buffer(T (&array)[N]) noexcept {
    return mutable_buffer{array, N * sizeof(T)};
}

template <class T, std::size_t N,
          class = typename std::enable_if<std::is_trivially_copyable<T>::value>::type>
const_buffer buffer(T const (&array)[N]) noexcept {
    return const_buffer{array, N * sizeof(T)};
}

template <class T, std::size_t N,
          class = typename std::enable_if<std::is_trivially_copyable<T>::value>::type>
mutable_buffer buffer(std::array<T, N>& array) noexcept {
    return mutable_buffer{array.data(), N * sizeof(T)};
}

template <class T, std::size_t N,
          class = typename std::enable_if<std::is_trivially_copyable<T>::value>::type>
const_buffer buffer(std::array<T, N> const& array) noexcept {
    return const_buffer{array.data(), N * sizeof(T)};
}

template <class T, class Allocator,
          class = typename std::enable_if<std::is_trivially_copyable<T>::value>::type>
mutable_buffer buffer(std::vector<T, Allocator>& vector) noexcept {
    return mutable_buffer{vector.data(), vector.size() * sizeof(T)};
}

template <class T, class Allocator,
          class = typename std::enable_if<std::is_trivially_copyable<T>::value>::type>
const_buffer buffer(std::vector<T, Allocator> const& vector) noexcept {
    return const_buffer{vector.data(), vector.size() * sizeof(T)};
}

template <class Char, class Traits, class Allocator>
mutable_buffer buffer(std::basic_string<Char, Traits, Allocator>& string) noexcept {
    return mutable_buffer{string.empty() ? nullptr : &string[0], string.size() * sizeof(Char)};
}

template <class Char, class Traits, class Allocator>
const_buffer buffer(std::basic_string<Char, Traits, Allocator> const& string) noexcept {
    return const_buffer{string.data(), string.size() * sizeof(Char)};
}

template <class T> mutable_buffer buffer(span<T> const s) noexcept {
    static_assert(not std::is_const<T>::value, "use span<T const> for a const_buffer");
    return mutable_buffer{s.data(), s.size_bytes()};
}

template <class T> const_buffer buffer(span<T const> const s) noexcept {
    return const_buffer{s.data(), s.size_bytes()};
}

// ---- 展平：把任意序列压成定长数组（类型擦除边界与系统调用用） ----

constexpr std::size_t max_iovec = NET_MAX_IOVEC;

template <class Buffer, std::size_t N> struct buffer_array {
    static_assert(N != 0U, "buffer_array needs at least one slot");

    buffer_array() noexcept = default;

    // 跳过空缓冲区；超过 N 个的元素被截断（部分传输是合法的：调用方会再次调用）。
    template <class Sequence> explicit buffer_array(Sequence const& sequence) noexcept {
        auto const last = buffer_sequence_end(sequence);
        for (auto it = buffer_sequence_begin(sequence); it != last && count_ != N; ++it) {
            auto const b = Buffer(*it);
            if (b.size() == 0U) continue;
            buffers_[count_++] = b;
        }
    }

    span<Buffer const> to_span() const noexcept { return span<Buffer const>{buffers_, count_}; }

    Buffer const* begin() const noexcept { return buffers_; }
    Buffer const* end() const noexcept { return buffers_ + count_; }
    Buffer const* data() const noexcept { return buffers_; }
    std::size_t size() const noexcept { return count_; }
    bool empty() const noexcept { return count_ == 0U; }
    Buffer const& operator[](std::size_t const index) const noexcept { return buffers_[index]; }

    std::size_t total_size() const noexcept {
        auto total = std::size_t{};
        for (auto index = std::size_t{}; index != count_; ++index)
            total += buffers_[index].size();
        return total;
    }

    // 丢弃前 n 个字节（跨元素）。
    void consume(std::size_t n) noexcept {
        auto kept = std::size_t{};
        for (auto index = std::size_t{}; index != count_; ++index) {
            auto b = buffers_[index];
            if (n >= b.size()) {
                n -= b.size();
                continue;
            }
            b += n;
            n = 0U;
            buffers_[kept++] = b;
        }
        count_ = kept;
    }

  private:
    Buffer buffers_[N];
    std::size_t count_ = 0U;
};

template <std::size_t N = max_iovec> using mutable_buffer_array = buffer_array<mutable_buffer, N>;
template <std::size_t N = max_iovec> using const_buffer_array = buffer_array<const_buffer, N>;

} // namespace net
