#pragma once

#include <cstddef>
#include <iterator>
#include <type_traits>

#include "net/buffers.hpp"

// 字节粒度的缓冲区切片（P4100R1 §8.4：ranges::drop 按元素切，缓冲区序列要按字节、跨元素边界切；
// Capy 的 buffer_slice / consuming_buffers / front 同形）。
//
//   auto rest = net::buffer_slice(buffers, n);          // 从第 n 字节起到末尾（借用视图，buffers 必须活着）
//   auto head = net::buffer_slice(buffers, 0, 16);      // 前 16 字节
//   net::consuming_buffers<Seq> cursor{buffers};        // 写循环的游标
//   while (buffer_size(cursor.data()) != 0) { n = co_await write_some(cursor.data()); cursor.consume(n); }
//   net::buffer_front(buffers)                          // 第一个非空缓冲区，没有则空缓冲区
//
// 单个缓冲区对切片封闭：切出来还是同类缓冲区；其它序列切出 slice_of<Seq>——存的是序列的迭代器加首尾
// 两端的字节偏移，不拷贝描述符，序列必须比视图活得久（右值重载被删掉，切临时对象编译不过）。
// slice_of 自身满足与 Seq 相同的缓冲区序列特征，可以直接交给任何接受序列的操作。

namespace net {
namespace detail {

template <class Sequence>
using buffer_type_of = typename std::conditional<is_mutable_buffer_sequence<Sequence>::value, mutable_buffer, const_buffer>::type;

template <class Sequence>
using is_single_buffer_sequence =
    std::integral_constant<bool, std::is_same<typename std::decay<Sequence>::type, mutable_buffer>::value ||
                                     std::is_same<typename std::decay<Sequence>::type, const_buffer>::value>;

// 借用视图：[offset, offset + length) 字节。
template <class Sequence> struct slice_of {
    using buffer_type = buffer_type_of<Sequence>;
    using value_type = buffer_type;
    using underlying_iterator = decltype(buffer_sequence_begin(std::declval<Sequence const&>()));

    struct const_iterator {
        using iterator_category = std::bidirectional_iterator_tag;
        using value_type = buffer_type;
        using difference_type = std::ptrdiff_t;
        using pointer = void;
        using reference = buffer_type;

        const_iterator() = default;
        const_iterator(underlying_iterator const position, slice_of const* const owner) noexcept
            : position_{position}, owner_{owner} {}

        reference operator*() const noexcept {
            auto b = buffer_type(*position_);
            auto front = std::size_t{};
            auto back = std::size_t{};
            if (position_ == owner_->first_) front = owner_->front_skip_;
            auto next = position_;
            ++next;
            if (next == owner_->last_) back = owner_->back_skip_;
            b += front;
            return buffer_type{b.data(), b.size() - back};
        }

        const_iterator& operator++() noexcept {
            ++position_;
            return *this;
        }
        const_iterator operator++(int) noexcept {
            auto copy = *this;
            ++*this;
            return copy;
        }
        const_iterator& operator--() noexcept {
            --position_;
            return *this;
        }
        const_iterator operator--(int) noexcept {
            auto copy = *this;
            --*this;
            return copy;
        }

        friend bool operator==(const_iterator const& a, const_iterator const& b) noexcept { return a.position_ == b.position_; }
        friend bool operator!=(const_iterator const& a, const_iterator const& b) noexcept { return a.position_ != b.position_; }

      private:
        underlying_iterator position_{};
        slice_of const* owner_ = nullptr;
    };

    slice_of() = default;

    // 一次向前扫到两个切点；不求整个序列的字节数。offset 超出总长时为空视图；length 默认到末尾。
    slice_of(Sequence const& sequence, std::size_t offset, std::size_t length) noexcept
        : first_{buffer_sequence_begin(sequence)}, last_{buffer_sequence_end(sequence)} {
        while (first_ != last_) {
            auto const size = buffer_type(*first_).size();
            if (offset < size) {
                front_skip_ = offset;
                break;
            }
            offset -= size;
            ++first_;
        }
        if (first_ == last_) return;
        if (length == static_cast<std::size_t>(-1)) return;
        // 从 first_ 起走 length 个有效字节，确定 last_ 与末段要砍掉的字节数。
        auto cursor = first_;
        auto skip = front_skip_;
        auto left = length;
        while (cursor != last_) {
            auto const available = buffer_type(*cursor).size() - skip;
            if (left <= available) {
                back_skip_ = available - left;
                ++cursor;
                last_ = cursor;
                return;
            }
            left -= available;
            skip = 0U;
            ++cursor;
        }
    }

    const_iterator begin() const noexcept { return const_iterator{first_, this}; }
    const_iterator end() const noexcept { return const_iterator{last_, this}; }

  private:
    underlying_iterator first_{};
    underlying_iterator last_{};
    std::size_t front_skip_ = 0U;
    std::size_t back_skip_ = 0U;
};

template <class Sequence, bool Single = is_single_buffer_sequence<Sequence>::value> struct slice_result {
    using type = slice_of<Sequence>;
    static type apply(Sequence const& sequence, std::size_t const offset, std::size_t const length) noexcept {
        return type{sequence, offset, length};
    }
};

template <class Buffer> struct slice_result<Buffer, true> {
    using type = Buffer;
    static type apply(Buffer const& b, std::size_t const offset, std::size_t const length) noexcept {
        auto result = b + offset; // operator+ 夹到 size()
        if (length < result.size()) result = Buffer{result.data(), length};
        return result;
    }
};

} // namespace detail

template <class Sequence> using slice_type = typename detail::slice_result<typename std::decay<Sequence>::type>::type;

// [offset, offset + length) 字节的子序列。单个缓冲区返回调整后的缓冲区；其它序列返回借用视图。
template <class Sequence>
slice_type<Sequence> buffer_slice(Sequence const& sequence, std::size_t const offset = 0U,
                                  std::size_t const length = static_cast<std::size_t>(-1)) noexcept {
    static_assert(is_mutable_buffer_sequence<Sequence>::value || is_const_buffer_sequence<Sequence>::value,
                  "buffer_slice requires a buffer sequence");
    return detail::slice_result<typename std::decay<Sequence>::type>::apply(sequence, offset, length);
}

// 切临时序列会得到悬空视图：编译期拒绝。把序列放进有名字的变量再切。（单个缓冲区切出来是值，临时的也行。）
template <class Sequence,
          class = typename std::enable_if<not detail::is_single_buffer_sequence<Sequence>::value>::type>
slice_type<Sequence> buffer_slice(Sequence const&& sequence, std::size_t offset = 0U,
                                  std::size_t length = static_cast<std::size_t>(-1)) = delete;

// 写循环的游标：data() 给出尚未消费的部分（借用视图），consume(n) 原地前进 n 字节。序列必须比游标活得久。
template <class Sequence> struct consuming_buffers {
    static_assert(is_mutable_buffer_sequence<Sequence>::value || is_const_buffer_sequence<Sequence>::value,
                  "consuming_buffers requires a buffer sequence");
    using buffer_type = detail::buffer_type_of<Sequence>;

    explicit consuming_buffers(Sequence const& sequence) noexcept : sequence_{&sequence} {}
    explicit consuming_buffers(Sequence const&&) = delete;

    slice_type<Sequence> data() const noexcept { return buffer_slice(*sequence_, consumed_); }

    void consume(std::size_t const n) noexcept { consumed_ += n; }

    std::size_t consumed() const noexcept { return consumed_; }

  private:
    Sequence const* sequence_;
    std::size_t consumed_ = 0U;
};

template <class Sequence> consuming_buffers<Sequence> make_consuming_buffers(Sequence const& sequence) noexcept {
    return consuming_buffers<Sequence>{sequence};
}

// 第一个非空缓冲区；没有则空缓冲区。
template <class Sequence> detail::buffer_type_of<Sequence> buffer_front(Sequence const& sequence) noexcept {
    static_assert(is_mutable_buffer_sequence<Sequence>::value || is_const_buffer_sequence<Sequence>::value,
                  "buffer_front requires a buffer sequence");
    auto const last = buffer_sequence_end(sequence);
    for (auto it = buffer_sequence_begin(sequence); it != last; ++it) {
        auto const b = detail::buffer_type_of<Sequence>(*it);
        if (b.size() != 0U) return b;
    }
    return detail::buffer_type_of<Sequence>{};
}

} // namespace net
