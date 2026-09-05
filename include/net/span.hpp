#pragma once

#include <array>
#include <cstddef>
#include <iterator>
#include <type_traits>

// C++14 的最小 std::span 子集：连续序列的非拥有视图，长度与数据一起传递。缓冲区序列在
// 类型擦除边界（any_stream 的 vtable）以 span<mutable_buffer const> 传递。

namespace net {

template <class T> struct span {
    using element_type = T;
    using value_type = typename std::remove_cv<T>::type;
    using size_type = std::size_t;
    using pointer = T*;
    using reference = T&;
    using iterator = T*;
    using const_iterator = T const*;

    constexpr span() noexcept = default;

    constexpr span(T* const data, size_type const size) noexcept : data_{data}, size_{size} {}

    constexpr span(T* const first, T* const last) noexcept
        : data_{first}, size_{static_cast<size_type>(last - first)} {}

    template <std::size_t N> constexpr span(T (&array)[N]) noexcept : data_{array}, size_{N} {}

    template <class U, std::size_t N,
              class = typename std::enable_if<std::is_convertible<U (*)[], T (*)[]>::value>::type>
    constexpr span(std::array<U, N>& array) noexcept : data_{array.data()}, size_{N} {}

    template <class U, std::size_t N,
              class = typename std::enable_if<
                  std::is_convertible<U const (*)[], T (*)[]>::value>::type>
    constexpr span(std::array<U, N> const& array) noexcept : data_{array.data()}, size_{N} {}

    template <class Container,
              class = typename std::enable_if<
                  not std::is_array<Container>::value &&
                  std::is_convertible<
                      typename std::remove_pointer<decltype(std::declval<Container&>().data())>::type (*)[],
                      T (*)[]>::value>::type>
    constexpr span(Container& container) noexcept
        : data_{container.data()}, size_{static_cast<size_type>(container.size())} {}

    template <class U,
              class = typename std::enable_if<std::is_convertible<U (*)[], T (*)[]>::value>::type>
    constexpr span(span<U> const& other) noexcept : data_{other.data()}, size_{other.size()} {}

    constexpr pointer data() const noexcept { return data_; }
    constexpr size_type size() const noexcept { return size_; }
    constexpr size_type size_bytes() const noexcept { return size_ * sizeof(T); }
    constexpr bool empty() const noexcept { return size_ == 0U; }

    constexpr reference operator[](size_type const index) const noexcept { return data_[index]; }
    constexpr reference front() const noexcept { return data_[0]; }
    constexpr reference back() const noexcept { return data_[size_ - 1U]; }

    constexpr iterator begin() const noexcept { return data_; }
    constexpr iterator end() const noexcept { return data_ + size_; }

    constexpr span first(size_type const count) const noexcept { return span{data_, count}; }
    constexpr span last(size_type const count) const noexcept {
        return span{data_ + (size_ - count), count};
    }
    constexpr span subspan(size_type const offset, size_type const count) const noexcept {
        return span{data_ + offset, count};
    }
    constexpr span subspan(size_type const offset) const noexcept {
        return span{data_ + offset, size_ - offset};
    }

  private:
    T* data_ = nullptr;
    size_type size_ = 0U;
};

} // namespace net
