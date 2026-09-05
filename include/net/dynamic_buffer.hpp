#pragma once

#include <cstddef>
#include <stdexcept>
#include <string>
#include <vector>

#include "net/buffers.hpp"

// DynamicBuffer（P4100R1 §8.5）：两阶段写（prepare(n) 拿到可写空间 → 填字节 → commit(m)
// 使其可读）与两阶段读（data() 查看可读区 → consume(k) 丢弃前 k 字节）的可增长字节缓冲。
// 协议解析器、HTTP 消息读取、TLS 记录缓冲都需要这个 FIFO 形状；一个概念下容纳平面、环形
// 与容器适配三类实现。
//
// 要求：成员类型 const_buffers_type / mutable_buffers_type（满足缓冲区序列特征），
// size() / max_size() / capacity() / data() / prepare(n) / commit(n) / consume(n)。

namespace net {

// 调用方拥有存储的平面缓冲：[readable | writable(prepared) | free]。
struct flat_dynamic_buffer {
    using const_buffers_type = const_buffer;
    using mutable_buffers_type = mutable_buffer;

    flat_dynamic_buffer() noexcept = default;

    flat_dynamic_buffer(void* const storage, std::size_t const capacity) noexcept
        : begin_{static_cast<unsigned char*>(storage)}, capacity_{capacity} {}

    template <std::size_t N>
    explicit flat_dynamic_buffer(unsigned char (&storage)[N]) noexcept
        : begin_{storage}, capacity_{N} {}

    template <std::size_t N>
    explicit flat_dynamic_buffer(char (&storage)[N]) noexcept
        : begin_{reinterpret_cast<unsigned char*>(storage)}, capacity_{N} {}

    std::size_t size() const noexcept { return in_size_; }
    std::size_t max_size() const noexcept { return capacity_; }
    std::size_t capacity() const noexcept { return capacity_; }

    const_buffers_type data() const noexcept { return const_buffer{begin_ + in_offset_, in_size_}; }

    // 至少 n 字节可写空间；空间不足时先把可读区搬到开头，仍不足抛 std::length_error。
    mutable_buffers_type prepare(std::size_t const n) {
        if (n > capacity_ - in_size_) throw std::length_error{"flat_dynamic_buffer too small"};
        if (in_offset_ + in_size_ + n > capacity_) compact();
        out_size_ = n;
        return mutable_buffer{begin_ + in_offset_ + in_size_, n};
    }

    void commit(std::size_t const n) noexcept {
        auto const committed = n < out_size_ ? n : out_size_;
        in_size_ += committed;
        out_size_ = 0U;
    }

    void consume(std::size_t const n) noexcept {
        auto const consumed = n < in_size_ ? n : in_size_;
        in_offset_ += consumed;
        in_size_ -= consumed;
        if (in_size_ == 0U) in_offset_ = 0U;
    }

  private:
    void compact() noexcept {
        if (in_offset_ == 0U) return;
        std::memmove(begin_, begin_ + in_offset_, in_size_);
        in_offset_ = 0U;
    }

    unsigned char* begin_ = nullptr;
    std::size_t capacity_ = 0U;
    std::size_t in_offset_ = 0U;
    std::size_t in_size_ = 0U;
    std::size_t out_size_ = 0U;
};

// 适配 std::vector<char-like> / std::basic_string：容器自身增长，可读区始终从 0 开始。
template <class Container> struct dynamic_container_buffer {
    using const_buffers_type = const_buffer;
    using mutable_buffers_type = mutable_buffer;

    explicit dynamic_container_buffer(Container& container,
                                      std::size_t const maximum_size = static_cast<std::size_t>(-1)) noexcept
        : container_{&container}, in_size_{container.size()}, max_size_{maximum_size} {}

    std::size_t size() const noexcept { return in_size_; }
    std::size_t max_size() const noexcept { return max_size_; }
    std::size_t capacity() const noexcept {
        auto const c = container_->capacity();
        return c < max_size_ ? c : max_size_;
    }

    const_buffers_type data() const noexcept {
        return const_buffer{container_->data(), in_size_};
    }

    mutable_buffers_type prepare(std::size_t const n) {
        if (in_size_ > max_size_ || n > max_size_ - in_size_)
            throw std::length_error{"dynamic_container_buffer too long"};
        container_->resize(in_size_ + n);
        return mutable_buffer{&(*container_)[0] + in_size_, n};
    }

    void commit(std::size_t const n) noexcept {
        auto const prepared = container_->size() - in_size_;
        in_size_ += n < prepared ? n : prepared;
        container_->resize(in_size_);
    }

    void consume(std::size_t const n) noexcept {
        auto const consumed = n < in_size_ ? n : in_size_;
        container_->erase(container_->begin(),
                          container_->begin() + static_cast<std::ptrdiff_t>(consumed));
        in_size_ -= consumed;
    }

  private:
    Container* container_;
    std::size_t in_size_;
    std::size_t max_size_;
};

template <class Allocator>
using vector_dynamic_buffer = dynamic_container_buffer<std::vector<char, Allocator>>;
template <class Traits, class Allocator>
using string_dynamic_buffer = dynamic_container_buffer<std::basic_string<char, Traits, Allocator>>;

template <class T, class Allocator>
dynamic_container_buffer<std::vector<T, Allocator>>
dynamic_buffer(std::vector<T, Allocator>& vector,
               std::size_t const max_size = static_cast<std::size_t>(-1)) noexcept {
    static_assert(sizeof(T) == 1U, "dynamic_buffer requires a byte container");
    return dynamic_container_buffer<std::vector<T, Allocator>>{vector, max_size};
}

template <class Traits, class Allocator>
dynamic_container_buffer<std::basic_string<char, Traits, Allocator>>
dynamic_buffer(std::basic_string<char, Traits, Allocator>& string,
               std::size_t const max_size = static_cast<std::size_t>(-1)) noexcept {
    return dynamic_container_buffer<std::basic_string<char, Traits, Allocator>>{string, max_size};
}

} // namespace net
