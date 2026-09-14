#pragma once

#include <cstddef>

#include "net/buffer_slice.hpp"
#include "net/buffers.hpp"

// 缓冲区切分器（Capy 的 test::bufgrind 同形）：把一个缓冲区序列在每个字节位置切成前后两半，逐个给出。
// 用来验证接受序列的算法对任意切分都正确——read / write / buffer_copy 对 "前半 + 后半" 的处理应与整段
// 一致。step 大于 1 时按步长切，长序列不必每个字节都试。
//
//   for (net::test::bufgrind<Seq> g{seq}; g.has_next();) {
//       auto const halves = g.next();          // halves.head / halves.tail 都是缓冲区序列
//       CHECK(buffer_size(halves.head) + buffer_size(halves.tail) == buffer_size(seq));
//   }

namespace net {
namespace test {

template <class Sequence> struct bufgrind {
    using half_type = slice_type<Sequence>;

    struct split {
        half_type head;
        half_type tail;
        std::size_t position;
    };

    explicit bufgrind(Sequence const& sequence, std::size_t const step = 1U) noexcept
        : sequence_{&sequence}, size_{buffer_size(sequence)}, step_{step == 0U ? 1U : step} {}
    explicit bufgrind(Sequence const&&, std::size_t = 1U) = delete;

    bool has_next() const noexcept { return not done_; }

    split next() noexcept {
        split result{buffer_slice(*sequence_, 0U, position_), buffer_slice(*sequence_, position_), position_};
        if (position_ >= size_)
            done_ = true;
        else
            position_ = position_ + step_ < size_ ? position_ + step_ : size_;
        return result;
    }

  private:
    Sequence const* sequence_;
    std::size_t size_;
    std::size_t step_;
    std::size_t position_ = 0U;
    bool done_ = false;
};

template <class Sequence> bufgrind<Sequence> make_bufgrind(Sequence const& sequence, std::size_t const step = 1U) noexcept {
    return bufgrind<Sequence>{sequence, step};
}

} // namespace test
} // namespace net
