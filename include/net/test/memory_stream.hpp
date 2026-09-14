#pragma once

#include <cstddef>
#include <string>
#include <utility>

#include "net/buffers.hpp"
#include "net/error.hpp"
#include "net/immediate.hpp"
#include "net/io_result.hpp"
#include "net/stream.hpp"
#include "net/test/fuse.hpp"

// 内存流：满足 Stream 概念的测试替身，所有操作立即完成（immediate）。读从 input 取，写追加到 output；
// max_read_size / max_write_size 限制每次传输的字节数，模拟分片到达；给了 fuse 就在每次操作前询问它，
// 让组合算法的每条错误路径都被走一次。
//
//   net::test::memory_stream s{"GET / HTTP/1.1\r\n\r\n"};
//   s.max_read_size = 3;                       // 每次最多 3 字节
//   CO2_AWAIT_SET(r, net::read_until(s, buffer, "\r\n\r\n"));
//   CHECK(s.output == "...");

namespace net {
namespace test {

struct memory_stream {
    std::string input;
    std::string output;
    std::size_t max_read_size = static_cast<std::size_t>(-1);
    std::size_t max_write_size = static_cast<std::size_t>(-1);
    fuse* failures = nullptr;

    memory_stream() = default;
    explicit memory_stream(std::string initial_input, fuse* const f = nullptr) : input(std::move(initial_input)), failures{f} {}

    template <class MutableBufferSequence>
    immediate<io_result<std::size_t>> read_some(MutableBufferSequence const& buffers) {
        if (failures != nullptr) {
            if (auto const ec = failures->maybe_fail()) return {{ec, 0U}};
        }
        if (buffer_size(buffers) == 0U) return {{std::error_code{}, 0U}};
        if (input.empty()) return {{make_error_code(error::eof), 0U}};
        auto const n = buffer_copy(buffers, buffer(input), max_read_size);
        input.erase(0, n);
        return {{std::error_code{}, n}};
    }

    template <class ConstBufferSequence>
    immediate<io_result<std::size_t>> write_some(ConstBufferSequence const& buffers) {
        if (failures != nullptr) {
            if (auto const ec = failures->maybe_fail()) return {{ec, 0U}};
        }
        auto const total = buffer_size(buffers);
        auto const n = total < max_write_size ? total : max_write_size;
        auto const before = output.size();
        output.resize(before + n);
        buffer_copy(buffer(output) + before, buffers, n);
        return {{std::error_code{}, n}};
    }
};

static_assert(is_stream<memory_stream>::value, "memory_stream must satisfy Stream");

} // namespace test
} // namespace net
