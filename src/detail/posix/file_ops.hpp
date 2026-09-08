#pragma once

#include <cstdint>
#include <system_error>

#include "net/buffers.hpp"

#include "detail/file_ops.hpp"

// POSIX 文件的按偏移分散读写（EINTR 重试）；同步部分在 detail/file_ops.hpp。

namespace net {
namespace detail {
namespace posix {

struct file_transfer {
    std::error_code ec;
    std::size_t bytes;
};

// preadv / pwritev 到 offset；读到 0 字节 → error::eof。
file_transfer preadv_at(int fd, std::uint64_t offset, span<mutable_buffer const> buffers) noexcept;
file_transfer pwritev_at(int fd, std::uint64_t offset, span<const_buffer const> buffers) noexcept;

} // namespace posix
} // namespace detail
} // namespace net
