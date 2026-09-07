#pragma once

#include <cstdint>
#include <string>
#include <system_error>

#include "net/buffers.hpp"
#include "net/file.hpp"

// 文件的 POSIX 系统调用封装：打开标志映射、按偏移的分散读写（EINTR 重试）、同步操作。

namespace net {
namespace detail {
namespace posix {

// open(2)：file_base::flags → O_*。返回描述符或 -1（errno 已设置）。带 O_CLOEXEC。
int open_file(std::string const& path, file_base::flags mode) noexcept;

struct file_transfer {
    std::error_code ec;
    std::size_t bytes;
};

// preadv / pwritev 到 offset；读到 0 字节 → error::eof。
file_transfer preadv_at(int fd, std::uint64_t offset, span<mutable_buffer const> buffers) noexcept;
file_transfer pwritev_at(int fd, std::uint64_t offset, span<const_buffer const> buffers) noexcept;

std::uint64_t file_size(int fd, std::error_code& ec) noexcept;
std::error_code file_resize(int fd, std::uint64_t size) noexcept;
std::error_code file_sync_data(int fd) noexcept;
std::error_code file_sync_all(int fd) noexcept;

} // namespace posix
} // namespace detail
} // namespace net
