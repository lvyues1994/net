#pragma once

#include <cstdint>
#include <string>
#include <system_error>

#include "net/detail/socket_types.hpp"
#include "net/file.hpp"

// 文件的同步系统调用（两个平台各一份实现：posix/file_ops.cpp、iocp/win_file_ops.cpp）。
// 异步读写属于后端（file_impl）。

namespace net {
namespace detail {
namespace fileops {

// 打开：file_base::flags → O_* / CreateFile 参数。失败返回 invalid_file_value() 且设置 ec。
// Windows 上以 FILE_FLAG_OVERLAPPED 打开（IOCP 需要）。
native_file_type open_file(std::string const& path, file_base::flags mode, std::error_code& ec) noexcept;
void close_file(native_file_type file) noexcept;

std::uint64_t file_size(native_file_type file, std::error_code& ec) noexcept;
std::error_code file_resize(native_file_type file, std::uint64_t size) noexcept;
std::error_code file_sync_data(native_file_type file) noexcept;
std::error_code file_sync_all(native_file_type file) noexcept;

} // namespace fileops
} // namespace detail
} // namespace net
