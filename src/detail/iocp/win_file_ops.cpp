#include "detail/file_ops.hpp"

#include <string>
#include <vector>

// Windows 的文件同步操作。句柄以 FILE_FLAG_OVERLAPPED 打开：IOCP 需要它，也意味着没有隐式文件位置
//（stream_file 自己维护）。append 用"只有 FILE_APPEND_DATA 没有 FILE_WRITE_DATA"的访问权限表达：
// 内核对这样的句柄忽略写偏移，总是追加——与 O_APPEND 下 pwrite 的 Linux 行为一致。

namespace net {
namespace detail {
namespace fileops {

namespace {

std::error_code win32_error() noexcept { return std::error_code{static_cast<int>(::GetLastError()), std::system_category()}; }

std::wstring widen(std::string const& utf8, std::error_code& ec) {
    if (utf8.empty()) return {};
    auto const needed = ::MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, utf8.data(), static_cast<int>(utf8.size()), nullptr, 0);
    if (needed <= 0) {
        ec = win32_error();
        return {};
    }
    std::wstring wide(static_cast<std::size_t>(needed), L'\0');
    ::MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, utf8.data(), static_cast<int>(utf8.size()), &wide[0], needed);
    return wide;
}

} // namespace

native_file_type open_file(std::string const& path, file_base::flags const mode, std::error_code& ec) noexcept {
    ec.clear();
    std::wstring wide;
    try {
        wide = widen(path, ec);
    } catch (...) {
        ec = std::make_error_code(std::errc::not_enough_memory);
        return INVALID_HANDLE_VALUE;
    }
    if (ec) return INVALID_HANDLE_VALUE;

    DWORD access = 0;
    if (mode & file_base::read_only) access |= GENERIC_READ;
    if (mode & file_base::write_only) access |= (mode & file_base::append) ? (FILE_GENERIC_WRITE & ~FILE_WRITE_DATA) : GENERIC_WRITE;

    DWORD disposition = OPEN_EXISTING;
    if (mode & file_base::create) {
        if (mode & file_base::exclusive)
            disposition = CREATE_NEW;
        else if (mode & file_base::truncate)
            disposition = CREATE_ALWAYS;
        else
            disposition = OPEN_ALWAYS;
    } else if (mode & file_base::truncate) {
        disposition = TRUNCATE_EXISTING;
    }

    DWORD attributes = FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OVERLAPPED;
    if (mode & file_base::sync_all_on_write) attributes |= FILE_FLAG_WRITE_THROUGH;

    auto const handle = ::CreateFileW(wide.c_str(), access, FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, nullptr,
                                      disposition, attributes, nullptr);
    if (handle == INVALID_HANDLE_VALUE) {
        ec = win32_error();
        return INVALID_HANDLE_VALUE;
    }
    return handle;
}

void close_file(native_file_type const file) noexcept { ::CloseHandle(file); }

std::uint64_t file_size(native_file_type const file, std::error_code& ec) noexcept {
    LARGE_INTEGER size{};
    if (not ::GetFileSizeEx(file, &size)) {
        ec = win32_error();
        return 0U;
    }
    ec.clear();
    return static_cast<std::uint64_t>(size.QuadPart);
}

std::error_code file_resize(native_file_type const file, std::uint64_t const size) noexcept {
    LARGE_INTEGER distance{};
    distance.QuadPart = static_cast<LONGLONG>(size);
    if (not ::SetFilePointerEx(file, distance, nullptr, FILE_BEGIN)) return win32_error();
    if (not ::SetEndOfFile(file)) return win32_error();
    return {};
}

std::error_code file_sync_data(native_file_type const file) noexcept {
    if (not ::FlushFileBuffers(file)) return win32_error();
    return {};
}

std::error_code file_sync_all(native_file_type const file) noexcept { return file_sync_data(file); }

} // namespace fileops
} // namespace detail
} // namespace net
