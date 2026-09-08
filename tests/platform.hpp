#pragma once

// 平台测试里少量绕过库、直接碰操作系统的地方（裸套接字、信号号、临时文件）的可移植封装。

#include <csignal>
#include <cstdio>
#include <string>

#include "net/detail/socket_types.hpp"

#if NET_PLATFORM_WINDOWS
#include <io.h>
#else
#include <fcntl.h>
#include <unistd.h>
#endif

namespace net_test {

// ---- 裸套接字 ----

inline net::native_socket_type raw_tcp_socket() {
#if NET_PLATFORM_WINDOWS
    return ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP); // Winsock 默认就是重叠属性
#else
    return ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
#endif
}

inline void close_raw_socket(net::native_socket_type const s) {
#if NET_PLATFORM_WINDOWS
    ::closesocket(s);
#else
    ::close(s);
#endif
}

inline int set_reuse_address(net::native_socket_type const s) {
    auto const one = 1;
    return ::setsockopt(s, SOL_SOCKET, SO_REUSEADDR, static_cast<net::detail::sockopt_pointer>(static_cast<void const*>(&one)),
                        sizeof(one));
}

// ---- 信号 ----
//
// POSIX 用 SIGUSR1 / SIGUSR2（默认处置是终止，所以测试必须先装处理器）；Windows 没有用户信号，
// 用 SIGINT / SIGTERM（CRT 支持 signal() + raise()）。

#if NET_PLATFORM_WINDOWS
constexpr int test_signal_a = SIGINT;
constexpr int test_signal_b = SIGTERM;
#else
constexpr int test_signal_a = SIGUSR1;
constexpr int test_signal_b = SIGUSR2;
#endif

inline bool ignore_signal(int const signo) {
#if NET_PLATFORM_WINDOWS
    return ::signal(signo, SIG_IGN) != SIG_ERR;
#else
    struct sigaction ignore{};
    ignore.sa_handler = SIG_IGN;
    return ::sigaction(signo, &ignore, nullptr) == 0;
#endif
}

// ---- 临时文件 ----

inline std::string make_temp_file() {
#if NET_PLATFORM_WINDOWS
    char directory[MAX_PATH + 1] = {};
    auto const n = ::GetTempPathA(MAX_PATH + 1, directory);
    if (n == 0 || n > MAX_PATH) return {};
    char name[MAX_PATH + 1] = {};
    if (::GetTempFileNameA(directory, "net", 0, name) == 0) return {};
    return name;
#else
    char name[] = "/tmp/net_file_test_XXXXXX";
    auto const fd = ::mkstemp(name);
    if (fd < 0) return {};
    ::close(fd);
    return name;
#endif
}

// 以只读方式打开一个可交给 basic_file::assign 的原生句柄（Windows 上带 FILE_FLAG_OVERLAPPED）。
inline net::native_file_type open_readonly_raw(std::string const& path) {
#if NET_PLATFORM_WINDOWS
    return ::CreateFileA(path.c_str(), GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, nullptr, OPEN_EXISTING,
                         FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OVERLAPPED, nullptr);
#else
    return ::open(path.c_str(), O_RDONLY);
#endif
}

inline void close_raw_file(net::native_file_type const file) {
#if NET_PLATFORM_WINDOWS
    ::CloseHandle(file);
#else
    ::close(file);
#endif
}

} // namespace net_test
