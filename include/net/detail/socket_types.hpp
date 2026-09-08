#pragma once

#include "net/config.hpp"

// 平台套接字 / 文件句柄类型与头文件（公共头唯一包含操作系统套接字头的地方）。
//
//   native_socket_type / invalid_socket   POSIX：int / -1；Windows：SOCKET / INVALID_SOCKET
//   native_file_type / invalid_file       POSIX：int / -1；Windows：HANDLE / INVALID_HANDLE_VALUE
//   shutdown_receive / shutdown_send / shutdown_both   SHUT_* 或 SD_*
//
// Windows 头的顺序与宏很挑：winsock2.h 必须先于 windows.h，WIN32_LEAN_AND_MEAN 阻止 windows.h 拉进
// winsock.h（与 winsock2.h 冲突），NOMINMAX 阻止 min / max 宏。

#if NET_PLATFORM_WINDOWS

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#ifndef _WIN32_WINNT
#define _WIN32_WINNT 0x0A00
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>

namespace net {
using native_socket_type = SOCKET;
using native_file_type = HANDLE;
constexpr native_socket_type invalid_socket = INVALID_SOCKET;
inline native_file_type invalid_file_value() noexcept { return INVALID_HANDLE_VALUE; }
constexpr int shutdown_receive = SD_RECEIVE;
constexpr int shutdown_send = SD_SEND;
constexpr int shutdown_both = SD_BOTH;
inline bool socket_is_valid(native_socket_type const s) noexcept { return s != INVALID_SOCKET; }
inline bool file_is_valid(native_file_type const h) noexcept { return h != INVALID_HANDLE_VALUE && h != nullptr; }
namespace detail {
using sockopt_pointer = char const*;      // Winsock 的 setsockopt 取 char const*
using sockopt_mutable_pointer = char*;
} // namespace detail
} // namespace net

#else

#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>

namespace net {
using native_socket_type = int;
using native_file_type = int;
constexpr native_socket_type invalid_socket = -1;
inline native_file_type invalid_file_value() noexcept { return -1; }
constexpr int shutdown_receive = SHUT_RD;
constexpr int shutdown_send = SHUT_WR;
constexpr int shutdown_both = SHUT_RDWR;
inline bool socket_is_valid(native_socket_type const s) noexcept { return s >= 0; }
inline bool file_is_valid(native_file_type const h) noexcept { return h >= 0; }
namespace detail {
using sockopt_pointer = void const*;
using sockopt_mutable_pointer = void*;
} // namespace detail
} // namespace net

#endif
