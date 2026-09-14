#pragma once

#define NET_VERSION_MAJOR 0
#define NET_VERSION_MINOR 1
#define NET_VERSION_PATCH 0
#define NET_VERSION_STRING "0.1.0"

#if __cplusplus < 201402L
#error "net requires C++14 or newer"
#endif

// 平台：Linux（epoll / poll / select / io_uring）或 Windows（IOCP）。
#if defined(_WIN32)
#define NET_PLATFORM_WINDOWS 1
#define NET_PLATFORM_LINUX 0
#elif defined(__linux__)
#define NET_PLATFORM_WINDOWS 0
#define NET_PLATFORM_LINUX 1
#else
#error "net's platform layer targets Linux (epoll / poll / select / io_uring) and Windows (IOCP)"
#endif

// 单次 scatter/gather 操作最多展开的缓冲区数量（POSIX readv/writev 的 iovec 上限的
// 保守子集，与 Asio 在 Windows 上的取值一致）。
#ifndef NET_MAX_IOVEC
#define NET_MAX_IOVEC 16
#endif

// 内联完成预算：执行循环每恢复一个协程，允许其中同步完成（不经调度器）的传输操作次数。用完之后
// 下一次本可同步完成的操作改为把续体 post 给执行器，同一上下文的其它协程得以运行——一条永远就绪的
// 连接（或内存流、就绪型后端上的文件）不能把线程独占。0 关闭预算。
#ifndef NET_INLINE_COMPLETION_BUDGET
#define NET_INLINE_COMPLETION_BUDGET 64
#endif
