#pragma once

// 后端标签：在构造 io_context 时选择事件机制。
//
//   net::io_context ctx;                 // 默认后端（Linux：epoll）
//   net::io_context ctx{net::poll};      // POSIX poll(2)
//   net::io_context ctx{net::select};    // POSIX select(2)，描述符号受 FD_SETSIZE 限制
//   net::io_context ctx{net::io_uring};  // Linux io_uring（完成型；内核 5.5+，运行时可能被禁用）
//
// 就绪型后端（epoll / poll / select）共享一个反应器，只换等待机制；io_uring 是完成型：操作
// 以 SQE 提交、以 CQE 完成。两族都以服务的形式注册在 io_context 里，套接字、定时器等 I/O
// 对象经抽象接口对接，与后端无关。标签只是编译期常量，公共头不包含任何平台头。这对应
// Corosio 的 corosio::epoll / select / kqueue / io_uring / iocp 标签与 io_context(backend)。

namespace net {

enum class backend_kind : unsigned char {
    epoll,
    poll,
    select,
    io_uring,
};

struct epoll_t {
    static constexpr backend_kind kind = backend_kind::epoll;
};

struct poll_t {
    static constexpr backend_kind kind = backend_kind::poll;
};

struct select_t {
    static constexpr backend_kind kind = backend_kind::select;
};

struct io_uring_t {
    static constexpr backend_kind kind = backend_kind::io_uring;
};

constexpr epoll_t epoll{};
constexpr poll_t poll{};
constexpr select_t select{};
constexpr io_uring_t io_uring{};

using default_backend_t = epoll_t;
constexpr default_backend_t default_backend{};

inline char const* to_string(backend_kind const kind) noexcept {
    switch (kind) {
    case backend_kind::epoll: return "epoll";
    case backend_kind::poll: return "poll";
    case backend_kind::select: return "select";
    case backend_kind::io_uring: return "io_uring";
    }
    return "unknown";
}

// 运行时探测：本进程能否使用该后端（io_uring 可能被内核 sysctl / seccomp 禁用）。
bool backend_available(backend_kind kind) noexcept;

} // namespace net
