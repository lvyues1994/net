#pragma once

// 后端标签：在构造 io_context 时选择事件循环的实现。
//
//   net::io_context ctx;               // 默认后端（Linux：epoll）
//   net::io_context ctx{net::poll};    // POSIX poll(2)
//   net::io_context ctx{net::select};  // POSIX select(2)，描述符号受 FD_SETSIZE 限制
//
// 标签只是编译期常量，公共头不包含任何平台头；具体后端（就绪型：epoll / poll / select；
// 将来完成型：io_uring / IOCP）以服务的形式注册在 io_context 里，套接字、定时器等 I/O 对象
// 经抽象接口对接，与后端无关。这对应 Corosio 的 corosio::epoll / select / kqueue /
// io_uring / iocp 标签与 io_context(backend) 构造函数。

namespace net {

enum class backend_kind : unsigned char {
    epoll,
    poll,
    select,
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

constexpr epoll_t epoll{};
constexpr poll_t poll{};
constexpr select_t select{};

using default_backend_t = epoll_t;
constexpr default_backend_t default_backend{};

inline char const* to_string(backend_kind const kind) noexcept {
    switch (kind) {
    case backend_kind::epoll: return "epoll";
    case backend_kind::poll: return "poll";
    case backend_kind::select: return "select";
    }
    return "unknown";
}

} // namespace net
