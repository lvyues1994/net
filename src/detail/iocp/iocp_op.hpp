#pragma once

#include <cstddef>
#include <system_error>

#include "net/detail/socket_types.hpp"

// IOCP 后端的操作对象。每个 I/O 对象为每个方向保存一个，地址稳定；OVERLAPPED 住在标准布局的
// overlapped_block 首位，事件循环从 GetQueuedCompletionStatus 拿到的 LPOVERLAPPED 反查出操作。
//
// 生命周期：发起（WSARecv / WSASend / AcceptEx / ConnectEx / ReadFile / WriteFile 返回成功或
// ERROR_IO_PENDING）之后一定会有恰好一个完成包到达端口——包括 closesocket / CancelIoEx 之后
//（ERROR_OPERATION_ABORTED）。所以发起后不能再碰操作对象，直到完成包被处理。

namespace net {
namespace detail {

struct iocp_op;

struct overlapped_block {
    OVERLAPPED overlapped;
    iocp_op* op;
};

struct iocp_op {
    iocp_op() noexcept : block{OVERLAPPED{}, this} {}
    iocp_op(iocp_op const&) = delete;
    iocp_op& operator=(iocp_op const&) = delete;
    virtual ~iocp_op() = default;

    // 事件循环线程：完成包到达。error 是 Win32 / Winsock 错误码（0 成功），bytes 是传输字节数。
    virtual void on_complete(DWORD error, DWORD bytes) noexcept = 0;
    // 事件循环线程，on_complete 之后：把续体交给执行器（常驻操作在这里重新武装）。
    virtual void complete() noexcept = 0;

    static iocp_op* from_overlapped(OVERLAPPED* const overlapped) noexcept {
        return reinterpret_cast<overlapped_block*>(overlapped)->op;
    }

    OVERLAPPED* overlapped() noexcept { return &block.overlapped; }

    void reset_overlapped() noexcept { block.overlapped = OVERLAPPED{}; }

    overlapped_block block;
    bool counts_as_work = true;
};

// Win32 / Winsock 错误码 → error_code。众所周知的连接状况用 generic_category 的 std::errc 表达：
// MSVC 的 system_category 把 WSA 段（10054…）映射到 std::errc 但不映射 Win32 的 12xx 段，libstdc++
//（MinGW）正相反；而 ConnectEx 的失败以 NTSTATUS 转出的 Win32 码到达（ERROR_CONNECTION_REFUSED 1225），
// WSARecv 的以 WSA 码到达（WSAECONNRESET 10054）。与 POSIX 后端从 errno 得到的结果比较一致。
std::error_code iocp_error(DWORD error, bool accept_path = false) noexcept;

} // namespace detail
} // namespace net
