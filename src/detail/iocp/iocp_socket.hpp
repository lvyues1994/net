#pragma once

#include <atomic>
#include <cstddef>

#include "net/continuation.hpp"
#include "net/coroutine.hpp"
#include "net/detail/storage.hpp"
#include "net/io_env.hpp"

#include "detail/backend.hpp"
#include "detail/iocp/iocp_op.hpp"

#include <mswsock.h> // 必须在 winsock2.h（socket_types.hpp）之后

// IOCP 的套接字实现：WSARecv / WSASend / WSARecvFrom / WSASendTo / ConnectEx / AcceptEx，全部重叠，
// 完成经端口到达。ready() 不做投机：发起就是提交（与 Corosio 一样，同步完成的调用也会投完成包，
// 这样 suspend 之后不需要再碰操作对象）。发起立刻失败（不是 WSA_IO_PENDING）时同步以错误完成。

namespace net {
namespace detail {

struct iocp_backend;
struct iocp_socket;
struct iocp_socket_op;

struct cancel_iocp_socket_op {
    iocp_socket_op* op;
    void operator()() const noexcept;
};

struct iocp_socket_op final : iocp_op {
    enum class kind : unsigned char { read, write, receive_from, send_to, connect, accept };

    iocp_socket_op(iocp_socket& owner, op_direction direction) noexcept : owner_{&owner}, direction_{direction} {}

    void on_complete(DWORD error, DWORD bytes) noexcept override;
    void complete() noexcept override;

    iocp_socket* owner_;
    op_direction direction_;
    kind op_kind = kind::read;

    WSABUF buffers[max_iovec];
    DWORD buffer_count = 0;
    DWORD flags = 0;

    sockaddr_storage address{};        // send_to 的目标 / connect 的对端
    int address_length = 0;
    sockaddr* address_out = nullptr;   // receive_from：调用方的地址存储
    int from_length = 0;               // receive_from：入为容量、出为实际长度（WSARecvFrom 要求它存活到完成）
    socklen_t* address_length_out = nullptr;

    native_socket_type accept_socket = invalid_socket; // accept：预先创建的对端套接字
    char accept_buffer[2 * (sizeof(sockaddr_storage) + 16)] = {};
    native_socket_type accepted_fd = invalid_socket;
    int accepted_family = 0;

    std::error_code ec;          // 同步失败（发起前）或完成结果
    std::size_t bytes_transferred = 0;
    bool sync_failed = false;    // begin_* / 发起 / 投机时已经有结果：ready() 直接为真
    // 本地取消（cancel / close / release / stop_token）：完成包带的错误码不一定是 995——closesocket 常给
    // ERROR_NETNAME_DELETED（与对端 RST 同码），所以以这个标记为准统一报 operation_aborted。
    std::atomic<bool> cancelled{false};
    continuation cont;
    io_env const* env = nullptr;
    bool pending = false;
    late_init<stop_callback<cancel_iocp_socket_op>> stop_cb;
};

struct iocp_socket final : socket_impl {
    iocp_socket(io_context& context, iocp_backend& backend) noexcept;
    ~iocp_socket() override;

    io_context& context() const noexcept override { return *context_; }
    iocp_backend& backend() noexcept { return *backend_; }

    std::error_code open(int family, int type, int protocol) noexcept override;
    std::error_code assign(int family, int type, int protocol, native_socket_type fd) noexcept override;
    std::error_code adopt(int family, int type, int protocol, native_socket_type fd) noexcept override;
    std::error_code listen(int backlog) noexcept override;
    std::error_code close() noexcept override;
    void cancel() noexcept override;
    native_socket_type release() noexcept override;
    native_socket_type native_handle() const noexcept override { return fd_; }

    void begin_read(span<mutable_buffer const> buffers) noexcept override;
    void begin_write(span<const_buffer const> buffers) noexcept override;
    void begin_receive_from(span<mutable_buffer const> buffers, sockaddr* sender, socklen_t capacity,
                            socklen_t* sender_length) noexcept override;
    void begin_send_to(span<const_buffer const> buffers, sockaddr const* target, socklen_t length) noexcept override;
    void begin_connect(sockaddr const* address, socklen_t length, int family, int type, int protocol) noexcept override;
    void begin_accept() noexcept override;

    bool ready(op_direction direction) noexcept override;
    coroutine_handle<> suspend(op_direction direction, coroutine_handle<> h, io_env const* env) noexcept override;
    io_result<std::size_t> finish_transfer(op_direction direction) noexcept override;
    io_result<> finish_connect() noexcept override;
    std::error_code finish_accept(native_socket_type& fd, int& family) noexcept override;

    bool has_pending() const noexcept override { return read_op_.pending || write_op_.pending; }

    int family() const noexcept { return family_; }
    int type() const noexcept { return type_; }

  private:
    iocp_socket_op& op_for(op_direction const direction) noexcept { return direction == op_direction::read ? read_op_ : write_op_; }
    // 发起系统调用；返回 true 表示已发布（完成包会到），false 表示同步失败（op.ec 已设）。
    bool issue(iocp_socket_op& op) noexcept;
    void cancel_pending() noexcept;
    // accept 的投机：监听套接字非阻塞后同步 accept()，队列里已有连接就不必经过端口。
    bool speculate_accept(iocp_socket_op& op) noexcept;

    io_context* context_;
    iocp_backend* backend_;
    native_socket_type fd_ = invalid_socket;
    int family_ = 0;
    int type_ = 0;
    int protocol_ = 0;
    bool listener_nonblocking_ = false;
    iocp_socket_op read_op_;
    iocp_socket_op write_op_;
};

} // namespace detail
} // namespace net
