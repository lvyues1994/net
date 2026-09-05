#pragma once

#include <cstddef>
#include <memory>
#include <system_error>

#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>

#include "net/buffers.hpp"
#include "net/coroutine.hpp"
#include "net/io_env.hpp"
#include "net/io_result.hpp"
#include "net/span.hpp"

// 套接字公共部分：同步的打开/关闭/绑定/选项，以及 I/O 操作的 awaiter。
//
// 实现由 io_context 的后端创建（detail::socket_impl，抽象）：就绪型后端（epoll / poll /
// select）在描述符就绪时执行系统调用，完成型后端（io_uring / IOCP）提交操作等待完成——
// 本头与它们无关，这一层是 Corosio 所说的"具体层：协议特定的完整 API，虚函数派发，可单独
// 编译"。
//
// 操作状态住在套接字实现里（每个方向一个），awaiter 只是一个指针——总能内联进协程帧的
// awaiter 槽，每次操作零分配。因此同一套接字同一方向同一时刻只能有一个未完成的操作
// （流的常规约束），且**有未完成操作时不能销毁套接字**（契约违规）；可以 close() /
// cancel()，操作以 error::operation_aborted 完成后协程照常恢复。移动套接字不影响未完成的
// 操作（实现对象地址不变）。
//
// I/O 对象持有它的 io_context（不是执行器）；协程在哪个执行器上恢复由调用方的 io_env 决定。

namespace net {

struct io_context;

namespace detail {
struct socket_impl;
}

enum class shutdown_type : int { receive = SHUT_RD, send = SHUT_WR, both = SHUT_RDWR };

// ---- 套接字选项（Networking TS 形态：level / name / data / size） ----

namespace socket_option {

template <int Level, int Name> struct boolean {
    boolean() noexcept = default;
    explicit boolean(bool const v) noexcept : value_{v ? 1 : 0} {}

    bool value() const noexcept { return value_ != 0; }
    explicit operator bool() const noexcept { return value(); }

    int level() const noexcept { return Level; }
    int name() const noexcept { return Name; }
    int* data() noexcept { return &value_; }
    int const* data() const noexcept { return &value_; }
    std::size_t size() const noexcept { return sizeof(value_); }
    void resize(std::size_t) noexcept {}

  private:
    int value_ = 0;
};

template <int Level, int Name> struct integer {
    integer() noexcept = default;
    explicit integer(int const v) noexcept : value_{v} {}

    int value() const noexcept { return value_; }

    int level() const noexcept { return Level; }
    int name() const noexcept { return Name; }
    int* data() noexcept { return &value_; }
    int const* data() const noexcept { return &value_; }
    std::size_t size() const noexcept { return sizeof(value_); }
    void resize(std::size_t) noexcept {}

  private:
    int value_ = 0;
};

using reuse_address = boolean<SOL_SOCKET, SO_REUSEADDR>;
using keep_alive = boolean<SOL_SOCKET, SO_KEEPALIVE>;
using broadcast = boolean<SOL_SOCKET, SO_BROADCAST>;
using receive_buffer_size = integer<SOL_SOCKET, SO_RCVBUF>;
using send_buffer_size = integer<SOL_SOCKET, SO_SNDBUF>;
using no_delay = boolean<IPPROTO_TCP, TCP_NODELAY>;
using v6_only = boolean<IPPROTO_IPV6, IPV6_V6ONLY>;

} // namespace socket_option

// ---- awaiter：一个指针宽，方法定义在库内 ----

struct socket_read_awaitable {
    detail::socket_impl* impl;

    bool await_ready() noexcept;
    coroutine_handle<> await_suspend(coroutine_handle<> h, io_env const* env) noexcept;
    io_result<std::size_t> await_resume() noexcept;
};

struct socket_write_awaitable {
    detail::socket_impl* impl;

    bool await_ready() noexcept;
    coroutine_handle<> await_suspend(coroutine_handle<> h, io_env const* env) noexcept;
    io_result<std::size_t> await_resume() noexcept;
};

struct socket_connect_awaitable {
    detail::socket_impl* impl;

    bool await_ready() noexcept;
    coroutine_handle<> await_suspend(coroutine_handle<> h, io_env const* env) noexcept;
    io_result<> await_resume() noexcept;
};

// ---- 基类 ----

struct socket_base {
    using native_handle_type = int;

    socket_base() noexcept;
    explicit socket_base(io_context& context);
    socket_base(socket_base&& other) noexcept;
    socket_base& operator=(socket_base&& other) noexcept;
    socket_base(socket_base const&) = delete;
    socket_base& operator=(socket_base const&) = delete;
    ~socket_base();

    // 前置条件：对象绑定了 io_context（不是默认构造的）。
    io_context& context() const noexcept;
    bool has_context() const noexcept { return impl_ != nullptr; }

    bool is_open() const noexcept;
    // 取消未完成的操作（以 operation_aborted 完成）并关闭描述符。
    std::error_code close() noexcept;
    // 取消未完成的操作，描述符保持打开。
    void cancel() noexcept;
    native_handle_type native_handle() const noexcept;
    // 从反应器注销并交出描述符的所有权；未完成的操作被取消。
    native_handle_type release() noexcept;

    template <class Option> std::error_code set_option(Option const& option) noexcept {
        return set_option_raw(option.level(), option.name(), option.data(), option.size());
    }

    template <class Option> std::error_code get_option(Option& option) const noexcept {
        auto length = static_cast<socklen_t>(option.size());
        auto const ec = get_option_raw(option.level(), option.name(), option.data(), &length);
        if (not ec) option.resize(length);
        return ec;
    }

    // 可无阻塞读取的字节数。
    std::size_t available(std::error_code& ec) const noexcept;

  protected:
    std::error_code open_raw(int family, int type, int protocol) noexcept;
    std::error_code assign_raw(int family, int type, int protocol, native_handle_type fd) noexcept;
    std::error_code bind_raw(sockaddr const* address, socklen_t length) noexcept;
    std::error_code listen_raw(int backlog) noexcept;
    std::error_code shutdown_raw(int how) noexcept;
    std::error_code local_endpoint_raw(sockaddr* address, socklen_t* length) const noexcept;
    std::error_code remote_endpoint_raw(sockaddr* address, socklen_t* length) const noexcept;
    std::error_code set_option_raw(int level, int name, void const* data, std::size_t size) noexcept;
    std::error_code get_option_raw(int level, int name, void* data, socklen_t* size) const noexcept;
    // 同步 connect（UDP）。
    std::error_code connect_raw(sockaddr const* address, socklen_t length) noexcept;

    socket_read_awaitable start_read(span<mutable_buffer const> buffers) noexcept;
    socket_write_awaitable start_write(span<const_buffer const> buffers) noexcept;
    // 未打开时先按给定协议打开。
    socket_connect_awaitable start_connect(sockaddr const* address, socklen_t length, int family,
                                           int type, int protocol) noexcept;
    // sender 指向至少 capacity 字节的地址存储，由内核回填。
    socket_read_awaitable start_receive_from(span<mutable_buffer const> buffers, sockaddr* sender,
                                             socklen_t capacity) noexcept;
    socket_write_awaitable start_send_to(span<const_buffer const> buffers, sockaddr const* target,
                                         socklen_t length) noexcept;

    detail::socket_impl* impl() const noexcept { return impl_.get(); }

  private:
    std::unique_ptr<detail::socket_impl> impl_;
};

} // namespace net
