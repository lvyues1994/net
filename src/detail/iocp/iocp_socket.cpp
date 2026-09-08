#include "detail/iocp/iocp_socket.hpp"

#include <atomic>
#include <cstring>

#include "co2/contract.hpp"

#include "net/error.hpp"
#include "net/io_context.hpp"

#include "detail/iocp/iocp_backend.hpp"

namespace net {
namespace detail {

namespace {

std::error_code wsa_error() noexcept { return iocp_error(static_cast<DWORD>(::WSAGetLastError())); }

template <class Buffer> DWORD fill_wsabuf(WSABUF (&out)[max_iovec], span<Buffer const> const buffers) noexcept {
    auto count = DWORD{};
    for (auto const& b : buffers) {
        if (count == max_iovec) break;
        out[count].buf = const_cast<CHAR*>(static_cast<CHAR const*>(static_cast<void const*>(b.data())));
        out[count].len = b.size() > 0xFFFFFFFFULL ? 0xFFFFFFFFUL : static_cast<ULONG>(b.size());
        ++count;
    }
    return count;
}

// ConnectEx 不在导入库里，只能经 WSAIoctl 取函数指针（进程内取一次）。
LPFN_CONNECTEX load_connect_ex(native_socket_type const s) noexcept {
    static std::atomic<LPFN_CONNECTEX> cached{nullptr};
    auto fn = cached.load(std::memory_order_acquire);
    if (fn != nullptr) return fn;
    GUID guid = WSAID_CONNECTEX;
    DWORD bytes = 0;
    if (::WSAIoctl(s, SIO_GET_EXTENSION_FUNCTION_POINTER, &guid, sizeof(guid), &fn, sizeof(fn), &bytes, nullptr, nullptr) != 0)
        return nullptr;
    cached.store(fn, std::memory_order_release);
    return fn;
}

// ConnectEx 要求套接字已绑定：没绑定就绑到本族的通配地址。
std::error_code ensure_bound(native_socket_type const s, int const family) noexcept {
    sockaddr_storage current{};
    auto length = static_cast<int>(sizeof(current));
    if (::getsockname(s, reinterpret_cast<sockaddr*>(&current), &length) == 0) return {};
    if (::WSAGetLastError() != WSAEINVAL) return wsa_error();
    if (family == AF_INET6) {
        sockaddr_in6 any{};
        any.sin6_family = AF_INET6;
        if (::bind(s, reinterpret_cast<sockaddr*>(&any), sizeof(any)) != 0) return wsa_error();
        return {};
    }
    sockaddr_in any{};
    any.sin_family = AF_INET;
    any.sin_addr.s_addr = htonl(INADDR_ANY);
    if (::bind(s, reinterpret_cast<sockaddr*>(&any), sizeof(any)) != 0) return wsa_error();
    return {};
}

} // namespace

void cancel_iocp_socket_op::operator()() const noexcept {
    op->cancelled.store(true, std::memory_order_release);
    auto const s = op->owner_->native_handle();
    if (socket_is_valid(s)) ::CancelIoEx(reinterpret_cast<HANDLE>(s), op->overlapped());
}

// ---- 操作 ----

void iocp_socket_op::on_complete(DWORD const error, DWORD const bytes) noexcept {
    using k = kind;
    if (error != 0) {
        ec = cancelled.load(std::memory_order_acquire) ? make_error_code(error::operation_aborted)
                                                       : iocp_error(error, op_kind == k::accept);
        bytes_transferred = 0U;
        if (op_kind == k::accept && socket_is_valid(accept_socket)) {
            ::closesocket(accept_socket);
            accept_socket = invalid_socket;
        }
        return;
    }
    switch (op_kind) {
    case k::read:
        ec = bytes == 0 ? make_error_code(error::eof) : std::error_code{};
        bytes_transferred = bytes;
        return;
    case k::receive_from:
        if (address_out != nullptr && address_length_out != nullptr) *address_length_out = static_cast<socklen_t>(from_length);
        ec.clear();
        bytes_transferred = bytes;
        return;
    case k::write:
    case k::send_to:
        ec.clear();
        bytes_transferred = bytes;
        return;
    case k::connect: {
        auto const s = owner_->native_handle();
        if (::setsockopt(s, SOL_SOCKET, SO_UPDATE_CONNECT_CONTEXT, nullptr, 0) != 0) {
            ec = wsa_error();
            return;
        }
        ec.clear();
        bytes_transferred = 0U;
        return;
    }
    case k::accept: {
        auto listener = owner_->native_handle();
        if (::setsockopt(accept_socket, SOL_SOCKET, SO_UPDATE_ACCEPT_CONTEXT, reinterpret_cast<char const*>(&listener),
                         sizeof(listener)) != 0) {
            ec = wsa_error();
            ::closesocket(accept_socket);
            accept_socket = invalid_socket;
            return;
        }
        ec.clear();
        accepted_fd = accept_socket;
        accepted_family = owner_->family();
        accept_socket = invalid_socket;
        return;
    }
    }
}

void iocp_socket_op::complete() noexcept { env->executor.post(cont); }

// ---- 套接字 ----

iocp_socket::iocp_socket(io_context& context, iocp_backend& backend) noexcept
    : context_{&context}, backend_{&backend}, read_op_{*this, op_direction::read}, write_op_{*this, op_direction::write} {}

iocp_socket::~iocp_socket() {
    CO2_CONTRACT_CHECK(not has_pending());
    if (socket_is_valid(fd_)) ::closesocket(fd_);
    if (socket_is_valid(read_op_.accept_socket)) ::closesocket(read_op_.accept_socket);
}

std::error_code iocp_socket::open(int const family, int const type, int const protocol) noexcept {
    if (socket_is_valid(fd_)) return make_error_code(error::already_open);
    ensure_networking_initialized();
    auto const s = ::WSASocketW(family, type, protocol, nullptr, 0, WSA_FLAG_OVERLAPPED);
    if (not socket_is_valid(s)) return wsa_error();
    auto const ec = backend_->associate(reinterpret_cast<HANDLE>(s));
    if (ec) {
        ::closesocket(s);
        return ec;
    }
    fd_ = s;
    family_ = family;
    type_ = type;
    protocol_ = protocol;
    return {};
}

std::error_code iocp_socket::assign(int const family, int const type, int const protocol, native_socket_type const fd) noexcept {
    if (socket_is_valid(fd_)) return make_error_code(error::already_open);
    auto const ec = backend_->associate(reinterpret_cast<HANDLE>(fd));
    if (ec) return ec;
    fd_ = fd;
    family_ = family;
    type_ = type;
    protocol_ = protocol;
    return {};
}

std::error_code iocp_socket::adopt(int const family, int const type, int const protocol, native_socket_type const fd) noexcept {
    return assign(family, type, protocol, fd);
}

std::error_code iocp_socket::listen(int const backlog) noexcept {
    if (not socket_is_valid(fd_)) return make_error_code(error::not_open);
    if (::listen(fd_, backlog) != 0) return wsa_error();
    return {};
}

void iocp_socket::cancel_pending() noexcept {
    if (not socket_is_valid(fd_)) return;
    if (read_op_.pending) read_op_.cancelled.store(true, std::memory_order_release);
    if (write_op_.pending) write_op_.cancelled.store(true, std::memory_order_release);
    if (read_op_.pending || write_op_.pending) ::CancelIoEx(reinterpret_cast<HANDLE>(fd_), nullptr);
}

std::error_code iocp_socket::close() noexcept {
    if (not socket_is_valid(fd_)) return {};
    auto const s = fd_;
    if (read_op_.pending) read_op_.cancelled.store(true, std::memory_order_release);
    if (write_op_.pending) write_op_.cancelled.store(true, std::memory_order_release);
    fd_ = invalid_socket; // 在飞的操作随 closesocket 以取消完成（错误码可能是 995 或 64，统一报 aborted）
    listener_nonblocking_ = false;
    if (::closesocket(s) != 0) return wsa_error();
    return {};
}

void iocp_socket::cancel() noexcept { cancel_pending(); }

native_socket_type iocp_socket::release() noexcept {
    cancel_pending();
    auto const s = fd_;
    fd_ = invalid_socket;
    listener_nonblocking_ = false;
    return s; // 仍挂在端口上（Windows 不允许解除关联）
}

// ---- 记参数 ----

void iocp_socket::begin_read(span<mutable_buffer const> const buffers) noexcept {
    CO2_CONTRACT_CHECK(not read_op_.pending);
    read_op_.op_kind = iocp_socket_op::kind::read;
    read_op_.buffer_count = fill_wsabuf(read_op_.buffers, buffers);
    read_op_.sync_failed = false;
    read_op_.cancelled.store(false, std::memory_order_relaxed);
}

void iocp_socket::begin_write(span<const_buffer const> const buffers) noexcept {
    CO2_CONTRACT_CHECK(not write_op_.pending);
    write_op_.op_kind = iocp_socket_op::kind::write;
    write_op_.buffer_count = fill_wsabuf(write_op_.buffers, buffers);
    write_op_.sync_failed = false;
    write_op_.cancelled.store(false, std::memory_order_relaxed);
}

void iocp_socket::begin_receive_from(span<mutable_buffer const> const buffers, sockaddr* const sender, socklen_t const capacity,
                                     socklen_t* const sender_length) noexcept {
    CO2_CONTRACT_CHECK(not read_op_.pending);
    read_op_.op_kind = iocp_socket_op::kind::receive_from;
    read_op_.buffer_count = fill_wsabuf(read_op_.buffers, buffers);
    read_op_.address_out = sender;
    read_op_.from_length = static_cast<int>(capacity);
    read_op_.address_length_out = sender_length;
    read_op_.sync_failed = false;
    read_op_.cancelled.store(false, std::memory_order_relaxed);
}

void iocp_socket::begin_send_to(span<const_buffer const> const buffers, sockaddr const* const target, socklen_t const length) noexcept {
    CO2_CONTRACT_CHECK(not write_op_.pending);
    write_op_.op_kind = iocp_socket_op::kind::send_to;
    write_op_.buffer_count = fill_wsabuf(write_op_.buffers, buffers);
    std::memcpy(&write_op_.address, target, static_cast<std::size_t>(length));
    write_op_.address_length = static_cast<int>(length);
    write_op_.sync_failed = false;
    write_op_.cancelled.store(false, std::memory_order_relaxed);
}

void iocp_socket::begin_connect(sockaddr const* const address, socklen_t const length, int const family, int const type,
                                int const protocol) noexcept {
    CO2_CONTRACT_CHECK(not write_op_.pending);
    write_op_.op_kind = iocp_socket_op::kind::connect;
    write_op_.sync_failed = false;
    write_op_.cancelled.store(false, std::memory_order_relaxed);
    if (not socket_is_valid(fd_)) {
        auto const ec = open(family, type, protocol);
        if (ec) {
            write_op_.ec = ec;
            write_op_.sync_failed = true;
            return;
        }
    }
    std::memcpy(&write_op_.address, address, static_cast<std::size_t>(length));
    write_op_.address_length = static_cast<int>(length);
    auto const ec = ensure_bound(fd_, family_);
    if (ec) {
        write_op_.ec = ec;
        write_op_.sync_failed = true;
    }
}

void iocp_socket::begin_accept() noexcept {
    CO2_CONTRACT_CHECK(not read_op_.pending);
    read_op_.op_kind = iocp_socket_op::kind::accept;
    read_op_.sync_failed = false;
    read_op_.cancelled.store(false, std::memory_order_relaxed);
    read_op_.accepted_fd = invalid_socket;
    if (not socket_is_valid(fd_)) {
        read_op_.ec = make_error_code(error::not_open);
        read_op_.sync_failed = true;
        return;
    }
    if (not socket_is_valid(read_op_.accept_socket)) {
        read_op_.accept_socket = ::WSASocketW(family_, SOCK_STREAM, protocol_, nullptr, 0, WSA_FLAG_OVERLAPPED);
        if (not socket_is_valid(read_op_.accept_socket)) {
            read_op_.ec = wsa_error();
            read_op_.sync_failed = true;
        }
    }
}

// ---- 三步 ----

bool iocp_socket::ready(op_direction const direction) noexcept {
    auto& op = op_for(direction);
    if (op.sync_failed) return true;
    if (not socket_is_valid(fd_)) {
        op.ec = make_error_code(error::not_open);
        op.bytes_transferred = 0U;
        op.sync_failed = true;
        return true;
    }
    if (op.op_kind == iocp_socket_op::kind::accept) return speculate_accept(op);
    if (op.op_kind != iocp_socket_op::kind::connect && op.buffer_count == 0U) {
        op.ec.clear(); // 空序列：不发起
        op.bytes_transferred = 0U;
        op.sync_failed = true;
        return true;
    }
    return false;
}

bool iocp_socket::speculate_accept(iocp_socket_op& op) noexcept {
    // 就绪型后端的 accept 在 ready() 里用非阻塞 accept4 投机，连接已排队时同步完成；多个协程在一个
    // 接受器上"connect 完就 accept"因此从不重叠。这里对齐：监听套接字设为非阻塞（不影响重叠调用），
    // 同步 accept() 成功就不经过端口。
    if (not listener_nonblocking_) {
        u_long nonblocking = 1;
        if (::ioctlsocket(fd_, static_cast<long>(FIONBIO), &nonblocking) != 0) return false; // 投机不了：走 AcceptEx
        listener_nonblocking_ = true;
    }
    auto const s = ::accept(fd_, nullptr, nullptr); // 继承监听套接字的重叠属性
    if (socket_is_valid(s)) {
        op.accepted_fd = s;
        op.accepted_family = family_;
        op.ec.clear();
        op.sync_failed = true;
        return true;
    }
    auto const error = ::WSAGetLastError();
    if (error == WSAEWOULDBLOCK) return false;
    op.ec = iocp_error(static_cast<DWORD>(error), true);
    op.sync_failed = true;
    return true;
}

bool iocp_socket::issue(iocp_socket_op& op) noexcept {
    using k = iocp_socket_op::kind;
    op.reset_overlapped();
    op.flags = 0;
    auto const handle = fd_;
    int result = 0;
    switch (op.op_kind) {
    case k::read:
        result = ::WSARecv(handle, op.buffers, op.buffer_count, nullptr, &op.flags, op.overlapped(), nullptr);
        break;
    case k::write:
        result = ::WSASend(handle, op.buffers, op.buffer_count, nullptr, 0, op.overlapped(), nullptr);
        break;
    case k::receive_from:
        result = ::WSARecvFrom(handle, op.buffers, op.buffer_count, nullptr, &op.flags, op.address_out, &op.from_length,
                               op.overlapped(), nullptr);
        break;
    case k::send_to:
        result = ::WSASendTo(handle, op.buffers, op.buffer_count, nullptr, 0, reinterpret_cast<sockaddr const*>(&op.address),
                             op.address_length, op.overlapped(), nullptr);
        break;
    case k::connect: {
        auto const connect_ex = load_connect_ex(handle);
        if (connect_ex == nullptr) {
            op.ec = wsa_error();
            return false;
        }
        DWORD sent = 0;
        auto const ok = connect_ex(handle, reinterpret_cast<sockaddr const*>(&op.address), op.address_length, nullptr, 0, &sent,
                                   op.overlapped());
        result = ok ? 0 : SOCKET_ERROR;
        break;
    }
    case k::accept: {
        auto const address_size = static_cast<DWORD>(sizeof(sockaddr_storage) + 16U);
        DWORD received = 0;
        auto const ok = ::AcceptEx(handle, op.accept_socket, op.accept_buffer, 0, address_size, address_size, &received, op.overlapped());
        result = ok ? 0 : SOCKET_ERROR;
        break;
    }
    }
    if (result == 0) return true; // 同步完成：完成包仍会到
    auto const error = ::WSAGetLastError();
    if (error == WSA_IO_PENDING) return true;
    op.ec = iocp_error(static_cast<DWORD>(error), op.op_kind == k::accept);
    op.bytes_transferred = 0U;
    return false;
}

coroutine_handle<> iocp_socket::suspend(op_direction const direction, coroutine_handle<> const h, io_env const* const env) noexcept {
    auto& op = op_for(direction);
    op.cont.h = h;
    op.env = env;
    op.pending = true;
    if (env->stop_token.stop_requested()) {
        op.ec = make_error_code(error::operation_aborted);
        op.bytes_transferred = 0U;
        return h;
    }
    if (env->stop_token.stop_possible()) op.stop_cb.emplace(env->stop_token, cancel_iocp_socket_op{&op});
    context_->get_executor().on_work_started(); // 发布之前
    if (not issue(op)) {                        // 同步失败：没有完成包会来
        context_->get_executor().on_work_finished();
        op.stop_cb.reset();
        return h;
    }
    return noop_coroutine(); // 发布之后不再碰 op / env / this
}

io_result<std::size_t> iocp_socket::finish_transfer(op_direction const direction) noexcept {
    auto& op = op_for(direction);
    op.stop_cb.reset();
    op.pending = false;
    op.sync_failed = false;
    op.env = nullptr;
    return io_result<std::size_t>{op.ec, op.bytes_transferred};
}

io_result<> iocp_socket::finish_connect() noexcept {
    auto& op = write_op_;
    op.stop_cb.reset();
    op.pending = false;
    op.sync_failed = false;
    op.env = nullptr;
    return io_result<>{op.ec};
}

std::error_code iocp_socket::finish_accept(native_socket_type& fd, int& family) noexcept {
    auto& op = read_op_;
    op.stop_cb.reset();
    op.pending = false;
    op.sync_failed = false;
    op.env = nullptr;
    fd = op.accepted_fd;
    family = op.accepted_family;
    op.accepted_fd = invalid_socket;
    return op.ec;
}

} // namespace detail
} // namespace net
