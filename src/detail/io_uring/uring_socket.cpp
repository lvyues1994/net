#include "detail/io_uring/uring_socket.hpp"

#include <cerrno>
#include <cstring>
#include <vector>

#include <sys/socket.h>
#include <unistd.h>

#include "co2/contract.hpp"

#include "net/error.hpp"
#include "net/io_context.hpp"

#include "detail/io_uring/uring_backend.hpp"
#include "detail/posix/socket_ops.hpp"

namespace net {
namespace detail {

#ifndef IORING_ACCEPT_MULTISHOT
#define IORING_ACCEPT_MULTISHOT (1U << 0)
#endif
#ifndef IORING_CQE_F_MORE
#define IORING_CQE_F_MORE (1U << 1)
#endif

namespace {

// 描述符是否处于监听状态（assign 已监听的 fd 时据此武装多发 accept）。查询失败按监听处理。
bool fd_is_listening(int const fd) noexcept {
    auto accepting = 0;
    socklen_t length = sizeof(accepting);
    if (::getsockopt(fd, SOL_SOCKET, SO_ACCEPTCONN, &accepting, &length) != 0) return true;
    return accepting != 0;
}

std::error_code error_from_result(int const res) noexcept {
    if (res == -ECANCELED) return make_error_code(error::operation_aborted);
    return std::error_code{-res, std::system_category()};
}

template <class Buffer, std::size_t N>
std::size_t fill_vectors(iovec (&vectors)[N], buffer_array<Buffer, N> const& buffers) noexcept {
    auto count = std::size_t{};
    for (auto const& b : buffers) {
        vectors[count].iov_base = const_cast<void*>(static_cast<void const*>(b.data()));
        vectors[count].iov_len = b.size();
        ++count;
    }
    return count;
}

} // namespace

// ---- uring_socket_op ----

void uring_socket_op::prepare(io_uring_sqe& sqe) noexcept {
    sqe.fd = owner->native_handle();
    switch (op_kind) {
    case kind::read:
    case kind::receive_from: {
        auto const count = fill_vectors(vectors, read_buffers);
        message = msghdr{};
        message.msg_iov = vectors;
        message.msg_iovlen = count;
        if (op_kind == kind::receive_from) {
            message.msg_name = address_out;
            message.msg_namelen = address_length;
        }
        sqe.opcode = IORING_OP_RECVMSG;
        sqe.addr = reinterpret_cast<std::uintptr_t>(&message);
        sqe.len = 1U;
        sqe.msg_flags = 0U;
        return;
    }
    case kind::write:
    case kind::send_to: {
        auto const count = fill_vectors(vectors, write_buffers);
        message = msghdr{};
        message.msg_iov = vectors;
        message.msg_iovlen = count;
        if (op_kind == kind::send_to) {
            message.msg_name = &address;
            message.msg_namelen = address_length;
        }
        sqe.opcode = IORING_OP_SENDMSG;
        sqe.addr = reinterpret_cast<std::uintptr_t>(&message);
        sqe.len = 1U;
        sqe.msg_flags = MSG_NOSIGNAL;
        return;
    }
    case kind::connect:
        sqe.opcode = IORING_OP_CONNECT;
        sqe.addr = reinterpret_cast<std::uintptr_t>(&address);
        sqe.off = address_length;
        return;
    case kind::accept:
        address_length = static_cast<socklen_t>(sizeof(address));
        sqe.opcode = IORING_OP_ACCEPT;
        sqe.addr = reinterpret_cast<std::uintptr_t>(&address);
        sqe.addr2 = reinterpret_cast<std::uintptr_t>(&address_length);
        sqe.accept_flags = SOCK_NONBLOCK | SOCK_CLOEXEC;
        return;
    case kind::none:
        sqe.opcode = IORING_OP_NOP;
        return;
    }
}

void uring_socket_op::on_complete(int const res, unsigned) noexcept {
    // 一次成功的异步完成证明该方向就绪：下一个操作可以再投机。
    if (res >= 0) {
        if (direction == op_direction::read)
            owner->speculation().on_async_read_ready();
        else
            owner->speculation().on_async_write_ready();
    }
    if (res < 0) {
        ec = error_from_result(res);
        bytes_transferred = 0U;
        accepted_fd = -1;
        return;
    }
    switch (op_kind) {
    case kind::read:
        ec = res == 0 ? make_error_code(error::eof) : std::error_code{};
        bytes_transferred = static_cast<std::size_t>(res);
        return;
    case kind::write:
    case kind::receive_from:
    case kind::send_to:
        ec.clear();
        bytes_transferred = static_cast<std::size_t>(res);
        return;
    case kind::connect:
        ec.clear();
        bytes_transferred = 0U;
        return;
    case kind::accept:
        ec.clear();
        accepted_fd = res;
        accepted_family = address.ss_family;
        return;
    case kind::none:
        ec = make_error_code(error::not_open);
        return;
    }
}

void uring_socket_op::complete() noexcept { env->executor.post(cont); }

void cancel_uring_socket_op::operator()() const noexcept { impl->cancel_op(impl->op_for(direction)); }

// ---- uring_multishot_accept_op ----

void uring_multishot_accept_op::prepare(io_uring_sqe& sqe) noexcept {
    sqe.opcode = IORING_OP_ACCEPT;
    sqe.fd = listen_fd;
    sqe.ioprio = IORING_ACCEPT_MULTISHOT;
    // 不要对端地址：多发下所有完成共用一块地址暂存，会互相覆盖；对端地址由 remote_endpoint()
    // 在接受后的套接字上取。
    sqe.addr = 0;
    sqe.addr2 = 0;
    sqe.accept_flags = SOCK_NONBLOCK | SOCK_CLOEXEC;
}

void uring_multishot_accept_op::on_complete(int const res, unsigned const flags) noexcept {
    if (owner != nullptr) {
        owner->on_multishot_accept(res, flags);
        return;
    }
    // 已退役：这个连接属于调用方已经拿走的监听套接字，没有人会接收它。
    if (res >= 0) ::close(res);
}

bool uring_multishot_accept_op::rearm() noexcept { return owner != nullptr && owner->multishot_wants_rearm(); }

// ---- uring_socket ----

uring_socket::uring_socket(io_context& context, uring_backend& backend) noexcept
    : context_{&context}, backend_{&backend}, read_op_{*this, op_direction::read},
      write_op_{*this, op_direction::write} {}

uring_socket::~uring_socket() {
    CO2_CONTRACT_CHECK(not has_pending());
    close();
}

std::error_code uring_socket::open(int const family, int const type, int const protocol) noexcept {
    if (fd_ >= 0) return make_error_code(error::already_open);
    auto const created = posix::create_socket(family, type, protocol);
    if (created < 0) return posix::last_error();
    auto const ec = assign(family, type, protocol, created);
    if (ec) posix::close_socket(created);
    return ec;
}

std::error_code uring_socket::assign(int const family, int const type, int, int const fd) noexcept {
    if (fd_ >= 0) return make_error_code(error::already_open);
    auto const ec = posix::set_nonblocking_cloexec(fd);
    if (ec) return ec;
    fd_ = fd;
    family_ = family;
    // 接管一个已在监听的描述符：像 listen() 一样武装多发 accept。
    if (type == SOCK_STREAM && fd_is_listening(fd)) arm_multishot_accept();
    return {};
}

std::error_code uring_socket::listen(int const backlog) noexcept {
    if (::listen(fd_, backlog) != 0) return posix::last_error();
    arm_multishot_accept(); // 再次 listen 只改 backlog：已武装则不重复
    return {};
}

void uring_socket::arm_multishot_accept() noexcept {
    if (not multishot_accept_supported()) return;
    if (not acceptor_) acceptor_.reset(new acceptor_state{});
    if (acceptor_->op) return;
    acceptor_->op.reset(new uring_multishot_accept_op{*this, fd_});
    if (not backend_->submit(*acceptor_->op)) acceptor_->op.reset(); // 上下文已 shutdown
}

void uring_socket::retire_multishot_accept() noexcept {
    if (not acceptor_ || not acceptor_->op) return;
    {
        // owner 由环锁保护：事件循环线程在锁内读它。
        std::lock_guard<std::mutex> lock{backend_->mutex()};
        acceptor_->op->owner = nullptr;
    }
    backend_->retire(std::move(acceptor_->op));
    acceptor_->broken = false;
}

void uring_socket::close_parked_fds() noexcept {
    if (not acceptor_) return;
    std::vector<int> stale;
    auto head = std::size_t{};
    {
        std::lock_guard<std::mutex> lock{acceptor_->mutex};
        stale.swap(acceptor_->parked);
        head = acceptor_->head;
        acceptor_->head = 0;
    }
    for (auto i = head; i < stale.size(); ++i)
        ::close(stale[i]);
}

void uring_socket::cancel_op(uring_socket_op& op) noexcept {
    if (not op.pending) return;
    if (&op == &read_op_ && op.op_kind == uring_socket_op::kind::accept && multishot_active()) {
        // 多发模式的 accept 是停着的 waiter，不在环里：在这里完成它。
        auto deliver = false;
        {
            std::lock_guard<std::mutex> lock{acceptor_->mutex};
            if (acceptor_->waiting) {
                acceptor_->waiting = false;
                deliver = true;
            }
        }
        if (deliver) {
            op.ec = make_error_code(error::operation_aborted);
            op.accepted_fd = -1;
            op.complete();
            context_->get_executor().on_work_finished();
        }
        return;
    }
    backend_->cancel(op);
}

std::error_code uring_socket::close() noexcept {
    if (fd_ < 0) return {};
    // 在飞的请求持有文件引用，关闭描述符不会结束它们：先请求取消，CQE 以 -ECANCELED 到达。
    cancel_op(read_op_);
    cancel_op(write_op_);
    retire_multishot_accept();
    close_parked_fds();
    auto const closing = fd_;
    fd_ = -1;
    return posix::close_socket(closing);
}

void uring_socket::cancel() noexcept {
    if (fd_ < 0) return;
    cancel_op(read_op_);
    cancel_op(write_op_);
}

int uring_socket::release() noexcept {
    if (fd_ < 0) return -1;
    cancel_op(read_op_);
    cancel_op(write_op_);
    retire_multishot_accept();
    close_parked_fds(); // 停着的连接属于被拿走的监听套接字
    auto const released = fd_;
    fd_ = -1;
    return released;
}

// ---- 多发 accept 的完成（环锁内） ----

void uring_socket::on_multishot_accept(int const res, unsigned const flags) noexcept {
    auto const more = (flags & IORING_CQE_F_MORE) != 0U;
    auto deliver = false;
    auto resubmit_oneshot = false;
    auto& state = *acceptor_;
    {
        std::lock_guard<std::mutex> lock{state.mutex};
        if (res >= 0) {
            if (state.waiting) {
                state.waiting = false;
                read_op_.accepted_fd = res;
                read_op_.accepted_family = family_;
                read_op_.ec.clear();
                deliver = true;
            } else {
                state.parked.push_back(res);
            }
        } else if (not more && (res == -EINVAL || res == -EOPNOTSUPP)) {
            // 内核不接受多发（uname 判断失误）：退回每次 accept 一个 SQE；停着的 waiter 改为一次性提交。
            state.broken = true;
            if (state.waiting) {
                state.waiting = false;
                resubmit_oneshot = true;
            }
        } else if (not more && res != -ECANCELED && state.waiting) {
            // 终止错误（如 EMFILE）交给等待者；之后 rearm() 重新武装。
            state.waiting = false;
            read_op_.accepted_fd = -1;
            read_op_.ec = error_from_result(res);
            deliver = true;
        }
    }
    if (deliver) {
        read_op_.complete();
        context_->get_executor().on_work_finished();
    }
    if (resubmit_oneshot) {
        // waiter 停着时已计过一份工作；submit_locked 会再计一份，这里抵掉。
        backend_->submit_locked(read_op_);
        context_->get_executor().on_work_finished();
    }
}

// ---- begin_* ----

void uring_socket::begin_read(span<mutable_buffer const> const buffers) noexcept {
    CO2_CONTRACT_CHECK(not read_op_.pending);
    read_op_.op_kind = uring_socket_op::kind::read;
    read_op_.read_buffers = mutable_buffer_array<>{buffers};
}

void uring_socket::begin_write(span<const_buffer const> const buffers) noexcept {
    CO2_CONTRACT_CHECK(not write_op_.pending);
    write_op_.op_kind = uring_socket_op::kind::write;
    write_op_.write_buffers = const_buffer_array<>{buffers};
}

void uring_socket::begin_receive_from(span<mutable_buffer const> const buffers, sockaddr* const sender,
                                      socklen_t const capacity) noexcept {
    CO2_CONTRACT_CHECK(not read_op_.pending);
    read_op_.op_kind = uring_socket_op::kind::receive_from;
    read_op_.read_buffers = mutable_buffer_array<>{buffers};
    read_op_.address_out = sender;
    read_op_.address_length = capacity;
}

void uring_socket::begin_send_to(span<const_buffer const> const buffers, sockaddr const* const target,
                                 socklen_t const length) noexcept {
    CO2_CONTRACT_CHECK(not write_op_.pending);
    write_op_.op_kind = uring_socket_op::kind::send_to;
    write_op_.write_buffers = const_buffer_array<>{buffers};
    std::memcpy(&write_op_.address, target, length);
    write_op_.address_length = length;
}

void uring_socket::begin_connect(sockaddr const* const address, socklen_t const length, int const family,
                                 int const type, int const protocol) noexcept {
    CO2_CONTRACT_CHECK(not write_op_.pending);
    auto& op = write_op_;
    op.op_kind = uring_socket_op::kind::connect;
    op.bytes_transferred = 0U;
    op.immediate = false;
    if (fd_ < 0) {
        op.ec = open(family, type, protocol);
        if (op.ec) {
            op.immediate = true; // 同步失败
            return;
        }
    }
    // IORING_OP_CONNECT 自己处理非阻塞套接字的 EINPROGRESS：整个连接交给内核。
    std::memcpy(&op.address, address, length);
    op.address_length = length;
}

void uring_socket::begin_accept() noexcept {
    CO2_CONTRACT_CHECK(not read_op_.pending);
    read_op_.op_kind = uring_socket_op::kind::accept;
    read_op_.accepted_fd = -1;
}

// ---- awaiter 三步 ----

bool uring_socket::ready(op_direction const direction) noexcept {
    auto& op = op_for(direction);
    if (op.op_kind == uring_socket_op::kind::connect) return op.immediate;
    if (fd_ < 0) {
        op.ec = make_error_code(error::not_open);
        op.bytes_transferred = 0U;
        return true;
    }
    auto& spec = speculation_;
    switch (op.op_kind) {
    case uring_socket_op::kind::read: {
        if (op.read_buffers.total_size() == 0U) break;
        if (not spec.may_read()) return false;
        auto const outcome = posix::readv(fd_, op.read_buffers);
        if (not outcome.done) {
            spec.on_read_exhausted();
            return false;
        }
        spec.on_read_success();
        op.ec = outcome.ec;
        op.bytes_transferred = outcome.bytes;
        return true;
    }
    case uring_socket_op::kind::receive_from: {
        if (op.read_buffers.total_size() == 0U) break;
        if (not spec.may_read()) return false;
        auto const outcome = posix::recvmsg(fd_, op.read_buffers, op.address_out, op.address_length);
        if (not outcome.done) {
            spec.on_read_exhausted();
            return false;
        }
        spec.on_read_success();
        op.ec = outcome.ec;
        op.bytes_transferred = outcome.bytes;
        return true;
    }
    case uring_socket_op::kind::write: {
        if (op.write_buffers.total_size() == 0U) break;
        if (not spec.may_write()) return false;
        auto const outcome = posix::writev(fd_, op.write_buffers);
        if (not outcome.done) {
            spec.on_write_exhausted();
            return false;
        }
        op.ec = outcome.ec;
        op.bytes_transferred = outcome.bytes;
        return true;
    }
    case uring_socket_op::kind::send_to: {
        if (op.write_buffers.total_size() == 0U) break;
        if (not spec.may_write()) return false;
        auto const outcome = posix::sendmsg(fd_, op.write_buffers, reinterpret_cast<sockaddr const*>(&op.address),
                                            op.address_length);
        if (not outcome.done) {
            spec.on_write_exhausted();
            return false;
        }
        op.ec = outcome.ec;
        op.bytes_transferred = outcome.bytes;
        return true;
    }
    case uring_socket_op::kind::accept: {
        if (multishot_active()) {
            std::lock_guard<std::mutex> lock{acceptor_->mutex};
            if (acceptor_->has_parked()) {
                op.accepted_fd = acceptor_->pop_parked();
                op.accepted_family = family_;
                op.ec.clear();
                return true;
            }
        }
        if (not spec.may_read()) return false;
        auto const outcome = posix::accept(fd_);
        if (not outcome.done) {
            spec.on_read_exhausted();
            return false;
        }
        spec.on_read_success();
        op.ec = outcome.ec;
        op.accepted_fd = outcome.fd;
        op.accepted_family = outcome.family;
        return true;
    }
    case uring_socket_op::kind::connect:
    case uring_socket_op::kind::none: break;
    }
    // 零长度传输立即完成。
    op.ec.clear();
    op.bytes_transferred = 0U;
    return true;
}

coroutine_handle<> uring_socket::suspend(op_direction const direction, coroutine_handle<> const h,
                                         io_env const* const env) noexcept {
    auto& op = op_for(direction);
    op.cont.h = h;
    op.env = env;
    op.pending = true;
    if (env->stop_token.stop_requested()) {
        op.ec = make_error_code(error::operation_aborted);
        op.bytes_transferred = 0U;
        return h;
    }
    if (env->stop_token.stop_possible())
        op.stop_cb.emplace(env->stop_token, cancel_uring_socket_op{this, direction});
    if (direction == op_direction::read && op.op_kind == uring_socket_op::kind::accept && multishot_active()) {
        // 多发模式：不提交 SQE，作为 waiter 停着，由多发 CQE 交付。回调已装好：之后到达的停止请求
        // 经 cancel_op 看到 waiting 完成我们；之前到达的在这里检测。
        std::lock_guard<std::mutex> lock{acceptor_->mutex};
        if (acceptor_->has_parked()) {
            op.accepted_fd = acceptor_->pop_parked();
            op.accepted_family = family_;
            op.ec.clear();
            return h;
        }
        if (env->stop_token.stop_requested()) {
            op.ec = make_error_code(error::operation_aborted);
            op.accepted_fd = -1;
            return h;
        }
        acceptor_->waiting = true;
        context_->get_executor().on_work_started();
        return noop_coroutine();
    }
    if (not backend_->submit(op)) {
        // 提交前已被取消：不会有 CQE。
        op.ec = make_error_code(error::operation_aborted);
        op.bytes_transferred = 0U;
        return h;
    }
    // 关闭"注册回调与提交之间停止请求到达"的窗口（取消请求幂等）。
    if (env->stop_token.stop_requested()) backend_->cancel(op);
    return noop_coroutine();
}

void uring_socket::finish(uring_socket_op& op) noexcept {
    op.stop_cb.reset(); // 之后不再有取消回调
    op.cancel_requested = false;
    op.pending = false;
    op.env = nullptr;
}

io_result<std::size_t> uring_socket::finish_transfer(op_direction const direction) noexcept {
    auto& op = op_for(direction);
    finish(op);
    return io_result<std::size_t>{op.ec, op.bytes_transferred};
}

io_result<> uring_socket::finish_connect() noexcept {
    auto& op = write_op_;
    finish(op);
    op.immediate = false;
    return io_result<>{op.ec};
}

std::error_code uring_socket::finish_accept(int& fd, int& family) noexcept {
    auto& op = read_op_;
    finish(op);
    fd = op.accepted_fd;
    family = op.accepted_family;
    op.accepted_fd = -1;
    return op.ec;
}

} // namespace detail
} // namespace net
