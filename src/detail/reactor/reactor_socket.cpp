#include "detail/reactor/reactor_socket.hpp"

#include <cstring>

#include "co2/contract.hpp"

#include "net/error.hpp"
#include "net/io_context.hpp"

#include "detail/posix/socket_ops.hpp"
#include "detail/reactor/reactor_backend.hpp"

namespace net {
namespace detail {

// ---- reactor_socket_op ----

bool reactor_socket_op::perform() noexcept {
    auto const fd = owner->native_handle();
    switch (op_kind) {
    case kind::read: {
        auto const outcome = posix::readv(fd, read_buffers);
        if (not outcome.done) return false;
        ec = outcome.ec;
        bytes_transferred = outcome.bytes;
        return true;
    }
    case kind::write: {
        auto const outcome = posix::writev(fd, write_buffers);
        if (not outcome.done) return false;
        ec = outcome.ec;
        bytes_transferred = outcome.bytes;
        return true;
    }
    case kind::receive_from: {
        auto const outcome = posix::recvmsg(fd, read_buffers, address_out, address_length, address_length_out);
        if (not outcome.done) return false;
        ec = outcome.ec;
        bytes_transferred = outcome.bytes;
        return true;
    }
    case kind::send_to: {
        auto const outcome =
            posix::sendmsg(fd, write_buffers, reinterpret_cast<sockaddr const*>(&address), address_length);
        if (not outcome.done) return false;
        ec = outcome.ec;
        bytes_transferred = outcome.bytes;
        return true;
    }
    case kind::connect: {
        // 可写时被调用。可写位可能是过期的（未连接套接字最初就报 EPOLLOUT|EPOLLHUP，或描述符 /
        // 状态地址复用带来的迟到事件）：SO_ERROR 为零还要 getpeername 确认真的连上了。
        auto const outcome = posix::connect_completed(fd);
        if (not outcome.done) return false;
        ec = outcome.ec;
        bytes_transferred = 0U;
        return true;
    }
    case kind::accept: {
        auto const outcome = posix::accept(fd);
        if (not outcome.done) return false;
        ec = outcome.ec;
        accepted_fd = outcome.fd;
        accepted_family = outcome.family;
        return true;
    }
    case kind::none: break;
    }
    ec = make_error_code(error::not_open);
    return true;
}

void reactor_socket_op::complete() noexcept { env->executor.post(cont); }

void cancel_reactor_socket_op::operator()() const noexcept {
    impl->backend().cancel_op(impl->state(), direction, impl->op_for(direction));
}

// ---- reactor_socket ----

reactor_socket::reactor_socket(io_context& context, reactor_backend& backend) noexcept
    : context_{&context}, backend_{&backend}, read_op_{*this, op_direction::read},
      write_op_{*this, op_direction::write} {}

reactor_socket::~reactor_socket() {
    CO2_CONTRACT_CHECK(not has_pending());
    close();
}

std::error_code reactor_socket::open(int const family, int const type, int const protocol) noexcept {
    if (fd_ >= 0) return make_error_code(error::already_open);
    auto const created = posix::create_socket(family, type, protocol); // 已带 SOCK_NONBLOCK | SOCK_CLOEXEC
    if (created < 0) return posix::last_error();
    auto const ec = adopt(family, type, protocol, created);
    if (ec) posix::close_socket(created);
    return ec;
}

std::error_code reactor_socket::assign(int const family, int const type, int const protocol, int const fd) noexcept {
    if (fd_ >= 0) return make_error_code(error::already_open);
    auto const ec = posix::set_nonblocking_cloexec(fd);
    if (ec) return ec;
    return adopt(family, type, protocol, fd);
}

std::error_code reactor_socket::adopt(int, int, int, int const fd) noexcept {
    if (fd_ >= 0) return make_error_code(error::already_open);
    auto const ec = backend_->register_descriptor(state_, fd);
    if (ec) return ec;
    fd_ = fd;
    return {};
}

std::error_code reactor_socket::close() noexcept {
    if (fd_ < 0) return {};
    backend_->deregister_descriptor(state_);
    auto const closing = fd_;
    fd_ = -1;
    return posix::close_socket(closing);
}

void reactor_socket::cancel() noexcept {
    if (fd_ >= 0) backend_->cancel_ops(state_);
}

std::error_code reactor_socket::listen(int const backlog) noexcept {
    if (::listen(fd_, backlog) != 0) return posix::last_error();
    return {};
}

int reactor_socket::release() noexcept {
    if (fd_ < 0) return -1;
    backend_->deregister_descriptor(state_);
    auto const released = fd_;
    fd_ = -1;
    return released;
}

// ---- begin_* ----

void reactor_socket::begin_read(span<mutable_buffer const> const buffers) noexcept {
    CO2_CONTRACT_CHECK(not read_op_.pending);
    read_op_.op_kind = reactor_socket_op::kind::read;
    read_op_.read_buffers = mutable_buffer_array<>{buffers};
}

void reactor_socket::begin_write(span<const_buffer const> const buffers) noexcept {
    CO2_CONTRACT_CHECK(not write_op_.pending);
    write_op_.op_kind = reactor_socket_op::kind::write;
    write_op_.write_buffers = const_buffer_array<>{buffers};
}

void reactor_socket::begin_receive_from(span<mutable_buffer const> const buffers, sockaddr* const sender,
                                        socklen_t const capacity, socklen_t* const sender_length) noexcept {
    CO2_CONTRACT_CHECK(not read_op_.pending);
    read_op_.op_kind = reactor_socket_op::kind::receive_from;
    read_op_.read_buffers = mutable_buffer_array<>{buffers};
    read_op_.address_out = sender;
    read_op_.address_length = capacity;
    read_op_.address_length_out = sender_length;
}

void reactor_socket::begin_send_to(span<const_buffer const> const buffers, sockaddr const* const target,
                                   socklen_t const length) noexcept {
    CO2_CONTRACT_CHECK(not write_op_.pending);
    write_op_.op_kind = reactor_socket_op::kind::send_to;
    write_op_.write_buffers = const_buffer_array<>{buffers};
    std::memcpy(&write_op_.address, target, length);
    write_op_.address_length = length;
}

void reactor_socket::begin_connect(sockaddr const* const address, socklen_t const length, int const family,
                                   int const type, int const protocol) noexcept {
    CO2_CONTRACT_CHECK(not write_op_.pending);
    auto& op = write_op_;
    op.op_kind = reactor_socket_op::kind::connect;
    op.bytes_transferred = 0U;
    op.immediate = true;
    if (fd_ < 0) {
        op.ec = open(family, type, protocol);
        if (op.ec) return; // 同步失败
    }
    // 未连接的套接字一注册就报 EPOLLOUT|EPOLLHUP：那次"可写"对 connect 毫无意义，先清掉。
    backend_->clear_ready(state_, write_ready_bit);
    auto const outcome = posix::connect(fd_, address, length);
    op.ec = outcome.ec;
    op.immediate = outcome.done; // EINPROGRESS → 等待可写
}

void reactor_socket::begin_accept() noexcept {
    CO2_CONTRACT_CHECK(not read_op_.pending);
    read_op_.op_kind = reactor_socket_op::kind::accept;
    read_op_.accepted_fd = -1;
}

// ---- awaiter 三步 ----

bool reactor_socket::ready(op_direction const direction) noexcept {
    auto& op = op_for(direction);
    if (op.op_kind == reactor_socket_op::kind::connect && op.immediate) return true;
    if (fd_ < 0) {
        op.ec = make_error_code(error::not_open);
        op.bytes_transferred = 0U;
        return true;
    }
    // 零长度传输立即完成。
    if ((op.op_kind == reactor_socket_op::kind::read || op.op_kind == reactor_socket_op::kind::receive_from) &&
        op.read_buffers.total_size() == 0U) {
        op.ec.clear();
        op.bytes_transferred = 0U;
        return true;
    }
    if ((op.op_kind == reactor_socket_op::kind::write || op.op_kind == reactor_socket_op::kind::send_to) &&
        op.write_buffers.total_size() == 0U) {
        op.ec.clear();
        op.bytes_transferred = 0U;
        return true;
    }
    if (op.op_kind == reactor_socket_op::kind::connect) return false; // 等待可写
    // 推测执行：数据已就绪就不必挂起。
    return op.perform();
}

coroutine_handle<> reactor_socket::suspend(op_direction const direction, coroutine_handle<> const h,
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
    // 回调装在发布之前：之后到达的停止请求经 cancel_op 取消已登记的操作；之前到达的（包括
    // emplace 时已停止、同步触发的）由 cancel_op 记为 cancel_requested，start_op 看到即同步中止。
    // start_op 之后不能再碰 env 与 op——别的线程可能已经完成操作、恢复并结束协程。
    if (env->stop_token.stop_possible())
        op.stop_cb.emplace(env->stop_token, cancel_reactor_socket_op{this, direction});
    if (backend_->start_op(state_, direction, op)) return h; // 就绪位命中或已被取消：已完成
    return noop_coroutine();
}

void reactor_socket::finish(reactor_socket_op& op) noexcept {
    op.stop_cb.reset(); // 之后不再有取消回调
    op.cancel_requested = false; // 完成后、恢复前到达的取消留下的过期标记
    op.pending = false;
    op.env = nullptr;
}

io_result<std::size_t> reactor_socket::finish_transfer(op_direction const direction) noexcept {
    auto& op = op_for(direction);
    finish(op);
    return io_result<std::size_t>{op.ec, op.bytes_transferred};
}

io_result<> reactor_socket::finish_connect() noexcept {
    auto& op = write_op_;
    finish(op);
    op.immediate = false;
    return io_result<>{op.ec};
}

std::error_code reactor_socket::finish_accept(int& fd, int& family) noexcept {
    auto& op = read_op_;
    finish(op);
    fd = op.accepted_fd;
    family = op.accepted_family;
    op.accepted_fd = -1;
    return op.ec;
}

} // namespace detail
} // namespace net
