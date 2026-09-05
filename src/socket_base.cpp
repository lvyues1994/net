#include "net/socket_base.hpp"

#include <cerrno>
#include <cstring>

#include <fcntl.h>
#include <sys/ioctl.h>
#include <sys/uio.h>
#include <unistd.h>

#include "co2/contract.hpp"

#include "net/error.hpp"
#include "net/io_context.hpp"

#include "detail/reactor.hpp"
#include "detail/socket_impl.hpp"

namespace net {
namespace detail {

namespace {

std::error_code last_error() noexcept { return std::error_code{errno, std::system_category()}; }

bool would_block(int const err) noexcept { return err == EAGAIN || err == EWOULDBLOCK; }

template <class Buffer, std::size_t N>
std::size_t fill_iovec(iovec (&vectors)[N], buffer_array<Buffer, N> const& buffers) noexcept {
    auto count = std::size_t{};
    for (auto const& b : buffers) {
        vectors[count].iov_base = const_cast<void*>(static_cast<void const*>(b.data()));
        vectors[count].iov_len = b.size();
        ++count;
    }
    return count;
}

} // namespace

// ---- socket_op ----

bool socket_op::perform() noexcept {
    auto const fd = owner->fd;
    switch (op_kind) {
    case kind::read: {
        iovec vectors[max_iovec];
        auto const count = fill_iovec(vectors, read_buffers);
        for (;;) {
            auto const n = ::readv(fd, vectors, static_cast<int>(count));
            if (n > 0) {
                ec.clear();
                bytes_transferred = static_cast<std::size_t>(n);
                return true;
            }
            if (n == 0) {
                ec = make_error_code(error::eof);
                bytes_transferred = 0U;
                return true;
            }
            if (errno == EINTR) continue;
            if (would_block(errno)) return false;
            ec = last_error();
            bytes_transferred = 0U;
            return true;
        }
    }
    case kind::write: {
        iovec vectors[max_iovec];
        auto const count = fill_iovec(vectors, write_buffers);
        for (;;) {
            auto const n = ::writev(fd, vectors, static_cast<int>(count));
            if (n >= 0) {
                ec.clear();
                bytes_transferred = static_cast<std::size_t>(n);
                return true;
            }
            if (errno == EINTR) continue;
            if (would_block(errno)) return false;
            ec = last_error();
            bytes_transferred = 0U;
            return true;
        }
    }
    case kind::connect: {
        auto pending_error = 0;
        auto length = static_cast<socklen_t>(sizeof(pending_error));
        if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &pending_error, &length) != 0)
            pending_error = errno;
        ec = pending_error == 0 ? std::error_code{}
                                : std::error_code{pending_error, std::system_category()};
        bytes_transferred = 0U;
        return true;
    }
    case kind::accept: {
        for (;;) {
            address_length = static_cast<socklen_t>(sizeof(address));
            auto const accepted = ::accept4(fd, reinterpret_cast<sockaddr*>(&address),
                                            &address_length, SOCK_NONBLOCK | SOCK_CLOEXEC);
            if (accepted >= 0) {
                ec.clear();
                accepted_fd = accepted;
                return true;
            }
            if (errno == EINTR || errno == ECONNABORTED) continue;
            if (would_block(errno)) return false;
            ec = last_error();
            accepted_fd = -1;
            return true;
        }
    }
    case kind::receive_from: {
        iovec vectors[max_iovec];
        auto const count = fill_iovec(vectors, read_buffers);
        for (;;) {
            msghdr message{};
            message.msg_name = address_out;
            message.msg_namelen = address_out != nullptr ? address_length : 0U;
            message.msg_iov = vectors;
            message.msg_iovlen = count;
            auto const n = ::recvmsg(fd, &message, 0);
            if (n >= 0) {
                ec.clear();
                bytes_transferred = static_cast<std::size_t>(n);
                return true;
            }
            if (errno == EINTR) continue;
            if (would_block(errno)) return false;
            ec = last_error();
            bytes_transferred = 0U;
            return true;
        }
    }
    case kind::send_to: {
        iovec vectors[max_iovec];
        auto const count = fill_iovec(vectors, write_buffers);
        for (;;) {
            msghdr message{};
            message.msg_name = &address;
            message.msg_namelen = address_length;
            message.msg_iov = vectors;
            message.msg_iovlen = count;
            auto const n = ::sendmsg(fd, &message, 0);
            if (n >= 0) {
                ec.clear();
                bytes_transferred = static_cast<std::size_t>(n);
                return true;
            }
            if (errno == EINTR) continue;
            if (would_block(errno)) return false;
            ec = last_error();
            bytes_transferred = 0U;
            return true;
        }
    }
    case kind::none: break;
    }
    ec = make_error_code(error::not_open);
    return true;
}

void socket_op::complete() noexcept { env->executor.post(cont); }

void cancel_socket_op::operator()() const noexcept {
    impl->reactor_->cancel_op(impl->state, direction, impl->op_for(direction));
}

// ---- socket_impl ----

socket_impl::socket_impl(io_context& context_) noexcept
    : context{&context_}, reactor_{&io_context_access::get_reactor(context_)},
      read_op{*this, op_direction::read}, write_op{*this, op_direction::write} {}

socket_impl::~socket_impl() {
    CO2_CONTRACT_CHECK(not read_op.pending && not write_op.pending);
    close();
}

std::error_code socket_impl::open(int const family_, int const type_, int const protocol_) noexcept {
    if (fd >= 0) return make_error_code(error::already_open);
    auto const created = ::socket(family_, type_ | SOCK_NONBLOCK | SOCK_CLOEXEC, protocol_);
    if (created < 0) return last_error();
    return assign(family_, type_, protocol_, created);
}

std::error_code socket_impl::assign(int const family_, int const type_, int const protocol_,
                                    int const new_fd) noexcept {
    if (fd >= 0) return make_error_code(error::already_open);
    auto const flags = ::fcntl(new_fd, F_GETFL, 0);
    if (flags < 0 || ::fcntl(new_fd, F_SETFL, flags | O_NONBLOCK) < 0) return last_error();
    auto const ec = reactor_->register_descriptor(state, new_fd);
    if (ec) return ec;
    fd = new_fd;
    family = family_;
    type = type_;
    protocol = protocol_;
    return {};
}

std::error_code socket_impl::close() noexcept {
    if (fd < 0) return {};
    reactor_->deregister_descriptor(state);
    auto const closing = fd;
    fd = -1;
    if (::close(closing) != 0 && errno != EINTR) return last_error();
    return {};
}

void socket_impl::cancel() noexcept {
    if (fd < 0) return;
    reactor_->cancel_ops(state);
}

int socket_impl::release() noexcept {
    if (fd < 0) return -1;
    reactor_->deregister_descriptor(state);
    auto const released = fd;
    fd = -1;
    return released;
}

bool socket_impl::op_ready(socket_op& op) noexcept {
    if (fd < 0) {
        op.ec = make_error_code(error::not_open);
        op.bytes_transferred = 0U;
        return true;
    }
    // 零长度传输立即完成。
    if ((op.op_kind == socket_op::kind::read || op.op_kind == socket_op::kind::receive_from) &&
        op.read_buffers.total_size() == 0U) {
        op.ec.clear();
        op.bytes_transferred = 0U;
        return true;
    }
    if ((op.op_kind == socket_op::kind::write || op.op_kind == socket_op::kind::send_to) &&
        op.write_buffers.total_size() == 0U) {
        op.ec.clear();
        op.bytes_transferred = 0U;
        return true;
    }
    if (op.op_kind == socket_op::kind::connect) {
        // connect 的推测尝试在 start_connect 里做过了（EINPROGRESS 才走到这里）。
        return false;
    }
    // 推测执行：数据已就绪就不必挂起。
    return op.perform();
}

coroutine_handle<> socket_impl::op_suspend(socket_op& op, coroutine_handle<> const h,
                                           io_env const* const env) noexcept {
    op.cont.h = h;
    op.env = env;
    op.pending = true;
    if (env->stop_token.stop_requested()) {
        op.ec = make_error_code(error::operation_aborted);
        op.bytes_transferred = 0U;
        return h;
    }
    if (env->stop_token.stop_possible())
        op.stop_cb.emplace(env->stop_token, cancel_socket_op{this, op.direction});
    if (reactor_->start_op(state, op.direction, op)) return h; // 就绪位命中：已完成
    // 关闭"注册回调与排队之间停止请求到达"的窗口。
    if (env->stop_token.stop_requested()) reactor_->cancel_op(state, op.direction, op);
    return noop_coroutine();
}

void socket_impl::op_finish(socket_op& op) noexcept {
    op.stop_cb.reset();
    op.pending = false;
    op.env = nullptr;
}

} // namespace detail

// ---- awaiter ----

bool socket_read_awaitable::await_ready() noexcept { return impl->op_ready(impl->read_op); }

coroutine_handle<> socket_read_awaitable::await_suspend(coroutine_handle<> const h,
                                                        io_env const* const env) noexcept {
    return impl->op_suspend(impl->read_op, h, env);
}

io_result<std::size_t> socket_read_awaitable::await_resume() noexcept {
    auto& op = impl->read_op;
    impl->op_finish(op);
    return io_result<std::size_t>{op.ec, op.bytes_transferred};
}

bool socket_write_awaitable::await_ready() noexcept { return impl->op_ready(impl->write_op); }

coroutine_handle<> socket_write_awaitable::await_suspend(coroutine_handle<> const h,
                                                         io_env const* const env) noexcept {
    return impl->op_suspend(impl->write_op, h, env);
}

io_result<std::size_t> socket_write_awaitable::await_resume() noexcept {
    auto& op = impl->write_op;
    impl->op_finish(op);
    return io_result<std::size_t>{op.ec, op.bytes_transferred};
}

bool socket_connect_awaitable::await_ready() noexcept {
    auto& op = impl->write_op;
    // start_connect 已把同步结果（成功或失败）记在 op 里时不必挂起；EINPROGRESS 等待可写。
    if (op.immediate) return true;
    return impl->op_ready(op);
}

coroutine_handle<> socket_connect_awaitable::await_suspend(coroutine_handle<> const h,
                                                           io_env const* const env) noexcept {
    return impl->op_suspend(impl->write_op, h, env);
}

io_result<> socket_connect_awaitable::await_resume() noexcept {
    auto& op = impl->write_op;
    impl->op_finish(op);
    op.immediate = false;
    return io_result<>{op.ec};
}

// ---- socket_base ----

socket_base::socket_base() noexcept = default;

socket_base::socket_base(io_context& context) : impl_{new detail::socket_impl{context}} {}

socket_base::socket_base(socket_base&& other) noexcept = default;

socket_base& socket_base::operator=(socket_base&& other) noexcept {
    if (this != &other) impl_ = std::move(other.impl_);
    return *this;
}

socket_base::~socket_base() = default;

io_context& socket_base::context() const noexcept {
    CO2_CONTRACT_CHECK(impl_ != nullptr);
    return *impl_->context;
}

bool socket_base::is_open() const noexcept { return impl_ != nullptr && impl_->fd >= 0; }

std::error_code socket_base::close() noexcept {
    if (impl_ == nullptr) return {};
    return impl_->close();
}

void socket_base::cancel() noexcept {
    if (impl_ != nullptr) impl_->cancel();
}

socket_base::native_handle_type socket_base::native_handle() const noexcept {
    return impl_ != nullptr ? impl_->fd : -1;
}

socket_base::native_handle_type socket_base::release() noexcept {
    return impl_ != nullptr ? impl_->release() : -1;
}

std::size_t socket_base::available(std::error_code& ec) const noexcept {
    if (not is_open()) {
        ec = make_error_code(error::not_open);
        return 0U;
    }
    auto count = 0;
    if (::ioctl(impl_->fd, FIONREAD, &count) != 0) {
        ec = detail::last_error();
        return 0U;
    }
    ec.clear();
    return static_cast<std::size_t>(count);
}

std::error_code socket_base::open_raw(int const family, int const type, int const protocol) noexcept {
    CO2_CONTRACT_CHECK(impl_ != nullptr);
    return impl_->open(family, type, protocol);
}

std::error_code socket_base::assign_raw(int const family, int const type, int const protocol,
                                        native_handle_type const fd) noexcept {
    CO2_CONTRACT_CHECK(impl_ != nullptr);
    return impl_->assign(family, type, protocol, fd);
}

std::error_code socket_base::bind_raw(sockaddr const* const address, socklen_t const length) noexcept {
    if (not is_open()) return make_error_code(error::not_open);
    if (::bind(impl_->fd, address, length) != 0) return detail::last_error();
    return {};
}

std::error_code socket_base::listen_raw(int const backlog) noexcept {
    if (not is_open()) return make_error_code(error::not_open);
    if (::listen(impl_->fd, backlog) != 0) return detail::last_error();
    return {};
}

std::error_code socket_base::shutdown_raw(int const how) noexcept {
    if (not is_open()) return make_error_code(error::not_open);
    if (::shutdown(impl_->fd, how) != 0) return detail::last_error();
    return {};
}

std::error_code socket_base::local_endpoint_raw(sockaddr* const address,
                                                socklen_t* const length) const noexcept {
    if (not is_open()) return make_error_code(error::not_open);
    if (::getsockname(impl_->fd, address, length) != 0) return detail::last_error();
    return {};
}

std::error_code socket_base::remote_endpoint_raw(sockaddr* const address,
                                                 socklen_t* const length) const noexcept {
    if (not is_open()) return make_error_code(error::not_open);
    if (::getpeername(impl_->fd, address, length) != 0) return detail::last_error();
    return {};
}

std::error_code socket_base::set_option_raw(int const level, int const name, void const* const data,
                                            std::size_t const size) noexcept {
    if (not is_open()) return make_error_code(error::not_open);
    if (::setsockopt(impl_->fd, level, name, data, static_cast<socklen_t>(size)) != 0)
        return detail::last_error();
    return {};
}

std::error_code socket_base::get_option_raw(int const level, int const name, void* const data,
                                            socklen_t* const size) const noexcept {
    if (not is_open()) return make_error_code(error::not_open);
    if (::getsockopt(impl_->fd, level, name, data, size) != 0) return detail::last_error();
    return {};
}

std::error_code socket_base::connect_raw(sockaddr const* const address,
                                         socklen_t const length) noexcept {
    if (not is_open()) return make_error_code(error::not_open);
    for (;;) {
        if (::connect(impl_->fd, address, length) == 0) return {};
        if (errno == EINTR) continue;
        return detail::last_error();
    }
}

socket_read_awaitable socket_base::start_read(span<mutable_buffer const> const buffers) noexcept {
    CO2_CONTRACT_CHECK(impl_ != nullptr);
    auto& op = impl_->read_op;
    CO2_CONTRACT_CHECK(not op.pending);
    op.op_kind = detail::socket_op::kind::read;
    op.read_buffers = mutable_buffer_array<>{buffers};
    return socket_read_awaitable{impl_.get()};
}

socket_write_awaitable socket_base::start_write(span<const_buffer const> const buffers) noexcept {
    CO2_CONTRACT_CHECK(impl_ != nullptr);
    auto& op = impl_->write_op;
    CO2_CONTRACT_CHECK(not op.pending);
    op.op_kind = detail::socket_op::kind::write;
    op.write_buffers = const_buffer_array<>{buffers};
    return socket_write_awaitable{impl_.get()};
}

socket_connect_awaitable socket_base::start_connect(sockaddr const* const address,
                                                    socklen_t const length, int const family,
                                                    int const type, int const protocol) noexcept {
    CO2_CONTRACT_CHECK(impl_ != nullptr);
    auto& op = impl_->write_op;
    CO2_CONTRACT_CHECK(not op.pending);
    op.op_kind = detail::socket_op::kind::connect;
    op.bytes_transferred = 0U;
    op.immediate = true;
    if (not is_open()) {
        op.ec = impl_->open(family, type, protocol);
        if (op.ec) return socket_connect_awaitable{impl_.get()}; // 同步失败
    }
    for (;;) {
        if (::connect(impl_->fd, address, length) == 0) {
            op.ec.clear(); // 同步成功
            return socket_connect_awaitable{impl_.get()};
        }
        if (errno == EINTR) continue;
        if (errno == EINPROGRESS) {
            op.ec.clear();
            op.immediate = false; // 等待可写
            return socket_connect_awaitable{impl_.get()};
        }
        op.ec = detail::last_error();
        return socket_connect_awaitable{impl_.get()};
    }
}

socket_read_awaitable socket_base::start_receive_from(span<mutable_buffer const> const buffers,
                                                      sockaddr* const sender,
                                                      socklen_t const capacity) noexcept {
    CO2_CONTRACT_CHECK(impl_ != nullptr);
    auto& op = impl_->read_op;
    CO2_CONTRACT_CHECK(not op.pending);
    op.op_kind = detail::socket_op::kind::receive_from;
    op.read_buffers = mutable_buffer_array<>{buffers};
    op.address_out = sender;
    op.address_length = capacity;
    return socket_read_awaitable{impl_.get()};
}

socket_write_awaitable socket_base::start_send_to(span<const_buffer const> const buffers,
                                                  sockaddr const* const target,
                                                  socklen_t const length) noexcept {
    CO2_CONTRACT_CHECK(impl_ != nullptr);
    auto& op = impl_->write_op;
    CO2_CONTRACT_CHECK(not op.pending);
    op.op_kind = detail::socket_op::kind::send_to;
    op.write_buffers = const_buffer_array<>{buffers};
    std::memcpy(&op.address, target, length);
    op.address_length = length;
    return socket_write_awaitable{impl_.get()};
}

} // namespace net
