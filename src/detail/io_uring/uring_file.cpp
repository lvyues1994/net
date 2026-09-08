#include "detail/io_uring/uring_file.hpp"

#include <cerrno>

#include <unistd.h>

#include "co2/contract.hpp"

#include "net/error.hpp"
#include "net/io_context.hpp"

#include "detail/io_uring/uring_backend.hpp"

namespace net {
namespace detail {

void cancel_uring_file_op::operator()() const noexcept { op->owner_->backend().cancel(*op); }

// ---- op ----

void uring_file_op::prepare(io_uring_sqe& sqe) noexcept {
    if (owner_->file_slot() >= 0) {
        sqe.fd = owner_->file_slot();
        sqe.flags |= IOSQE_FIXED_FILE;
    } else {
        sqe.fd = owner_->native_handle();
    }
    sqe.off = offset;
    // 单缓冲且落在注册缓冲区域内：READ_FIXED / WRITE_FIXED
    if (vector_count == 1U) {
        auto const slot = owner_->backend().fixed_buffer_slot_locked(vectors[0].iov_base, vectors[0].iov_len);
        if (slot >= 0) {
            sqe.opcode = direction_ == op_direction::read ? IORING_OP_READ_FIXED : IORING_OP_WRITE_FIXED;
            sqe.addr = reinterpret_cast<std::uintptr_t>(vectors[0].iov_base);
            sqe.len = static_cast<unsigned>(vectors[0].iov_len);
            sqe.buf_index = static_cast<std::uint16_t>(slot);
            return;
        }
    }
    sqe.opcode = direction_ == op_direction::read ? IORING_OP_READV : IORING_OP_WRITEV;
    sqe.addr = reinterpret_cast<std::uintptr_t>(vectors);
    sqe.len = vector_count;
}

void uring_file_op::on_complete(int const res, unsigned) noexcept {
    if (res > 0) {
        ec.clear();
        bytes = static_cast<std::size_t>(res);
        return;
    }
    bytes = 0U;
    if (res == 0) {
        // 读到文件尾 → eof；写 0 字节（空序列）→ 成功
        ec = direction_ == op_direction::read ? make_error_code(error::eof) : std::error_code{};
        return;
    }
    ec = res == -ECANCELED ? make_error_code(error::operation_aborted) : std::error_code{-res, std::system_category()};
}

void uring_file_op::complete() noexcept { env->executor.post(cont); }

// ---- file ----

uring_file::uring_file(io_context& context, uring_backend& backend) noexcept
    : context_{&context}, backend_{&backend}, read_{*this, op_direction::read}, write_{*this, op_direction::write} {}

uring_file::~uring_file() {
    CO2_CONTRACT_CHECK(not has_pending());
    close();
}

std::error_code uring_file::assign(int const fd) noexcept {
    if (fd_ >= 0) return make_error_code(error::already_open);
    fd_ = fd;
    file_slot_ = backend_->register_file(fd);
    return {};
}

std::error_code uring_file::close() noexcept {
    if (fd_ < 0) return {};
    cancel();
    backend_->unregister_file(file_slot_);
    file_slot_ = -1;
    auto const fd = fd_;
    fd_ = -1;
    if (::close(fd) != 0) return std::error_code{errno, std::system_category()};
    return {};
}

void uring_file::cancel() noexcept {
    if (read_.pending) backend_->cancel(read_);
    if (write_.pending) backend_->cancel(write_);
}

int uring_file::release() noexcept {
    cancel();
    backend_->unregister_file(file_slot_);
    file_slot_ = -1;
    auto const fd = fd_;
    fd_ = -1;
    return fd;
}

namespace {
template <class Buffer> unsigned fill_vectors(iovec (&vectors)[max_iovec], span<Buffer const> const buffers) noexcept {
    auto count = 0U;
    for (auto const& b : buffers) {
        if (count == max_iovec) break;
        vectors[count].iov_base = const_cast<void*>(static_cast<void const*>(b.data()));
        vectors[count].iov_len = b.size();
        ++count;
    }
    return count;
}
} // namespace

void uring_file::begin_read(std::uint64_t const offset, span<mutable_buffer const> const buffers) noexcept {
    CO2_CONTRACT_CHECK(not read_.pending);
    read_.offset = offset;
    read_.vector_count = fill_vectors(read_.vectors, buffers);
}

void uring_file::begin_write(std::uint64_t const offset, span<const_buffer const> const buffers) noexcept {
    CO2_CONTRACT_CHECK(not write_.pending);
    write_.offset = offset;
    write_.vector_count = fill_vectors(write_.vectors, buffers);
}

bool uring_file::ready(op_direction const direction) noexcept {
    auto& op = op_for(direction);
    if (fd_ < 0) {
        op.ec = make_error_code(error::not_open);
        op.bytes = 0U;
        return true;
    }
    if (op.vector_count == 0U) { // 空序列：不提交
        op.ec.clear();
        op.bytes = 0U;
        return true;
    }
    return false;
}

coroutine_handle<> uring_file::suspend(op_direction const direction, coroutine_handle<> const h, io_env const* const env) noexcept {
    auto& op = op_for(direction);
    op.cont.h = h;
    op.env = env;
    op.pending = true;
    if (env->stop_token.stop_requested()) {
        op.ec = make_error_code(error::operation_aborted);
        op.bytes = 0U;
        return h;
    }
    if (env->stop_token.stop_possible()) op.stop_cb.emplace(env->stop_token, cancel_uring_file_op{&op});
    if (not backend_->submit(op)) { // 提交前已被取消
        op.ec = make_error_code(error::operation_aborted);
        op.bytes = 0U;
        return h;
    }
    return noop_coroutine();
}

io_result<std::size_t> uring_file::finish_transfer(op_direction const direction) noexcept {
    auto& op = op_for(direction);
    op.stop_cb.reset();
    op.cancel_requested = false;
    op.pending = false;
    op.env = nullptr;
    return io_result<std::size_t>{op.ec, op.bytes};
}

} // namespace detail
} // namespace net
