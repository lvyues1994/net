#include "detail/iocp/iocp_file.hpp"

#include "co2/contract.hpp"

#include "net/error.hpp"
#include "net/io_context.hpp"

#include "detail/iocp/iocp_backend.hpp"

namespace net {
namespace detail {

void cancel_iocp_file_op::operator()() const noexcept {
    op->cancelled.store(true, std::memory_order_release);
    auto const h = op->owner_->native_handle();
    if (file_is_valid(h)) ::CancelIoEx(h, op->overlapped());
}

void iocp_file_op::on_complete(DWORD const error, DWORD const bytes) noexcept {
    if (error == ERROR_HANDLE_EOF || (error == 0 && bytes == 0 && direction_ == op_direction::read)) {
        ec = make_error_code(error::eof);
        bytes_transferred = 0U;
        return;
    }
    if (error != 0) {
        ec = cancelled.load(std::memory_order_acquire) ? make_error_code(error::operation_aborted) : iocp_error(error);
        bytes_transferred = 0U;
        return;
    }
    ec.clear();
    bytes_transferred = bytes;
}

void iocp_file_op::complete() noexcept { env->executor.post(cont); }

iocp_file::iocp_file(io_context& context, iocp_backend& backend) noexcept
    : context_{&context}, backend_{&backend}, read_{*this, op_direction::read}, write_{*this, op_direction::write} {}

iocp_file::~iocp_file() {
    CO2_CONTRACT_CHECK(not has_pending());
    close();
}

std::error_code iocp_file::assign(native_file_type const handle) noexcept {
    if (file_is_valid(handle_)) return make_error_code(error::already_open);
    auto const ec = backend_->associate(handle);
    if (ec) return ec;
    handle_ = handle;
    return {};
}

std::error_code iocp_file::close() noexcept {
    if (not file_is_valid(handle_)) return {};
    auto const h = handle_;
    if (read_.pending) read_.cancelled.store(true, std::memory_order_release);
    if (write_.pending) write_.cancelled.store(true, std::memory_order_release);
    handle_ = INVALID_HANDLE_VALUE;
    skip_on_success_ = false;
    if (not ::CloseHandle(h)) return std::error_code{static_cast<int>(::GetLastError()), std::system_category()};
    return {};
}

void iocp_file::cancel() noexcept {
    if (not file_is_valid(handle_)) return;
    if (read_.pending) read_.cancelled.store(true, std::memory_order_release);
    if (write_.pending) write_.cancelled.store(true, std::memory_order_release);
    if (read_.pending || write_.pending) ::CancelIoEx(handle_, nullptr);
}

native_file_type iocp_file::release() noexcept {
    cancel();
    auto const h = handle_;
    handle_ = INVALID_HANDLE_VALUE;
    skip_on_success_ = false;
    return h;
}

namespace {
template <class Buffer> void pick_first(span<Buffer const> const buffers, void*& data, DWORD& length) noexcept {
    data = nullptr;
    length = 0;
    for (auto const& b : buffers) {
        if (b.size() == 0U) continue;
        data = const_cast<void*>(static_cast<void const*>(b.data()));
        length = b.size() > 0xFFFFFFFFULL ? 0xFFFFFFFFUL : static_cast<DWORD>(b.size());
        return;
    }
}
} // namespace

void iocp_file::begin_read(std::uint64_t const offset, span<mutable_buffer const> const buffers) noexcept {
    CO2_CONTRACT_CHECK(not read_.pending);
    read_.offset = offset;
    pick_first(buffers, read_.data, read_.length);
    read_.sync_failed = false;
    read_.cancelled.store(false, std::memory_order_relaxed);
}

void iocp_file::begin_write(std::uint64_t const offset, span<const_buffer const> const buffers) noexcept {
    CO2_CONTRACT_CHECK(not write_.pending);
    write_.offset = offset;
    pick_first(buffers, write_.data, write_.length);
    write_.sync_failed = false;
    write_.cancelled.store(false, std::memory_order_relaxed);
}

bool iocp_file::ready(op_direction const direction) noexcept {
    auto& op = op_for(direction);
    if (not file_is_valid(handle_)) {
        op.ec = make_error_code(error::not_open);
        op.bytes_transferred = 0U;
        op.sync_failed = true;
        return true;
    }
    if (op.length == 0U) { // 空序列：不发起
        op.ec.clear();
        op.bytes_transferred = 0U;
        op.sync_failed = true;
        return true;
    }
    return false;
}

iocp_file::issue_result iocp_file::issue(iocp_file_op& op) noexcept {
    op.reset_overlapped();
    op.overlapped()->Offset = static_cast<DWORD>(op.offset & 0xFFFFFFFFULL);
    op.overlapped()->OffsetHigh = static_cast<DWORD>(op.offset >> 32);
    DWORD transferred = 0; // 只在同步完成时有效
    BOOL ok = FALSE;
    if (op.direction_ == op_direction::read)
        ok = ::ReadFile(handle_, op.data, op.length, &transferred, op.overlapped());
    else
        ok = ::WriteFile(handle_, op.data, op.length, &transferred, op.overlapped());
    if (ok) {
        if (not skip_on_success_) return issue_result::pending; // 完成包仍会到
        op.on_complete(0, transferred);                          // 页缓存命中：不绕端口
        return issue_result::completed;
    }
    auto const error = ::GetLastError();
    if (error == ERROR_IO_PENDING) return issue_result::pending;
    op.on_complete(error, 0); // 例如越过文件尾的读立刻 ERROR_HANDLE_EOF，没有完成包
    return issue_result::failed;
}

coroutine_handle<> iocp_file::suspend(op_direction const direction, coroutine_handle<> const h, io_env const* const env) noexcept {
    auto& op = op_for(direction);
    op.cont.h = h;
    op.env = env;
    op.pending = true;
    if (env->stop_token.stop_requested()) {
        op.ec = make_error_code(error::operation_aborted);
        op.bytes_transferred = 0U;
        return h;
    }
    if (env->stop_token.stop_possible()) op.stop_cb.emplace(env->stop_token, cancel_iocp_file_op{&op});
    context_->get_executor().on_work_started();
    if (issue(op) != issue_result::pending) { // 同步完成 / 失败：没有完成包，直接恢复
        context_->get_executor().on_work_finished();
        op.stop_cb.reset();
        return h;
    }
    return noop_coroutine();
}

io_result<std::size_t> iocp_file::finish_transfer(op_direction const direction) noexcept {
    auto& op = op_for(direction);
    op.stop_cb.reset();
    op.pending = false;
    op.sync_failed = false;
    op.env = nullptr;
    return io_result<std::size_t>{op.ec, op.bytes_transferred};
}

} // namespace detail
} // namespace net
