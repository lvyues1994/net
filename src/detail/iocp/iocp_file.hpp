#pragma once

#include <atomic>
#include <cstdint>

#include "net/continuation.hpp"
#include "net/coroutine.hpp"
#include "net/detail/storage.hpp"
#include "net/io_env.hpp"

#include "detail/backend.hpp"
#include "detail/iocp/iocp_op.hpp"

// IOCP 的文件实现：以 FILE_FLAG_OVERLAPPED 打开的句柄上做 ReadFile / WriteFile，偏移放在 OVERLAPPED 里
//（Offset / OffsetHigh），完成经端口到达。ReadFile 一次只有一个缓冲区：取序列里第一个非空的
//（部分读 / 写语义允许）。

namespace net {
namespace detail {

struct iocp_backend;
struct iocp_file;
struct iocp_file_op;

struct cancel_iocp_file_op {
    iocp_file_op* op;
    void operator()() const noexcept;
};

struct iocp_file_op final : iocp_op {
    iocp_file_op(iocp_file& owner, op_direction direction) noexcept : owner_{&owner}, direction_{direction} {}

    void on_complete(DWORD error, DWORD bytes) noexcept override;
    void complete() noexcept override;

    iocp_file* owner_;
    op_direction direction_;
    std::uint64_t offset = 0;
    void* data = nullptr;
    DWORD length = 0;
    std::error_code ec;
    std::size_t bytes_transferred = 0;
    bool sync_failed = false;
    std::atomic<bool> cancelled{false}; // 本地取消：完成包的错误码统一报 operation_aborted
    continuation cont;
    io_env const* env = nullptr;
    bool pending = false;
    late_init<stop_callback<cancel_iocp_file_op>> stop_cb;
};

struct iocp_file final : file_impl {
    iocp_file(io_context& context, iocp_backend& backend) noexcept;
    ~iocp_file() override;

    io_context& context() const noexcept override { return *context_; }

    std::error_code assign(native_file_type handle) noexcept override;
    std::error_code close() noexcept override;
    void cancel() noexcept override;
    native_file_type release() noexcept override;
    native_file_type native_handle() const noexcept override { return handle_; }

    void begin_read(std::uint64_t offset, span<mutable_buffer const> buffers) noexcept override;
    void begin_write(std::uint64_t offset, span<const_buffer const> buffers) noexcept override;
    bool ready(op_direction direction) noexcept override;
    coroutine_handle<> suspend(op_direction direction, coroutine_handle<> h, io_env const* env) noexcept override;
    io_result<std::size_t> finish_transfer(op_direction direction) noexcept override;
    bool has_pending() const noexcept override { return read_.pending || write_.pending; }

  private:
    iocp_file_op& op_for(op_direction const direction) noexcept { return direction == op_direction::read ? read_ : write_; }
    bool issue(iocp_file_op& op) noexcept;

    io_context* context_;
    iocp_backend* backend_;
    native_file_type handle_ = INVALID_HANDLE_VALUE;
    iocp_file_op read_;
    iocp_file_op write_;
};

} // namespace detail
} // namespace net
