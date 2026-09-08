#pragma once

#include <cstdint>

#include <sys/uio.h>

#include "net/continuation.hpp"
#include "net/coroutine.hpp"
#include "net/detail/storage.hpp"
#include "net/io_env.hpp"

#include "detail/backend.hpp"
#include "detail/io_uring/uring_op.hpp"

// io_uring 的文件实现：READV / WRITEV 带偏移的 SQE，真正的异步（内核对缓冲 I/O 用 io-wq 线程完成）。
// 常规文件没有非阻塞语义，不做投机：每个操作都提交。取消经 ASYNC_CANCEL（已进入 io-wq 的操作可能
// 取消不掉，那样它以正常结果完成）。

namespace net {
namespace detail {

struct uring_backend;
struct uring_file;
struct uring_file_op;

struct cancel_uring_file_op {
    uring_file_op* op;
    void operator()() const noexcept;
};

struct uring_file_op final : uring_op {
    uring_file_op(uring_file& owner, op_direction direction) noexcept : owner_{&owner}, direction_{direction} {}

    void prepare(io_uring_sqe& sqe) noexcept override;
    void on_complete(int res, unsigned flags) noexcept override;
    void complete() noexcept override;

    uring_file* owner_;
    op_direction direction_;
    std::uint64_t offset = 0;
    iovec vectors[max_iovec];
    unsigned vector_count = 0;
    std::error_code ec;
    std::size_t bytes = 0;
    continuation cont;
    io_env const* env = nullptr;
    bool pending = false;
    late_init<stop_callback<cancel_uring_file_op>> stop_cb;
};

struct uring_file final : file_impl {
    uring_file(io_context& context, uring_backend& backend) noexcept;
    ~uring_file() override;

    io_context& context() const noexcept override { return *context_; }
    uring_backend& backend() noexcept { return *backend_; }

    std::error_code assign(int fd) noexcept override;
    std::error_code close() noexcept override;
    void cancel() noexcept override;
    int release() noexcept override;
    int native_handle() const noexcept override { return fd_; }
    int file_slot() const noexcept { return file_slot_; }

    void begin_read(std::uint64_t offset, span<mutable_buffer const> buffers) noexcept override;
    void begin_write(std::uint64_t offset, span<const_buffer const> buffers) noexcept override;
    bool ready(op_direction direction) noexcept override;
    coroutine_handle<> suspend(op_direction direction, coroutine_handle<> h, io_env const* env) noexcept override;
    io_result<std::size_t> finish_transfer(op_direction direction) noexcept override;
    bool has_pending() const noexcept override { return read_.pending || write_.pending; }

  private:
    uring_file_op& op_for(op_direction const direction) noexcept { return direction == op_direction::read ? read_ : write_; }

    io_context* context_;
    uring_backend* backend_;
    int fd_ = -1;
    int file_slot_ = -1; // 注册文件表槽位（-1：用裸 fd）
    uring_file_op read_;
    uring_file_op write_;
};

} // namespace detail
} // namespace net
