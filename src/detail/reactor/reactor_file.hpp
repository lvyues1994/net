#pragma once

#include <cstdint>

#include "detail/backend.hpp"

// 就绪型后端的文件实现：常规文件对 epoll / poll / select 没有就绪概念（epoll_ctl 直接 EPERM），
// 所以操作在 ready() 里同步 preadv / pwritev 完成——Corosio 的 POSIX 回退相同。suspend() 永不被调用。

namespace net {
namespace detail {

struct reactor_file final : file_impl {
    reactor_file(io_context& context) noexcept : context_{&context} {}
    ~reactor_file() override;

    io_context& context() const noexcept override { return *context_; }

    std::error_code assign(int fd) noexcept override;
    std::error_code close() noexcept override;
    void cancel() noexcept override {}
    int release() noexcept override;
    int native_handle() const noexcept override { return fd_; }

    void begin_read(std::uint64_t offset, span<mutable_buffer const> buffers) noexcept override;
    void begin_write(std::uint64_t offset, span<const_buffer const> buffers) noexcept override;
    bool ready(op_direction direction) noexcept override;
    coroutine_handle<> suspend(op_direction direction, coroutine_handle<> h, io_env const* env) noexcept override;
    io_result<std::size_t> finish_transfer(op_direction direction) noexcept override;
    bool has_pending() const noexcept override { return false; }

  private:
    struct op {
        std::uint64_t offset = 0;
        mutable_buffer_array<> read_buffers;
        const_buffer_array<> write_buffers;
        std::error_code ec;
        std::size_t bytes = 0;
    };

    op& op_for(op_direction const direction) noexcept { return direction == op_direction::read ? read_ : write_; }

    io_context* context_;
    int fd_ = -1;
    op read_;
    op write_;
};

} // namespace detail
} // namespace net
