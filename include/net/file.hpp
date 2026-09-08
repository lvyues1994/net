#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <system_error>

#include "net/buffers.hpp"
#include "net/coroutine.hpp"
#include "net/detail/socket_types.hpp"
#include "net/io_env.hpp"
#include "net/io_result.hpp"
#include "net/source_sink.hpp"
#include "net/stream.hpp"

// 异步文件 I/O（P4100R1 §8.8 Paper 10；形态取自 Corosio 的 stream_file / random_access_file）。
//
//   net::stream_file f{ctx};
//   if (auto ec = f.open("data.bin", net::file_base::read_only)) ...
//   CO2_AWAIT_SET(r, f.read_some(net::buffer(buf)));          // 从当前位置读，位置前进；文件尾 → error::eof
//   net::random_access_file raf{ctx, "data.bin", net::file_base::read_write};
//   CO2_AWAIT_SET(r, raf.read_some_at(4096, net::buffer(buf))); // 显式偏移，没有隐式位置
//
// stream_file 满足 Stream：read / write / read_until、any_stream、as_buffer_source（"读文件内容用于发送"）
// 都直接可用。random_access_file 不满足 Stream（没有位置），提供 read_some_at / write_some_at。
//
// 后端：io_uring 提交带偏移的 READV / WRITEV（真正的异步）；就绪型后端（epoll / poll / select）对常规
// 文件没有就绪概念，在操作发起时同步 pread / pwrite 完成（Corosio 的 POSIX 回退相同）。
//
// 契约：同一文件同一方向同时只能有一个操作；有未完成操作时不能销毁。

namespace net {

struct io_context;

namespace detail {
struct file_impl;
struct file_access;
} // namespace detail

struct file_base {
    enum flags : unsigned {
        read_only = 1,
        write_only = 2,
        read_write = read_only | write_only,
        append = 4,
        create = 8,
        exclusive = 16,
        truncate = 32,
        sync_all_on_write = 64
    };

    enum class seek_basis { seek_set, seek_cur, seek_end };

    friend constexpr flags operator|(flags const a, flags const b) noexcept {
        return static_cast<flags>(static_cast<unsigned>(a) | static_cast<unsigned>(b));
    }
    friend constexpr flags operator&(flags const a, flags const b) noexcept {
        return static_cast<flags>(static_cast<unsigned>(a) & static_cast<unsigned>(b));
    }
    friend flags& operator|=(flags& a, flags const b) noexcept { return a = a | b; }
};

// ---- awaiter：一个指针宽 ----

struct file_read_awaitable {
    detail::file_impl* impl;
    std::uint64_t* advance; // stream_file：完成后把位置前进 n；random_access_file：空

    bool await_ready() noexcept;
    coroutine_handle<> await_suspend(coroutine_handle<> h, io_env const* env) noexcept;
    io_result<std::size_t> await_resume() noexcept;
};

struct file_write_awaitable {
    detail::file_impl* impl;
    std::uint64_t* advance;

    bool await_ready() noexcept;
    coroutine_handle<> await_suspend(coroutine_handle<> h, io_env const* env) noexcept;
    io_result<std::size_t> await_resume() noexcept;
};

// 两种文件的公共部分：打开 / 关闭 / 同步操作。
struct basic_file : file_base {
    using native_handle_type = native_file_type;

    basic_file() noexcept;
    explicit basic_file(io_context& context);
    basic_file(basic_file&& other) noexcept;
    basic_file& operator=(basic_file&& other) noexcept;
    basic_file(basic_file const&) = delete;
    basic_file& operator=(basic_file const&) = delete;
    ~basic_file();

    io_context& context() const noexcept;

    // 打开（已打开则先关闭）。找不到文件、权限不足等是运行时状况，经 error_code 报告。
    std::error_code open(std::string const& path, flags mode = read_only) noexcept;
    std::error_code assign(native_handle_type fd) noexcept;
    std::error_code close() noexcept;
    native_handle_type release() noexcept;
    bool is_open() const noexcept;
    native_handle_type native_handle() const noexcept;
    // 取消在飞的操作（完成型后端；就绪型后端的操作同步完成，无事可做）。
    void cancel() noexcept;

    std::uint64_t size(std::error_code& ec) const noexcept;
    std::error_code resize(std::uint64_t new_size) noexcept;
    std::error_code sync_data() noexcept;
    std::error_code sync_all() noexcept;

  protected:
    detail::file_impl* impl() const noexcept { return impl_.get(); }

  private:
    friend struct detail::file_access;
    struct impl_deleter {
        void operator()(detail::file_impl* impl) const noexcept;
    };
    std::unique_ptr<detail::file_impl, impl_deleter> impl_;
};

// 顺序文件：维护隐式位置；满足 Stream。
struct stream_file : basic_file {
    stream_file() noexcept = default;
    explicit stream_file(io_context& context) : basic_file{context} {}
    stream_file(io_context& context, std::string const& path, flags mode);
    stream_file(stream_file&&) noexcept = default;
    stream_file& operator=(stream_file&&) noexcept = default;

    // 移动位置；返回新位置。seek_end 相对文件末尾。
    std::uint64_t seek(std::int64_t offset, seek_basis origin, std::error_code& ec) noexcept;
    std::uint64_t position() const noexcept { return position_; }

    template <class MutableBufferSequence> file_read_awaitable read_some(MutableBufferSequence const& buffers) noexcept {
        static_assert(is_mutable_buffer_sequence<MutableBufferSequence>::value, "read_some requires a MutableBufferSequence");
        return start_read(mutable_buffer_array<>{buffers});
    }

    template <class ConstBufferSequence> file_write_awaitable write_some(ConstBufferSequence const& buffers) noexcept {
        static_assert(is_const_buffer_sequence<ConstBufferSequence>::value, "write_some requires a ConstBufferSequence");
        return start_write(const_buffer_array<>{buffers});
    }

  private:
    file_read_awaitable start_read(mutable_buffer_array<> const& buffers) noexcept;
    file_write_awaitable start_write(const_buffer_array<> const& buffers) noexcept;

    std::uint64_t position_ = 0;
};

// 随机访问文件：显式偏移，没有隐式位置。
struct random_access_file : basic_file {
    random_access_file() noexcept = default;
    explicit random_access_file(io_context& context) : basic_file{context} {}
    random_access_file(io_context& context, std::string const& path, flags mode);
    random_access_file(random_access_file&&) noexcept = default;
    random_access_file& operator=(random_access_file&&) noexcept = default;

    template <class MutableBufferSequence>
    file_read_awaitable read_some_at(std::uint64_t const offset, MutableBufferSequence const& buffers) noexcept {
        static_assert(is_mutable_buffer_sequence<MutableBufferSequence>::value, "read_some_at requires a MutableBufferSequence");
        return start_read_at(offset, mutable_buffer_array<>{buffers});
    }

    template <class ConstBufferSequence>
    file_write_awaitable write_some_at(std::uint64_t const offset, ConstBufferSequence const& buffers) noexcept {
        static_assert(is_const_buffer_sequence<ConstBufferSequence>::value, "write_some_at requires a ConstBufferSequence");
        return start_write_at(offset, const_buffer_array<>{buffers});
    }

  private:
    file_read_awaitable start_read_at(std::uint64_t offset, mutable_buffer_array<> const& buffers) noexcept;
    file_write_awaitable start_write_at(std::uint64_t offset, const_buffer_array<> const& buffers) noexcept;
};

} // namespace net
