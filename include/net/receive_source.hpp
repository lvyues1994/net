#pragma once

#include <cstddef>
#include <memory>
#include <system_error>
#include <vector>

#include "net/buffers.hpp"
#include "net/coroutine.hpp"
#include "net/io_env.hpp"
#include "net/io_result.hpp"
#include "net/socket_base.hpp"
#include "net/source_sink.hpp"

// 套接字上被调方拥有缓冲区的接收：一个 BufferSource（Paper 6），数据由内核 / 后端直接收进它自己的
// 缓冲池，pull 交出已到达的块，consume 归还——没有用户缓冲区、没有拷贝。
//
//   net::receive_source source{socket, 16, 16 * 1024}; // 16 块 × 16 KiB
//   auto [ec, chunks] = co_await source.pull(scratch);  // chunks：一到多块已收到的数据
//   ... 用完 ...
//   source.consume(n);                                   // 归还给缓冲池
//
// io_uring（6.0+）：一个常驻的多发 RECV 加提供缓冲环（IORING_REGISTER_PBUF_RING）——内核每收到一段
// 就从环里挑一块填好、投一个 CQE，不用每次重新提交；consume 把块放回环。这是 io_uring 网络路径的
// 最高吞吐形态。其它后端：内部缓冲池 + 套接字的 read_some，pull / consume 语义相同。
//
// 契约：receive_source 存活期间它拥有套接字的读方向，不要再对该套接字调 read_some；同一时刻只能有
// 一个 pull 在飞；有未完成的 pull 时不能销毁。

namespace net {

namespace detail {
struct receive_stream_impl;
}

struct receive_source;

struct receive_pull_awaitable {
    receive_source* self;
    const_buffer_span dest;

    bool await_ready() noexcept;
    coroutine_handle<> await_suspend(coroutine_handle<> h, io_env const* env) noexcept;
    io_result<const_buffer_span> await_resume() noexcept;
};

struct receive_source {
    // buffer_count 块，每块 buffer_size 字节（io_uring 上 count 向上取到 2 的幂作为环长）。
    receive_source(socket_base& socket, std::size_t buffer_count = 16U, std::size_t buffer_size = 16U * 1024U);
    receive_source(receive_source const&) = delete;
    receive_source& operator=(receive_source const&) = delete;
    ~receive_source();

    receive_pull_awaitable pull(const_buffer_span const dest) noexcept { return receive_pull_awaitable{this, dest}; }
    void consume(std::size_t n) noexcept;
    // 取消在飞的 pull（以 operation_aborted 完成）。
    void cancel() noexcept;
    // 是否走了后端的专门实现（io_uring 多发接收），而不是回退路径。
    bool kernel_owned() const noexcept { return kernel_owned_; }

  private:
    friend struct receive_pull_awaitable;
    std::unique_ptr<detail::receive_stream_impl> impl_;
    bool kernel_owned_ = false;
};

static_assert(is_buffer_source<receive_source>::value, "receive_source models BufferSource");

} // namespace net
