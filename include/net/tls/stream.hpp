#pragma once

#include <cstddef>
#include <memory>
#include <string>

#include "net/any_stream.hpp"
#include "net/buffers.hpp"
#include "net/io_result.hpp"
#include "net/source_sink.hpp"
#include "net/task.hpp"
#include "net/tls/context.hpp"

// TLS 流（P4100R1 §8.8 Paper 14；形态取自 Corosio 的 tls_stream / openssl_stream）。
//
// tls::stream 是抽象基类：TLS 操作是协程，在底层流（任何 Stream，经 any_stream 类型擦除）
// 上编排子操作——引擎（OpenSSL / BoringSSL 的 SSL 对象 + 内存 BIO）只做密码学，不碰 I/O，
// 因此与 io_context 的后端（epoll / poll / select / io_uring）完全无关。非虚的 read_some /
// write_some 模板让它满足 Stream 概念：TLS 流可以放进 any_stream，业务逻辑对明文流编译一次。
//
//   net::tls::context ctx;
//   ctx.set_verify_mode(net::tls::verify_mode::peer);
//   ctx.set_default_verify_paths();
//   net::tls::openssl_stream tls{&sock, ctx};          // 引用底层套接字
//   tls.set_hostname("example.com");                   // SNI + 证书主机名校验
//   CO2_AWAIT_SET(h, tls.handshake(net::tls::role::client));
//   CO2_AWAIT_SET(r, tls.read_some(net::buffer(buf)));
//   CO2_AWAIT_SET(s, tls.shutdown());
//
// 线程模型：同一流上握手 / shutdown 不能与其它操作并发；握手之后允许一个读与一个写同时在
// 飞（两者都在同一个执行器 / strand 上）。传输方向的写由内部门闩串行化。
//
// 结束语义：对端发送 close_notify 后 read_some 返回 error::eof；传输在 close_notify 之前
// 结束返回 error::stream_truncated（无法与截断攻击区分，不能报告为干净关闭）。

namespace net {
namespace tls {

enum class role : unsigned char { client, server };

struct stream {
    stream() = default;
    stream(stream const&) = delete;
    stream& operator=(stream const&) = delete;
    virtual ~stream() = default;

    template <class MutableBufferSequence>
    task<io_result<std::size_t>> read_some(MutableBufferSequence const& buffers) {
        static_assert(is_mutable_buffer_sequence<MutableBufferSequence>::value,
                      "read_some requires a MutableBufferSequence");
        return do_read_some(mutable_buffer_array<>{buffers});
    }

    template <class ConstBufferSequence>
    task<io_result<std::size_t>> write_some(ConstBufferSequence const& buffers) {
        static_assert(is_const_buffer_sequence<ConstBufferSequence>::value,
                      "write_some requires a ConstBufferSequence");
        return do_write_some(const_buffer_array<>{buffers});
    }

    // 执行握手。前置条件：底层流已连接，没有其它 TLS 操作在进行。失败或成功都消耗流状态：
    // 再次调用等价于先 reset() 再握手。
    virtual task<io_result<>> handshake(role r) = 0;

    // 发送 close_notify 并等待对端的 close_notify。可与一个挂起的读重叠。
    virtual task<io_result<>> shutdown() = 0;

    // 释放会话状态，回到可以再次 handshake() 的状态。前置条件：没有操作在进行。
    virtual void reset() = 0;

    // 下一次客户端握手用的主机名：SNI 与证书校验。IP 字面量只做证书匹配、不发 SNI。空串
    // 关闭两者。跨 reset() 保留。
    virtual void set_hostname(std::string hostname) = 0;

    // 协商出的 ALPN 协议（握手后有效；未协商为空）。
    virtual std::string alpn_selected() const = 0;

    // 类型擦除的底层流（用于取消、取原生句柄等）。不要重新赋值它。
    virtual any_stream& next_layer() noexcept = 0;

  protected:
    virtual task<io_result<std::size_t>> do_read_some(mutable_buffer_array<> buffers) = 0;
    virtual task<io_result<std::size_t>> do_write_some(const_buffer_array<> buffers) = 0;
};

namespace detail {
struct openssl_stream_impl;
struct openssl_stream_impl_deleter {
    void operator()(openssl_stream_impl* impl) const noexcept;
};
} // namespace detail

// OpenSSL / BoringSSL 实现。两种构造：按值（拥有底层流）或按指针（引用，调用方保证生存期）。
struct openssl_stream final : stream {
    template <class S, class = typename std::enable_if<
                           is_stream<S>::value &&
                           not std::is_same<typename std::decay<S>::type, openssl_stream>::value>::type>
    openssl_stream(S next_layer, context const& ctx) : stream_{std::move(next_layer)} {
        init(ctx);
    }

    template <class S, class = typename std::enable_if<is_stream<S>::value>::type>
    openssl_stream(S* const next_layer, context const& ctx) : stream_{next_layer} {
        init(ctx);
    }

    openssl_stream(openssl_stream&& other) noexcept;
    openssl_stream& operator=(openssl_stream&& other) noexcept;
    ~openssl_stream() override;

    task<io_result<>> handshake(role r) override;
    task<io_result<>> shutdown() override;
    void reset() override;
    void set_hostname(std::string hostname) override;
    std::string alpn_selected() const override;
    any_stream& next_layer() noexcept override { return stream_; }

    // 提供者的原生句柄（SSL*）。
    void* native_handle() const noexcept;

  protected:
    task<io_result<std::size_t>> do_read_some(mutable_buffer_array<> buffers) override;
    task<io_result<std::size_t>> do_write_some(const_buffer_array<> buffers) override;

  private:
    void init(context const& ctx);

    any_stream stream_; // 必须在 impl_ 之前：impl 持有它的指针
    std::unique_ptr<detail::openssl_stream_impl, detail::openssl_stream_impl_deleter> impl_;
};

// WriteSink 适配器（source_sink.hpp）的流结束定制点：TLS 的 EOF 是 close_notify（shutdown()）。
// 模板而不是 stream& 重载：对 openssl_stream 这类派生类，两个参数都精确匹配才能压过通用回退。
template <class S, class = typename std::enable_if<std::is_base_of<stream, S>::value>::type>
auto signal_stream_eof(S& secure, net::detail::eof_preferred) CO2_BEG((task<io_result<>>), (secure), io_result<> r;) {
    CO2_AWAIT_SET(r, secure.shutdown());
    CO2_RETURN(r);
}
CO2_END

} // namespace tls
} // namespace net
