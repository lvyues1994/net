#pragma once

#include <mutex>
#include <string>
#include <vector>

#include <openssl/ssl.h>

#include "net/tls/context.hpp"

// 提供者的 OCSP stapling 能力差异（三选一）：
//   NET_TLS_OCSP_VERIFY  本库自己解析并验证服务端附上的响应——OpenSSL 有完整的 OCSP API；
//   NET_TLS_OCSP_NATIVE  提供者自己验证 staple 并实施 must-staple——wolfSSL，本库不碰响应；
//   两者都为 0           只传输——BoringSSL 没有 OCSP 解析 API，响应交给调用方，require 在
//                        握手后按"有没有响应"判定。
#if defined(NET_TLS_WOLFSSL)
#define NET_TLS_OCSP_VERIFY 0
#define NET_TLS_OCSP_NATIVE 1
#elif defined(OPENSSL_IS_BORINGSSL)
#define NET_TLS_OCSP_VERIFY 0
#define NET_TLS_OCSP_NATIVE 0
#else
#define NET_TLS_OCSP_VERIFY 1
#define NET_TLS_OCSP_NATIVE 0
#endif

// tls::context 的实现：一个 SSL_CTX + 回调状态。三个提供者共用（OpenSSL API 子集；wolfSSL 经它的
// OpenSSL 兼容层）。
// 回调经 SSL_CTX 的 app-data（ex_data）找回本对象。

namespace net {
namespace tls {
namespace detail {

struct context_impl {
    context_impl();
    context_impl(context_impl const&) = delete;
    context_impl& operator=(context_impl const&) = delete;
    ~context_impl();

    static context_impl* from_ssl_ctx(SSL_CTX* ctx) noexcept;
    static int ex_data_index() noexcept;

    SSL_CTX* ctx = nullptr;
    verify_callback on_verify;
    password_callback on_password;
    servername_callback on_servername;
    std::vector<unsigned char> alpn_wire; // ALPN 线格式（长度前缀）
    verify_mode mode = verify_mode::none;
    crl_check crl_mode = crl_check::none;
    std::string ocsp_response;   // 服务端：随握手附上的 DER 响应
    bool ocsp_requested = false; // 客户端：请求 stapling
    bool ocsp_required = false;  // 客户端：没有响应就失败
};

// tls::session 的私有入口：从 SSL_SESSION* 构造（接管一个引用）。
struct session_access {
    static session make(SSL_SESSION* owned) noexcept;
};

// 流一侧在 SSL 对象上登记自己（会话回调 / OCSP 回调经此找回流）。
int stream_ex_data_index() noexcept;
struct stream_hooks {
    virtual ~stream_hooks() = default;
    virtual void on_new_session(SSL_SESSION* owned) noexcept = 0; // 接管引用
    virtual bool is_client() const noexcept = 0;
    virtual void set_ocsp_error(std::error_code ec) noexcept = 0;
};

} // namespace detail
} // namespace tls
} // namespace net
