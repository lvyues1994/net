#pragma once

#include <mutex>
#include <string>
#include <vector>

#include <openssl/ssl.h>

#include "net/tls/context.hpp"

// tls::context 的实现：一个 SSL_CTX + 回调状态。OpenSSL 与 BoringSSL 共用（OpenSSL API 子集）。
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
