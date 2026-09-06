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
    std::vector<unsigned char> alpn_wire; // ALPN 线格式（长度前缀）
    verify_mode mode = verify_mode::none;
};

} // namespace detail
} // namespace tls
} // namespace net
