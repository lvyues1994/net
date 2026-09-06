#pragma once

#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <system_error>
#include <vector>

// TLS 上下文（P4100R1 §8.8 Paper 14：传输安全包装器；形态取自 Corosio 的 tls_context）：
// 证书、私钥、信任锚、验证模式、协议版本、密码套件、ALPN 与回调。它对提供者中立——
// 同一份 API 由 OpenSSL 或 BoringSSL 实现（构建期 NET_TLS_PROVIDER 选择，两者共用
// OpenSSL API 子集）；公共头不包含任何 OpenSSL 头。
//
// context 是共享句柄：拷贝共享同一份配置（与 SSL_CTX 相同）。设置在调用时即刻应用并
// 返回错误；之后创建的 tls::stream 使用当时的配置。

namespace net {
namespace tls {

enum class version : unsigned char { tls_1_2, tls_1_3 };
enum class file_format : unsigned char { pem, der };
enum class verify_mode : unsigned char { none, peer, require_peer };
enum class password_purpose : unsigned char { for_reading, for_writing };

// 验证回调看到的上下文：native_handle() 是 X509_STORE_CTX*。
struct verify_context {
    explicit verify_context(void* const handle) noexcept : handle_{handle} {}

    void* native_handle() const noexcept { return handle_; }
    // 当前验证错误（X509_V_*）及其文字。
    int error() const noexcept;
    std::string error_string() const;
    // 当前证书在链中的深度（0 = 叶）。
    int depth() const noexcept;
    // 当前证书的主题名（一行文本）。
    std::string subject() const;

  private:
    void* handle_;
};

using verify_callback = std::function<bool(bool preverified, verify_context& ctx)>;
using password_callback = std::function<std::string(std::size_t max_length, password_purpose purpose)>;

namespace detail {
struct context_impl;
}

struct context {
    // 默认：TLS 1.2+，验证模式 none，无证书，无信任锚。
    context();
    context(context const&) noexcept = default;
    context(context&&) noexcept = default;
    context& operator=(context const&) noexcept = default;
    context& operator=(context&&) noexcept = default;
    ~context();

    // ---- 本端身份 ----
    std::error_code use_certificate(std::string const& certificate, file_format format);
    std::error_code use_certificate_file(std::string const& filename, file_format format);
    // PEM 链：第一张是本端证书，其余是中间证书。
    std::error_code use_certificate_chain(std::string const& chain);
    std::error_code use_certificate_chain_file(std::string const& filename);
    std::error_code use_private_key(std::string const& private_key, file_format format);
    std::error_code use_private_key_file(std::string const& filename, file_format format);

    // ---- 信任锚 ----
    std::error_code add_certificate_authority(std::string const& certificate_pem);
    std::error_code load_verify_file(std::string const& filename);
    std::error_code add_verify_path(std::string const& path);
    std::error_code set_default_verify_paths();

    // ---- 协商 ----
    std::error_code set_min_protocol_version(version v);
    std::error_code set_max_protocol_version(version v);
    // TLS 1.2 及以下的密码套件（OpenSSL 语法）。
    std::error_code set_ciphersuites(std::string const& ciphers);
    // TLS 1.3 密码套件。BoringSSL 不允许配置，返回 std::errc::function_not_supported。
    std::error_code set_ciphersuites_tls13(std::string const& ciphers);
    // ALPN 协议列表，按偏好排序（例如 {"h2", "http/1.1"}）。
    std::error_code set_alpn(std::vector<std::string> const& protocols);

    // ---- 验证 ----
    std::error_code set_verify_mode(verify_mode mode);
    std::error_code set_verify_depth(int depth);
    // 每张证书验证后调用；返回 false 使握手失败。
    void set_verify_callback(verify_callback callback);
    // 读取加密私钥时提供口令。
    void set_password_callback(password_callback callback);

    // 提供者的原生句柄（OpenSSL / BoringSSL：SSL_CTX*）。
    void* native_handle() const noexcept;

    detail::context_impl& impl() const noexcept { return *impl_; }

  private:
    std::shared_ptr<detail::context_impl> impl_;
};

// 链接的提供者："OpenSSL 3.0.13" / "BoringSSL"。
char const* provider_name() noexcept;
bool is_boringssl() noexcept;

} // namespace tls
} // namespace net
