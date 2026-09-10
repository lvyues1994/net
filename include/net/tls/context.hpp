#pragma once

#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <system_error>
#include <vector>

// TLS 上下文（P4100R1 §8.8 Paper 14：传输安全包装器；形态取自 Corosio 的 tls_context）：
// 证书、私钥、信任锚、验证模式、协议版本、密码套件、ALPN 与回调。它对提供者中立——
// 同一份 API 由 OpenSSL、BoringSSL 或 wolfSSL 实现（构建期 NET_TLS_PROVIDER 选择，三者共用
// OpenSSL API 子集，wolfSSL 经它的 OpenSSL 兼容层）；公共头不包含任何 OpenSSL 头。
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
struct session_access;
} // namespace detail

struct context;

// SNI 服务端回调：按客户端发来的主机名选另一个 context（证书 / 密钥 / 验证设置随之切换）。返回空
// 指针保持当前 context。被选中的 context 必须活到握手结束。
using servername_callback = std::function<context const*(std::string const& hostname)>;

// 吊销检查范围（CRL）：只查叶子证书，或整条链。
enum class crl_check : unsigned char { none, leaf, chain };

// 会话复用的句柄：客户端握手完成（TLS 1.3：收到 NewSessionTicket）后从流上取得，交给下一条连接的
// stream::set_session 做会话恢复（省一次完整握手）。可复制、可跨线程传递；不透明。
struct session {
    session() noexcept = default;
    bool valid() const noexcept { return handle_ != nullptr; }
    explicit operator bool() const noexcept { return valid(); }
    void* native_handle() const noexcept { return handle_.get(); } // SSL_SESSION*

  private:
    friend struct detail::session_access;
    std::shared_ptr<void> handle_;
};

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
    // TLS 1.3 密码套件。BoringSSL 不允许配置，返回 std::errc::function_not_supported；wolfSSL 只有
    // 一张统一的套件表，这里与 set_ciphersuites 是同一个设置。
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

    // ---- SNI 服务端 ----
    void set_servername_callback(servername_callback callback);

    // ---- 证书库与吊销 ----
    // 操作系统的根证书库：Windows 用 CertOpenSystemStore("ROOT")，其它平台是提供者的默认路径。
    std::error_code add_os_certificates();
    // 加入一份或多份 PEM 编码的 CRL；set_crl_check 决定验证时是否使用。
    std::error_code add_crl(std::string const& crl_pem);
    std::error_code set_crl_check(crl_check mode);

    // ---- OCSP stapling ----
    // 服务端：随握手附上的 DER 编码 OCSP 响应（由部署方定期从 OCSP 响应方取得）。
    std::error_code set_ocsp_response(std::string der_response);
    // 客户端：在握手里请求 stapling。收到的响应会被验证（签名、对应本证书、状态 good、有效期），
    // 不通过则握手以 ocsp_response_invalid 失败；require 为真时没有响应以 ocsp_response_missing
    // 失败。OpenSSL 上验证由本库做，wolfSSL 上由提供者自己做（因此 stream::ocsp_response() 在
    // wolfSSL 的客户端上为空——提供者不交出原始响应）。BoringSSL 没有 OCSP 解析 API：只传输，
    // 响应经 stream::ocsp_response() 交给调用方验证，require 只判定"有没有响应"。
    std::error_code request_ocsp_stapling(bool require);

    // 提供者的原生句柄（OpenSSL / BoringSSL：SSL_CTX*；wolfSSL：WOLFSSL_CTX*）。
    void* native_handle() const noexcept;

    detail::context_impl& impl() const noexcept { return *impl_; }

  private:
    std::shared_ptr<detail::context_impl> impl_;
};

// 链接的提供者："OpenSSL 3.0.13" / "BoringSSL" / "wolfSSL 5.8.2"。
char const* provider_name() noexcept;
bool is_boringssl() noexcept;
bool is_wolfssl() noexcept;

} // namespace tls
} // namespace net
