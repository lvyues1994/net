#include "net/tls/context.hpp"

#include <cstdint>
#include <cstring>
#include <memory>

#include <openssl/bio.h>
#include <openssl/crypto.h>
#include <openssl/err.h>
#include <openssl/pem.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>
#include <openssl/x509_vfy.h>

#include "tls/context_impl.hpp" // OPENSSL_IS_BORINGSSL / NET_TLS_* 判定要在提供者头之后
#if NET_TLS_OCSP_VERIFY
#include <openssl/ocsp.h>
#endif
#if defined(NET_TLS_WOLFSSL)
#include <wolfssl/ssl.h> // CRL 与 OCSP stapling 的开关没有 OpenSSL 兼容名，用 wolfSSL 原生 API
#endif

#include "net/config.hpp"
#include "net/error.hpp"
#include "net/tls/error.hpp"
#if NET_PLATFORM_WINDOWS
#include "net/detail/socket_types.hpp"
#include <wincrypt.h>
#endif

namespace net {
namespace tls {

namespace {

std::error_code provider_error() noexcept {
    return take_provider_error(std::make_error_code(std::errc::invalid_argument));
}

struct bio_deleter {
    void operator()(BIO* const bio) const noexcept { ::BIO_free(bio); }
};
struct x509_deleter {
    void operator()(X509* const cert) const noexcept { ::X509_free(cert); }
};
struct pkey_deleter {
    void operator()(EVP_PKEY* const key) const noexcept { ::EVP_PKEY_free(key); }
};

using bio_ptr = std::unique_ptr<BIO, bio_deleter>;
using x509_ptr = std::unique_ptr<X509, x509_deleter>;
using pkey_ptr = std::unique_ptr<EVP_PKEY, pkey_deleter>;

bio_ptr memory_bio(std::string const& data) noexcept {
    return bio_ptr{::BIO_new_mem_buf(data.data(), static_cast<int>(data.size()))};
}

// OpenSSL 的 SSL_CTX_set_{min,max}_proto_version 取 int，BoringSSL 取 uint16_t：用 uint16_t 两边都不窄化。
std::uint16_t protocol_version_of(version const v) noexcept {
    return static_cast<std::uint16_t>(v == version::tls_1_3 ? TLS1_3_VERSION : TLS1_2_VERSION);
}

// 口令回调：把 SSL_CTX 的 app-data 找回 context_impl。
int password_thunk(char* const buffer, int const size, int const rwflag, void* const userdata) {
    auto* const impl = static_cast<detail::context_impl*>(userdata);
    if (impl == nullptr || not impl->on_password || size <= 0) return 0;
    auto const purpose = rwflag != 0 ? password_purpose::for_writing : password_purpose::for_reading;
    auto password = impl->on_password(static_cast<std::size_t>(size), purpose);
    if (password.size() > static_cast<std::size_t>(size)) password.resize(static_cast<std::size_t>(size));
    std::memcpy(buffer, password.data(), password.size());
    return static_cast<int>(password.size());
}

// 服务端 ALPN 选择：按本端偏好顺序在客户端列表里挑第一个共有的协议；没有共同协议时不协商。
int alpn_select_thunk(SSL*, unsigned char const** out, unsigned char* outlen, unsigned char const* in,
                      unsigned inlen, void* arg) {
    auto* const impl = static_cast<detail::context_impl*>(arg);
    if (impl == nullptr || impl->alpn_wire.empty()) return SSL_TLSEXT_ERR_NOACK;
    unsigned char* selected = nullptr;
    auto const status = ::SSL_select_next_proto(&selected, outlen, impl->alpn_wire.data(),
                                                static_cast<unsigned>(impl->alpn_wire.size()), in, inlen);
    if (status != OPENSSL_NPN_NEGOTIATED) return SSL_TLSEXT_ERR_NOACK;
    *out = selected;
    return SSL_TLSEXT_ERR_OK;
}

// SNI：按主机名换 SSL_CTX。SSL_set_SSL_CTX 带走证书 / 私钥 / 验证设置；验证模式与回调按 OpenSSL 的建议
// 显式同步。
int servername_thunk(SSL* const ssl, int* const alert, void* const arg) {
    static_cast<void>(alert);
    auto* const impl = static_cast<detail::context_impl*>(arg);
    if (impl == nullptr || not impl->on_servername) return SSL_TLSEXT_ERR_OK;
    auto const* const name = ::SSL_get_servername(ssl, TLSEXT_NAMETYPE_host_name);
    if (name == nullptr) return SSL_TLSEXT_ERR_OK;
    context const* selected = nullptr;
    try {
        selected = impl->on_servername(std::string{name});
    } catch (...) {
        return SSL_TLSEXT_ERR_ALERT_FATAL;
    }
    if (selected == nullptr) return SSL_TLSEXT_ERR_OK;
    auto* const target = static_cast<SSL_CTX*>(selected->native_handle());
    if (target == ::SSL_get_SSL_CTX(ssl)) return SSL_TLSEXT_ERR_OK;
    if (::SSL_set_SSL_CTX(ssl, target) == nullptr) return SSL_TLSEXT_ERR_ALERT_FATAL;
    ::SSL_set_verify(ssl, ::SSL_CTX_get_verify_mode(target), ::SSL_CTX_get_verify_callback(target));
    return SSL_TLSEXT_ERR_OK;
}

// 会话回调：客户端流拿到可复用的会话（TLS 1.3 在 NewSessionTicket 到达时）。返回 1 表示接管引用。
int new_session_thunk(SSL* const ssl, SSL_SESSION* const sess) {
    auto* const hooks = static_cast<detail::stream_hooks*>(::SSL_get_ex_data(ssl, detail::stream_ex_data_index()));
    if (hooks == nullptr || not hooks->is_client()) return 0;
    hooks->on_new_session(sess);
    return 1;
}

// OCSP status 回调：服务端附上响应；客户端验证收到的响应。wolfSSL 只在服务端调用它——客户端的
// staple 由提供者自己验证（见 request_ocsp_stapling）。BoringSSL 两侧都不用它（服务端的响应经
// SSL_CTX_set_ocsp_response 直接给），连编译都不需要：它的 SSL_get_tlsext_status_ocsp_resp 取
// const uint8_t**，与另外两家的签名对不上。
#if !defined(OPENSSL_IS_BORINGSSL)
int ocsp_status_thunk(SSL* const ssl, void* const arg) {
    auto* const impl = static_cast<detail::context_impl*>(arg);
    auto* const hooks = static_cast<detail::stream_hooks*>(::SSL_get_ex_data(ssl, detail::stream_ex_data_index()));
    if (impl == nullptr) return SSL_TLSEXT_ERR_NOACK;
    if (hooks == nullptr || not hooks->is_client()) { // 服务端
        if (impl->ocsp_response.empty()) return SSL_TLSEXT_ERR_NOACK;
        auto* const copy = static_cast<unsigned char*>(::OPENSSL_malloc(impl->ocsp_response.size()));
        if (copy == nullptr) return SSL_TLSEXT_ERR_ALERT_FATAL;
        std::memcpy(copy, impl->ocsp_response.data(), impl->ocsp_response.size());
        ::SSL_set_tlsext_status_ocsp_resp(ssl, copy, static_cast<long>(impl->ocsp_response.size())); // 接管
        return SSL_TLSEXT_ERR_OK;
    }
    // 客户端：0 使握手失败，1 接受。
    unsigned char* raw = nullptr;
    auto const length = ::SSL_get_tlsext_status_ocsp_resp(ssl, &raw);
    if (raw == nullptr || length <= 0) {
        if (not impl->ocsp_required) return 1;
        hooks->set_ocsp_error(make_error_code(error::ocsp_response_missing));
        return 0;
    }
#if !NET_TLS_OCSP_VERIFY
    return 1; // wolfSSL：客户端根本不会走到这里，留着让两种构型都能编译
#else
    auto const invalid = [&] {
        hooks->set_ocsp_error(make_error_code(error::ocsp_response_invalid));
        return 0;
    };
    auto const* p = raw;
    std::unique_ptr<OCSP_RESPONSE, void (*)(OCSP_RESPONSE*)> response{::d2i_OCSP_RESPONSE(nullptr, &p, length), &::OCSP_RESPONSE_free};
    if (not response || ::OCSP_response_status(response.get()) != OCSP_RESPONSE_STATUS_SUCCESSFUL) return invalid();
    std::unique_ptr<OCSP_BASICRESP, void (*)(OCSP_BASICRESP*)> basic{::OCSP_response_get1_basic(response.get()), &::OCSP_BASICRESP_free};
    if (not basic) return invalid();
    auto* const chain = ::SSL_get0_verified_chain(ssl);
    auto* const store = ::SSL_CTX_get_cert_store(::SSL_get_SSL_CTX(ssl));
    if (chain == nullptr || sk_X509_num(chain) == 0 || ::OCSP_basic_verify(basic.get(), chain, store, 0) != 1) return invalid();
    auto* const leaf = sk_X509_value(chain, 0);
    auto* const issuer = sk_X509_num(chain) > 1 ? sk_X509_value(chain, 1) : leaf; // 自签：自己是签发者
    std::unique_ptr<OCSP_CERTID, void (*)(OCSP_CERTID*)> id{::OCSP_cert_to_id(nullptr, leaf, issuer), &::OCSP_CERTID_free};
    if (not id) return invalid();
    auto status = 0;
    auto reason = 0;
    ASN1_GENERALIZEDTIME* revoked_at = nullptr;
    ASN1_GENERALIZEDTIME* this_update = nullptr;
    ASN1_GENERALIZEDTIME* next_update = nullptr;
    if (::OCSP_resp_find_status(basic.get(), id.get(), &status, &reason, &revoked_at, &this_update, &next_update) != 1) return invalid();
    if (status != V_OCSP_CERTSTATUS_GOOD) return invalid();
    if (::OCSP_check_validity(this_update, next_update, 300L, -1L) != 1) return invalid();
    return 1;
#endif
}
#endif

int verify_thunk(int const preverified, X509_STORE_CTX* const store) {
    auto* const ssl = static_cast<SSL*>(::X509_STORE_CTX_get_ex_data(store, ::SSL_get_ex_data_X509_STORE_CTX_idx()));
    if (ssl == nullptr) return preverified;
    auto* const impl = detail::context_impl::from_ssl_ctx(::SSL_get_SSL_CTX(ssl));
    if (impl == nullptr || not impl->on_verify) return preverified;
    verify_context ctx{store};
    return impl->on_verify(preverified != 0, ctx) ? 1 : 0;
}

} // namespace

// ---- verify_context ----

int verify_context::error() const noexcept {
    return ::X509_STORE_CTX_get_error(static_cast<X509_STORE_CTX*>(handle_));
}

std::string verify_context::error_string() const {
    auto const* const text = ::X509_verify_cert_error_string(error());
    return text != nullptr ? text : "";
}

int verify_context::depth() const noexcept {
    return ::X509_STORE_CTX_get_error_depth(static_cast<X509_STORE_CTX*>(handle_));
}

std::string verify_context::subject() const {
    auto* const cert = ::X509_STORE_CTX_get_current_cert(static_cast<X509_STORE_CTX*>(handle_));
    if (cert == nullptr) return {};
    char text[512];
    ::X509_NAME_oneline(::X509_get_subject_name(cert), text, static_cast<int>(sizeof(text)));
    return text;
}

// ---- context_impl ----

namespace detail {

int context_impl::ex_data_index() noexcept {
    static int const index = ::SSL_CTX_get_ex_new_index(0, nullptr, nullptr, nullptr, nullptr);
    return index;
}

context_impl* context_impl::from_ssl_ctx(SSL_CTX* const ssl_ctx) noexcept {
    return static_cast<context_impl*>(::SSL_CTX_get_ex_data(ssl_ctx, ex_data_index()));
}

context_impl::context_impl() {
    ctx = ::SSL_CTX_new(::TLS_method());
    if (ctx == nullptr) throw std::system_error{provider_error(), "SSL_CTX_new"};
    ::SSL_CTX_set_ex_data(ctx, ex_data_index(), this);
    ::SSL_CTX_set_min_proto_version(ctx, TLS1_2_VERSION);
    ::SSL_CTX_set_options(ctx, SSL_OP_NO_COMPRESSION);
#ifdef SSL_OP_NO_RENEGOTIATION
    // 没有重协商，握手之后写方向永远不需要读传输：读与写可以并发。
    ::SSL_CTX_set_options(ctx, SSL_OP_NO_RENEGOTIATION);
#endif
    ::SSL_CTX_set_mode(ctx, SSL_MODE_ENABLE_PARTIAL_WRITE | SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);
    // 客户端会话经 new-session 回调交给流，不进内部缓存；服务端靠无状态的会话票据恢复，不需要缓存。
    ::SSL_CTX_set_session_cache_mode(ctx, SSL_SESS_CACHE_CLIENT | SSL_SESS_CACHE_NO_INTERNAL_STORE);
    ::SSL_CTX_sess_set_new_cb(ctx, &new_session_thunk);
    static unsigned char const session_id_context[] = "net";
    ::SSL_CTX_set_session_id_context(ctx, session_id_context, sizeof(session_id_context) - 1U);
    ::SSL_CTX_set_verify(ctx, SSL_VERIFY_NONE, nullptr);
    ::SSL_CTX_set_default_passwd_cb(ctx, &password_thunk);
    ::SSL_CTX_set_default_passwd_cb_userdata(ctx, this);
#if defined(OPENSSL_IS_BORINGSSL)
    ::SSL_CTX_set_tlsext_servername_callback(ctx, &servername_thunk);
    ::SSL_CTX_set_tlsext_servername_arg(ctx, this);
#elif defined(NET_TLS_WOLFSSL)
    ::SSL_CTX_set_tlsext_servername_callback(ctx, &servername_thunk);
    ::SSL_CTX_set_tlsext_servername_arg(ctx, this);
    // 顺序有讲究：装 status 回调会顺带打开 OCSP stapling，而打开 stapling 会把回调的 arg
    // （wolfSSL 内部就是 OCSP 的 IO 上下文 cm->ocspIOCtx）重置掉。arg 必须最后设，此后不能再调
    // wolfSSL_CTX_EnableOCSPStapling——否则服务端的回调拿到空 arg，直接不附响应。
    ::SSL_CTX_set_tlsext_status_cb(ctx, &ocsp_status_thunk);
    ::SSL_CTX_set_tlsext_status_arg(ctx, this);
#else
    // OpenSSL 的 SSL_CTX_set_tlsext_*_cb 是带 C 风格转换的宏；直接调 callback_ctrl。
    ::SSL_CTX_callback_ctrl(ctx, SSL_CTRL_SET_TLSEXT_SERVERNAME_CB, reinterpret_cast<void (*)()>(&servername_thunk));
    ::SSL_CTX_ctrl(ctx, SSL_CTRL_SET_TLSEXT_SERVERNAME_ARG, 0, this);
    ::SSL_CTX_callback_ctrl(ctx, SSL_CTRL_SET_TLSEXT_STATUS_REQ_CB, reinterpret_cast<void (*)()>(&ocsp_status_thunk));
    ::SSL_CTX_ctrl(ctx, SSL_CTRL_SET_TLSEXT_STATUS_REQ_CB_ARG, 0, this);
#endif
}

int stream_ex_data_index() noexcept {
    static int const index = ::SSL_get_ex_new_index(0, nullptr, nullptr, nullptr, nullptr);
    return index;
}

session session_access::make(SSL_SESSION* const owned) noexcept {
    session s;
    if (owned != nullptr) s.handle_ = std::shared_ptr<void>{owned, [](void* const p) { ::SSL_SESSION_free(static_cast<SSL_SESSION*>(p)); }};
    return s;
}

context_impl::~context_impl() {
    if (ctx != nullptr) ::SSL_CTX_free(ctx);
}

} // namespace detail

// ---- context ----

context::context() : impl_{std::make_shared<detail::context_impl>()} {}

context::~context() = default;

void* context::native_handle() const noexcept { return impl_->ctx; }

std::error_code context::use_certificate(std::string const& certificate, file_format const format) {
    ::ERR_clear_error();
    auto bio = memory_bio(certificate);
    if (not bio) return provider_error();
    auto cert = x509_ptr{format == file_format::pem ? ::PEM_read_bio_X509(bio.get(), nullptr, nullptr, nullptr)
                                                     : ::d2i_X509_bio(bio.get(), nullptr)};
    if (not cert) return provider_error();
    if (::SSL_CTX_use_certificate(impl_->ctx, cert.get()) != 1) return provider_error();
    return {};
}

std::error_code context::use_certificate_file(std::string const& filename, file_format const format) {
    ::ERR_clear_error();
    if (::SSL_CTX_use_certificate_file(impl_->ctx, filename.c_str(),
                                       format == file_format::pem ? SSL_FILETYPE_PEM : SSL_FILETYPE_ASN1) != 1)
        return provider_error();
    return {};
}

std::error_code context::use_certificate_chain(std::string const& chain) {
    ::ERR_clear_error();
    auto bio = memory_bio(chain);
    if (not bio) return provider_error();
    auto leaf = x509_ptr{::PEM_read_bio_X509_AUX(bio.get(), nullptr, nullptr, nullptr)};
    if (not leaf) return provider_error();
    if (::SSL_CTX_use_certificate(impl_->ctx, leaf.get()) != 1) return provider_error();
    ::SSL_CTX_clear_chain_certs(impl_->ctx);
    for (;;) {
        auto* const intermediate = ::PEM_read_bio_X509(bio.get(), nullptr, nullptr, nullptr);
        if (intermediate == nullptr) break;
        if (::SSL_CTX_add0_chain_cert(impl_->ctx, intermediate) != 1) {
            ::X509_free(intermediate);
            return provider_error();
        }
    }
    // 读到链尾是正常的 PEM 结束，不是错误。
    ::ERR_clear_error();
    return {};
}

std::error_code context::use_certificate_chain_file(std::string const& filename) {
    ::ERR_clear_error();
    if (::SSL_CTX_use_certificate_chain_file(impl_->ctx, filename.c_str()) != 1) return provider_error();
    return {};
}

std::error_code context::use_private_key(std::string const& private_key, file_format const format) {
    ::ERR_clear_error();
    auto bio = memory_bio(private_key);
    if (not bio) return provider_error();
    auto key = pkey_ptr{format == file_format::pem
                            ? ::PEM_read_bio_PrivateKey(bio.get(), nullptr, &password_thunk, impl_.get())
                            : ::d2i_PrivateKey_bio(bio.get(), nullptr)};
    if (not key) return provider_error();
    if (::SSL_CTX_use_PrivateKey(impl_->ctx, key.get()) != 1) return provider_error();
    return {};
}

std::error_code context::use_private_key_file(std::string const& filename, file_format const format) {
    ::ERR_clear_error();
    if (::SSL_CTX_use_PrivateKey_file(impl_->ctx, filename.c_str(),
                                      format == file_format::pem ? SSL_FILETYPE_PEM : SSL_FILETYPE_ASN1) != 1)
        return provider_error();
    return {};
}

std::error_code context::add_certificate_authority(std::string const& certificate_pem) {
    ::ERR_clear_error();
    auto bio = memory_bio(certificate_pem);
    if (not bio) return provider_error();
    auto* const store = ::SSL_CTX_get_cert_store(impl_->ctx);
    auto added = 0;
    for (;;) {
        auto cert = x509_ptr{::PEM_read_bio_X509(bio.get(), nullptr, nullptr, nullptr)};
        if (not cert) break;
        if (::X509_STORE_add_cert(store, cert.get()) != 1) return provider_error();
        ++added;
    }
    ::ERR_clear_error();
    if (added == 0) return std::make_error_code(std::errc::invalid_argument);
    return {};
}

std::error_code context::load_verify_file(std::string const& filename) {
    ::ERR_clear_error();
    if (::SSL_CTX_load_verify_locations(impl_->ctx, filename.c_str(), nullptr) != 1) return provider_error();
    return {};
}

std::error_code context::add_verify_path(std::string const& path) {
    ::ERR_clear_error();
    if (::SSL_CTX_load_verify_locations(impl_->ctx, nullptr, path.c_str()) != 1) return provider_error();
    return {};
}

std::error_code context::set_default_verify_paths() {
    ::ERR_clear_error();
    if (::SSL_CTX_set_default_verify_paths(impl_->ctx) != 1) return provider_error();
    return {};
}

std::error_code context::set_min_protocol_version(version const v) {
    ::ERR_clear_error();
    if (::SSL_CTX_set_min_proto_version(impl_->ctx, protocol_version_of(v)) != 1) return provider_error();
    return {};
}

std::error_code context::set_max_protocol_version(version const v) {
    ::ERR_clear_error();
    if (::SSL_CTX_set_max_proto_version(impl_->ctx, protocol_version_of(v)) != 1) return provider_error();
    return {};
}

std::error_code context::set_ciphersuites(std::string const& ciphers) {
    ::ERR_clear_error();
    if (::SSL_CTX_set_cipher_list(impl_->ctx, ciphers.c_str()) != 1) return provider_error();
    return {};
}

std::error_code context::set_ciphersuites_tls13(std::string const& ciphers) {
#if defined(OPENSSL_IS_BORINGSSL)
    static_cast<void>(ciphers);
    return std::make_error_code(std::errc::function_not_supported); // BoringSSL 固定 TLS 1.3 套件
#else
    ::ERR_clear_error();
    if (::SSL_CTX_set_ciphersuites(impl_->ctx, ciphers.c_str()) != 1) return provider_error();
    return {};
#endif
}

std::error_code context::set_alpn(std::vector<std::string> const& protocols) {
    std::vector<unsigned char> wire;
    for (auto const& protocol : protocols) {
        if (protocol.empty() || protocol.size() > 255U) return std::make_error_code(std::errc::invalid_argument);
        wire.push_back(static_cast<unsigned char>(protocol.size()));
        wire.insert(wire.end(), protocol.begin(), protocol.end());
    }
    ::ERR_clear_error();
    // 注意：OpenSSL 的这个函数成功返回 0。客户端用它提供列表；服务端经选择回调按本端偏好挑选。
    if (::SSL_CTX_set_alpn_protos(impl_->ctx, wire.data(), static_cast<unsigned>(wire.size())) != 0)
        return provider_error();
    impl_->alpn_wire = std::move(wire);
    ::SSL_CTX_set_alpn_select_cb(impl_->ctx, &alpn_select_thunk, impl_.get());
    return {};
}

std::error_code context::set_verify_mode(verify_mode const mode) {
    impl_->mode = mode;
    int flags = SSL_VERIFY_NONE;
    if (mode == verify_mode::peer) flags = SSL_VERIFY_PEER;
    if (mode == verify_mode::require_peer) flags = SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT;
    ::SSL_CTX_set_verify(impl_->ctx, flags, impl_->on_verify ? &verify_thunk : nullptr);
    return {};
}

std::error_code context::set_verify_depth(int const depth) {
    ::SSL_CTX_set_verify_depth(impl_->ctx, depth);
    return {};
}

void context::set_verify_callback(verify_callback callback) {
    impl_->on_verify = std::move(callback);
    set_verify_mode(impl_->mode); // 重新安装回调
}

void context::set_password_callback(password_callback callback) { impl_->on_password = std::move(callback); }

// ---- SNI ----

void context::set_servername_callback(servername_callback callback) { impl_->on_servername = std::move(callback); }

// ---- 证书库与吊销 ----

std::error_code context::add_os_certificates() {
#if NET_PLATFORM_WINDOWS
    auto* const store = ::SSL_CTX_get_cert_store(impl_->ctx);
    auto const system_store = ::CertOpenSystemStoreW(0, L"ROOT");
    if (system_store == nullptr) return std::error_code{static_cast<int>(::GetLastError()), std::system_category()};
    PCCERT_CONTEXT entry = nullptr;
    auto added = 0;
    while ((entry = ::CertEnumCertificatesInStore(system_store, entry)) != nullptr) {
        auto const* der = entry->pbCertEncoded;
        std::unique_ptr<X509, void (*)(X509*)> cert{::d2i_X509(nullptr, &der, static_cast<long>(entry->cbCertEncoded)), &::X509_free};
        if (cert && ::X509_STORE_add_cert(store, cert.get()) == 1) ++added;
        ::ERR_clear_error(); // 重复证书等非致命错误
    }
    ::CertCloseStore(system_store, 0);
    return added > 0 ? std::error_code{} : std::make_error_code(std::errc::no_such_file_or_directory);
#else
    return set_default_verify_paths();
#endif
}

std::error_code context::add_crl(std::string const& crl_pem) {
    std::unique_ptr<BIO, void (*)(BIO*)> bio{::BIO_new_mem_buf(crl_pem.data(), static_cast<int>(crl_pem.size())), &::BIO_free_all};
    if (not bio) return provider_error();
    auto* const store = ::SSL_CTX_get_cert_store(impl_->ctx);
    auto added = 0;
    for (;;) {
        std::unique_ptr<X509_CRL, void (*)(X509_CRL*)> crl{::PEM_read_bio_X509_CRL(bio.get(), nullptr, nullptr, nullptr), &::X509_CRL_free};
        if (not crl) break;
        if (::X509_STORE_add_crl(store, crl.get()) != 1) return provider_error();
        ++added;
    }
    ::ERR_clear_error(); // PEM 读到末尾的"no start line"
    if (added == 0) return std::make_error_code(std::errc::invalid_argument);
#if defined(NET_TLS_WOLFSSL)
    return set_crl_check(impl_->crl_mode); // wolfSSL 装入 CRL 就开始检查：按当前模式重设
#else
    return {};
#endif
}

std::error_code context::set_crl_check(crl_check const mode) {
    impl_->crl_mode = mode;
#if defined(NET_TLS_WOLFSSL)
    auto* const manager = ::wolfSSL_CTX_GetCertManager(impl_->ctx);
    if (manager == nullptr) return provider_error();
    if (mode == crl_check::none) {
        if (::wolfSSL_CertManagerDisableCRL(manager) != WOLFSSL_SUCCESS) return provider_error();
        return {};
    }
    if (::wolfSSL_CertManagerEnableCRL(manager, mode == crl_check::chain ? WOLFSSL_CRL_CHECKALL : WOLFSSL_CRL_CHECK) != WOLFSSL_SUCCESS)
        return provider_error();
    return {};
#else
    auto* const store = ::SSL_CTX_get_cert_store(impl_->ctx);
    ::X509_STORE_set_flags(store, 0); // 先清掉再按需置位
    auto* const param = ::X509_STORE_get0_param(store);
    ::X509_VERIFY_PARAM_clear_flags(param, X509_V_FLAG_CRL_CHECK | X509_V_FLAG_CRL_CHECK_ALL);
    if (mode == crl_check::none) return {};
    auto flags = static_cast<unsigned long>(X509_V_FLAG_CRL_CHECK);
    if (mode == crl_check::chain) flags |= X509_V_FLAG_CRL_CHECK_ALL;
    if (::X509_STORE_set_flags(store, flags) != 1) return provider_error();
    return {};
#endif
}

// ---- OCSP stapling ----

std::error_code context::set_ocsp_response(std::string der_response) {
    impl_->ocsp_response = std::move(der_response);
#if defined(OPENSSL_IS_BORINGSSL)
    if (::SSL_CTX_set_ocsp_response(impl_->ctx, reinterpret_cast<std::uint8_t const*>(impl_->ocsp_response.data()),
                                    impl_->ocsp_response.size()) != 1)
        return provider_error();
#endif
    // wolfSSL：服务端的 stapling 开关与 status 回调在 context_impl 的构造里就位，这里只存响应。
    return {};
}

std::error_code context::request_ocsp_stapling(bool const require) {
    impl_->ocsp_requested = true;
    impl_->ocsp_required = require;
#if defined(OPENSSL_IS_BORINGSSL)
    ::SSL_CTX_enable_ocsp_stapling(impl_->ctx);
#elif defined(NET_TLS_WOLFSSL)
    // wolfSSL 自己验证收到的 staple（签名、对应本证书、状态、有效期，全部离线），并自己实施
    // must-staple；扩展本身是每个 SSL 在握手前挂上的（见 openssl_stream 的 handshake）。
    if (require && ::wolfSSL_CTX_EnableOCSPMustStaple(impl_->ctx) != WOLFSSL_SUCCESS) return provider_error();
#else
    if (::SSL_CTX_set_tlsext_status_type(impl_->ctx, TLSEXT_STATUSTYPE_ocsp) != 1) return provider_error();
#endif
    return {};
}

// ---- 提供者 ----

char const* provider_name() noexcept {
#if defined(OPENSSL_IS_BORINGSSL)
    return "BoringSSL";
#else
    return ::OpenSSL_version(OPENSSL_VERSION); // wolfSSL 的兼容层给出 "wolfSSL x.y.z"
#endif
}

bool is_boringssl() noexcept {
#if defined(OPENSSL_IS_BORINGSSL)
    return true;
#else
    return false;
#endif
}

bool is_wolfssl() noexcept {
#if defined(NET_TLS_WOLFSSL)
    return true;
#else
    return false;
#endif
}

} // namespace tls
} // namespace net
