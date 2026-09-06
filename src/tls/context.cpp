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

#include "net/tls/error.hpp"

#include "tls/context_impl.hpp"

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
    ::SSL_CTX_set_session_cache_mode(ctx, SSL_SESS_CACHE_OFF);
    ::SSL_CTX_set_verify(ctx, SSL_VERIFY_NONE, nullptr);
    ::SSL_CTX_set_default_passwd_cb(ctx, &password_thunk);
    ::SSL_CTX_set_default_passwd_cb_userdata(ctx, this);
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
    auto flags = SSL_VERIFY_NONE;
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

// ---- 提供者 ----

char const* provider_name() noexcept {
#if defined(OPENSSL_IS_BORINGSSL)
    return "BoringSSL";
#else
    return ::OpenSSL_version(OPENSSL_VERSION);
#endif
}

bool is_boringssl() noexcept {
#if defined(OPENSSL_IS_BORINGSSL)
    return true;
#else
    return false;
#endif
}

} // namespace tls
} // namespace net
