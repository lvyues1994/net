#include "net/tls/error.hpp"

#include <string>

#include <openssl/err.h>
#include <openssl/x509.h>
#include <openssl/x509_vfy.h>

namespace net {
namespace tls {

namespace {

struct provider_error_category final : std::error_category {
    char const* name() const noexcept override { return "tls.provider"; }

    std::string message(int const value) const override {
        char text[256];
        // 错误码是 32 位无符号打包值，经 int 往返时必须先转回无符号再扩展。
        ::ERR_error_string_n(static_cast<unsigned long>(static_cast<unsigned>(value)), text, sizeof(text));
        return text;
    }
};

struct verify_error_category final : std::error_category {
    char const* name() const noexcept override { return "tls.verify"; }

    std::string message(int const value) const override {
        auto const* const text = ::X509_verify_cert_error_string(static_cast<long>(value));
        return text != nullptr ? text : ("certificate verify error " + std::to_string(value));
    }
};

} // namespace

std::error_category const& provider_category() noexcept {
    static provider_error_category const category;
    return category;
}

std::error_category const& verify_category() noexcept {
    static verify_error_category const category;
    return category;
}

std::error_code take_provider_error(std::error_code const fallback) noexcept {
    auto const code = ::ERR_get_error();
    ::ERR_clear_error();
    if (code == 0U) return fallback;
    return std::error_code{static_cast<int>(static_cast<unsigned>(code)), provider_category()};
}

} // namespace tls
} // namespace net
