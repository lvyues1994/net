#pragma once

#include <system_error>

// TLS 错误分类：
//   - provider_category()：提供者（OpenSSL / BoringSSL）的错误队列码（ERR_get_error），文字来自
//     ERR_error_string；
//   - verify_category()：证书验证结果（X509_V_*），文字来自 X509_verify_cert_error_string。
// 引擎级条件（流被截断、操作被取消）用 net::error / std::errc 报告。

namespace net {
namespace tls {

std::error_category const& provider_category() noexcept;
std::error_category const& verify_category() noexcept;

// 把提供者错误队列里最早的一条错误取出为 error_code（队列为空时返回 fallback），并清空队列。
std::error_code take_provider_error(std::error_code fallback) noexcept;

} // namespace tls
} // namespace net
