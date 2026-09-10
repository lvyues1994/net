#pragma once

#include <cstring>
#include <string>

// 测试用的自签名证书（10 年有效期，openssl req 生成）。
//   server：CN=localhost，SAN = DNS:localhost, IP:127.0.0.1, IP:::1，可作 CA（自签）
//   other： CN=other.example，用于验证"证书不匹配主机名 / 不受信任"的失败路径
// 只用于测试，不要在别处使用。

namespace net_test_certs {

inline char const* server_certificate() noexcept {
    return
        "-----BEGIN CERTIFICATE-----\n"
        "MIIB5DCCAYugAwIBAgIUPv8b4Nk4W7DVcMYVNc+5NRR8YfQwCgYIKoZIzj0EAwIw\n"
        "KDESMBAGA1UEAwwJbG9jYWxob3N0MRIwEAYDVQQKDAluZXQgdGVzdHMwHhcNMjYw\n"
        "OTA2MDIzMTA1WhcNMzYwOTAzMDIzMTA1WjAoMRIwEAYDVQQDDAlsb2NhbGhvc3Qx\n"
        "EjAQBgNVBAoMCW5ldCB0ZXN0czBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABCEH\n"
        "eiSd4YrreyqLk9bqkDIvmBrzHtx1/VcFNhUC6OqGc8Ix/FeAcC/cp/CXaGjiSkoD\n"
        "iIgLwQu7lVSqUv4zVDWjgZIwgY8wHQYDVR0OBBYEFI5U08T2fY73bIJcqZi8n837\n"
        "TA9nMB8GA1UdIwQYMBaAFI5U08T2fY73bIJcqZi8n837TA9nMCwGA1UdEQQlMCOC\n"
        "CWxvY2FsaG9zdIcEfwAAAYcQAAAAAAAAAAAAAAAAAAAAATAPBgNVHRMBAf8EBTAD\n"
        "AQH/MA4GA1UdDwEB/wQEAwIChDAKBggqhkjOPQQDAgNHADBEAiAPqOSbthFsgfwz\n"
        "H6BMngMG9MS/itmIpuxJGk6PIaE67wIgBinH+UYHHCTr93d59syUmpeOmB/hyVVv\n"
        "VxqlHiensjM=\n"
        "-----END CERTIFICATE-----\n";
}

inline char const* server_private_key() noexcept {
    return
        "-----BEGIN PRIVATE KEY-----\n"
        "MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgFVdC64g6uIBjXW6l\n"
        "QURABGdYHQWqqJBjGvYrmjUEstKhRANCAAQhB3okneGK63sqi5PW6pAyL5ga8x7c\n"
        "df1XBTYVAujqhnPCMfxXgHAv3Kfwl2ho4kpKA4iIC8ELu5VUqlL+M1Q1\n"
        "-----END PRIVATE KEY-----\n";
}

inline char const* other_certificate() noexcept {
    return
        "-----BEGIN CERTIFICATE-----\n"
        "MIIBxzCCAW2gAwIBAgIUTYrdwLXNnpTW8+kzK5zwVZjQn3owCgYIKoZIzj0EAwIw\n"
        "LDEWMBQGA1UEAwwNb3RoZXIuZXhhbXBsZTESMBAGA1UECgwJbmV0IHRlc3RzMB4X\n"
        "DTI2MDkwNjAyMzEwNVoXDTM2MDkwMzAyMzEwNVowLDEWMBQGA1UEAwwNb3RoZXIu\n"
        "ZXhhbXBsZTESMBAGA1UECgwJbmV0IHRlc3RzMFkwEwYHKoZIzj0CAQYIKoZIzj0D\n"
        "AQcDQgAEXUqxU7TwOFfPd3joj63TNAcPMGH5hZcHpByK2AwiaGeEvc+Y0pklpXIV\n"
        "JuuEBTR1Ct2gl6Aw3fMnsXKx1MEuFaNtMGswHQYDVR0OBBYEFAFLPZr3CUK6nBuv\n"
        "+2Qy6VT+VkziMB8GA1UdIwQYMBaAFAFLPZr3CUK6nBuv+2Qy6VT+VkziMA8GA1Ud\n"
        "EwEB/wQFMAMBAf8wGAYDVR0RBBEwD4INb3RoZXIuZXhhbXBsZTAKBggqhkjOPQQD\n"
        "AgNIADBFAiEAy+GSmOUUWvkts1VqPjinA6NEJP+3EUJvzMhRNOPWuGwCIA6cO3Rv\n"
        "T5/8VHik0UyYpZoat6cLT41/R2Qk1wL8hH2Y\n"
        "-----END CERTIFICATE-----\n";
}

inline char const* other_private_key() noexcept {
    return
        "-----BEGIN PRIVATE KEY-----\n"
        "MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgMyudBhxI+VD70aza\n"
        "mSehS/Tchh+eCP7grnI9gFUTrU2hRANCAARdSrFTtPA4V893eOiPrdM0Bw8wYfmF\n"
        "lwekHIrYDCJoZ4S9z5jSmSWlchUm64QFNHUK3aCXoDDd8yexcrHUwS4V\n"
        "-----END PRIVATE KEY-----\n";
}

// 一对 CA + 叶子（CN=localhost，SAN = DNS:localhost, IP:127.0.0.1，由 CA 签发；ECDSA P-256，10 年）：
// 用于链验证、CRL 与 OCSP stapling 的测试。CA 私钥在此只为测试时签 CRL / OCSP 响应。

inline char const* ca_certificate() noexcept {
    return
        "-----BEGIN CERTIFICATE-----\n"
        "MIIBkjCCATegAwIBAgIULCkEuHO+9/l+cEoKRYWkXbcw9BQwCgYIKoZIzj0EAwIw\n"
        "FjEUMBIGA1UEAwwLbmV0IHRlc3QgQ0EwHhcNMjYwOTA4MTYwNzUzWhcNMzYwOTA1\n"
        "MTYwNzUzWjAWMRQwEgYDVQQDDAtuZXQgdGVzdCBDQTBZMBMGByqGSM49AgEGCCqG\n"
        "SM49AwEHA0IABI+Rib6MClT4E75obT4x1H0jG8R5u+WWoOFjoujxYFzD0/tgTT61\n"
        "TUrvS40/HApgL+vsT9S40cPtFysabmHRafyjYzBhMB0GA1UdDgQWBBTtKoO1FsLq\n"
        "rZPXpKBOMjsgHpwarjAfBgNVHSMEGDAWgBTtKoO1FsLqrZPXpKBOMjsgHpwarjAP\n"
        "BgNVHRMBAf8EBTADAQH/MA4GA1UdDwEB/wQEAwIBBjAKBggqhkjOPQQDAgNJADBG\n"
        "AiEAxL0uHZ6Zwn0Oi0BkiNNBdlnw/6IielnlFhw9xEBT0YACIQDN0JeGnW2Rjb4i\n"
        "SoANaF4X+td8qZHWITDZRH2gBKL0fw==\n"
        "-----END CERTIFICATE-----\n";
}

inline char const* ca_private_key() noexcept {
    return
        "-----BEGIN PRIVATE KEY-----\n"
        "MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQg/l3QJHcOpKT7Iw4H\n"
        "d2ZqL/yQSMfa1m+4PUiA2NqALA+hRANCAASPkYm+jApU+BO+aG0+MdR9IxvEebvl\n"
        "lqDhY6Lo8WBcw9P7YE0+tU1K70uNPxwKYC/r7E/UuNHD7RcrGm5h0Wn8\n"
        "-----END PRIVATE KEY-----\n";
}

inline char const* leaf_certificate() noexcept {
    return
        "-----BEGIN CERTIFICATE-----\n"
        "MIIBuDCCAV+gAwIBAgIUSbyXsDLLvTtHnuetQdtEsoTGAiwwCgYIKoZIzj0EAwIw\n"
        "FjEUMBIGA1UEAwwLbmV0IHRlc3QgQ0EwHhcNMjYwOTA4MTYwNzUzWhcNMzYwOTA1\n"
        "MTYwNzUzWjAUMRIwEAYDVQQDDAlsb2NhbGhvc3QwWTATBgcqhkjOPQIBBggqhkjO\n"
        "PQMBBwNCAAQvHcyW7D3DafTQduIdAi6mbn1FQGFQu74nomoB29Ykt3e/dpgxbgwh\n"
        "IeE4m9AydvUHEePlVAQ/bqmE2anrYbRKo4GMMIGJMBoGA1UdEQQTMBGCCWxvY2Fs\n"
        "aG9zdIcEfwAAATAJBgNVHRMEAjAAMB8GA1UdIwQYMBaAFO0qg7UWwuqtk9ekoE4y\n"
        "OyAenBquMAsGA1UdDwQEAwIHgDATBgNVHSUEDDAKBggrBgEFBQcDATAdBgNVHQ4E\n"
        "FgQUvgypHyZr2RM2i1yxekua8EShHPIwCgYIKoZIzj0EAwIDRwAwRAIgeXjkYWBA\n"
        "EK2G3Bh78Z7ZhKQcZgNRtz/flYh5HEw2x/4CIA0RsqhuPk9pCG4aLL2TJaOkaRbj\n"
        "Va6BMfnY/dq735rV\n"
        "-----END CERTIFICATE-----\n";
}

inline char const* leaf_private_key() noexcept {
    return
        "-----BEGIN PRIVATE KEY-----\n"
        "MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgtAULG0BgJ1sYC0v+\n"
        "u2/hUhbdi/MnQFoSsGVUY6XGSZ6hRANCAAQvHcyW7D3DafTQduIdAi6mbn1FQGFQ\n"
        "u74nomoB29Ykt3e/dpgxbgwhIeE4m9AydvUHEePlVAQ/bqmE2anrYbRK\n"
        "-----END PRIVATE KEY-----\n";
}

// CA 签发的 CRL，吊销上面的叶子（nextUpdate 10 年）：CRL 检查的测试用。
inline char const* leaf_crl() noexcept {
    return
        "-----BEGIN X509 CRL-----\n"
        "MIHGMG4CAQEwCgYIKoZIzj0EAwIwFjEUMBIGA1UEAwwLbmV0IHRlc3QgQ0EXDTI2\n"
        "MDkwODE2MzA1N1oXDTM2MDkwNTE2MzA1N1owJzAlAhRJvJewMsu9O0ee561B20Sy\n"
        "hMYCLBcNMjYwOTA4MTYzMDU3WjAKBggqhkjOPQQDAgNIADBFAiBrfcxP5eItR20u\n"
        "q0hRvvq7EUvC1MPD3cbelhuzpzUl6AIhAKJuWYErHy7oeyf1h8CWSXSjnusQXMQJ\n"
        "OLTR3kxaV0zg\n"
        "-----END X509 CRL-----\n";
}

namespace detail {

// base64 → 原始字节（忽略换行与 '=' 填充）：下面的 OCSP 响应是 DER，不适合直接写成字符串字面量。
inline std::string from_base64(char const* const text) {
    static char const* const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string out;
    auto bits = 0U;
    auto count = 0;
    for (auto const* p = text; *p != '\0'; ++p) {
        auto const* const found = std::strchr(alphabet, *p);
        if (found == nullptr) continue;
        bits = (bits << 6) | static_cast<unsigned>(found - alphabet);
        count += 6;
        if (count >= 8) {
            count -= 8;
            out.push_back(static_cast<char>((bits >> count) & 0xFFU));
        }
    }
    return out;
}

} // namespace detail

// CA 为上面的叶子签的 OCSP 响应（DER；nextUpdate 10 年，不内嵌响应方证书——签发者已经在信任锚里）：
// OCSP stapling 的测试用。写成常量而不是现场签，是因为只有 OpenSSL 有构造 OCSP 响应的 API，
// BoringSSL 与 wolfSSL 下同一份测试也要能跑。
inline std::string leaf_ocsp_response_good() {
    return detail::from_base64(
        "MIIBGAoBAKCCAREwggENBgkrBgEFBQcwAQEEgf8wgfwwgaShGDAWMRQwEgYDVQQD"
        "DAtuZXQgdGVzdCBDQRgPMjAyNjA5MTAxNDA1NDlaMHcwdTBNMAkGBSsOAwIaBQAE"
        "FDOankq906KQl1JIsLgoo2HwUx6TBBTtKoO1FsLqrZPXpKBOMjsgHpwargIUSbyX"
        "sDLLvTtHnuetQdtEsoTGAiyAABgPMjAyNjA5MTAxNDA1NDlaoBEYDzIwMzYwOTA3"
        "MTQwNTQ5WjAKBggqhkjOPQQDAgNHADBEAiBFs0aEFtpH/TEKKSffz0TJuOysX/K9"
        "HpIXFV3FG6udLgIgbm3wemgmjG7Nzt87lVN2NvgIBqo4lgCm/8sVPWCCsk4=");
}

inline std::string leaf_ocsp_response_revoked() {
    return detail::from_base64(
        "MIIBLwoBAKCCASgwggEkBgkrBgEFBQcwAQEEggEVMIIBETCBt6EYMBYxFDASBgNV"
        "BAMMC25ldCB0ZXN0IENBGA8yMDI2MDkxMDE0MDU0OVowgYkwgYYwTTAJBgUrDgMC"
        "GgUABBQzmp5KvdOikJdSSLC4KKNh8FMekwQU7SqDtRbC6q2T16SgTjI7IB6cGq4C"
        "FEm8l7Ayy707R57nrUHbRLKExgIsoREYDzIwMjYwOTA4MTYzMDU3WhgPMjAyNjA5"
        "MTAxNDA1NDlaoBEYDzIwMzYwOTA3MTQwNTQ5WjAKBggqhkjOPQQDAgNJADBGAiEA"
        "zfW/aAzIPo6Um69oJiA+iZUb4dHFse2HQDo2k0TUjIwCIQCfMTi9eB0bJpY0TzmU"
        "Ke+IsmtWHT/w4EQdfSr6ACxpZA==");
}

} // namespace net_test_certs
