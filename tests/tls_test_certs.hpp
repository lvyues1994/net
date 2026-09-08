#pragma once

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

} // namespace net_test_certs
