#pragma once

// 基准用的自签名证书（与 tests/tls_test_certs.hpp 相同）（10 年有效期，openssl req 生成）。
//   server：CN=localhost，SAN = DNS:localhost, IP:127.0.0.1, IP:::1，可作 CA（自签）
//   other： CN=other.example，用于验证"证书不匹配主机名 / 不受信任"的失败路径
// 只用于测试，不要在别处使用。

namespace net_bench_certs {

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

} // namespace net_bench_certs
