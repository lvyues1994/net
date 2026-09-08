#include "net/ip.hpp"

#if !NET_PLATFORM_WINDOWS
#include <arpa/inet.h>
#endif

#include "net/error.hpp"

#include "detail/backend.hpp"

namespace net {
namespace ip {

// ---- address_v4 ----

address_v4::address_v4(uint_type const value) noexcept {
    bytes_[0] = static_cast<unsigned char>((value >> 24U) & 0xFFU);
    bytes_[1] = static_cast<unsigned char>((value >> 16U) & 0xFFU);
    bytes_[2] = static_cast<unsigned char>((value >> 8U) & 0xFFU);
    bytes_[3] = static_cast<unsigned char>(value & 0xFFU);
}

address_v4::uint_type address_v4::to_uint() const noexcept {
    return (static_cast<uint_type>(bytes_[0]) << 24U) | (static_cast<uint_type>(bytes_[1]) << 16U) |
           (static_cast<uint_type>(bytes_[2]) << 8U) | static_cast<uint_type>(bytes_[3]);
}

std::string address_v4::to_string() const {
    char text[INET_ADDRSTRLEN];
    in_addr addr{};
    std::memcpy(&addr, bytes_.data(), 4U);
    detail::ensure_networking_initialized();
    if (::inet_ntop(AF_INET, &addr, text, sizeof(text)) == nullptr) return {};
    return text;
}

// ---- address_v6 ----

bool address_v6::is_unspecified() const noexcept {
    for (auto const byte : bytes_)
        if (byte != 0) return false;
    return true;
}

bool address_v6::is_loopback() const noexcept {
    for (auto index = 0U; index != 15U; ++index)
        if (bytes_[index] != 0) return false;
    return bytes_[15] == 1;
}

bool address_v6::is_v4_mapped() const noexcept {
    for (auto index = 0U; index != 10U; ++index)
        if (bytes_[index] != 0) return false;
    return bytes_[10] == 0xFF && bytes_[11] == 0xFF;
}

std::string address_v6::to_string() const {
    char text[INET6_ADDRSTRLEN];
    in6_addr addr{};
    std::memcpy(&addr, bytes_.data(), 16U);
    detail::ensure_networking_initialized();
    if (::inet_ntop(AF_INET6, &addr, text, sizeof(text)) == nullptr) return {};
    auto result = std::string{text};
    if (scope_id_ != 0U) result += "%" + std::to_string(scope_id_);
    return result;
}

address_v6 address_v6::loopback() noexcept {
    bytes_type bytes{};
    bytes[15] = 1;
    return address_v6{bytes};
}

// ---- 解析 ----

address_v4 make_address_v4(char const* const text, std::error_code& ec) noexcept {
    in_addr addr{};
    detail::ensure_networking_initialized();
    if (::inet_pton(AF_INET, text, &addr) != 1) {
        ec = make_error_code(error::invalid_address);
        return {};
    }
    ec.clear();
    address_v4::bytes_type bytes;
    std::memcpy(bytes.data(), &addr, 4U);
    return address_v4{bytes};
}

address_v4 make_address_v4(char const* const text) {
    std::error_code ec;
    auto const result = make_address_v4(text, ec);
    if (ec) throw std::system_error{ec, "make_address_v4"};
    return result;
}

address_v6 make_address_v6(char const* const text, std::error_code& ec) noexcept {
    // 允许 "addr%scope" 形式。
    std::string buffer;
    auto scope = scope_id_type{};
    auto const* numeric = text;
    auto const* percent = std::strchr(text, '%');
    if (percent != nullptr) {
        buffer.assign(text, static_cast<std::size_t>(percent - text));
        numeric = buffer.c_str();
        scope = static_cast<scope_id_type>(std::strtoul(percent + 1, nullptr, 10));
    }
    in6_addr addr{};
    detail::ensure_networking_initialized();
    if (::inet_pton(AF_INET6, numeric, &addr) != 1) {
        ec = make_error_code(error::invalid_address);
        return {};
    }
    ec.clear();
    address_v6::bytes_type bytes;
    std::memcpy(bytes.data(), &addr, 16U);
    return address_v6{bytes, scope};
}

address_v6 make_address_v6(char const* const text) {
    std::error_code ec;
    auto const result = make_address_v6(text, ec);
    if (ec) throw std::system_error{ec, "make_address_v6"};
    return result;
}

address make_address(char const* const text, std::error_code& ec) noexcept {
    auto const v6 = make_address_v6(text, ec);
    if (not ec) return v6;
    auto const v4 = make_address_v4(text, ec);
    if (not ec) return v4;
    return {};
}

address make_address(char const* const text) {
    std::error_code ec;
    auto const result = make_address(text, ec);
    if (ec) throw std::system_error{ec, "make_address"};
    return result;
}

} // namespace ip
} // namespace net
