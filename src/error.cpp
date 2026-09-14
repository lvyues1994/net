#include "net/error.hpp"

#include <cerrno>
#include <string>

#include "net/config.hpp"

#if NET_PLATFORM_WINDOWS
#include "net/detail/socket_types.hpp" // ERROR_OPERATION_ABORTED / WSAETIMEDOUT
#endif

namespace net {

namespace {

// error → cond：没有对应条件的返回 0。
int cond_of(error const value) noexcept {
    switch (value) {
    case error::eof: return static_cast<int>(cond::eof);
    case error::operation_aborted: return static_cast<int>(cond::canceled);
    case error::stream_truncated: return static_cast<int>(cond::stream_truncated);
    case error::timed_out: return static_cast<int>(cond::timeout);
    default: return 0;
    }
}

// std::errc → cond：canceled / timeout 有通用条件的对应物。
int cond_of_generic(int const value) noexcept {
    if (value == static_cast<int>(std::errc::operation_canceled)) return static_cast<int>(cond::canceled);
    if (value == static_cast<int>(std::errc::timed_out)) return static_cast<int>(cond::timeout);
    return 0;
}

// 平台码（system_category）→ cond。
int cond_of_system(int const value) noexcept {
#if NET_PLATFORM_WINDOWS
    if (value == ERROR_OPERATION_ABORTED || value == WSAECANCELLED) return static_cast<int>(cond::canceled);
    if (value == WSAETIMEDOUT || value == ERROR_SEM_TIMEOUT) return static_cast<int>(cond::timeout);
#else
    if (value == ECANCELED) return static_cast<int>(cond::canceled);
    if (value == ETIMEDOUT) return static_cast<int>(cond::timeout);
#endif
    return 0;
}

struct net_error_category final : std::error_category {
    char const* name() const noexcept override { return "net"; }

    std::string message(int const value) const override {
        switch (static_cast<error>(value)) {
        case error::eof: return "End of file";
        case error::operation_aborted: return "Operation aborted";
        case error::not_open: return "I/O object is not open";
        case error::already_open: return "I/O object is already open";
        case error::already_started: return "An operation in this direction is already pending";
        case error::host_not_found: return "Host not found (authoritative)";
        case error::host_not_found_try_again: return "Host not found (non-authoritative), try again later";
        case error::service_not_found: return "Service not found";
        case error::socket_type_not_supported: return "Socket type not supported";
        case error::no_recovery: return "A non-recoverable error occurred during database lookup";
        case error::no_data: return "The query is valid but does not have associated address data";
        case error::invalid_address: return "Invalid address string";
        case error::stream_truncated: return "Stream truncated before close_notify";
        case error::ocsp_response_missing: return "OCSP stapling required but no response was sent";
        case error::ocsp_response_invalid: return "OCSP response invalid or certificate not good";
        case error::timed_out: return "Operation timed out";
        }
        return "Unknown net error " + std::to_string(value);
    }

    // 默认条件是可移植的 cond（有对应物时）；否则是自己。
    std::error_condition default_error_condition(int const value) const noexcept override {
        if (auto const c = cond_of(static_cast<error>(value))) return std::error_condition{c, cond_category()};
        return std::error_condition{value, *this};
    }

    // 与 std::errc 的比较也要成立：ec == std::errc::operation_canceled / timed_out。
    bool equivalent(int const code, std::error_condition const& condition) const noexcept override {
        if (condition.category() == std::generic_category()) {
            auto const c = cond_of(static_cast<error>(code));
            return c != 0 && c == cond_of_generic(condition.value());
        }
        return default_error_condition(code) == condition;
    }
};

struct cond_error_category final : std::error_category {
    char const* name() const noexcept override { return "net.cond"; }

    std::string message(int const value) const override {
        switch (static_cast<cond>(value)) {
        case cond::eof: return "End of stream";
        case cond::canceled: return "Operation canceled";
        case cond::stream_truncated: return "Stream truncated";
        case cond::timeout: return "Operation timed out";
        }
        return "Unknown net condition " + std::to_string(value);
    }

    // 任何来源的 error_code 与 cond 比较：net 自己的码、平台码、std::errc 的通用码。
    bool equivalent(std::error_code const& code, int const condition) const noexcept override {
        auto const& category = code.category();
        if (category == net_category()) return cond_of(static_cast<error>(code.value())) == condition;
        if (category == std::system_category()) return cond_of_system(code.value()) == condition;
        if (category == std::generic_category()) return cond_of_generic(code.value()) == condition;
        return false;
    }
};

} // namespace

std::error_category const& net_category() noexcept {
    static net_error_category const category;
    return category;
}

std::error_category const& cond_category() noexcept {
    static cond_error_category const category;
    return category;
}

} // namespace net
