#include "net/error.hpp"

#include <string>

namespace net {

namespace {

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
        }
        return "Unknown net error " + std::to_string(value);
    }

    std::error_condition default_error_condition(int const value) const noexcept override {
        if (static_cast<error>(value) == error::operation_aborted)
            return std::errc::operation_canceled;
        return std::error_condition{value, *this};
    }
};

} // namespace

std::error_category const& net_category() noexcept {
    static net_error_category const category;
    return category;
}

} // namespace net
