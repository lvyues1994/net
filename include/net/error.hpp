#pragma once

#include <system_error>
#include <type_traits>

// net 自己的错误码。平台错误（errno）以 std::system_category() 报告；这里只定义没有
// errno 对应物的条件：流结束、操作被取消、I/O 对象状态错误、名字解析失败。

namespace net {

enum class error : int {
    eof = 1,           // 对端关闭，没有更多数据
    operation_aborted, // 操作被取消（cancel / close / stop_token）
    not_open,          // I/O 对象未打开
    already_open,      // I/O 对象已打开
    already_started,   // 同方向已有未完成的操作
    host_not_found,    // DNS：主机不存在
    host_not_found_try_again,
    service_not_found,
    socket_type_not_supported,
    no_recovery,
    no_data,
    invalid_address,   // 地址字符串无法解析
    stream_truncated,  // TLS：传输在 close_notify 之前结束（无法与截断攻击区分）
    ocsp_response_missing, // TLS：要求 OCSP stapling 但服务端没有附上响应
    ocsp_response_invalid, // TLS：OCSP 响应无法验证 / 状态不是 good
};

std::error_category const& net_category() noexcept;

inline std::error_code make_error_code(error const value) noexcept {
    return std::error_code{static_cast<int>(value), net_category()};
}

inline std::error_condition make_error_condition(error const value) noexcept {
    return std::error_condition{static_cast<int>(value), net_category()};
}

} // namespace net

namespace std {

template <> struct is_error_code_enum<::net::error> : true_type {};

} // namespace std
