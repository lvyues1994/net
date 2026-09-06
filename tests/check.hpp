#pragma once

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <thread>

#include "co2/contract.hpp"

#include "net/backend.hpp"
#include "net/io_context.hpp"

// 测试用的最小断言：失败即打印并以非零退出码结束进程。
//
// 平台测试用 test_context 而不是 net::io_context：后端由编译期宏 NET_TEST_BACKEND 选择
// （tests/CMakeLists.txt 为每个平台测试各编译 epoll / poll / select / io_uring 四个变体）。
// 后端在当前内核 / 沙箱里不可用时（典型：io_uring 被 seccomp 禁止），测试以退出码 77 跳过，
// CTest 通过 SKIP_RETURN_CODE 把它记为 skipped 而不是 failed。

#ifndef NET_TEST_BACKEND
#define NET_TEST_BACKEND ::net::default_backend
#endif

constexpr int test_skip_exit_code = 77;

template <class Backend> Backend require_backend_or_skip(Backend const backend) {
    if (net::backend_available(Backend::kind)) return backend;
    std::cerr << "backend " << net::to_string(Backend::kind) << " unavailable on this system: skipping\n";
    std::exit(test_skip_exit_code);
}

struct test_context : net::io_context {
    test_context() : net::io_context(require_backend_or_skip(NET_TEST_BACKEND)) {}
    explicit test_context(int const concurrency_hint)
        : net::io_context(require_backend_or_skip(NET_TEST_BACKEND), concurrency_hint) {}
};

namespace net_test {

inline void fail(char const* const expression, char const* const file, int const line) {
    std::cerr << file << ':' << line << ": check failed: " << expression << '\n';
    std::exit(EXIT_FAILURE);
}

// 契约违规默认直接 std::terminate()，只会打印"terminate called without an active exception"。
// 测试进程装一个会说出条件与位置的处理器；NET_TEST_CONTRACT_DELAY_MS 让它在终止前等一会
//（sanitizer 在别的线程上打印报告时不被打断）。
struct contract_reporter {
    contract_reporter() {
        co2::setContractViolationHandler([](co2::ContractViolation const& violation) {
            std::cerr << "contract violation: " << violation.condition << " at " << violation.file << ':'
                      << violation.line << '\n';
            if (auto const* const delay = std::getenv("NET_TEST_CONTRACT_DELAY_MS"))
                std::this_thread::sleep_for(std::chrono::milliseconds{std::atoi(delay)});
        });
    }
};
static contract_reporter const contract_reporter_instance{};

} // namespace net_test

#define CHECK(expression)                                                                          \
    do {                                                                                           \
        if (not(expression)) ::net_test::fail(#expression, __FILE__, __LINE__);                    \
    } while (false)

#define CHECK_EQ(left, right)                                                                      \
    do {                                                                                           \
        if (not((left) == (right))) {                                                              \
            std::cerr << "  left:  " << (left) << "\n  right: " << (right) << '\n';                \
            ::net_test::fail(#left " == " #right, __FILE__, __LINE__);                             \
        }                                                                                          \
    } while (false)
