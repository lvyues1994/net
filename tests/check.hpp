#pragma once

#include <cstdlib>
#include <iostream>

#include "net/io_context.hpp"

// 测试用的最小断言：失败即打印并以非零退出码结束进程。
//
// 平台测试用 test_context 而不是 net::io_context：后端由编译期宏 NET_TEST_BACKEND 选择
// （tests/CMakeLists.txt 为每个平台测试各编译 epoll / poll / select 三个变体）。

#ifndef NET_TEST_BACKEND
#define NET_TEST_BACKEND ::net::default_backend
#endif

struct test_context : net::io_context {
    test_context() : net::io_context(NET_TEST_BACKEND) {}
    explicit test_context(int const concurrency_hint) : net::io_context(NET_TEST_BACKEND, concurrency_hint) {}
};

namespace net_test {

inline void fail(char const* const expression, char const* const file, int const line) {
    std::cerr << file << ':' << line << ": check failed: " << expression << '\n';
    std::exit(EXIT_FAILURE);
}

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
