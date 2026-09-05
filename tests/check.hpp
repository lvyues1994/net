#pragma once

#include <cstdlib>
#include <iostream>

// 测试用的最小断言：失败即打印并以非零退出码结束进程。

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
