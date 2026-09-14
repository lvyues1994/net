#pragma once

#include <cstddef>
#include <system_error>

// 失效注入（Capy 的 test::fuse 同形）：把同一段代码跑很多遍，每遍在下一个失效点上失败一次，直到有一遍
// 走完全部失效点都没触发。测试替身在每个可能出错的地方调 maybe_fail()：
//
//   for (net::test::fuse f; f.next();) {
//       net::test::memory_stream s{"payload", &f};
//       auto r = run_blocking(scenario(s));
//       if (f.triggered()) CHECK(r.ec == f.error()); else CHECK(!r.ec);
//   }
//
// 这样每条错误路径（第 1 次读失败、第 2 次读失败、第 1 次写失败……）都被走过一次，而不是只断言"总是成功"。

namespace net {
namespace test {

struct fuse {
    fuse() noexcept : fuse{std::make_error_code(std::errc::io_error)} {}
    explicit fuse(std::error_code const ec) noexcept : error_{ec} {}

    // 开始下一遍：失效点推到下一处。上一遍没有触发（所有点都过了）返回 false。第一次调用总是 true。
    bool next() noexcept {
        if (started_ && not triggered_) return false;
        if (started_) ++point_;
        started_ = true;
        seen_ = 0U;
        triggered_ = false;
        return true;
    }

    // 到达一个失效点：本遍第 point() 个点返回错误，其余返回空。
    std::error_code maybe_fail() noexcept {
        auto const index = seen_++;
        if (index != point_) return {};
        triggered_ = true;
        return error_;
    }

    bool triggered() const noexcept { return triggered_; }
    std::size_t point() const noexcept { return point_; }
    std::error_code error() const noexcept { return error_; }

  private:
    std::error_code error_;
    std::size_t point_ = 0U;
    std::size_t seen_ = 0U;
    bool started_ = false;
    bool triggered_ = false;
};

} // namespace test
} // namespace net
