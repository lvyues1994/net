#pragma once

#include <type_traits>

#include "co2/coroutine_handle.hpp"

#include "net/coroutine.hpp"

// 手写的、不占堆的"协程帧"：一个标准布局对象，首成员是 co2 帧头。把它的 handle()
// 设为某个 task 的续体，task 完成时对称转移到它，co2 的恢复循环调用 step → 我们的
// 回调。回调返回下一个要恢复的句柄（对称转移目标）或空。这是 co2 自己的 syncWait /
// spawn / whenAll 使用的技术，net 的启动函数与组合子都建立在它之上。
//
// 契约：回调运行期间可以销毁拥有本帧的对象（step 在调用回调之后不再触碰它）；对象一旦
// 把 handle() 交出去就不能再移动。

namespace net {
namespace detail {

struct completion_frame {
    using callback = coroutine_handle<> (*)(void* user);

    completion_frame() noexcept : header_{&noop_destroy, &step, false, false} {}

    completion_frame(callback const on_resume, void* const user) noexcept
        : header_{&noop_destroy, &step, false, false}, callback_{on_resume}, user_{user} {}

    completion_frame(completion_frame const&) = delete;
    completion_frame& operator=(completion_frame const&) = delete;

    void set(callback const on_resume, void* const user) noexcept {
        callback_ = on_resume;
        user_ = user;
    }

    coroutine_handle<> handle() noexcept { return coroutine_handle<>::from_address(&header_); }

  private:
    static void noop_destroy(co2::detail::FrameHeader*) noexcept {}

    static co2::detail::FrameHeader* step(co2::detail::FrameHeader* const header) {
        static_assert(std::is_standard_layout<completion_frame>::value,
                      "completion_frame must stay standard-layout");
        auto* const self = reinterpret_cast<completion_frame*>(header);
        auto const on_resume = self->callback_;
        auto* const user = self->user_;
        // 回调可能销毁 self：此后不再访问。
        auto const next = on_resume(user);
        if (not next || next == noop_coroutine()) return nullptr;
        return static_cast<co2::detail::FrameHeader*>(next.address());
    }

    co2::detail::FrameHeader header_;
    callback callback_ = nullptr;
    void* user_ = nullptr;
};

} // namespace detail
} // namespace net
