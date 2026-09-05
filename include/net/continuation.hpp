#pragma once

#include "net/coroutine.hpp"

// P4003R3 §4.3：执行器排队的最小单元。协程句柄与一个侵入式链表指针配对——continuation
// 住在 awaiter（协程帧）里或栈上，执行器排队时不再为它分配节点，热路径最后一处稳态
// 分配由此消失。
//
// 契约：一个 continuation 同一时刻只能在一个执行器队列里；被执行器恢复之前它的地址
// 必须稳定（awaiter 一旦进入 await_suspend 就不再移动，这是协程核心的保证）。

namespace net {

struct continuation {
    coroutine_handle<> h;
    continuation* next = nullptr;
};

} // namespace net
