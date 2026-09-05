#pragma once

#include "net/coroutine.hpp"
#include "net/executor_ref.hpp"
#include "net/memory_resource.hpp"

// P4003R3 §4.1：协程在挂起点需要的三样东西——谁来恢复我（执行器）、我该不该停
// （stop_token）、子协程的帧从哪来（帧分配器）。启动函数拥有 io_env，链上每个协程借用
// 它（指针语义显式表达所有权）；环境在启动时确定、链存续期间不变（const）。需要不同
// 环境的子链用 run(...) 建一个新的。

namespace net {

struct io_env {
    executor_ref executor;
    co2::stop_token stop_token;
    memory_resource* frame_allocator = nullptr;
};

} // namespace net
