#pragma once

// 在协程体内读取当前环境（P4003R3 §3.3 的 `co_await get_stop_token` 形态）：
//
//   CO2_AWAIT_SET(env,   net::this_coro::environment);      // io_env const*
//   CO2_AWAIT_SET(ex,    net::this_coro::executor);         // executor_ref
//   CO2_AWAIT_SET(token, net::this_coro::stop_token);       // stop_token
//   CO2_AWAIT_SET(mr,    net::this_coro::frame_allocator);  // memory_resource*
//
// 这些标签由 promise 的 await_transform 截获，从不挂起。

namespace net {
namespace this_coro {

struct environment_tag {};
struct executor_tag {};
struct stop_token_tag {};
struct frame_allocator_tag {};

constexpr environment_tag environment{};
constexpr executor_tag executor{};
constexpr stop_token_tag stop_token{};
constexpr frame_allocator_tag frame_allocator{};

} // namespace this_coro
} // namespace net
