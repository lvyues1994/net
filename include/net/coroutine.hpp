#pragma once

#include "co2/coroutine.hpp"
#include "co2/stop_token.hpp"

#include "net/config.hpp"

// net 的协程基座是 co2：C++14 的无栈协程（宏生成的 switch 状态机），其句柄、awaiter
// 协议与 promise 协议逐条对齐 C++20 [coroutine.handle] / [expr.await]。本头把 net 用到
// 的名字引入 net 命名空间，让其余头文件不必直接拼写 co2::。
//
// 协程体的写法（详见 co2 README）：
//
//   auto echo(net::tcp_socket& sock)
//       CO2_BEG(net::task<>, (sock), char buf[1024]; net::io_result<std::size_t> r;) {
//       for (;;) {
//           CO2_AWAIT_SET(r, sock.read_some(net::buffer(buf)));
//           if (r.ec) break;
//           CO2_AWAIT_SET(r, net::write(sock, net::buffer(buf, r.value)));
//           if (r.ec) break;
//       }
//   }
//   CO2_END

namespace net {

template <class Promise = void> using coroutine_handle = co2::coroutine_handle<Promise>;

using co2::noop_coroutine;
using co2::suspend_always;
using co2::suspend_never;

using co2::nostopstate;
using co2::nostopstate_t;
using co2::stop_callback;
using co2::stop_source;
using co2::stop_token;

} // namespace net
