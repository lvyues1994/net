# net

`net` 是一个 C++14 的协程原生 I/O 库，实现 WG21 "Network Endeavor" 系列提案
（P4003R3《A Minimal Coroutine Execution Model》、P4172R1、P4100R1、P4124R0）描述的
**IoAwaitable 协议**及其上的 `task<T>`、启动函数、执行器、缓冲区、流概念、组合子，
以及 Linux 平台层：`io_context`（epoll / poll / select 三种后端）、TCP/UDP 套接字、
定时器、DNS、信号。

无栈协程由姊妹库 [co2](../../coro/coro)（C++14 宏生成的状态机，协议与 C++20 协程
规范同形）提供：提案里写 `co_await f()` 的地方，这里写 `CO2_AWAIT(f())`。

```cpp
#include "net/net.hpp"

// 只依赖 any_stream&：与 TCP / TLS / 内存流 / 测试替身一起编译一次
auto echo(net::any_stream& stream)
    CO2_BEG(net::task<>, (stream), char buf[4096]; net::io_result<std::size_t> r; net::io_result<std::size_t> w;) {
    for (;;) {
        CO2_AWAIT_SET(r, stream.read_some(net::buffer(buf)));      // auto [ec, n] = co_await ...
        if (r.ec) break;
        CO2_AWAIT_SET(w, net::write(stream, net::buffer(buf, r.value)));
        if (w.ec) break;
    }
}
CO2_END

int main() {
    net::io_context ctx;
    net::tcp_acceptor acceptor{ctx, net::ip::tcp::endpoint{net::ip::address_v4::any(), 7777}};
    net::run_async(ctx.get_executor())(accept_loop(&ctx, &acceptor));   // 见 examples/echo_server.cpp
    ctx.run();
}
```

## 协议：三件事，一个函数

P4003R3 把协程执行模型压缩到最小：协程挂起时需要知道**谁来恢复我**（执行器）、
**我该不该停**（stop_token）、**子协程的帧从哪来**（帧分配器）。三样东西打包成
`io_env`，由调用方 promise 的 `await_transform` 作为第二个参数注入 awaiter：

```cpp
struct io_env { executor_ref executor; stop_token stop_token; memory_resource* frame_allocator; };

// IoAwaitable：awaiter 提供双参数 await_suspend
coroutine_handle<> await_suspend(coroutine_handle<> h, io_env const* env);
```

在 co2 上，`io_awaitable_promise_base::await_transform` 把每个 IoAwaitable 包进
`detail::env_awaiter`：它是 co2 眼中的（单参数）awaiter，把带类型的父句柄擦成
`coroutine_handle<>` 后连同 `io_env*` 交给内层的双参数 `await_suspend`，并在每次恢复时
把帧分配器写回线程局部槽位（§8.3 的执行窗口协议）。不满足协议的 awaitable 在这里
编译失败——这正是协议想要的编译期边界检查。

| 提案里的名字 | net 里的对应 | 头文件 |
| --- | --- | --- |
| `io_env`, `continuation`, `executor_ref`, `Executor` | 同名；`is_executor<E>` 代替 concept | `io_env.hpp`, `continuation.hpp`, `executor_ref.hpp` |
| `execution_context` + `service` | 同名，服务注册表逆序 shutdown，自带回收式帧分配器 | `execution_context.hpp` |
| `std::pmr::memory_resource`, `get/set_cached_frame_allocator`, `safe_resume` | `net::memory_resource`, `recycling_memory_resource`, 同名函数 | `memory_resource.hpp` |
| `task<T>`, `IoRunnable`, `io_awaitable_promise_base` | 同名 | `task.hpp`, `io_awaitable_promise_base.hpp` |
| `co_await this_coro::environment / executor / stop_token / frame_allocator` | `CO2_AWAIT_SET(x, net::this_coro::…)` | `this_coro.hpp` |
| `run_async(ex[, on_value, on_error])(task)`, `run(ex / token / mr)(task)` | 同名，两段调用 | `run_async.hpp`, `run.hpp` |
| `io_result<Ts...>` | 同名聚合体，tuple 协议（C++17 可结构化绑定） | `io_result.hpp` |
| `mutable_buffer`, `const_buffer`, `*BufferSequence`, `buffer_copy` | 同名；`is_*_buffer_sequence<T>` | `buffers.hpp`, `span.hpp` |
| `DynamicBuffer` | `flat_dynamic_buffer`, `dynamic_buffer(vector/string)` | `dynamic_buffer.hpp` |
| `ReadStream` / `WriteStream` / `Stream` | `is_read_stream` / `is_write_stream` / `is_stream`；`read` / `write` / `read_until` | `stream.hpp` |
| `any_read_stream`, `any_write_stream`, `any_stream` | 同名，零每操作分配 | `any_stream.hpp` |
| `when_all`, `when_any` | 同名，I/O 感知（P4124R0 §2 的表） | `when_all.hpp`, `when_any.hpp` |
| `thread_pool`, `strand`, `any_executor` | 同名 | `thread_pool.hpp`, `strand.hpp`, `any_executor.hpp` |
| `io_context`, `steady_timer`, `signal_set`, `tcp_socket`, `tcp_acceptor`, `udp_socket`, `resolver`, `ip::*` | 同名（Networking TS 形态去掉 `async_` 与完成令牌） | `io_context.hpp`, `timer.hpp`, `signal_set.hpp`, `tcp.hpp`, `udp.hpp`, `resolver.hpp`, `ip.hpp` |
| `corosio::epoll` / `select` … 后端标签，`io_context(backend)` | `net::epoll` / `net::poll` / `net::select`，`io_context{net::poll}` | `backend.hpp` |

## 用法要点

**协程体**（详见 co2 README）：`CO2_BEG(返回类型, (参数...), 帧局部;) { ... } CO2_END`；
跨挂起点存活的局部写在帧局部列表里；`CO2_AWAIT(e)` / `CO2_AWAIT_SET(v, e)` 一行一个；
返回类型含逗号时加括号：`CO2_BEG((net::task<net::io_result<std::size_t>>), ...)`。

**启动**：`run_async(ex)(t())` 两段调用——第一段把帧分配器放进带外槽位，第二段调用
协程（它的 `operator new` 读槽位）。C++17 保证求值顺序；C++14 未规定（GCC/Clang 实际
也先求后缀表达式），需要确定性时用工厂形态 `run_async(ex)([&] { return t(); })`。没有
处理器时异常在执行器线程重抛（从 `ctx.run()` 抛出）。

**环境切换**：`CO2_AWAIT_SET(v, net::run(pool.get_executor())(cpu_work()))` 在线程池上跑
子任务，完成后经父执行器恢复。`net::run(token)` / `net::run(resource)` 只换 stop_token /
帧分配器。

**取消**是协作式的：`stop_token` 请求停止 → 未完成的 I/O 以 `error::operation_aborted`
完成 → 协程照常恢复、走到 `final_suspend` → 拥有者销毁。`when_all` 任一子任务返回 `ec`
或抛出即向兄弟请求停止；`when_any` 第一个成功者胜出后取消其余。

**后端**：`net::io_context ctx{net::poll};` 选择事件机制（默认 epoll；select 受
`FD_SETSIZE` 限制）。套接字、定时器等 I/O 对象经抽象接口对接后端，代码与后端无关；
后端的设计、与 Corosio 的对照以及 io_uring / IOCP 的接入方案见 `docs/backends.md`。

**缓冲区描述符不拥有内存**：`net::buffer(std::string{"x"})` 指向的临时对象在 co_await
表达式求值后就销毁——要发送的数据必须活到操作完成（帧局部或参数）。

## 契约

以下不是可恢复错误，交给进程级契约处理器（默认 `std::terminate()`，
`co2::setContractViolationHandler` 可替换）：

- 销毁已启动、未完成的 `task`（它挂起在某个操作上）；
- 带着未完成的操作销毁 I/O 对象（套接字、定时器、signal_set、resolver）——先
  `cancel()` / `close()`，等它以 `operation_aborted` 完成；
- 同一 I/O 对象同一方向同时发起两个操作；
- `execution_context` 已 shutdown 后再注册服务（抛 `std::logic_error`）。

销毁 `io_context` 时仍未完成的操作被放弃（等待它们的协程不再恢复，帧不会释放）：
销毁前应通过 stop_token 请求停止并 `run()` 到所有链完成。

## 构建

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug     # 默认从 ../../coro/coro 加入 co2；或 -DNET_CO2_DIR=...
cmake --build build
ctest --test-dir build --output-on-failure
```

抽象层仅含头文件；平台层编译进 `libnet.a`。作为子项目：

```cmake
add_subdirectory(path/to/net)
target_link_libraries(my-target PRIVATE net::net)
```

测试覆盖：task / 环境传播 / 帧分配器、执行器（多线程 `run()`、strand 串行化、服务、后端
选择）、缓冲区、流与 `any_stream`（零分配断言）、组合子（错误传播、取消、异常）、定时器、
TCP 回环（取消、EOF、超时、多线程）、UDP、DNS、信号，以及一个契约违规测试。平台测试为
epoll / poll / select 各编译一个变体（`<name>`、`<name>_poll`、`<name>_select`）。全部测试
在 ASan+UBSan+LSan 与 TSan 下通过。

`examples/`：`echo_server`（accept 循环 + `any_stream` 会话 + SIGINT 优雅退出）、
`echo_client`（DNS + connect + `when_any` 超时读）、`http_get`（`read_until` +
动态缓冲）、`timers`（组合子 / 线程池 / stop_token）。

## 目录

```
include/net/            公共头：协议核心、执行器、缓冲区、流、组合子（仅头文件）；平台层的具体层接口
src/                    具体层：io_context 调度器、套接字/定时器/DNS/信号（只依赖 detail/backend.hpp）
src/detail/backend.hpp  后端接缝：io_backend / socket_impl / timer_impl（抽象）
src/detail/posix/       POSIX 系统调用封装
src/detail/reactor/     就绪型后端族：reactor_backend + epoll / poll / select 解复用器
docs/                   architecture.md（分层与决策）、backends.md（后端设计与 io_uring / IOCP 接入）
tests/  examples/
```

## 尚未提供

TLS、文件 I/O、Unix 域套接字、`system_context`、回调风格的流概念（`BufferSource` /
`BufferSink`）、与 `std::execution` 的桥（P4092/P4093）、io_uring / IOCP / kqueue 后端
（接缝已就位，方案见 `docs/backends.md`）。
