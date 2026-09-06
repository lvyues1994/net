# net

`net` 是一个 C++14 的协程原生 I/O 库，实现 WG21 "Network Endeavor" 系列提案
（P4003R3《A Minimal Coroutine Execution Model》、P4172R1、P4100R1、P4124R0）描述的
**IoAwaitable 协议**及其上的 `task<T>`、启动函数、执行器、缓冲区、流概念、组合子，
以及 Linux 平台层：`io_context`（epoll / poll / select / io_uring 四种后端）、TCP/UDP
套接字、定时器、DNS、信号，和 TLS 传输安全包装器（OpenSSL / BoringSSL 两个提供者）。

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
| `corosio::epoll` / `select` / `io_uring` … 后端标签，`io_context(backend)` | `net::epoll` / `net::poll` / `net::select` / `net::io_uring`，`io_context{net::io_uring}`，`backend_available()` | `backend.hpp` |
| `tls_context`, `tls_stream`, `openssl_stream`（Paper 14） | `net::tls::context`, `net::tls::stream`, `net::tls::openssl_stream`（同一份实现覆盖 OpenSSL 与 BoringSSL） | `tls/context.hpp`, `tls/stream.hpp`, `tls/error.hpp` |

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

**后端**：`net::io_context ctx{net::io_uring};` 选择事件机制（默认 epoll；poll / select
是电平触发的就绪型，select 受 `FD_SETSIZE` 限制；io_uring 是完成型，裸系统调用实现、不依赖
liburing，`net::backend_available(net::backend_kind::io_uring)` 运行时探测）。
`net::io_context ctx{net::io_uring, net::single_thread_hint}` 是一个承诺——只有一个线程、且
始终是同一个线程调用 `run()`——io_uring 据此以 `SINGLE_ISSUER | DEFER_TASKRUN` 创建；普通的
`concurrency_hint`（包括 1）只是提示。套接字、定时器等 I/O 对象经抽象接口对接后端，代码与
后端无关；设计、与 Corosio 的对照以及 IOCP 的接入方案见 `docs/backends.md`。

**TLS**：`net::tls::openssl_stream tls{&sock, ctx}; tls.set_hostname("example.com");
CO2_AWAIT_SET(h, tls.handshake(net::tls::role::client));` 之后它就是一个 `Stream`——可以放进
`any_stream`，`read` / `write` / `read_until` 照常工作。引擎是 sans-I/O 的（内存 BIO），驱动
协程在底层流上泵字节，所以 TLS 与后端无关。对端 close_notify → `error::eof`；传输提前结束 →
`error::stream_truncated`。设计与提供者差异见 `docs/tls.md`。

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
cmake --build build                              # 本地没有 co2 时自动 FetchContent lvyues1994/coro
ctest --test-dir build --output-on-failure
```

| 选项 | 默认 | 说明 |
| --- | --- | --- |
| `NET_TLS_PROVIDER` | `OpenSSL` | `OpenSSL`（`find_package`）/ `BoringSSL` / `OFF` |
| `NET_BORINGSSL_ROOT` | 空 | 现成的 BoringSSL 安装根（`include/`、`lib/`）；为空则 FetchContent 从源码构建（`NET_BORINGSSL_GIT_TAG`，无需 Go / Perl） |
| `NET_DEFAULT_FRAME_ALLOCATOR` | `new_delete` | 上下文默认帧分配器：`new_delete` 或 `recycling`（见下文"性能"） |
| `NET_BUILD_TESTS` / `NET_BUILD_EXAMPLES` / `NET_BUILD_BENCHMARKS` | ON / ON / OFF | 作为子项目时测试与示例默认关闭 |

抽象层仅含头文件；平台层与 TLS 编译进 `libnet.a`。作为子项目：

```cmake
add_subdirectory(path/to/net)
target_link_libraries(my-target PRIVATE net::net)
```

测试覆盖：task / 环境传播 / 帧分配器（含多线程回收器）、执行器（多线程 `run()`、strand
串行化、服务、后端选择）、缓冲区、流与 `any_stream`（零分配断言）、组合子（错误传播、取消、
异常）、定时器、TCP 回环（取消、EOF、超时、多线程、接受器：连接先到后取 / 取消 / 关闭丢弃排队连接 / assign
已监听的描述符）、UDP、DNS、信号、TLS（握手 / 回显 /
干净关闭、证书与主机名校验失败、验证回调、传输截断、ALPN、3 MiB 经 `any_stream` 传输、取消、
版本不匹配、拥有式流与移动、同一条流上全双工、`when_any` 握手超时、多线程 + strand 会话），以及
一个契约违规测试。`stress_tests` 把这些原语组合起来放到 4 个线程上加随机性：回显风暴、
多线程 `when_any(read, timer)`、随机时刻的取消风暴、200 个随机到期 / 随机取消的定时器、64 个
并发连接的接受风暴、io_context 与 thread_pool 交替、strand 串行化、按种子随机动作的混沌会话、
`stop()` / `restart()` / `run_for` 带着在飞操作、UDP `when_any`（`stress_tests <名字>` 单跑，
`NET_TEST_REPEAT=n` 重复）。平台测试为 epoll / poll / select / io_uring 各编译一个变体
（`<name>`、`<name>_poll`、`<name>_select`、`<name>_io_uring`），后端不可用时以退出码 77 跳过；
共 38 个，全部在 ASan+UBSan+LSan 与 TSan 下（含 `taskset -c 0,1` 模拟 CI 的 2 核调度反复运行）、
OpenSSL 与 BoringSSL 两个提供者下通过。TSan 只抑制未插桩的 libcrypto / libssl 内部
（`tests/tsan.supp`）。

CI（`.github/workflows/ci.yml`）：GCC / Clang × Debug / Release × 两种默认帧分配器的构建与
测试、ASan+UBSan 与 TSan 全量运行、BoringSSL 提供者作业（FetchContent 构建并缓存）、quick 模式
基准（结果写入 step summary 并上传 artifact）。

`examples/`：`echo_server`（accept 循环 + `any_stream` 会话 + SIGINT 优雅退出，
`./echo_server 7777 io_uring` 选后端）、
`echo_client`（DNS + connect + `when_any` 超时读）、`http_get`（`read_until` +
动态缓冲）、`timers`（组合子 / 线程池 / stop_token）。

## 性能

`benchmarks/`（`-DNET_BUILD_BENCHMARKS=ON`，Release；`--quick` 供 CI）。无第三方依赖的小
harness：多轮取中位数，全局 `operator new` 计数给出 allocs/op。**与 Boost.Asio 的逐行对照
（callbacks 与 C++20 awaitable 两种写法）见 `docs/benchmarks.md`**：协程机制与执行器 hop 同一
量级或更快（启动一条链快 3 倍），定时器快 20%，TCP 往返慢 10–15%（系统调用数相同，差在每次
完成经 io_context 互斥锁三次；Asio 用线程局部私有队列 + 原子工作计数——已列为下一步优化）。
i7-13700KF、GCC 13 -O3、Linux 7.0，单线程：

| 核心路径（`bench_core`） | ns/op | allocs/op |
| --- | --- | --- |
| `read_some`：原生（具体类型） | 2.4 | 0 |
| `read_some`：抽象（对 `Stream` 的模板） | 2.3 | 0 |
| `read_some`：类型擦除（`any_stream&`） | 17.8 | 0 |
| task 等待子 task（对称转移 + 一个子帧） | 15.2 | 1（默认 `new_delete`；`recycling` 为 0） |
| `io_context` post 一跳 | 12.8 | 0 |
| strand post 一跳 | 58.1 | 0 |
| `thread_pool` post 一跳（池线程内） | 87.0 | 0 |
| `run_async` + `run()` 往返 | 49.5 | 2 |
| `when_all` 两个就绪 task | 137.0 | 6 |
| 85 帧 task 树：`recycling_memory_resource` | 1594 | 0 |
| 85 帧 task 树：`new_delete_resource` | 1433 | 85 |

对照 P4088R1 §1.1 的表（原生 31.4 / 抽象 32.1 / 类型擦除 36.4 ns，均 0 分配）：类型擦除的
`any_stream` 同样是零每操作分配，代价是一次 vtable 派发加就地构造 awaitable。

帧分配器一行值得说明：P4003R3 §3.5 报告回收式分配器在 MSVC 上快 3.1 倍、Apple clang 上
1.55 倍，但 glibc 的 malloc 自带每线程免锁缓存（tcache），这里比每尺寸类一个自旋锁的回收器
还快约 2 ns/帧。因此 Linux 上默认帧分配器是 `new_delete_resource()`；`recycling_memory_resource`
保留给 malloc 较慢的平台和需要有界内存池的场景（`-DNET_DEFAULT_FRAME_ALLOCATOR=recycling`，
或按上下文 `ctx.set_frame_allocator(&ctx.recycling_frame_allocator())`）。

| 后端回环（`bench_net`） | epoll | poll | select | io_uring |
| --- | --- | --- | --- | --- |
| TCP 回显往返 64 B（µs） | 4.18 | 4.54 | 5.28 | 4.03 |
| TCP 回显往返 4 KiB（µs） | 4.60 | 5.01 | 5.71 | 4.49 |
| TCP 吞吐，64 KiB 写（MiB/s，3 次运行） | 9874–10442 | ≈10000 | ≈9800 | 10192–10847 |
| 定时器到期 + 恢复（µs） | 1.53 | 1.56 | 1.90 | 1.38 |

吞吐行的差别在噪声内（单次运行波动 ±5%，瓶颈是内核回环路径的两次拷贝）；往返与定时器行
io_uring 领先，是把它按 Corosio（参考实现）的做法对齐之后的结果，`strace -c` 可验证
（`bench_net --backend io_uring`）：同一 quick 基准全程 epoll 133k 次系统调用，io_uring 92k。
对齐的五点见 `docs/backends.md`——提交推迟到 `run()` 与等待合并成一次 `io_uring_enter`、
自适应投机（连续 EAGAIN 后不再白跑 `read`，直接走完成型路径）、`net::single_thread_hint` 下
的 `SINGLE_ISSUER | DEFER_TASKRUN`、多发 POLL_ADD 唤醒、多发 accept（`listen()` 武装一个
`IORING_ACCEPT_MULTISHOT` SQE，连接先于 `accept()` 到达时停在 parked 队列里）。顺带修了两处影响所有后端的浪费：
`io_context` 在 `run()` 线程自己 post 续体时会向自己写 eventfd（每次完成多 1 写 2 读），
以及就绪型后端的定时器把到期向上取整到毫秒、又被这次自打断掩盖——现在最早到期经
timerfd（hrtimer，不受 50 µs timer slack 影响）送进解复用器。io_uring 尚未使用的：多发
recv、注册缓冲区、零拷贝发送。TLS 的数字（OpenSSL 与 BoringSSL 对照）见 `docs/tls.md`。

## 目录

```
include/net/            公共头：协议核心、执行器、缓冲区、流、组合子（仅头文件）；平台层的具体层接口
include/net/tls/        TLS：context / stream / error（公共头不含 OpenSSL 头）
src/                    具体层：io_context 调度器、套接字/定时器/DNS/信号（只依赖 detail/backend.hpp）
src/detail/backend.hpp  后端接缝：io_backend / socket_impl / timer_impl（抽象）
src/detail/posix/       POSIX 系统调用封装
src/detail/reactor/     就绪型后端族：reactor_backend + epoll / poll / select 解复用器
src/detail/io_uring/    完成型后端：裸系统调用的 io_uring 环、提交/取消/收割、套接字与定时器实现
src/tls/                TLS 引擎（OpenSSL API 子集，OpenSSL / BoringSSL 共用）与驱动协程
benchmarks/             bench_core / bench_net / bench_tls / bench_asio（Boost.Asio 对照）与 harness
docs/                   architecture.md（分层与决策）、backends.md（后端设计与 io_uring / IOCP 接入）、tls.md、benchmarks.md（与 Asio 对照）
tests/  examples/  .github/workflows/ci.yml
```

## 尚未提供

文件 I/O、Unix 域套接字、`system_context`、回调风格的流概念（`BufferSource` /
`BufferSink`）、与 `std::execution` 的桥（P4092/P4093）、IOCP / kqueue 后端（接缝已就位，
方案见 `docs/backends.md`）、wolfSSL 提供者与 TLS 的 PKCS#12 / CRL / SNI 服务端回调 / 会话
复用（见 `docs/tls.md`）。
