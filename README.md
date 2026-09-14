# net

`net` 是一个 C++14 的协程原生 I/O 库，实现 WG21 "Network Endeavor" 系列提案
（P4003R3《A Minimal Coroutine Execution Model》、P4172R1、P4100R1、P4124R0）描述的
**IoAwaitable 协议**及其上的 `task<T>`、启动函数、执行器、缓冲区、流概念、组合子，
以及平台层：`io_context`（Linux 上 epoll / poll / select / io_uring 四种后端，Windows 上 IOCP）、
TCP/UDP 套接字、Unix 域套接字、文件、定时器、DNS、信号，和 TLS 传输安全包装器（OpenSSL / BoringSSL /
wolfSSL 三个提供者）。

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
| 字节粒度切片（Paper 4；Capy `buffer_slice` / `consuming_buffers` / `front`） | `buffer_slice`（单缓冲区返回值，序列返回借用视图 `slice_of`）、`consuming_buffers`、`buffer_front` | `buffer_slice.hpp` |
| `DynamicBuffer` | `flat_dynamic_buffer`、`circular_dynamic_buffer`（环形，`data()` / `prepare()` 可能是两段 `buffer_pair`）、`dynamic_buffer(vector/string)` | `dynamic_buffer.hpp` |
| `ReadStream` / `WriteStream` / `Stream` | `is_read_stream` / `is_write_stream` / `is_stream`；`read` / `write` / `read_until` | `stream.hpp` |
| `any_read_stream`, `any_write_stream`, `any_stream` | 同名，零每操作分配 | `any_stream.hpp` |
| `ReadSource` / `WriteSink`（读满 / 写完 + `write_eof`）、`BufferSource` / `BufferSink`（被调方拥有缓冲区：`pull` / `consume`，`prepare` / `commit` / `commit_eof`）（Paper 6） | `is_read_source` / `is_write_sink` / `is_buffer_source` / `is_buffer_sink`；模型 `memory_source`、`dynamic_buffer_source` / `dynamic_buffer_sink`；适配器 `as_read_source` / `as_write_sink` / `as_buffer_source` / `as_buffer_sink`；`transfer_to_stream` / `transfer_to_sink` | `source_sink.hpp` |
| `any_read_source`, `any_write_sink`, `any_buffer_source`, `any_buffer_sink` | 同名，零每操作分配；整序列操作按 16 个缓冲区一窗穿过边界；`any_buffer_*` 转发或合成另一族的操作 | `any_source_sink.hpp` |
| `stream_file`, `random_access_file`, `file_base`（Paper 10） | 同名；`stream_file` 满足 Stream（隐式位置 + `seek`），`random_access_file` 是 `read_some_at` / `write_some_at`；io_uring 走 READV / WRITEV，就绪型后端同步 `preadv` / `pwritev` | `file.hpp` |
| `local::stream_protocol` / `datagram_protocol`，Unix 域套接字（Paper 11） | `local_stream_socket`, `local_stream_acceptor`, `local_datagram_socket`；端点支持 Linux 抽象命名空间 | `local.hpp` |
| `ip::multicast::{join_group, leave_group, outbound_interface, hops, enable_loopback}`, `ip::unicast::hops` | 同名，v4 / v6 同一类型 | `multicast.hpp` |
| `when_all`, `when_any` | 同名，I/O 感知（P4124R0 §2 的表） | `when_all.hpp`, `when_any.hpp` |
| `timeout(op, dur)`, `delay(dur)`（Corosio 同形） | 同名；专用 awaiter，不建 runner 帧，操作先完成撤定时器、到期取消操作给 `error::timed_out` | `timeout.hpp` |
| 可移植错误条件（Capy `cond`） | `net::cond::{eof, canceled, stream_truncated, timeout}`：任何来源的 `error_code`（本库、`errno`、`std::errc`）都可比较 | `error.hpp` |
| 测试替身（Capy `test/*`） | `test::memory_stream`、`test::fuse`（失效注入）、`test::bufgrind`（切分枚举）、`test::run_blocking` | `test/*.hpp` |
| `thread_pool`, `strand`, `any_executor` | 同名 | `thread_pool.hpp`, `strand.hpp`, `any_executor.hpp` |
| `io_context`, `steady_timer`, `signal_set`, `tcp_socket`, `tcp_acceptor`, `udp_socket`, `resolver`, `ip::*` | 同名（Networking TS 形态去掉 `async_` 与完成令牌） | `io_context.hpp`, `timer.hpp`, `signal_set.hpp`, `tcp.hpp`, `udp.hpp`, `resolver.hpp`, `ip.hpp` |
| `corosio::epoll` / `select` / `io_uring` / `iocp` 后端标签，`io_context(backend)` | `net::epoll` / `net::poll` / `net::select` / `net::io_uring` / `net::iocp`（Windows 默认），`io_context{net::io_uring}`，`backend_available()` | `backend.hpp` |
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
| `NET_TLS_PROVIDER` | `OpenSSL` | `OpenSSL`（`find_package`）/ `BoringSSL` / `wolfSSL` / `OFF` |
| `NET_BORINGSSL_ROOT` | 空 | 现成的 BoringSSL 安装根（`include/`、`lib/`）；为空则 FetchContent 从源码构建（`NET_BORINGSSL_GIT_TAG`，无需 Go / Perl） |
| `NET_WOLFSSL_GIT_TAG` | `v5.8.2-stable` | wolfSSL 经 FetchContent 从源码构建（要 OpenSSL 兼容层那组开关，发行版的包不带） |
| `NET_DEFAULT_FRAME_ALLOCATOR` | `new_delete` | 上下文默认帧分配器：`new_delete` 或 `recycling`（见下文"性能"） |
| `NET_INLINE_COMPLETION_BUDGET`（宏） | `64` | 执行循环每恢复一个协程允许同步完成的传输次数，用完后改为经执行器恢复（公平性）；0 关闭 |
| `NET_AWAIT_STORAGE_SIZE` | `192` | 协程帧里内联 awaiter 槽的字节数（co2 的 `CO2_AWAIT_STORAGE_SIZE`）；`read` / `write` / `run` / `when_all` 的 awaiter 都在此内，放不下的由 co2 堆分配。PUBLIC 定义，随 `net::net` 传给消费方 |
| `NET_BUILD_TESTS` / `NET_BUILD_EXAMPLES` / `NET_BUILD_BENCHMARKS` | ON / ON / OFF | 作为子项目时测试与示例默认关闭 |

Windows（MSVC，Visual Studio 生成器是多配置的）：

```bat
cmake -S . -B build -A x64 -DNET_CO2_DIR=path\to\coro -DOPENSSL_ROOT_DIR="C:/Program Files/OpenSSL"
cmake --build build --config Debug --parallel
ctest --test-dir build -C Debug --output-on-failure
```

MSVC 需要 `/permissive-`（`not` / `and` 作为关键字）与 `/Zc:preprocessor`（co2 协程 DSL 的标准
`__VA_ARGS__` 展开），`net::net` 目标以 PUBLIC 编译选项带出去。Windows 上只有 IOCP 一个后端；Unix 域
只有流套接字（afunix.h，无数据报、无抽象命名空间）；TLS 用 runner / 本机安装的 OpenSSL 3（没有就
`-DNET_TLS_PROVIDER=OFF`）。在没有 Windows 机器时可以用 llvm-mingw（clang + mingw-w64 头）交叉编译做编译期
检查：`-DCMAKE_SYSTEM_NAME=Windows -DCMAKE_CXX_COMPILER=x86_64-w64-mingw32-clang++`。

抽象层仅含头文件；平台层与 TLS 编译进 `libnet.a`。作为子项目：

```cmake
add_subdirectory(path/to/net)
target_link_libraries(my-target PRIVATE net::net)
```

或者安装后 `find_package`（`cmake --install build --prefix <prefix>`，co2 也要先装到同一 prefix；0.x 阶段版本文件只接受
精确版本；TLS 只有 OpenSSL 提供者可以从安装树消费，BoringSSL / wolfSSL 是 FetchContent 构建的，只在 net 自己的构建树里链接）：

```cmake
find_package(net 0.1.0 CONFIG REQUIRED)   # 连带找到 co2、Threads、OpenSSL；PUBLIC 编译选项与定义随目标带出
target_link_libraries(my-target PRIVATE net::net)
```

测试覆盖：task / 环境传播 / 帧分配器（含多线程回收器）、执行器（多线程 `run()`、strand
串行化、服务、后端选择）、缓冲区（含字节切片、环形缓冲、切分枚举）、流与 `any_stream`（零分配断言；`read_until`
跨环形缓冲的段边界；用 `fuse` 走遍每条错误路径）、`timeout` / `delay` 与 `cond`（到期取消、操作先到、已就绪不建定时器、
父停止不是超时、异常透传、绝对截止）、内联完成预算（总是就绪的连接让出线程）、源 / 汇（概念判定、模型、
适配器、`transfer`、四个 `any_*` 的零分配 / 40 缓冲区窗口化 / 转发与合成两条路径）、组合子（错误传播、取消、
异常）、定时器、TCP 回环（取消、EOF、超时、多线程、接受器：连接先到后取 / 取消 / 关闭丢弃排队连接 / assign
已监听的描述符、`write_eof` / `commit_eof` → `shutdown(send)`）、UDP（含组播加入 / 发送 / 收到 / 离开与
选项读回）、Unix 域套接字（文件系统路径与抽象命名空间回显、数据报与发送方端点）、文件（写-读回-eof-seek、
`read_until`、经 `as_buffer_source` 发到 TCP、按偏移读写与洞 / 越界 / resize、两个句柄并发读、打开标志与错误）、
DNS、信号、TLS（握手 / 回显 /
干净关闭、证书与主机名校验失败、验证回调、传输截断、ALPN、3 MiB 经 `any_stream` 传输、取消、
版本不匹配、拥有式流与移动、同一条流上全双工、`when_any` 握手超时、多线程 + strand 会话），以及
一个契约违规测试。`stress_tests` 把这些原语组合起来放到 4 个线程上加随机性：回显风暴、
多线程 `when_any(read, timer)`、随机时刻的取消风暴、200 个随机到期 / 随机取消的定时器、64 个
并发连接的接受风暴、io_context 与 thread_pool 交替、strand 串行化、按种子随机动作的混沌会话、
`stop()` / `restart()` / `run_for` 带着在飞操作、UDP `when_any`（`stress_tests <名字>` 单跑，
`NET_TEST_REPEAT=n` 重复）。平台测试为 epoll / poll / select / io_uring 各编译一个变体
（`<name>`、`<name>_poll`、`<name>_select`、`<name>_io_uring`），后端不可用时以退出码 77 跳过；
共 51 个，全部在 ASan+UBSan+LSan 与 TSan 下（含 `taskset -c 0,1` 模拟 CI 的 2 核调度反复运行）、
OpenSSL 与 BoringSSL 两个提供者下通过。TSan 只抑制未插桩的 libcrypto / libssl 内部
（`tests/tsan.supp`）。

CI（`.github/workflows/ci.yml`）：GCC / Clang × Debug / Release × 两种默认帧分配器的构建与
测试、ASan+UBSan 与 TSan 全量运行、BoringSSL 与 wolfSSL 提供者作业（FetchContent 构建并缓存）、quick 模式
基准（结果写入 step summary 并上传 artifact；与 main 上一次成功运行的 artifact 逐行比较，ns/op 退化超过 15% 发
warning 注解）、gcovr 覆盖率（GCC Debug，`include/` 与 `src/`，报告上传 artifact），以及 MSVC × Debug / Release 的 IOCP 作业
（windows-latest，runner 自带的 OpenSSL 3；cl 的诊断经 `.github/matchers/msvc.json`、ctest 失败经
`.github/scripts/annotate_ctest.py` 变成注解，公开仓库匿名可读）。

`examples/`：`echo_server`（accept 循环 + `any_stream` 会话 + SIGINT 优雅退出，
`./echo_server 7777 io_uring` 选后端）、
`echo_client`（DNS + connect + `when_any` 超时读）、`http_get`（`read_until` +
动态缓冲）、`timers`（组合子 / 线程池 / stop_token）。

## 性能

`benchmarks/`（`-DNET_BUILD_BENCHMARKS=ON`，Release；`--quick` 供 CI）。无第三方依赖的小
harness：多轮取中位数，全局 `operator new` 计数给出 allocs/op，`getrusage` 给出每操作的用户态 / 内核态
CPU。**与 Boost.Asio（callbacks 与 C++20 awaitable 两种写法）和 libuv 的逐行对照见 `docs/benchmarks.md`**：
协程机制与执行器 hop 同一量级或更快（启动一条链快 3.5 倍），定时器快 20–25%，64 B 回环往返 epoll 比 Asio
callbacks 慢 4%、io_uring 慢 2%，两者都快过 Asio 的 awaitable，每趟往返零分配。2026-09 这一轮把往返从慢 18% 收到
4% 的三处改动，每处都有同一会话的 A/B：单缓冲传输用 `recv` / `send` 而不是 `readv` / `sendmsg`（−280 ns，内核时间
追平 Asio）、`net::read` / `net::write` 改成无帧 awaiter（−200 ns，4 allocs → 0）、io_uring 套接字懒注册文件表
（connect + accept −7%）；`io_context` 的私有队列 + 原子工作计数对单线程往返是中性的。
i7-13700KF、GCC 13 -O3、Linux 7.0，单线程，`taskset -c 6`，2026-09-15：

| 核心路径（`bench_core`） | ns/op | allocs/op |
| --- | --- | --- |
| `read_some`：原生（具体类型） | 2.5 | 0 |
| `read_some`：抽象（对 `Stream` 的模板） | 2.5 | 0 |
| `read_some`：类型擦除（`any_stream&`） | 19.0 | 0 |
| task 等待子 task（对称转移 + 一个子帧） | 14.7 | 1（默认 `new_delete`；`recycling` 为 0） |
| `io_context` post 一跳 | 13.0 | 0 |
| strand post 一跳 | 54.7 | 0 |
| `thread_pool` post 一跳（池线程内） | 23.5 | 0 |
| `run_async` + `run()` 往返 | 49.9 | 2 |
| `when_all` 两个就绪 task | 131.7 | 6 |
| 85 帧 task 树：`recycling_memory_resource` | 1449 | 0 |
| 85 帧 task 树：`new_delete_resource` | 1393 | 85 |

对照 P4088R1 §1.1 的表（原生 31.4 / 抽象 32.1 / 类型擦除 36.4 ns，均 0 分配）：类型擦除的
`any_stream` 同样是零每操作分配，代价是一次 vtable 派发加就地构造 awaitable。

帧分配器一行值得说明：P4003R3 §3.5 报告回收式分配器在 MSVC 上快 3.1 倍、Apple clang 上
1.55 倍，但 glibc 的 malloc 自带每线程免锁缓存（tcache），这里比每尺寸类一个自旋锁的回收器
还快约 2 ns/帧。因此 Linux 上默认帧分配器是 `new_delete_resource()`；`recycling_memory_resource`
保留给 malloc 较慢的平台和需要有界内存池的场景（`-DNET_DEFAULT_FRAME_ALLOCATOR=recycling`，
或按上下文 `ctx.set_frame_allocator(&ctx.recycling_frame_allocator())`）。

| 后端回环（`bench_net`） | epoll | poll | select | io_uring |
| --- | --- | --- | --- | --- |
| TCP 回显往返 64 B（µs，0 allocs） | 3.12 | 3.48 | 4.23 | 3.06 |
| TCP 回显往返 4 KiB（µs） | 3.54 | 3.91 | 4.63 | 3.47 |
| TCP 吞吐，64 KiB 写（MiB/s） | 11102 | 11099 | 11137 | 11340 |
| TCP connect + accept（µs） | 8.51 | 8.30 | 8.62 | 7.12 |
| 定时器到期 + 恢复（µs） | 1.54 | 1.58 | 1.93 | 1.39 |

吞吐行的差别在噪声内（瓶颈是内核回环路径的两次拷贝）；往返、connect+accept 与定时器行
io_uring 领先，是把它按 Corosio（参考实现）的做法对齐之后的结果，`strace -c` 可验证
（`bench_net --backend io_uring`）：5 101 趟 64 B 往返 epoll 4.1 万次系统调用，io_uring 2.1 万。
对齐的五点见 `docs/backends.md`——提交推迟到 `run()` 与等待合并成一次 `io_uring_enter`、
自适应投机（连续 EAGAIN 后不再白跑 `recv`，直接走完成型路径）、`net::single_thread_hint` 下
的 `SINGLE_ISSUER | DEFER_TASKRUN`、多发 POLL_ADD 唤醒、多发 accept（`listen()` 武装一个
`IORING_ACCEPT_MULTISHOT` SQE，连接先于 `accept()` 到达时停在 parked 队列里）；注册文件表对套接字懒注册
（提交过 32 个 SQE 才进表，短连接不为进表 / 出表的两次 `io_uring_register` 付费）。顺带修过两处影响所有后端的浪费：
`io_context` 在 `run()` 线程自己 post 续体时会向自己写 eventfd（每次完成多 1 写 2 读），
以及就绪型后端的定时器把到期向上取整到毫秒、又被这次自打断掩盖——现在最早到期经
timerfd（hrtimer，不受 50 µs timer slack 影响）送进解复用器。已知问题：4 个线程 `run()` 同一个就绪型
`io_context` 对 32 条连接的 ping-pong 没有加速（单反应器 + 条件变量交接，每个续体只有 ~0.5 µs 工作），要吞吐得每线程
一个 `io_context`。io_uring 尚未使用的：多发 recv 只在 `receive_source`、零拷贝发送。TLS 的数字见 `docs/tls.md`。

## 目录

```
include/net/            公共头：协议核心、执行器、缓冲区（含字节切片、环形缓冲）、流 / 源 / 汇与类型擦除、组合子、timeout / delay（仅头文件）；平台层的具体层接口（套接字、文件、Unix 域、组播选项）
include/net/test/       公开的测试替身：memory_stream / fuse / bufgrind / run_blocking
include/net/tls/        TLS：context / stream / error（公共头不含 OpenSSL 头）
src/                    具体层：io_context 调度器、套接字/文件/定时器/DNS/信号/Unix 域（只依赖 detail/backend.hpp）
src/detail/backend.hpp  后端接缝：io_backend / socket_impl / file_impl / timer_impl（抽象）；timer_heap / heap_timer 是 reactor 与 iocp 共用的定时器堆
src/detail/posix/       POSIX 系统调用封装（套接字与文件）
src/detail/reactor/     就绪型后端族：reactor_backend + epoll / poll / select 解复用器；文件同步回退
src/detail/io_uring/    完成型后端：裸系统调用的 io_uring 环、提交/取消/收割、套接字 / 文件 / 定时器实现
src/detail/iocp/        完成型后端（Windows）：完成端口、WSARecv / WSASend / AcceptEx / ConnectEx、ReadFile / WriteFile 重叠 I/O
src/tls/                TLS 引擎（OpenSSL API 子集，OpenSSL / BoringSSL / wolfSSL 共用）与驱动协程
benchmarks/             bench_core / bench_net / bench_tls / bench_asio（Boost.Asio 对照）/ bench_libuv（libuv 对照）与 harness
docs/                   architecture.md（分层、决策、审查记录）、backends.md（后端设计与 io_uring / IOCP 接入）、tls.md、benchmarks.md（与 Asio / libuv 对照）
cmake/                  netConfig.cmake.in（find_package(net CONFIG) 的包配置）
tests/  examples/  .github/workflows/ci.yml  .github/scripts/（ctest 注解、基准回归比较）
```

## 尚未提供

对照 P4100R1 的 14 篇与 Corosio / Capy 的清单（2026-09）：

- 提案形态：`system_context`（Paper 2）；`timer::cancel_one()`（Paper 8）；`signal_set` 的信号标志
  `flags_t`（Paper 9）；与 `std::execution` 的桥（P4092 / P4093）；P4100R1 §5.1 的 Asio 适配器。
- 平台：kqueue 后端（接缝已就位，无 macOS CI）；Windows 上的 Unix 域数据报套接字与抽象命名空间；文件操作
  的取消在就绪型后端上不可用（同步完成）；IOCP 的 `release()` 只在没有在飞操作时解除端口关联。
- TLS：PKCS#12；`shutdown()` 与挂起读的重叠；验证回调只暴露 `native_handle()`（见 `docs/tls.md`）。
- Corosio / Capy 有的便利层：范围 `connect(socket, endpoints)`、`tcp_server`、`local_connect_pair`、`message_flags`、
  `async_mutex` / `async_event` / `work_guard`。
