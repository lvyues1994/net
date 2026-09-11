# 基准：net 与 Boost.Asio 对照

`benchmarks/`（`-DNET_BUILD_BENCHMARKS=ON`，Release）。四个可执行文件共用一个 harness
（`bench.hpp`：多轮取中位数，全局 `operator new` 计数给出 allocs/op；`--quick` 供 CI，`--only 子串`
只跑匹配的行，便于 `strace -c` / `perf` 单独观察）：

| 可执行文件 | 内容 |
| --- | --- |
| `bench_core` | 协议核心：对称转移、启动、执行器 hop、`when_all`、原生 / 抽象 / 类型擦除 `read_some`、帧分配器 |
| `bench_net` | 四种后端（`--backend`）的 TCP 回环往返 / 吞吐 / connect+accept / 定时器 |
| `bench_tls` | TLS 握手、加密往返、吞吐（提供者由构建决定，见 `docs/tls.md`） |
| `bench_asio` | **同样的场景用 Boost.Asio 写两遍**：callbacks（Asio 最快的写法）与 C++20 `awaitable`（与 `net::task` 模型最接近）。需要 Boost ≥ 1.75，以 C++20 编译 |

## 方法

- 同一台机器（i7-13700KF，Linux 7.0，GCC 13 -O3），同一进程内单线程：`net::io_context{kind,
  net::single_thread_hint}` 对 `asio::io_context{1}`；回环 TCP，`TCP_NODELAY`，两端在同一个线程上。
- 每行 5 轮取中位数；先预热一轮。
- connect + accept 行的客户端设 `SO_LINGER{0}`（RST，不进 TIME_WAIT）：否则十几万个连接之后
  内核的 TIME_WAIT 表让 `connect` 慢 4 倍，数字取决于之前跑过什么。Asio 的析构路径会清掉用户
  设置的 linger，所以 `bench_asio` 里显式 `close()`。
- allocs/op 是全局 `operator new` 的次数。Asio 的完成处理器与 awaitable 帧走它自己的线程局部
  回收分配器，稳态下为 0；net 的默认帧分配器是 `new_delete`（glibc tcache 比任何带原子操作的
  回收器都快，见 README「性能」），所以 task 帧按 1 计。

## 数字

单线程，ns/op；括号里是 allocs/op。

| 场景 | net epoll | net io_uring | Asio callbacks | Asio awaitable |
| --- | --- | --- | --- | --- |
| 协程等待一个子协程（一帧） | 14.6 (1) | — | — | 11.5 (0) |
| 启动一条链 + `run()` 往返 | 54.7 (2) | — | — | 173.4 (0)（`co_spawn`） |
| 执行器 post 一跳 | 12.8 (0) | — | 13.8 (0) | 45.9 (0) |
| 定时器到期 + 恢复 | 1 530 (0) | **1 413** (0) | 1 912 (0) | 1 933 (0) |
| TCP 回显往返 64 B | 3 515 (4) | 4 083 (4) | **2 994** (0) | 3 169 (0) |
| TCP 回显往返 4 KiB | 3 945 (4) | 4 376 (4) | **3 379** (0) | 3 586 (0) |
| TCP 吞吐 64 KiB 写 | 10 560 MiB/s | 10 385 MiB/s | — | **12 241 MiB/s** |
| TCP connect + accept | 8 339 (3) | **7 907** (1) | — | 8 879 (0) |

## 怎么读

- **协程机制本身**（前三行）：`net::task` 的对称转移与 Asio 的 `awaitable` 同一量级（14.6 vs
  11.5 ns，差的是那一次 malloc）；启动一条协程链 net 快 3 倍（`run_async` 是一个堆状态 +
  一个手写帧，`co_spawn` 要建 `awaitable_thread`）；经执行器 post 一跳 net 与 Asio callbacks
  相同（12.8 vs 13.8 ns），Asio 的 awaitable 走 `use_awaitable` 的 handler 机制慢 3.5 倍。
- **定时器**：net 快 20%。两边都是 timerfd + 二叉堆；差别在完成路径。
- **TCP 往返**：Asio callbacks 比 net epoll 快 15%（3.0 vs 3.5 µs），awaitable 快 10%。系统调用数
  相同（`strace -c` 可验：`read`(EAGAIN) + `epoll_wait` + `sendmsg` 各一次每方向）。差在每次完成
  的固定开销。反应器线程上的完成与 Asio 同形：`complete()` 的 `post` 进本线程私有队列（不加锁、
  不唤醒），`outstanding_work` 是原子（仅 1→0 时才拿队列锁叫醒），`do_one` 先排空私有队列。
  其它线程的 `post`（解析器工作线程、`stop`、取消）仍走全局队列锁。剩余差距主要在 `read` /
  `write` 组合操作每次一个 task 帧（4 allocs/往返），Asio 的 `async_read` 复合操作用回收分配器。
- **吞吐**：同一原因，每 64 KiB 一次完成，Asio 快 15%；瓶颈仍是内核回环的两次拷贝。
- **connect + accept**：三者在噪声内（7.9–8.9 µs，内核握手为主）。net 曾慢 5 µs：`assign`
  对本库自己 `socket()` / `accept4()` 出来的、已带 `SOCK_NONBLOCK | SOCK_CLOEXEC` 的描述符又做
  四次 `fcntl`——加了 `adopt()` 路径之后持平。io_uring 的多发 accept 每连接 1 次分配。
- **io_uring 对 epoll**：定时器与 connect+accept 领先，往返落后 15%。单连接 ping-pong 是
  io_uring 最不占优的场景（每次操作仍要一次 `io_uring_enter`，而 epoll 的投机 `read` 命中时
  连 `epoll_wait` 都省了）；它的优势在多连接批量提交、多发 recv、注册缓冲区——后两者尚未使用。

## 复现

```sh
cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release -DNET_BUILD_BENCHMARKS=ON -DNET_BUILD_TESTS=OFF
cmake --build build-release
cd build-release/benchmarks
./bench_core; ./bench_net --backend epoll; ./bench_net --backend io_uring; ./bench_asio
strace -f -c ./bench_net --quick --backend epoll --only "round trip, 64"   # 数系统调用
```
