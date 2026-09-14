# 基准：net 与 Boost.Asio、libuv 对照

`benchmarks/`（`-DNET_BUILD_BENCHMARKS=ON`，Release）。五个可执行文件共用一个 harness
（`bench.hpp`：多轮取中位数；全局 `operator new` 计数给出 allocs/op；POSIX 上 `getrusage` 给出每操作的
用户态 / 内核态 CPU（`usr ns` / `sys ns`，进程全部线程之和），把"系统调用贵"和"用户态贵"分开看——
`perf` 不可用时的替代；`--quick` 供 CI，`--only 子串` 只跑匹配的行，便于 `strace -c` 单独观察）：

| 可执行文件 | 内容 |
| --- | --- |
| `bench_core` | 协议核心：对称转移、启动、执行器 hop、`when_all`、原生 / 抽象 / 类型擦除 `read_some`、帧分配器 |
| `bench_net` | 四种后端（`--backend`）的 TCP 回环往返（组合算法与裸 `read_some` 两行）/ 32 连接 × 4 线程 / 吞吐 / connect+accept / 定时器 |
| `bench_tls` | TLS 握手、加密往返、吞吐（提供者由构建决定，见 `docs/tls.md`） |
| `bench_asio` | **同样的场景用 Boost.Asio 写两遍**：callbacks（Asio 最快的写法）与 C++20 `awaitable`（与 `net::task` 模型最接近）。需要 Boost ≥ 1.75，以 C++20 编译 |
| `bench_libuv` | 同样的 TCP 场景用 libuv 的 C 回调写一遍（pkg-config 找到 libuv 时构建）。它的定时器是毫秒分辨率、`uv_idle` 不是跨线程 post，这两行不可比 |

## 方法

- 同一台机器（i7-13700KF，Linux 7.0，GCC 13 -O3），`taskset -c 6` 钉在一个 P 核上；同一进程内单线程：
  `net::io_context{kind, net::single_thread_hint}` 对 `asio::io_context{1}` 对一个 `uv_loop`；回环 TCP，
  `TCP_NODELAY`，两端在同一个线程上。
- 每行 5 轮取中位数；先预热一轮。调频器是 powersave，机器会在两个频率档之间跳（同一二进制 4.2 与 3.5 µs
  两种读数），所以**不同日期的表不能横比**，A/B 必须同一会话交替跑；同一表内的相对顺序稳定。
- connect + accept 行的客户端设 `SO_LINGER{0}`（RST，不进 TIME_WAIT）：否则十几万个连接之后
  内核的 TIME_WAIT 表让 `connect` 慢 4 倍，数字取决于之前跑过什么。Asio 的析构路径会清掉用户
  设置的 linger，所以 `bench_asio` 里显式 `close()`。
- allocs/op 是全局 `operator new` 的次数。Asio 的完成处理器与 awaitable 帧走它自己的线程局部
  回收分配器，稳态下为 0；libuv 内部走 malloc，计数看见 0 不代表没分配。

## 数字（2026-09-15）

单线程，ns/op；括号里是 allocs/op。

| 场景 | net epoll | net io_uring | Asio callbacks | Asio awaitable | libuv |
| --- | --- | --- | --- | --- | --- |
| 协程等待一个子协程（一帧） | 14.7 (1) | — | — | 11.5 (0) | — |
| 启动一条链 + `run()` 往返 | 49.9 (2) | — | — | 172.9 (0)（`co_spawn`） | — |
| 执行器 post 一跳 | 13.0 (0) | — | 12.7 (0) | 44.8 (0) | 93.4（idle，不可比） |
| 定时器到期 + 恢复 | 1 543 (0) | **1 394** (0) | 1 886 (0) | 1 930 (0) | 不可比（0 ms） |
| TCP 回显往返 64 B | 3 124 (0) | 3 058 (0) | **2 994** (0) | 3 167 (0) | 3 277 (0) |
| 同上，裸 `read_some` / `write_some` | 3 065 (0) | 3 028 (0) | — | — | — |
| 同上，每次读套 1 s `timeout()` | +200（5） | — | — | — | — |
| 同上，每次读套 `when_any(read, timer.wait())` | +195（4） | — | — | — | — |
| TCP 回显往返 4 KiB | 3 541 (0) | 3 472 (0) | **3 377** (0) | 3 531 (0) | 4 030 (0) |
| TCP 吞吐 64 KiB 写 | 11 102 MiB/s | 11 340 MiB/s | — | **12 065 MiB/s** | 10 762 MiB/s |
| TCP connect + accept | 8 510 (3) | **7 120** (1) | — | 8 867 (0) | 8 415 (2) |
| 64 B 回显，32 连接 × 4 线程（每轮 32 个往返） | 95.0 µs | 100.0 µs | — | — | — |

同一场景 64 B 往返的 CPU 拆分（usr / sys，ns）：net epoll 597 / 2 527，net io_uring 510 / 2 548，
Asio callbacks 417 / 2 576，Asio awaitable 564 / 2 602，libuv 508 / 2 771。

## 这一轮做了什么，各值多少

2026-09-10 的同一表是 epoll 3 557 (4)、io_uring 3 374 (4)，比 Asio callbacks 慢 18% / 12%。当时文档把差距记在
"每次完成经 `io_context` 互斥锁三次"上。逐项 A/B（同一会话交替跑）之后：

| 改动 | 64 B 往返 | 说明 |
| --- | --- | --- |
| `io_context`：运行线程私有队列 + 原子工作计数 | **0**（3 546 → 3 537，噪声内） | 无争用的互斥锁 ~20 ns，三次也不到 100 ns；32 连接 × 4 线程也在噪声内。保留：多线程下少三次锁争用，且是 Asio / Corosio 的做法——但它不是那 550 ns |
| 单缓冲 `recv` / `send` 代替 `readv` / `sendmsg` | **−280 ns** | 内核时间 2 700 → 2 460，追平 Asio。`readv` 走 VFS 的 `read_iter`（`rw_verify_area`、fsnotify、iovec 导入），`sendmsg` 要拷 msghdr；`recv` / `send` 直进 `sock_recvmsg` / `sock_sendmsg`。一趟往返 6 次调用 |
| `net::read` / `net::write` 改成无帧 awaiter，awaiter 槽 64 → 192 字节 | **−200 ns，4 allocs → 0** | 组合算法原来每次一个 task 帧；现在在 awaiter 里同步推进，要等时用 `completion_frame` 当续体。`bench_net` 的"裸 `read_some`"行是下界，两行现在相差不到 60 ns |
| io_uring 套接字懒注册文件表（32 个 SQE 之后） | connect+accept **−7%**（7 656 → 7 120） | 原先每条连接 4 次 `io_uring_register`，占该场景 24% 内核时间；长连接仍受益（往返约 25 ns） |

合计：epoll 3 557 → 3 124（−12%），io_uring 3 374 → 3 058（−9%），吞吐 10.2 → 11.1–11.3 GiB/s。与 Asio callbacks 的差
从 18% / 12% 收到 4% / 2%；两个后端都已快过 Asio 的 awaitable。剩下 60–130 ns 在用户态（net 597 vs Asio callbacks
417）：co2 状态机的恢复、`env_awaiter` 的 TLS 写、`late_init` 的 stop_callback 槽。

第二轮（2026-09-15，同一会话 A/B）：

| 改动 | 效果 | 说明 |
| --- | --- | --- |
| `timeout()` 专用 awaiter | 与 `when_any(read, timer.wait())` **持平**（+200 vs +195 ns / 读） | 原以为省下的两个 runner 帧是主项；实测每次读套一个时限的成本在定时器路径本身，不在帧。`timeout()` 每次调用建一个 `steady_timer`（多一次分配），bench 里的 `when_any` 复用一个——tcache 级别的差别 |
| timerfd 懒武装：已武装在不晚于堆顶的时刻就不重武装，堆空也不解除 | 每次读套时限的 `timerfd_settime` 从每趟往返 1 次到每个超时周期 1 次（quick 基准 5 101 → 1）；每读的时限成本 +400 → +200 ns | 过早醒来一次是空转，重武装是一次系统调用；`timer expire + resume` 行不变 |
| 内联完成预算（64 / 恢复） | 往返、吞吐**中性**（3 121 vs 3 128） | 单连接 ping-pong 每次恢复只消耗一两份；预算只在总是就绪的连接 / 内存流 / 就绪型后端的文件上生效（`tcp_tests` 有公平性断言） |

## 怎么读

- **协程机制本身**（前三行）：`net::task` 的对称转移与 Asio 的 `awaitable` 同一量级（14.7 vs
  11.5 ns，差的是那一次 malloc）；启动一条协程链 net 快 3.5 倍（`run_async` 是一个堆状态 +
  一个手写帧，`co_spawn` 要建 `awaitable_thread`）；经执行器 post 一跳 net 与 Asio callbacks
  相同（13.0 vs 12.7 ns），Asio 的 awaitable 走 `use_awaitable` 的 handler 机制慢 3.5 倍。
- **定时器**：net 快 20–25%。两边都是 timerfd + 二叉堆；Asio 每次重臂打两次 `timerfd_settime`（`strace -c`：
  102k 次 / 51k 次到期），net 一次；io_uring 用 `IORING_OP_TIMEOUT`，一次 `enter` 做完。
- **TCP 往返**：四家就绪型实现的系统调用**结构相同**（一次发送 + 两次接收（第二次 EAGAIN）+ 一次 wait，
  5 101 趟往返 4.1 万次调用），io_uring 是 2.1 万次（读在环里）。所以差距不能靠"少进内核"解释，而是每次
  调用的形态（上表第二行）和用户态每次完成的固定开销。
- **吞吐**：每 64 KiB 一次完成，瓶颈是内核回环的两次拷贝，四家都在 10.8–12.1 GiB/s。
- **connect + accept**：内核握手为主（sys 6.5–8.3 µs）。io_uring 的多发 accept 把每连接的 `accept4` 投机省掉，
  是四家最快；epoll 一侧 `adopt()` 路径不再对自己 `accept4()` 出来的描述符做四次 `fcntl`。
- **32 连接 × 4 线程**：每轮 32 个往返 95 µs ≈ 单线程串行跑 32 个往返的时间——4 个线程在一个反应器上
  **没有加速**：只有一个线程在 `epoll_wait` 里，其余靠条件变量交接续体（futex 唤醒几微秒），而每个续体只有
  ~0.5 µs 用户态工作。这是就绪型单反应器设计的固有形状（Asio 同构，未测）；要吞吐得每线程一个 `io_context`。
  列为已知问题。
- **io_uring 对 epoll**：往返、定时器、connect+accept 都领先 2–16%；单连接 ping-pong 本是它最不占优的场景
  （每次操作仍要一次 `io_uring_enter`，epoll 的投机 `recv` 命中时连 `epoll_wait` 都省），领先靠的是提交推迟到
  `run()`、自适应投机、`SINGLE_ISSUER | DEFER_TASKRUN`（见 `docs/backends.md`）。尚未用的：多发 recv 只在
  `receive_source`、零拷贝发送。

## 复现

```sh
cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release -DNET_BUILD_BENCHMARKS=ON -DNET_BUILD_TESTS=OFF
cmake --build build-release
cd build-release/benchmarks
taskset -c 6 ./bench_core; taskset -c 6 ./bench_net --backend epoll; taskset -c 6 ./bench_net --backend io_uring
taskset -c 6 ./bench_asio; taskset -c 6 ./bench_libuv
strace -f -c ./bench_net --quick --backend epoll --only "round trip, 64 B"   # 数系统调用
```

A/B 一个改动：把改动前后的二进制各留一份，同一会话里交替各跑 5 次，看中位数——机器在两个频率档之间跳，
单次读数差 20% 是常态。
