# 后端

本文描述 net 的平台层如何把多种事件机制（epoll / poll / select，将来 io_uring / IOCP /
kqueue）对接到同一套上层 API，参考实现 Corosio 是怎么做的，以及完成型后端接入时需要
改动什么、不需要改动什么。

## 1. 参考：Corosio 的三层 + 后端标签

P4100R1 §6 给每个 I/O 对象规定了三个 API 层，Corosio 的目录直接映射它们：

| 层 | Corosio | 派发 | 单独编译 |
| --- | --- | --- | --- |
| 抽象 | `io/io_stream.hpp`、`io_read_stream`、`io_write_stream` | 虚函数 | 是 |
| 具体 | `tcp_socket.hpp`、`io_context.hpp`、`timer.hpp`… | 虚函数 | 是 |
| 原生 | `native/native_tcp_socket.hpp`：`native_tcp_socket<Backend>` | 无（成员遮蔽，全内联） | 否 |

后端对接发生在**具体层与原生层之间**，靠三个抽象：

- `detail::scheduler`（`detail/scheduler.hpp`）：`post / work_started / work_finished /
  run / run_one / poll / stop / restart / register_signal_reader`。`io_context` 只持有一个
  `scheduler*`，所有调度都委托给它。
- `detail::tcp_service`（`detail/tcp_service.hpp`）等：`execution_context::service`，
  `open_socket / assign_socket / bind_socket`，以及创建 `tcp_socket::implementation`。
- `tcp_socket::implementation`：套接字的一切异步操作是它的虚函数
  （`connect(h, ex, endpoint, token, ec*)`、`read_some(...)`、`cancel`、`shutdown`…）。
  `tcp_socket` 本身只是一个持有 `implementation*` 的句柄。

选择后端是**构造 `io_context` 时的一个标签**（`backend.hpp`）：

```cpp
struct epoll_t {
    using scheduler_type    = detail::epoll_scheduler;
    using tcp_socket_type   = detail::epoll_tcp_socket;
    using tcp_service_type  = detail::epoll_tcp_service;
    // ... udp / acceptor / local / signal / resolver / file 的实现类型
    static detail::scheduler& construct(capy::execution_context&, unsigned concurrency_hint);
};
inline constexpr epoll_t epoll{};
// 同样有 select_t / kqueue_t / io_uring_t / iocp_t
```

`io_context(epoll)` 调用 `epoll_t::construct`：创建调度器并用 `make_service` 注册该后端的
全部服务；`tcp_socket.cpp` 里 `use_service<tcp_service>()` 拿到的就是这个后端的实现。
公共头 `backend.hpp` 只有前向声明，不包含任何平台头。

后端本身分两族：

- **就绪型**（epoll / kqueue / select）：`native/detail/reactor/*` 是共享代码——
  `reactor_descriptor_state`（每描述符的 read_op / write_op / connect_op 与就绪位）、
  `reactor_op`（`perform_io()`：在就绪时执行系统调用）、`reactor_stream_socket_impl<Derived,
  Traits, Service, ...>`（推测 I/O、排队、取消）。每个后端只提供两样东西：`*_traits`
  （系统调用策略：`create_socket`、`accept_policy`、`write_policy`、
  `needs_write_notification`、`FD_SETSIZE` 校验）和 `*_scheduler`（怎样等待、怎样登记）。
  `epoll_types.hpp` / `select_types.hpp` 把模板实例化成有名字的 final 类，供 `backend.hpp`
  前向声明。
- **完成型**（io_uring / IOCP）：各自一族（`native/detail/io_uring/*`、`native/detail/iocp/*`），
  共享一个 `coro_op` 基类（句柄、执行器、输出指针、stop_token、impl 保活）。io_uring 的
  `io_uring_op` 加 `prep_func`（填 SQE）、`cqe_func`（处理 CQE）、`res / cqe_flags`、
  `sqe_set`（取消可见性）；IOCP 的 `win_overlapped_op` 派生自 `OVERLAPPED`。套接字操作是
  "提交 → 完成"，没有推测系统调用，缓冲区钉住到 CQE / 完成包到达，取消是异步的
  （`IORING_OP_ASYNC_CANCEL` / `CancelIoEx`）并且**仍会收到一个完成**。POSIX 公共部分
  （`posix_resolver`、`posix_signal`、文件）在就绪型与 io_uring 之间共享。

## 2. net 的对应

```
include/net/backend.hpp        后端标签：net::epoll / poll / select，backend_kind，default_backend
include/net/io_context.hpp     io_context(tag)；只持有抽象 detail::io_backend
include/net/socket_base.hpp    具体层：awaiter 一个指针，方法定义在库内；impl_ 是抽象 socket_impl
include/net/tcp.hpp udp.hpp timer.hpp signal_set.hpp resolver.hpp

src/detail/backend.hpp         后端接缝（抽象）：io_backend / socket_impl / timer_impl / io_context_access
src/detail/posix/              POSIX 系统调用封装（就绪型与将来 io_uring 的同步部分共享）
src/detail/reactor/            就绪型后端族
    reactor_op.hpp                 reactor_op / descriptor_state / timer_op
    demultiplexer.hpp              策略点：add / update / remove / wait / interrupt
    reactor_backend.hpp/.cpp       共享实现：描述符表、就绪位、排队、定时器堆、信号泵、工厂
    reactor_socket.hpp/.cpp        socket_impl 的就绪型实现（perform 调 posix::*）
    reactor_timer.hpp/.cpp         timer_impl 的就绪型实现
    epoll_demultiplexer.cpp        边沿触发
    poll_demultiplexer.cpp         电平触发，pollfd 数组 + 快照
    select_demultiplexer.cpp       电平触发，fd_set，FD_SETSIZE
src/detail/io_uring/           完成型后端：io_uring
    uring.hpp/.cpp                 裸系统调用的环封装（setup / enter / mmap SQ、CQ、SQE 数组），不依赖 liburing
    uring_op.hpp                   操作基类：prepare(sqe) / on_complete(res) / complete()，状态位由环锁保护
    uring_backend.hpp/.cpp         io_backend：提交 / 取消 / 延迟队列 / CQE 收割；eventfd 与信号管道的常驻 POLL_ADD
    uring_socket.hpp/.cpp          socket_impl：RECVMSG / SENDMSG / ACCEPT / CONNECT（保留推测系统调用）
    uring_timer.hpp/.cpp           timer_impl：绝对时间 TIMEOUT，取消用 TIMEOUT_REMOVE
src/io_context.cpp             调度器（队列、工作计数、run 循环）+ make_backend(kind)
src/socket_base.cpp tcp.cpp udp.cpp timer.cpp signal_set.cpp resolver.cpp   具体层
```

| Corosio | net | 说明 |
| --- | --- | --- |
| `detail::scheduler` | `io_context::impl`（队列、工作计数、run 循环）+ `detail::io_backend`（等待/唤醒/工厂） | Corosio 把队列也放进每个后端的 scheduler；net 的队列与后端无关，后端只负责"等一批事件、完成操作" |
| `tcp_socket::implementation`、`tcp_service` | `detail::socket_impl`（同一接口覆盖 TCP/UDP/acceptor）、`io_backend::create_socket` | 操作的三步协议 `begin_* → ready → suspend → finish_*` 与 awaiter 一一对应 |
| `timer_service` | `detail::timer_impl`、`io_backend::create_timer` | |
| `scheduler::register_signal_reader(fd)` | `io_backend::register_signal_reader(fd, deliver)` | 同名同义 |
| `epoll_t::construct(ctx)` | `io_context.cpp: make_backend(kind)` → `make_service<reactor_backend>(demux)` | |
| `reactor_*` 模板 + `*_traits` + `*_scheduler` | `reactor_backend` + `demultiplexer` | Corosio 用模板参数化（为原生层全内联），net 用注入的抽象解复用器（后端代码全在 `src/`，虚函数只在每次 wait / 登记时调用） |
| `native_tcp_socket<Backend>` | 无 | net 没有原生层；`socket_impl` 的虚调用是每次操作一次，量级远小于系统调用 |

## 3. 就绪型后端族的内部协议

`reactor_backend` 对所有解复用器相同的部分：

- **描述符表**：`descriptor_state{fd, ops[2], ready, interest, demux_index}`；每个方向最多
  一个排队操作。`registered_` 集合用来识别注销后迟到的事件。
- **就绪位**（边沿触发用）：事件到达而无人排队时记下，下一次 `start_op` 先消费——
  避免推测尝试与排队之间丢失边沿。
- **兴趣刷新**（电平触发用）：每次排队/摘除后 `refresh_interest`，把 `wanted()`
  （哪些方向有操作）交给 `demux.update`；否则一个可读而无人读的描述符会让 `wait` 忙转。
- **定时器堆**、**信号泵**（`counts_as_work = false` 的常驻读操作）、**工作计数**。

解复用器要回答的只有：怎样登记、怎样等、怎样被唤醒。

| | epoll | poll | select |
| --- | --- | --- | --- |
| 触发 | 边沿（一次登记 IN\|OUT\|RDHUP\|ET） | 电平 | 电平 |
| 注册表 | 内核持有（`data.ptr = &state`） | `vector<pollfd>` + 并行 `states`，按 fd 反查 | `by_fd[FD_SETSIZE]` + `interest[]` |
| 兴趣为 0 | 不需要 | `fd` 取负让 poll 忽略（否则 HUP/ERR 仍报告 → 忙转） | 不放进 fd_set |
| wait 与并发修改 | 内核处理 | 锁内复制快照再阻塞；兴趣增加时写 eventfd 唤醒重建 | 锁内重建 fd_set 再阻塞；同上 |
| 中断 | eventfd（`data.ptr = nullptr` 标记） | eventfd 是数组第 0 项 | eventfd 在 read 集合里 |
| 限制 | — | O(n) 每次 wait | fd < FD_SETSIZE（超出 EMFILE），O(FD_SETSIZE) 每次 wait |

`wait()` 只由当前运行事件循环的一个线程在反应器锁外调用；`add / update / remove` 在反应器
锁内被任何线程调用。电平触发的实现自己再加一把注册表锁保护快照构建。锁序：反应器锁 →
解复用器锁；反应器锁 → 信号状态锁 → io_context 队列锁。

## 4. io_uring 后端（完成型）

io_uring 是完成型：把"读 fd 到这些缓冲区"作为一个 SQE 提交，内核完成后给一个 CQE。它
**不属于就绪型族**，不是再写一个 `demultiplexer`，而是另一个 `io_backend` 实现
（`src/detail/io_uring/`）。环是裸系统调用实现的（`io_uring_setup` / `io_uring_enter` +
mmap 三块内存），只需要 `<linux/io_uring.h>`，不依赖 liburing。

### 线程模型

- **SQ 单生产者**：所有提交（`submit`、取消请求、常驻轮询的重新武装）都在 `uring_backend`
  的环锁内取 SQE、填参数、发布尾指针——**只是用户态内存写，不进内核**。环满时操作进入延迟
  队列，由 `run()` 补提交。
- **CQ 单消费者**：只有当前运行事件循环的线程调用 `run()`（`io_context` 以 `reactor_busy`
  保证）。`run()` 在环锁内补提交延迟队列、发布尾指针、置 `waiting_`，然后**一次**
  `io_uring_enter(to_submit, min_complete=1, GETEVENTS[, EXT_ARG 超时])` 把提交与等待合并，
  收割全部 CQE。已有 CQE 且无待提交时连这一次也省掉。
- **跨线程提交者**：另一个线程在 `run()` 阻塞期间 `submit` / `cancel` 时看到 `waiting_`，写
  eventfd 叫醒它去冲提交。单线程 io_context 里永远不会发生（提交者就是运行线程，回到
  `run()` 时自然冲掉）。
- **一条 C++ 内存模型上的 happens-before**：提交者在环锁内写完操作参数后 unlock；内核发布
  CQE 这一跳对 C++ 不可见，所以 `run()` 在读操作对象之前先取一次环锁，再 `on_complete`。
  否则 TSan 会（正确地）报告 `begin_read` 的写与 `on_complete` 的读之间没有同步边。
- 完成的操作在锁外 `complete()`（`env->executor.post(cont)`）并归还工作计数；
  `complete()` 之后不再触碰操作对象（续体恢复后套接字可能已被销毁）。

### 三步协议的实现

| 步骤 | 就绪型 | io_uring |
| --- | --- | --- |
| `begin_*` | 记缓冲区描述符 | 同；`iovec[]` 与 `msghdr` 住在操作里，钉住到 CQE 到达 |
| `ready()` | 推测系统调用，成功则不挂起 | **自适应推测**（`speculation_state`，照 Corosio）：先试一次非阻塞系统调用；EAGAIN 后关掉该方向的推测直到一次异步完成证明就绪；读方向连续 4 次 EAGAIN 永久关闭——服务端的读、acceptor 这类"总是先等"的套接字不再白跑 `read`，直接走完成型路径。connect 除外——`IORING_OP_CONNECT` 自己处理 EINPROGRESS |
| `suspend()` | `start_op` 排队，等就绪 | 环锁内取 SQE、`prepare`、`user_data = &op`、发布尾指针；返回 noop。进内核推迟到 `run()` |
| 完成 | 反应器线程 `perform()` 后 `complete()` | `run()` 收 CQE → `on_complete(res)` 记 ec / bytes（`-ECANCELED` → `operation_aborted`，读到 0 → `eof`）→ `complete()` |
| 取消 | 锁内摘下，立即 `complete(operation_aborted)` | 提交 `IORING_OP_ASYNC_CANCEL`（定时器：`TIMEOUT_REMOVE`），**等原操作的 CQE**；在延迟队列里则摘下立即完成；尚未提交则记 `cancel_requested`，`submit` 返回 false，`suspend` 直接以 aborted 恢复；多发模式下停着的 accept waiter 不在环里，由套接字自己以 aborted 完成 |
| accept | 就绪 → `accept4` | **多发 accept**（`IORING_ACCEPT_MULTISHOT`，5.19+）：`listen()` / assign 已监听的 fd 时武装一个 SQE，不计入用户工作；每个带 `F_MORE` 的 CQE 送来一个 fd——有停着的 `accept()` 就交给它，否则进 parked 队列；`accept()` 先看 parked 队列、再投机 `accept4`（自适应关闭）、最后作为 waiter 停着**不提交 SQE**。终止 CQE 后重新武装；`-EINVAL` 退回每次 accept 一个 SQE。不取对端地址（多发下所有完成共用一块暂存会互相覆盖），由 `remote_endpoint()` 事后取。关闭 / 释放监听描述符时 `ASYNC_CANCEL` 并把操作**退役**给后端持有到终止 CQE（内核仍引用 user_data），排队的连接被关闭 |
| `close()` | 注销 + 取消 + `::close` | 先对在飞操作请求取消再 `::close`——在飞请求持有文件引用，关闭描述符不会结束它们 |
| 中断 | eventfd 在解复用器集合里 | eventfd 上一个常驻**多发** `POLL_ADD`（`IORING_POLL_ADD_MULTI`）：CQE 带 `F_MORE` 表示仍在武装，只在终止或内核不支持（`-EINVAL` → 退回一次性）时重新武装；`interrupt()` 只是 `write(eventfd)`，不碰环、不加锁 |
| 信号管道 | 常驻读操作 | 常驻多发 `POLL_ADD` 监视读端，可读时排空并 `deliver` |
| 定时器 | 二叉堆；最早到期经 timerfd（`TFD_TIMER_ABSTIME`）送进解复用器 | 每次 wait 一个 `IORING_OP_TIMEOUT`（`IORING_TIMEOUT_ABS`，CLOCK_MONOTONIC 与 steady_clock 同源）；`-ETIME` 成功，`-ECANCELED` 取消 |

取消的可见差异：就绪型的 `steady_timer::cancel()` / `socket::cancel()` 同步完成操作；
io_uring 的取消异步，`cancel()` 返回 1 只表示"已请求"，操作在 CQE 到达时以
`operation_aborted` 完成。`has_pending()` 的语义因此是"CQE 尚未到达"——销毁契约照旧。
一个微妙点：`cancel()` 在操作已完成、协程尚未恢复时到达会留下过期的 `cancel_requested`，
`finish_*` 在销毁 stop_callback 之后把它清零。

### 上层没有改动

`tcp_socket` / `udp_socket` / `steady_timer` / `signal_set` / `resolver` 一行未改；
`io_context.cpp` 的 `make_backend` 多一个 `backend_kind::io_uring` 分支，`backend.hpp` 多一个
`io_uring_t` 标签与运行时探测 `backend_available()`（内核 sysctl `io_uring_disabled` 或
seccomp 可能禁用它）。`posix::*` 的同步部分（`create_socket`、`set_nonblocking_cloexec`）
直接复用。全部平台测试为四种后端各编译一个变体，ASan / UBSan / LSan / TSan 全绿。

### 与 Corosio io_uring 调度器的对齐

最初的实现每个操作 `suspend` 时立刻 `io_uring_enter(to_submit)`，`run()` 再 `io_uring_enter(GETEVENTS)`
等待，且每个操作都无条件推测——单连接回环里阻塞路径是 `read`(EAGAIN) + 两次 enter，比 epoll 的
`read`(EAGAIN) + `epoll_wait` 多一次，吞吐自然不占优。对照 Corosio
`native/detail/io_uring/io_uring_scheduler.hpp` 之后按它的做法改了四点：

| | Corosio | net（现在） |
| --- | --- | --- |
| 提交时机 | `io_uring_submit_op` 只写 SQE；首个提交者 CAS 后向调度器队列 post 一个 `submit_sqes_op`，它用一次 `io_uring_submit_and_get_events` 冲整批 | `submit` 只写 SQE；`run()` 用一次 `enter(to_submit, 1, GETEVENTS)` 合并提交与等待；跨线程提交者经 eventfd 叫醒等待者 |
| 推测 | `speculative_state`：EAGAIN 关、异步就绪开、读连续 4 次永久关 | 同（`uring_socket.hpp` 的 `speculation_state`） |
| 环标志 | 单线程模式 `SINGLE_ISSUER \| DEFER_TASKRUN`，多线程不设 | `io_context{net::io_uring, net::single_thread_hint}` 时同样两个标志 + `R_DISABLED`，第一个 `run()` 的线程 `IORING_REGISTER_ENABLE_RINGS` 成为提交者；老内核 EINVAL 退回普通模式 |
| 唤醒 | eventfd 多发 poll | 同 |
| accept | 多发 accept + parked fd 队列；`retire_op` 把旧武装交给调度器等终止 CQE | 同：`uring_multishot_accept_op` + `acceptor_state`（parked FIFO / waiter / broken），`uring_backend::retire` 持有退役操作到终止 CQE 后删除 |
| 内联完成预算 | 推测成功直接对称转移，有 `try_consume_inline_budget` 上限 | 推测成功 `await_ready` 为真直接继续，无预算 |

`single_thread_hint` 是**承诺**而不是提示（对应 Asio 的 `BOOST_ASIO_CONCURRENCY_HINT_UNSAFE`）：
`io_context` 文档说 `concurrency_hint` 只是提示、`run()` 可以从任意多线程调用，所以不能把
硬约束挂在 `concurrency_hint == 1` 上——测试里默认构造的 `io_context` 就在 4 个线程上 `run()`。
违反承诺时内核以 `EEXIST` 拒绝第二个线程的 `io_uring_enter`，后端抛出。

顺带发现并修掉两处影响**所有**后端的浪费，都是用 `strace -c` 看基准时暴露的：

1. `io_context::post` 在 `reactor_busy` 时向 eventfd 写一字节打断反应器——但后端在 `run()`
   里完成操作时 post 续体的正是运行线程自己，它不在等待。每次完成白付 1 写 + 2 读 + 1 个多余
   的就绪事件 / CQE。现在记下进入 `backend.run()` 的线程 id，自己 post 不打断。
2. 就绪型后端把定时器最早到期向上取整到毫秒作为 `epoll_wait` 超时；1 µs 的定时器本该睡满
   1 ms，只是被上面的自打断掩盖了（`epoll_wait` 立即返回，1 µs 早已过去）。去掉自打断后暴露；
   换成纳秒超时（`epoll_pwait2` / `ppoll` / `pselect`）又撞上线程的 timer slack（默认 50 µs，
   poll/select/epoll 的超时都受它影响，定时器变成 56 µs）。最终照 Asio 的做法：一个 timerfd
   （hrtimer，不受 slack 影响）武装到堆顶到期、注册进解复用器；只在堆顶变化时 `timerfd_settime`。
   定时器从 2.9 µs 降到 1.5 µs。

效果（`bench_net`，同一台机器）：往返 epoll 4.85 → 4.18 µs，io_uring 4.95 → 4.03 µs；定时器
epoll 2.94 → 1.53 µs，io_uring 1.97 → 1.38 µs；quick 基准全程系统调用 epoll 357k → 133k，
io_uring 378k → 92k。多发 accept：`tcp_tests_io_uring` 的 42 次成功 accept 全部经 CQE 交付，
`accept4` 只剩投机的 13 次 EAGAIN；每接受一个连接 1 次分配（新套接字的 impl），epoll 是 3 次。
回环 connect + accept 两边都在 53 µs 左右——瓶颈是 TCP 握手与套接字创建 / 关闭，多发省的是
每个 accept 的一次提交，不是这条路径的大头。

多发 accept 的两个生存期细节值得记下：
- **退役而不是删除**：关闭监听描述符不会结束在飞的多发请求（它持有文件引用），必须
  `ASYNC_CANCEL`，而终止 CQE 异步到达——此时操作对象若已随套接字销毁，内核还会用它的
  `user_data` 投递。所以 `close()` / `release()` 把操作的 `owner` 清空（环锁内，事件循环线程也在
  锁内读它）后交给 `uring_backend::retire`，后端持有到终止 CQE 才删；退役后送来的 fd 直接关闭。
- **多发操作的完成在环锁内处理**：`on_complete` / `rearm()` 读 `owner`，退役也改 `owner`，两者
  都在环锁内；普通操作的完成仍在锁外。锁序是环锁 → acceptor 锁 → io_context 锁，`accept()`
  一侧只拿 acceptor 锁，退回一次性提交时用 `submit_locked`（已持环锁）。

## 5. 接入 IOCP

Windows 完成端口同样是完成型，且没有 fd：

```
src/detail/iocp/
    iocp_backend.hpp/.cpp       io_backend：CreateIoCompletionPort、GetQueuedCompletionStatusEx、PostQueuedCompletionStatus 唤醒
    overlapped_op.hpp           op 基类：OVERLAPPED 首成员 + cont/env/pending/stop_cb；完成包的 key 是 op 指针
    iocp_socket.hpp/.cpp        socket_impl：WSARecv / WSASend / ConnectEx / AcceptEx（预创建接受套接字）/ WSARecvFrom / WSASendTo
    iocp_timer.hpp/.cpp         timer_impl：定时器线程 + PostQueuedCompletionStatus，或 CreateWaitableTimerEx（Corosio: win_timers_thread / win_timers_none）
```

差异点（与 io_uring 后端相同的部分——环锁 / 完成收割 / 异步取消 / `complete()` 后不触碰
操作——可以直接照搬 `uring_backend` 的骨架）：`native_handle_type` 是 `SOCKET`；`accept` 必须先 `WSASocket` 出接受套接字再
`AcceptEx`，完成后 `SO_UPDATE_ACCEPT_CONTEXT`；`connect` 需先 `bind`；取消用
`CancelIoEx(handle, &overlapped)` 并等完成包（`ERROR_OPERATION_ABORTED`）；信号没有管道，
`register_signal_reader` 改为 CRT `signal()` + `PostQueuedCompletionStatus`（Corosio:
`win_signals`）；`socket_base.cpp` 里 `::bind / ::listen / setsockopt` 的 POSIX 调用要
经 `posix::` 同名封装换成 Winsock 版本。公共头里 `native_handle_type = int` 需要改成平台
类型别名。

## 6. 为什么这样切

- **后端只做两件事**：等一批事件、把完成的操作交给它们的执行器。队列、工作计数、
  `dispatch/post`、线程与上下文的关系都在 `io_context::impl` 里，与后端无关——Corosio
  把它们放进每个 scheduler，五个后端各复制一份。
- **操作状态住在 I/O 对象里**（而不是 awaiter 或堆上）对完成型后端尤其重要：内核持有
  指向缓冲区与 op 的指针直到完成，op 的地址必须稳定、生命周期由销毁契约保证。
- **抽象接口的粒度是"一次操作"**（`begin/ready/suspend/finish`），不是"一个系统调用"。
  就绪型与完成型的差别全部封装在实现里，awaiter 与 `tcp_socket` 一行不改——io_uring
  后端的接入验证了这一点。
