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

## 4. 接入 io_uring

io_uring 是完成型：把"读 fd 到这些缓冲区"作为一个 SQE 提交，内核完成后给一个 CQE。它
**不属于就绪型族**，所以不是再写一个 `demultiplexer`，而是另一个 `io_backend` 实现：

```
src/detail/io_uring/
    uring_backend.hpp/.cpp      io_backend：ring（liburing）、提交队列、CQE 分发、eventfd 唤醒
    uring_op.hpp                op 基类：prep(sqe) / on_cqe(res, flags)；cont、env、pending、stop_cb
    uring_socket.hpp/.cpp       socket_impl：IORING_OP_RECVMSG / SENDMSG / CONNECT / ACCEPT
    uring_timer.hpp/.cpp        timer_impl：IORING_OP_TIMEOUT（或复用反应器的堆 + 最早到期作 wait 超时）
```

与就绪型实现相比，三步协议的语义变化：

| 步骤 | 就绪型 | io_uring |
| --- | --- | --- |
| `begin_*` | 记缓冲区描述符 | 同；缓冲区必须钉住到 CQE 到达（op 状态住在 I/O 对象里已经保证） |
| `ready()` | 推测系统调用，成功则不挂起 | 一般返回 false（可选：`IORING_RECVSEND_POLL_FIRST`；零长度传输仍立即完成） |
| `suspend()` | `start_op` 排队，等就绪 | 取 SQE、`prep`、`io_uring_submit`（或批量在 `run` 里提交）；返回 noop |
| 完成 | 反应器线程 `perform()` 后 `complete()` | `run()` 里 `io_uring_wait_cqe_timeout` 取 CQE → `op.on_cqe(res)` 记 ec/bytes → `complete()`（`env->executor.post`） |
| 取消 | 锁内摘下，立即 `complete(operation_aborted)` | 提交 `IORING_OP_ASYNC_CANCEL`，**等原操作的 CQE**（`-ECANCELED`）再完成；期间 op 保持 pending |
| `close()` | 注销 + 取消 + `::close` | 先取消并等待 in-flight CQE 排空（或 `IORING_ASYNC_CANCEL_FD`），再 close；简化做法：close 后仍把 CQE 路由给 op |
| 中断 | eventfd 在 epoll 里 | eventfd 用 `IORING_OP_POLL_ADD` 多次注册，或 `IORING_OP_MSG_RING` |
| 信号管道 | 常驻读操作 | `IORING_OP_POLL_ADD`（multishot）监视读端，可读时排空 |
| 定时器 | 二叉堆 + wait 超时 | `IORING_OP_TIMEOUT` 每个定时器一个 SQE，取消用 `IORING_OP_TIMEOUT_REMOVE`；或沿用堆 |

上层**不需要改动**：`tcp_socket` / `udp_socket` / `steady_timer` / `signal_set` / `resolver`
只经 `detail/backend.hpp` 的抽象接口对接；`io_context.cpp` 的 `make_backend` 多一个
`backend_kind::io_uring` 分支；`include/net/backend.hpp` 多一个 `io_uring_t` 标签
（编译期 `NET_HAS_IO_URING` 控制）。`posix::*` 的同步部分（`create_socket`、
`set_nonblocking_cloexec`、`connect_result`）直接复用。

一条需要注意的契约：就绪型后端的 `cancel_op` 是同步的（摘下即完成），而 io_uring 的取消
是异步的——`suspend()` 里 stop_token 回调触发 `ASYNC_CANCEL` 后，操作要等 CQE 才算完成。
`socket_impl::has_pending()` 的语义因此是"CQE 尚未到达"，销毁契约照旧成立。

## 5. 接入 IOCP

Windows 完成端口同样是完成型，且没有 fd：

```
src/detail/iocp/
    iocp_backend.hpp/.cpp       io_backend：CreateIoCompletionPort、GetQueuedCompletionStatusEx、PostQueuedCompletionStatus 唤醒
    overlapped_op.hpp           op 基类：OVERLAPPED 首成员 + cont/env/pending/stop_cb；完成包的 key 是 op 指针
    iocp_socket.hpp/.cpp        socket_impl：WSARecv / WSASend / ConnectEx / AcceptEx（预创建接受套接字）/ WSARecvFrom / WSASendTo
    iocp_timer.hpp/.cpp         timer_impl：定时器线程 + PostQueuedCompletionStatus，或 CreateWaitableTimerEx（Corosio: win_timers_thread / win_timers_none）
```

差异点：`native_handle_type` 是 `SOCKET`；`accept` 必须先 `WSASocket` 出接受套接字再
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
  就绪型与完成型的差别全部封装在实现里，awaiter 与 `tcp_socket` 一行不改。
