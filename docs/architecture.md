# 架构

本文描述 net 的分层、每层与提案（P4003R3 / P4172R1 / P4100R1 / P4124R0）的对应关系、
在 co2（C++14 无栈协程）上落地时的关键决策，以及生命周期契约。

## 问题

Network Endeavor 主张：C++20 协程本身就是网络 I/O 的执行模型，不需要在其上再叠一层
sender 抽象。协议只回答三个问题——协程在哪个执行器上恢复、该不该停、帧在哪分配——
以 `io_env` 打包、经双参数 `await_suspend(coroutine_handle<>, io_env const*)` 注入。
net 用 C++14 与 co2 实现这套协议：语言里没有 `co_await`，但 co2 的 promise/awaiter 协议
与标准同形，`await_transform` 这个注入点是存在的。

## 分层

```
┌──────────────────────────────────────────────────────────────────────┐
│ 应用          examples/ echo_server echo_client http_get timers       │
├──────────────────────────────────────────────────────────────────────┤
│ TLS（libnet）   tls::context  tls::stream  openssl_stream            │
│                 sans-I/O 引擎（OpenSSL / BoringSSL）+ 驱动协程，只依赖 Stream │
├──────────────────────────────────────────────────────────────────────┤
│ 具体层（libnet） io_context  tcp / udp / timer / resolver / signal_set │
│                 ↓ 抽象接缝 src/detail/backend.hpp                    │
│ 后端            reactor_backend + demultiplexer{epoll, poll, select} │
│                 uring_backend（io_uring，完成型）                     │
├──────────────────────────────────────────────────────────────────────┤
│ 组合子        when_all  when_any          detail/combinator          │
│ 流            stream (read/write/read_until)  any_stream             │
│ 缓冲区        buffers  dynamic_buffer  span                          │
├──────────────────────────────────────────────────────────────────────┤
│ 执行器        thread_pool  strand  any_executor  executor_ref        │
│ 启动          run_async  run              detail/completion_frame    │
├──────────────────────────────────────────────────────────────────────┤
│ 协议核心      io_env  continuation  task  io_awaitable_promise_base  │
│               this_coro  io_result  immediate  execution_context     │
│               memory_resource (帧分配器带外通道, safe_resume)        │
├──────────────────────────────────────────────────────────────────────┤
│ co2           coroutine_handle  CO2_* 宏  stop_token  contract       │
└──────────────────────────────────────────────────────────────────────┘
```

依赖只向下。协议核心到组合子仅含头文件、与平台无关；平台层编译进 `libnet.a`：具体层
只依赖 `src/detail/backend.hpp` 的抽象接口（`io_backend` / `socket_impl` / `timer_impl`），
后端实现在 `src/detail/reactor/`（就绪型族）、`src/detail/io_uring/` 与 `src/detail/posix/`，
全部只在 `src/` 内可见。后端的选择、与 Corosio 的对照、io_uring / IOCP 的接入方案见
`docs/backends.md`。

TLS 层位于具体层之上、应用之下，但它只依赖 `Stream` 概念（经 `any_stream` 类型擦除）而不
依赖任何具体 I/O 对象或后端：引擎是 sans-I/O 的（`SSL` + 内存 BIO），驱动协程用底层流的
`read_some` / `write_some` 泵字节。`tls::stream` 自身满足 `Stream`，所以业务逻辑对
`any_stream&` 编译一次就同时覆盖明文 TCP、TLS 与测试替身。设计见 `docs/tls.md`。

## 协议核心

### env_awaiter：把 IoAwaitable 装进 co2

co2 的 `CO2_AWAIT(e)` 展开为标准的 `co_await` 序列：`promise.await_transform(e)` →
`operator co_await` 查找 → `await_ready` → `await_suspend(coroutine_handle<Promise>)` →
`await_resume`。`io_awaitable_promise_base<Derived>::await_transform(A&&)`：

1. 截获 `this_coro::*` 标签，返回不挂起的 `immediate_value<T>`；
2. 其余交给 `Derived::transform_awaitable`（扩展点），再用 co2 的 `getAwaiter` 完成
   `operator_co_await` 查找，得到最终 awaiter；
3. 把 awaiter **按值**移进 `env_awaiter<Inner>`（co2 D10：awaitable 表达式里的临时对象
   活不过挂起点，awaiter 必须自己拥有一切），连同 promise 里的 `io_env const*`。

`env_awaiter::await_suspend(coroutine_handle<Promise>)` 调用
`inner.await_suspend(static_cast<coroutine_handle<>>(h), env)`；`await_resume` 先
`set_cached_frame_allocator(env->frame_allocator)` 再转发。`Inner` 没有双参数
`await_suspend` 时 `static_assert` 给出清晰的诊断。

一个实现细节：基类模板里对 `Derived` 成员的访问必须是依赖名（`template <class A,
class D = Derived>`），否则 GCC 在基类实例化时就检查它，而此时 `Derived` 不完整——
co2 的 D9 宽松规则会把 SFINAE 失败当成"没有 await_transform"静默回落到原始 awaitable。

### task<T>

与 co2 `Task<T>` 同形（惰性、单消费者、只可移动、右值本身是 awaiter、左值经
`operator_co_await() &` 借用），差别是协议要求的部分：`await_suspend(h, io_env const*)`
让子 promise 继承整个环境；`initial_awaiter::await_resume` 把环境里的帧分配器写进线程
局部槽位；promise 满足 IoRunnable（`handle() / release() / exception() / result() /
set_continuation() / set_environment()`）。

### 帧分配

`io_awaitable_promise_base::operator new(size)` 读 `get_cached_frame_allocator()`
（空则 `new_delete_resource()`），多分配一个 `memory_resource*` 存在帧尾；
`operator delete(p, size)` 从帧尾取回。co2 检测到 promise 有 `operator new` 就用它
（忽略 allocator 参数路径）。`safe_resume` 在恢复前后保存/恢复槽位——所有执行循环
（`io_context::run`、`thread_pool` 工作线程、`strand` 派发帧）都经由它恢复协程。

`execution_context` 自带一个 `recycling_memory_resource`：尺寸按 64 字节粒度归类
（≤ 8 KiB），每类一条侵入式空闲链表加一个自旋锁（临界区三条指令），缓存最近释放的块；
协程帧尺寸重复、生命周期嵌套，稳态下每次帧分配命中缓存。它是否作为**默认**帧分配器由
构建选项 `NET_DEFAULT_FRAME_ALLOCATOR` 决定：基准（`benchmarks/bench_core`）显示 glibc 的
tcache malloc 比任何带原子操作的回收器都快，所以 Linux 上默认是 `new_delete_resource()`；
回收器留给 malloc 慢的平台和有界内存池的场景（`ctx.set_frame_allocator(&ctx.recycling_frame_allocator())`）。
最初的实现是无锁 Treiber 栈；它读取"可能已被别的线程弹出并投入使用"的块的 `next`，实践中
无害但按 C++ 内存模型是数据竞争（TSan 会报），故换成原子操作数相同的自旋锁。

### completion_frame

启动函数与组合子需要"task 完成时通知我"的续体，但没有协程可以 `co_await`。
`detail::completion_frame` 是一个手写的标准布局帧（首成员是 co2 的 `FrameHeader`）：
把它的 `handle()` 设为 task 的续体，task 的 `final_suspend` 对称转移到它，co2 的恢复
循环调用它的 `step` → 我们的回调；回调返回下一个要恢复的句柄（通常是
`executor.dispatch(parent)` 的结果）或空。这是 co2 自己的 `syncWait` / `spawn` /
`whenAll` 使用的技术。

## 启动

- `run_async(ex[, token, mr][, on_value, on_error])(task)`：第一段构造 launcher，把帧分配器
  （显式给出的，否则 `ex.context().get_frame_allocator()`）写进槽位并在析构时恢复；第二段
  堆分配 `run_async_state`（执行器副本、`io_env`、task、处理器、completion_frame），
  `on_work_started()`，`post` 启动句柄。完成回调：调用处理器（默认丢弃结果、重抛
  异常）、`on_work_finished()`、释放状态。`io_env::executor` 指向状态里的执行器副本，
  因此 `executor_ref` 的生存期由链的生存期保证。
- `run(ex / token / mr)(task)`：awaiter 拥有子 task 与一个新 `io_env`；`await_suspend`
  经新执行器 `dispatch` 启动子任务（已在该上下文则对称转移），子任务完成后边界帧经
  **父**执行器 `dispatch` 恢复父协程。子任务运行期间持有新执行器的工作计数。

## 执行器

`is_executor<E>` 以 SFINAE 检查 P4003R3 §4.3 的七条要求。`executor_ref` 是两指针的
非拥有视图（`detail::executor_vtable_for<E>`），`any_executor` 是拥有型的。

- `io_context`：互斥锁保护的侵入式 `continuation` 队列 + 工作计数 + 反应器。`run()`
  循环：有队列元素就 `safe_resume`；无工作则返回；否则一个线程进反应器
  （`epoll_wait`），其它线程等条件变量。`post` 叫醒空闲线程或（唯一的线程在
  `epoll_wait` 里时）写 eventfd 打断它。`dispatch` 在本线程正 `run()` 本上下文时直接
  返回 `c.h`。线程与上下文的关系用线程局部的调用栈记录（允许嵌套 `run()`）。
- `thread_pool`：固定线程数，同一份队列/工作计数模型；池自身持有一份初始工作直到
  `join()`。
- `strand<Ex>`：互斥锁 + 侵入式队列 + 一个派发帧（completion_frame）。首个到达的续体
  把派发帧 `post` 到内层执行器；派发帧取走整批续体逐个 `safe_resume`，期间到达的排入
  下一批，批结束后重新 `post`（公平性）。派发帧排队期间实现自我保活。

## 平台层：具体层 + 后端

`io_context(tag)` 在构造时选择后端（`net::epoll` 默认、`net::poll`、`net::select`、
`net::io_uring`）：
`io_context.cpp` 的 `make_backend(kind)` 用 `make_service` 在上下文里注册一个
`detail::io_backend` 服务。`io_context::impl` 自己拥有队列、工作计数与 run 循环，后端只
负责"等一批事件、把完成的操作交给它们的执行器"，以及创建 `socket_impl` / `timer_impl`。

`tcp_socket` / `udp_socket` / `tcp_acceptor` 是 `socket_base` 的薄包装：同步操作（bind /
listen / setsockopt / getsockname…）直接对 `impl_->native_handle()` 做系统调用；异步操作是
一个三步协议 `begin_*`（记参数）→ `ready`（推测）→ `suspend`（排队/提交）→ `finish_*`
（取结果），awaiter 的三个方法一一转发。`steady_timer` / `signal_set` 同理。

### 就绪型后端族（`src/detail/reactor/`）

`reactor_backend`（对 epoll / poll / select 相同）：

- `descriptor_state`（每个描述符）：fd、两个方向各一个 `reactor_op*`、就绪位、兴趣位；
- `reactor_op`：`perform()`（就绪时在锁内执行非阻塞系统调用，返回是否完成）、
  `complete()`（锁外恰好一次：`env->executor.post(cont)`）；
- 定时器二叉堆、信号泵（`counts_as_work = false`）、工作计数、注销后迟到事件的识别。

等待机制注入为 `demultiplexer`（`add / update / remove / wait / interrupt`）：epoll 边沿
触发一次登记、由反应器维护就绪位；poll / select 电平触发，反应器在每次排队/摘除后以
`update` 调整兴趣（否则可读而无人读的描述符会让 `wait` 忙转），解复用器在锁内构建快照
再阻塞、兴趣增加时写 eventfd 唤醒重建。

### suspend() 的发布规则

多线程 `io_context` 下，`await_suspend` 一旦把操作发布出去（登记到反应器、写进 SQ 环、置
`waiting`），另一个线程可能立刻完成它、恢复协程、跑到结束并销毁帧——帧里有套接字（连同
操作对象）和 `run_async` 的状态（连同 `io_env`）。所以三条规则（`src/detail/backend.hpp`）：
发布之后不碰 `env` / `op` / `this`；`on_work_started` 在发布之前、同一把锁内（否则完成方的
`on_work_finished` 可能先到，把 `outstanding_work` 打到 0，`run()` 提前返回）；`stop_callback`
在发布之前装好，"装好之后、发布之前"到达的停止请求由取消路径记为 `cancel_requested`，发布时
看到即同步中止——早期实现在发布之后再读一次 `env->stop_token.stop_requested()` 来关这个窗口，
在 2 核的 CI 机器上被 ASan 抓到 use-after-free。

### 操作状态住在 I/O 对象里

`tcp_socket::read_some(buffers)` 把缓冲区序列展平交给 `socket_impl::begin_read`，返回的
awaiter 只有一个指针（`socket_read_awaitable{impl}`）——总能内联进协程帧的 awaiter 槽，
每次操作零分配，且 awaiter 的三个方法都定义在库内（ABI 稳定）。代价是每个套接字
每个方向同一时刻只能有一个未完成操作（流的常规约束），以及**有未完成操作时不能销毁
I/O 对象**（契约违规；移动是安全的，实现对象地址不变）。对完成型后端这一点更重要：
内核持有指向缓冲区与操作的指针直到完成。

`ready` 做推测性系统调用（数据已就绪则不挂起）；`suspend` 记下续体与环境、注册
stop_callback（回调 `cancel_op`）、`start_op`；排队后复查 `stop_requested()` 关闭"注册
回调与排队之间停止到达"的窗口。`finish_*` 销毁 stop_callback、清 pending、交出结果。

### 其它 I/O 对象

- `steady_timer`：`reactor_timer` 即 `timer_op`；`expires_*` / `cancel` 取消挂起的 wait。
- `resolver`：每个 io_context 一个 `resolver_service`，惰性启动一条工作线程串行执行
  `getaddrinfo` / `getnameinfo`；取消不能中断进行中的调用，结果到达后以
  `operation_aborted` 完成。与后端无关。
- `signal_set`：进程唯一的自管道 + 每个信号的注册表；每个 io_context 的
  `signal_service` 经 `io_backend::register_signal_reader` 请求后端监视管道读端（就绪型
  用一个永不完成的读操作），可读时排空并分发给所有注册了该信号的 signal_set。

锁序：反应器锁 → 解复用器锁；反应器锁 → 信号状态锁 → io_context 队列锁。

## 组合子

`when_all` / `when_any` 的每个子 awaitable 由一个 runner 协程（`task<void>`）
`co_await`：runner 的 promise 把组合子的 `child_env`（父执行器、组合子自己的
`stop_source` 的 token、父帧分配器）注入子 awaitable，并捕获异常。runner 的续体是嵌在
组合子里的 `completion_frame`；子完成时记录结果、必要时请求兄弟停止，最后一个到达者
经父执行器 `dispatch` 恢复父协程。计数初值 N + 1，启动方放下自己那一份时若归零则不
挂起。

结果类型（P4124R0 §2.3）：`io_result<T>` 载荷 T、`io_result<>` 与 void 不占位、
`io_result<T, U..>` 载荷 `tuple<T, U..>`；有 io 子任务时结果是 `io_result<载荷...>`，
否则 `std::tuple<载荷...>`（全空为 void）。`when_any` 同构载荷给出
`when_any_result<P>`，异构给出 `when_any_result<std::tuple<P...>>`（只有赢家下标处有意义
——C++14 没有 `std::variant`）。

## 与提案的有意偏离

全部来自 C++14 / co2 的限制：

- `co_await e` → `CO2_AWAIT(e)` / `CO2_AWAIT_SET(v, e)`；结构化绑定 → `r.ec` / `r.value`
  （io_result 提供 tuple 协议，C++17 可结构化绑定）；
- concept → `is_*` 特征 + `static_assert`；
- `std::pmr::memory_resource` → `net::memory_resource`（同形）；`std::span` →
  `net::span`（子集）；`std::stop_token` → `co2::stop_token`（同形）；
- `when_any` 异构结果用 tuple 而不是 variant；
- 两段调用的求值顺序在 C++14 未规定：提供工厂形态作为确定性替代；
- 操作状态住在 I/O 对象里而不是 awaiter 里（awaiter 需要放进 co2 固定容量的槽位）。

## 审查记录（2026-09）

两轮独立审查（平台层 / 抽象层）对照上面的不变量逐路径核对，发现并修掉的问题，按严重度：

| 层 | 问题 | 触发 | 修法 / 回归测试 |
| --- | --- | --- | --- |
| 反应器 | connect 报假成功：未连接套接字一注册就报 `EPOLLOUT\|EPOLLHUP`，过期的可写位让 `start_op` 立刻 `perform()`，`SO_ERROR == 0` 就当连上了 | `open()` 之后有任何挂起点再 `connect()`（resolve 后 connect 是常态） | `perform()` 用 `getpeername` 确认（`ENOTCONN` → 继续等），`begin_connect` 先清位；`tcp_tests::connect_after_idle_open_never_reports_a_false_success`（accept 队列满的监听端口作为确定性的 SYN_SENT 目标） |
| strand | 某个续体抛出后 `on_drain` 带着 `locked == true` 和一批未恢复的续体离开，之后所有投递被吞 | `run_async` 默认 `rethrow_error` 在 strand 上 | 异常时把剩余批次放回队首、重新排队排空帧再重抛；`executor_tests::strand_survives_a_throwing_continuation` |
| resolver | 工作线程 `complete()` 在 `post` 之后读成员：协程可能已在 io 线程上结束并销毁 resolver | 任何 resolve | 先取执行器再 post |
| 组合算法 | `read` / `write` 只展平前 16 个缓冲区，之后的静默丢弃 | 序列超过 `max_iovec` 个非空缓冲区 | 每次从已传输位置重新展平；`stream_tests::read_and_write_cover_sequences_longer_than_max_iovec` |
| 缓冲区概念 | `is_*_buffer_sequence` 接受"可转换为缓冲区"的用户类型，单元素遍历返回转换临时对象的地址 | 用户类型带 `operator const_buffer()` | 只认缓冲区类型本身 + 元素可转换的范围；编译期 `static_assert` 回归 |
| signal_set | 没在等时 `cancel()` 留下 `cancel_requested`，下一次 `wait()` 被误中止 | 关闭流程里先 `cancel()` 再 `wait()` | 只有 stop_token 路径记标记；`signal_tests::cancel_without_a_pending_wait_is_a_no_op` |
| io_uring | SQ 满时取消 SQE 被静默丢弃：对端永不发数据的读挂到永远；退役的多发 accept 在已关闭的描述符上继续接受 | 1024 个 SQ 项在高负载下并不难满 | `cancel_pending` + 后端的待发取消列表，`run()` 有空位时补发；操作自己完成时摘掉 |
| io_uring | 多发 accept 终止错误（EMFILE）立刻重武装 → 内核原地打转 100% CPU；没人等时错误被丢 | fd 用尽 | 终止错误不重武装，记下交给下一次 `accept()`，由它重新武装 |
| io_uring | `broken`（退回一次性）锁外读：EINVAL 退回路径与取消竞争会丢掉取消 | 老内核 | `broken` 原子；`cancel_op` 在 acceptor 锁内看 `waiting`，否则交给后端取消 |
| io_uring | `IORING_ENTER_EXT_ARG` 在 5.5–5.10 上 `-EINVAL` | 老内核的 `run_for` | `uring_available()` 要求 `IORING_FEAT_EXT_ARG` |
| 组合子 | `when_all` / `when_any` 逐个"建 runner + 启动"，第 k 个建 runner 抛出时前 k-1 个已在飞 → 析构在飞的 task 是契约违规 | `bad_alloc` | 两阶段：先建全部 runner（唯一会抛的步骤），再一次性启动 |
| run(ex) | 子帧用目标上下文的分配器，但子帧活到父协程从 co_await 返回；目标上下文可能先析构 | `recycling` 默认 + `pool.join()` 后父协程才恢复 | 换执行器不换分配器（显式 `run(mr)` 除外） |
| run_async | 工作计数在状态（帧）释放之前归还 | — | 守卫先于状态声明 |
| any_stream | `start()` 不检查上一个操作是否还在；`await_ready` 抛出时标志泄漏；`new S` 裸指针在 `adopt` 抛出时泄漏 | 误用 / OOM | 契约检查前移；守卫；`unique_ptr` |
| 其它 | `dynamic_container_buffer::prepare(0)` 对空 vector 取 `&v[0]`；`built_in_frame_allocator()` 内联在头里、取决于构建宏（ODR） | — | 判空；移到 .cpp |

审查确认干净的部分：`io_context` 调度器（工作计数、stop/restart、截止、反应器交接、自打断避免）、
三条发布规则在全部 `suspend()` 路径上的落实、锁序（环锁 → acceptor 锁 → io_context 锁；反应器锁
→ 信号锁 → io_context 锁，无反向边）、`uring_backend::run` 的 happens-before 与 F_MORE / 退役处理、
帧所有权（没有恢复已销毁帧的路径）、线程局部帧分配器槽位在所有恢复路径上的保存与恢复、组合子
计数器与结果槽的内存序、strand 串行化与再入、`recycling_memory_resource`。

有意保留的限制（已写进相应文档）：TLS 的 `shutdown()` 不能与挂起的读重叠（两者都读底层流；
Corosio 支持重叠）；`close()` / `cancel()` 不是跨线程安全的（用 stop_token 从别的线程取消）；
`thread_pool` 上没有 `run()` 可以重抛，协程逃出的异常会 `std::terminate`——在池上启动的链应给
`on_error`；`when_any` 输掉的读可能已消费数据（首个完成者胜出语义固有）。