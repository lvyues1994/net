# TLS：传输安全包装器

P4100R1 §8.8 把 TLS 列为 Paper 14 "TLS: transport security wrappers"，Corosio 的实现形态是
`tls_context`（配置）+ 抽象 `tls_stream` + 具体 `openssl_stream` / `wolfssl_stream`。`net` 沿用
这个形态，用 OpenSSL API 子集同时覆盖 OpenSSL 与 BoringSSL 两个提供者。

## 形态

```
net::tls::context        提供者中立的配置：证书 / 私钥 / 信任锚 / 版本 / 密码套件 / ALPN / 验证 / 回调
net::tls::stream         抽象基类：handshake(role) / shutdown() / reset() / set_hostname() / alpn_selected()
                         非虚模板 read_some / write_some → 虚 do_read_some / do_write_some
                         ⇒ 满足 Stream 概念，可放进 any_stream
net::tls::openssl_stream 具体实现：拥有（按值）或引用（按指针）一个 Stream，经 any_stream 类型擦除
```

```cpp
net::tls::context ctx;
ctx.set_verify_mode(net::tls::verify_mode::peer);
ctx.set_default_verify_paths();
ctx.set_alpn({"h2", "http/1.1"});

net::tls::openssl_stream tls{&sock, ctx};      // 或 {std::move(sock), ctx} 拥有它
tls.set_hostname("example.com");               // SNI + 证书主机名校验；IP 字面量只校验、不发 SNI
CO2_AWAIT_SET(h, tls.handshake(net::tls::role::client));
net::any_stream erased{&tls};                  // 业务逻辑对明文流编译一次
CO2_AWAIT_SET(r, erased.read_some(net::buffer(buf)));
CO2_AWAIT_SET(s, tls.shutdown());
```

## 引擎与驱动

引擎是 sans-I/O 的：`SSL` 对象挂两个内存 BIO，不做任何系统调用。每个 TLS 操作是一个驱动
协程 `run_op`（`src/tls/openssl_stream.cpp`）：

```
loop:
  step = 引擎一步（SSL_do_handshake / SSL_read / SSL_write / SSL_shutdown）
  若输出 BIO 有密文：拿写门闩 → 全部 net::write 到底层流 → 放门闩
  step.completed ？ 返回 (ec, bytes)
  step.want_input ？ 底层流 read_some 一批密文 → BIO_write 进输入 BIO
```

因为引擎不碰 I/O，TLS 与 `io_context` 的后端（epoll / poll / select / io_uring）完全无关——
`tls_tests` 为四个后端各编译一个变体，同一份 TLS 代码全部通过。

**写门闩**：读操作也可能产生输出（TLS 1.3 的 KeyUpdate 应答、告警），两个操作同时冲输出会
违反底层流"同一方向只能有一个操作"的契约。`write_gate` 是单线程（同一 strand）的异步门闩：
拿不到就把 continuation 挂进侵入式等待队列，释放时经等待者的执行器 `post` 恢复。上下文里设置
了 `SSL_OP_NO_RENEGOTIATION`，握手之后写方向永远不需要读传输，所以不需要读门闩。

**结束语义**（与 Corosio 一致）：

| 情形 | 结果 |
| --- | --- |
| 对端发送 close_notify | `read_some` → `error::eof`；`shutdown()` 完成 |
| 传输在 close_notify 之前结束 | `error::stream_truncated`——无法与截断攻击区分，不报告为干净关闭 |
| stop_token 请求停止 | 底层操作以 `operation_aborted` 完成并原样传出 |
| 证书验证失败 | `tls::verify_category()` 的 `X509_V_*` 码，`message()` 是 OpenSSL 的文字 |
| 其它提供者错误 | `tls::provider_category()`，码来自 `ERR_get_error()` |

`handshake()` 每次都是全新会话（重新 `SSL_new`）：成功或失败都消耗流状态，再次调用等价于
先 `reset()`。

## 线程模型

与 Corosio 相同：不同对象安全；同一对象上握手 / shutdown 不能与其它操作并发；握手后允许一个
读与一个写同时在飞。多线程 `io_context` 上同一流的所有操作必须在同一个 `strand` 里。

与 Corosio 的一个差别：这里 `shutdown()` 不能与挂起的读重叠（两者都会读底层流）。先让读以
`eof` 结束再 `shutdown()`，或者只在写侧调用 `shutdown()` 并让对端先关。

## 提供者：OpenSSL 与 BoringSSL

构建期 `-DNET_TLS_PROVIDER=OpenSSL|BoringSSL|OFF`。两者共用同一份源码；差异全部在
`OPENSSL_IS_BORINGSSL` 分支里：

| 点 | OpenSSL | BoringSSL |
| --- | --- | --- |
| TLS 1.3 密码套件配置 | `SSL_CTX_set_ciphersuites` | 不可配置：`set_ciphersuites_tls13` 返回 `function_not_supported` |
| `SSL_set_tlsext_host_name` | 带 C 风格转换的宏 → 直接调 `SSL_ctrl` | 真函数 |
| `SSL_CTX_set_{min,max}_proto_version` 参数 | `int` | `uint16_t`（统一传 `uint16_t`） |
| `provider_name()` | `OpenSSL_version(OPENSSL_VERSION)` | `"BoringSSL"` |

BoringSSL 可以给现成的安装（`-DNET_BORINGSSL_ROOT=<含 include/ lib/>`），或让 CMake 用
FetchContent 从源码构建（`NET_BORINGSSL_GIT_TAG`，默认 `0.20250701.0`；约 400 个目标，不需要
Go / Perl，24 核约 1.5 分钟）。CI 有一个 BoringSSL 作业，`tls_tests` 四个后端变体在两个提供者
下都通过。

回环基准（i7-13700KF，单线程，P-256 证书，TLS 1.3，`benchmarks/bench_tls`）：

| | OpenSSL 3.0.13 | BoringSSL 0.20250701 |
| --- | --- | --- |
| TCP connect + 完整握手（双端同一线程） | 457 µs | 203 µs |
| 加密回环往返 64 B | 7.7 µs | 6.2 µs |
| 加密回环往返 4 KiB | 10.6 µs | 8.9 µs |
| 单向吞吐（16 KiB 写） | 1758 MiB/s | 2448 MiB/s |

明文 TCP 回环往返为 4.9 µs（`bench_net`），可对照 TLS 记录层的开销。

## 尚未提供

wolfSSL 提供者、PKCS#12、CRL / 吊销策略、SNI 服务端回调（按主机名切换证书）、会话复用、
`shutdown()` 与挂起读的重叠。`context` 的验证回调只暴露 `native_handle()`（`X509_STORE_CTX*`）
与 `error() / depth() / subject()`。
