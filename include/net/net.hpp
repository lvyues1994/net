#pragma once

// net：C++14 上的协程原生 I/O（Network Endeavor / IoAwaitable 协议），协程核心为 co2。
//
// 第一阶段（纯抽象，仅头文件）
#include "net/any_executor.hpp"
#include "net/any_source_sink.hpp"
#include "net/any_stream.hpp"
#include "net/buffers.hpp"
#include "net/continuation.hpp"
#include "net/coroutine.hpp"
#include "net/dynamic_buffer.hpp"
#include "net/error.hpp"
#include "net/execution_context.hpp"
#include "net/executor_ref.hpp"
#include "net/file.hpp"
#include "net/immediate.hpp"
#include "net/io_awaitable_promise_base.hpp"
#include "net/io_env.hpp"
#include "net/io_result.hpp"
#include "net/local.hpp"
#include "net/memory_resource.hpp"
#include "net/multicast.hpp"
#include "net/run.hpp"
#include "net/run_async.hpp"
#include "net/span.hpp"
#include "net/strand.hpp"
#include "net/source_sink.hpp"
#include "net/stream.hpp"
#include "net/task.hpp"
#include "net/this_coro.hpp"
#include "net/thread_pool.hpp"
#include "net/when_all.hpp"
#include "net/when_any.hpp"

// 第二阶段（平台层，Linux epoll / poll / select / io_uring，编译进 libnet）
#include "net/backend.hpp"
#include "net/io_context.hpp"
#include "net/ip.hpp"
#include "net/receive_source.hpp"
#include "net/resolver.hpp"
#include "net/signal_set.hpp"
#include "net/socket_base.hpp"
#include "net/tcp.hpp"
#include "net/timer.hpp"
#include "net/udp.hpp"

// TLS（Paper 14：传输安全包装器；构建期 NET_TLS_PROVIDER=OpenSSL|BoringSSL 时可用）
#if defined(NET_HAS_TLS)
#include "net/tls/context.hpp"
#include "net/tls/error.hpp"
#include "net/tls/stream.hpp"
#endif
