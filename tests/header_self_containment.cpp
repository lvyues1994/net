// 每个公共头都能独立包含（先单独包含再包含全部，验证没有隐藏的顺序依赖）。

#include "net/any_executor.hpp"
#include "net/any_source_sink.hpp"
#include "net/any_stream.hpp"
#include "net/buffers.hpp"
#include "net/config.hpp"
#include "net/continuation.hpp"
#include "net/coroutine.hpp"
#include "net/dynamic_buffer.hpp"
#include "net/error.hpp"
#include "net/execution_context.hpp"
#include "net/executor_ref.hpp"
#include "net/file.hpp"
#include "net/immediate.hpp"
#include "net/io_awaitable_promise_base.hpp"
#include "net/io_context.hpp"
#include "net/io_env.hpp"
#include "net/io_result.hpp"
#include "net/local.hpp"
#include "net/ip.hpp"
#include "net/memory_resource.hpp"
#include "net/multicast.hpp"
#include "net/resolver.hpp"
#include "net/run.hpp"
#include "net/run_async.hpp"
#include "net/signal_set.hpp"
#include "net/socket_base.hpp"
#include "net/span.hpp"
#include "net/strand.hpp"
#include "net/source_sink.hpp"
#include "net/stream.hpp"
#include "net/task.hpp"
#include "net/tcp.hpp"
#include "net/this_coro.hpp"
#include "net/thread_pool.hpp"
#include "net/timer.hpp"
#include "net/udp.hpp"
#include "net/when_all.hpp"
#include "net/when_any.hpp"
#if defined(NET_HAS_TLS)
#include "net/tls/context.hpp"
#include "net/tls/error.hpp"
#include "net/tls/stream.hpp"
#endif

#include "net/net.hpp"

#include <iostream>

int main() {
    static_assert(NET_VERSION_MAJOR == 0, "");
    std::cout << "net " NET_VERSION_STRING " headers are self-contained\n";
    return 0;
}
