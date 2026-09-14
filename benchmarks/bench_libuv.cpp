// 与 libuv 的对照：同一 harness、同一回环 TCP 场景。libuv 是 C 回调、Linux 上走 epoll，
// 没有协程帧；allocs/op 只计全局 operator new，libuv 内部走 malloc，所以这里几乎是 0。
// 定时器分辨率是毫秒：0 ms 表示「下一圈循环」，不能和 net / Asio 的 1 µs timerfd 直接比绝对值。

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include <sys/socket.h>

#include <uv.h>

#include "bench.hpp"

namespace {

void die(int const rc, char const* const what) {
    if (rc < 0) {
        std::fprintf(stderr, "libuv %s: %s\n", what, uv_strerror(rc));
        std::abort();
    }
}

void set_nodelay(uv_tcp_t* const t) { die(uv_tcp_nodelay(t, 1), "nodelay"); }

void set_linger0(uv_tcp_t* const t) {
    ::linger lg{1, 0};
    uv_os_fd_t fd = -1;
    die(uv_fileno(reinterpret_cast<uv_handle_t*>(t), &fd), "fileno");
    if (::setsockopt(static_cast<int>(fd), SOL_SOCKET, SO_LINGER, &lg, sizeof(lg)) != 0) std::abort();
}

void close_tcp(uv_tcp_t* const t) { uv_close(reinterpret_cast<uv_handle_t*>(t), nullptr); }

struct closing_tcp {
    uv_tcp_t handle{};
    static void on_closed(uv_handle_t* const h) { delete static_cast<closing_tcp*>(h->data); }
};

// ---- 下一圈循环（uv_idle）：最接近 post hop ----

struct idle_hop {
    uv_idle_t idle{};
    std::size_t remaining = 0;
};

void on_idle(uv_idle_t* const h) {
    auto* const s = static_cast<idle_hop*>(h->data);
    if (--s->remaining == 0U) uv_idle_stop(h);
}

void run_idle_hops(uv_loop_t* const loop, std::size_t const n) {
    idle_hop s;
    s.remaining = n;
    die(uv_idle_init(loop, &s.idle), "idle_init");
    s.idle.data = &s;
    die(uv_idle_start(&s.idle, &on_idle), "idle_start");
    uv_run(loop, UV_RUN_DEFAULT);
    uv_close(reinterpret_cast<uv_handle_t*>(&s.idle), nullptr);
    uv_run(loop, UV_RUN_DEFAULT);
}

// ---- 0 ms 定时器链 ----

struct timer_hop {
    uv_timer_t timer{};
    std::size_t remaining = 0;
};

void on_timer(uv_timer_t* const h) {
    auto* const s = static_cast<timer_hop*>(h->data);
    if (--s->remaining == 0U) return;
    die(uv_timer_start(h, &on_timer, 0, 0), "timer_start");
}

void run_timers(uv_loop_t* const loop, std::size_t const n) {
    timer_hop s;
    s.remaining = n;
    die(uv_timer_init(loop, &s.timer), "timer_init");
    s.timer.data = &s;
    die(uv_timer_start(&s.timer, &on_timer, 0, 0), "timer_start");
    uv_run(loop, UV_RUN_DEFAULT);
    uv_close(reinterpret_cast<uv_handle_t*>(&s.timer), nullptr);
    uv_run(loop, UV_RUN_DEFAULT);
}

// ---- TCP 回显往返：两端在同一个 loop ----

struct echo_session {
    uv_loop_t* loop = nullptr;
    uv_tcp_t listener{};
    uv_tcp_t server{};
    uv_tcp_t client{};
    uv_connect_t connect{};
    uv_write_t client_write{};
    uv_write_t server_write{};
    std::vector<char> payload;
    std::vector<char> client_slab;
    std::vector<char> server_slab;
    std::size_t remaining = 0;
    std::size_t client_have = 0;
    std::size_t server_have = 0;
    bool server_ready = false;
    bool client_ready = false;
};

void echo_alloc(uv_handle_t* const h, size_t, uv_buf_t* const buf) {
    auto* const s = static_cast<echo_session*>(h->data);
    auto& slab = (h == reinterpret_cast<uv_handle_t*>(&s->client)) ? s->client_slab : s->server_slab;
    buf->base = slab.data();
    buf->len = slab.size();
}

void start_client_write(echo_session* s);

void on_client_write(uv_write_t* const, int const status) { die(status, "client write"); }

void on_server_write(uv_write_t* const, int const status) { die(status, "server write"); }

void on_server_read(uv_stream_t* const stream, ssize_t const nread, uv_buf_t const*) {
    auto* const s = static_cast<echo_session*>(stream->data);
    if (nread < 0) {
        if (nread == UV_EOF) return;
        die(static_cast<int>(nread), "server read");
    }
    s->server_have += static_cast<std::size_t>(nread);
    if (s->server_have < s->payload.size()) return;
    s->server_have = 0;
    uv_buf_t const buf = uv_buf_init(s->payload.data(), static_cast<unsigned>(s->payload.size()));
    die(uv_write(&s->server_write, stream, &buf, 1, &on_server_write), "server write");
}

void on_client_read(uv_stream_t* const stream, ssize_t const nread, uv_buf_t const*) {
    auto* const s = static_cast<echo_session*>(stream->data);
    if (nread < 0) {
        if (nread == UV_EOF) return;
        die(static_cast<int>(nread), "client read");
    }
    s->client_have += static_cast<std::size_t>(nread);
    if (s->client_have < s->payload.size()) return;
    s->client_have = 0;
    if (--s->remaining == 0U) {
        uv_read_stop(stream);
        uv_read_stop(reinterpret_cast<uv_stream_t*>(&s->server));
        uv_stop(s->loop);
        return;
    }
    start_client_write(s);
}

void start_client_write(echo_session* const s) {
    uv_buf_t const buf = uv_buf_init(s->payload.data(), static_cast<unsigned>(s->payload.size()));
    die(uv_write(&s->client_write, reinterpret_cast<uv_stream_t*>(&s->client), &buf, 1, &on_client_write), "client write");
}

void maybe_start_echo(echo_session* const s) {
    if (not s->server_ready || not s->client_ready) return;
    die(uv_read_start(reinterpret_cast<uv_stream_t*>(&s->server), &echo_alloc, &on_server_read), "server read_start");
    die(uv_read_start(reinterpret_cast<uv_stream_t*>(&s->client), &echo_alloc, &on_client_read), "client read_start");
    start_client_write(s);
}

void on_incoming(uv_stream_t* const listener, int const status) {
    die(status, "accept");
    auto* const s = static_cast<echo_session*>(listener->data);
    die(uv_tcp_init(s->loop, &s->server), "server init");
    s->server.data = s;
    die(uv_accept(listener, reinterpret_cast<uv_stream_t*>(&s->server)), "accept");
    set_nodelay(&s->server);
    s->server_ready = true;
    maybe_start_echo(s);
}

void on_connected(uv_connect_t* const req, int const status) {
    die(status, "connect");
    auto* const s = static_cast<echo_session*>(req->data);
    set_nodelay(&s->client);
    s->client_ready = true;
    maybe_start_echo(s);
}

void run_echo(uv_loop_t* const loop, std::size_t const size, std::size_t const n) {
    echo_session s;
    s.loop = loop;
    s.payload.assign(size, 'x');
    s.client_slab.resize(size < 4096U ? 4096U : size);
    s.server_slab.resize(s.client_slab.size());
    s.remaining = n;
    die(uv_tcp_init(loop, &s.listener), "listen init");
    die(uv_tcp_init(loop, &s.client), "client init");
    s.listener.data = &s;
    s.client.data = &s;
    s.connect.data = &s;
    sockaddr_in addr{};
    die(uv_ip4_addr("127.0.0.1", 0, &addr), "ip4");
    die(uv_tcp_bind(&s.listener, reinterpret_cast<sockaddr*>(&addr), 0), "bind");
    die(uv_listen(reinterpret_cast<uv_stream_t*>(&s.listener), 128, &on_incoming), "listen");
    int namelen = sizeof(addr);
    die(uv_tcp_getsockname(&s.listener, reinterpret_cast<sockaddr*>(&addr), &namelen), "getsockname");
    die(uv_tcp_connect(&s.connect, &s.client, reinterpret_cast<sockaddr*>(&addr), &on_connected), "connect");
    uv_run(loop, UV_RUN_DEFAULT);
    close_tcp(&s.client);
    close_tcp(&s.server);
    close_tcp(&s.listener);
    uv_run(loop, UV_RUN_DEFAULT);
}

// ---- 单向吞吐 ----

struct pump {
    uv_loop_t* loop = nullptr;
    uv_tcp_t listener{};
    uv_tcp_t server{};
    uv_tcp_t client{};
    uv_connect_t connect{};
    uv_write_t write{};
    std::vector<char> chunk;
    std::vector<char> slab;
    std::size_t left_to_send = 0;
    std::size_t left_to_recv = 0;
    bool writing = false;
};

void pump_alloc(uv_handle_t* const h, size_t, uv_buf_t* const buf) {
    auto* const s = static_cast<pump*>(h->data);
    buf->base = s->slab.data();
    buf->len = s->slab.size();
}

void pump_write_more(pump* s);

void on_pump_write(uv_write_t* const req, int const status) {
    die(status, "pump write");
    auto* const s = static_cast<pump*>(req->data);
    s->writing = false;
    s->left_to_send -= s->chunk.size();
    pump_write_more(s);
}

void pump_write_more(pump* const s) {
    if (s->writing || s->left_to_send == 0U) return;
    s->writing = true;
    uv_buf_t const buf = uv_buf_init(s->chunk.data(), static_cast<unsigned>(s->chunk.size()));
    die(uv_write(&s->write, reinterpret_cast<uv_stream_t*>(&s->client), &buf, 1, &on_pump_write), "pump write");
}

void on_pump_read(uv_stream_t* const stream, ssize_t const nread, uv_buf_t const*) {
    auto* const s = static_cast<pump*>(stream->data);
    if (nread < 0) {
        if (nread == UV_EOF) return;
        die(static_cast<int>(nread), "pump read");
    }
    if (static_cast<std::size_t>(nread) > s->left_to_recv) s->left_to_recv = 0;
    else s->left_to_recv -= static_cast<std::size_t>(nread);
    if (s->left_to_recv == 0U) {
        uv_read_stop(stream);
        uv_stop(s->loop);
    }
}

void on_pump_incoming(uv_stream_t* const listener, int const status) {
    die(status, "pump accept");
    auto* const s = static_cast<pump*>(listener->data);
    die(uv_tcp_init(s->loop, &s->server), "pump server");
    s->server.data = s;
    die(uv_accept(listener, reinterpret_cast<uv_stream_t*>(&s->server)), "pump accept");
    set_nodelay(&s->server);
    die(uv_read_start(reinterpret_cast<uv_stream_t*>(&s->server), &pump_alloc, &on_pump_read), "pump read");
}

void on_pump_connected(uv_connect_t* const req, int const status) {
    die(status, "pump connect");
    auto* const s = static_cast<pump*>(req->data);
    set_nodelay(&s->client);
    pump_write_more(s);
}

void run_throughput(uv_loop_t* const loop, std::size_t const chunk, std::size_t const total) {
    pump s;
    s.loop = loop;
    s.chunk.assign(chunk, 'y');
    s.slab.resize(64U * 1024U);
    s.left_to_send = total;
    s.left_to_recv = total;
    s.write.data = &s;
    s.connect.data = &s;
    die(uv_tcp_init(loop, &s.listener), "pump listen");
    die(uv_tcp_init(loop, &s.client), "pump client");
    s.listener.data = &s;
    s.client.data = &s;
    sockaddr_in addr{};
    die(uv_ip4_addr("127.0.0.1", 0, &addr), "ip4");
    die(uv_tcp_bind(&s.listener, reinterpret_cast<sockaddr*>(&addr), 0), "bind");
    die(uv_listen(reinterpret_cast<uv_stream_t*>(&s.listener), 128, &on_pump_incoming), "listen");
    int namelen = sizeof(addr);
    die(uv_tcp_getsockname(&s.listener, reinterpret_cast<sockaddr*>(&addr), &namelen), "getsockname");
    die(uv_tcp_connect(&s.connect, &s.client, reinterpret_cast<sockaddr*>(&addr), &on_pump_connected), "connect");
    uv_run(loop, UV_RUN_DEFAULT);
    close_tcp(&s.client);
    close_tcp(&s.server);
    close_tcp(&s.listener);
    uv_run(loop, UV_RUN_DEFAULT);
}

// ---- connect + accept：每个连接独立 handle，连上即 RST 关闭（对齐 bench_net 的 SO_LINGER{0}） ----

struct accept_burst {
    uv_loop_t* loop = nullptr;
    uv_tcp_t listener{};
    uv_connect_t connect{};
    std::size_t remaining = 0;
    sockaddr_in target{};
};

struct burst_client {
    uv_tcp_t handle{};
    accept_burst* owner = nullptr;
    static void on_closed(uv_handle_t* const h) { delete static_cast<burst_client*>(h->data); }
};

void kick_connect(accept_burst* s);

void on_burst_incoming(uv_stream_t* const listener, int const status) {
    die(status, "burst accept");
    auto* const s = static_cast<accept_burst*>(listener->data);
    auto* const peer = new closing_tcp{};
    die(uv_tcp_init(s->loop, &peer->handle), "peer init");
    peer->handle.data = peer;
    die(uv_accept(listener, reinterpret_cast<uv_stream_t*>(&peer->handle)), "burst accept");
    uv_close(reinterpret_cast<uv_handle_t*>(&peer->handle), &closing_tcp::on_closed);
}

void on_burst_connected(uv_connect_t* const req, int const status) {
    die(status, "burst connect");
    auto* const c = static_cast<burst_client*>(req->data);
    auto* const s = c->owner;
    set_linger0(&c->handle);
    uv_close(reinterpret_cast<uv_handle_t*>(&c->handle), &burst_client::on_closed);
    if (--s->remaining == 0U) {
        uv_stop(s->loop);
        return;
    }
    kick_connect(s);
}

void kick_connect(accept_burst* const s) {
    auto* const c = new burst_client{};
    c->owner = s;
    die(uv_tcp_init(s->loop, &c->handle), "burst client");
    c->handle.data = c;
    s->connect.data = c;
    die(uv_tcp_connect(&s->connect, &c->handle, reinterpret_cast<sockaddr*>(&s->target), &on_burst_connected),
        "burst connect");
}

void run_connect_accept(uv_loop_t* const loop, std::size_t const n) {
    accept_burst s;
    s.loop = loop;
    s.remaining = n;
    die(uv_tcp_init(loop, &s.listener), "burst listen");
    s.listener.data = &s;
    sockaddr_in addr{};
    die(uv_ip4_addr("127.0.0.1", 0, &addr), "ip4");
    die(uv_tcp_bind(&s.listener, reinterpret_cast<sockaddr*>(&addr), 0), "bind");
    die(uv_listen(reinterpret_cast<uv_stream_t*>(&s.listener), 512, &on_burst_incoming), "listen");
    int namelen = sizeof(addr);
    die(uv_tcp_getsockname(&s.listener, reinterpret_cast<sockaddr*>(&addr), &namelen), "getsockname");
    s.target = addr;
    kick_connect(&s);
    uv_run(loop, UV_RUN_DEFAULT);
    close_tcp(&s.listener);
    uv_run(loop, UV_RUN_DEFAULT);
}

} // namespace

int main(int argc, char** argv) {
    auto const o = bench::options::parse(argc, argv);
    std::vector<bench::result> results;
    uv_loop_t loop;
    die(uv_loop_init(&loop), "loop_init");

    results.push_back(bench::run(o, "libuv: idle hop (next loop turn)", o.scale(2000000U),
                                 [&](std::size_t n) { run_idle_hops(&loop, n); }, "uv_idle, not a cross-thread post"));

    results.push_back(bench::run(o, "libuv: 0 ms timer expire + resume", o.scale(200000U),
                                 [&](std::size_t n) { run_timers(&loop, n); }, "uv_timer 0 ms: next tick, not 1 us timerfd"));

    for (auto const size : {std::size_t{64}, std::size_t{4096}}) {
        results.push_back(bench::run(o, "libuv: tcp echo round trip, " + std::to_string(size) + " B", o.scale(20000U),
                                     [&](std::size_t n) { run_echo(&loop, size, n); },
                                     "ns per round trip, both peers on one loop"));
    }

    {
        constexpr std::size_t chunk = 64U * 1024U;
        auto const total = o.scale(64U) * 1024U * 1024U;
        auto r = bench::run(o, "libuv: tcp throughput, 64 KiB writes", 1U, [&](std::size_t) { run_throughput(&loop, chunk, total); });
        auto const seconds = r.ns_per_op / 1e9;
        r.note = std::to_string(static_cast<long>(static_cast<double>(total) / (1024.0 * 1024.0) / seconds)) + " MiB/s";
        r.ns_per_op = r.ns_per_op / static_cast<double>(total / chunk);
        r.user_ns_per_op /= static_cast<double>(total / chunk);
        r.system_ns_per_op /= static_cast<double>(total / chunk);
        r.iterations = total / chunk;
        results.push_back(std::move(r));
    }

    results.push_back(bench::run(o, "libuv: tcp connect + accept", o.scale(20000U),
                                 [&](std::size_t n) { run_connect_accept(&loop, n); },
                                 "ns per connection, both peers on one loop"));

    die(uv_loop_close(&loop), "loop_close");
    bench::print_table(("libuv " + std::string{uv_version_string()} + " (epoll, single loop)").c_str(), results);
    return 0;
}
