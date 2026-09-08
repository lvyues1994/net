// Unix 域套接字（Paper 11 的 "Unix sockets"）：流（acceptor / connect / 回显 / shutdown → eof）、
// 抽象命名空间、数据报（send_to / receive_from 与发送方端点、connect + send / receive）、端点、错误。

#include <cstdio>
#include <string>

#include <sys/stat.h>
#if NET_PLATFORM_WINDOWS
#include <process.h>
#else
#include <unistd.h>
#endif

#include "net/any_source_sink.hpp"
#include "net/buffers.hpp"
#include "net/error.hpp"
#include "net/io_context.hpp"
#include "net/local.hpp"
#include "net/run_async.hpp"
#include "net/source_sink.hpp"
#include "net/stream.hpp"
#include "net/task.hpp"

#include "check.hpp"

namespace {

static_assert(net::is_stream<net::local_stream_socket>::value, "");
#if !NET_PLATFORM_WINDOWS
static_assert(net::is_stream<net::local_datagram_socket>::value, "");
#endif

std::string process_tag() {
#if NET_PLATFORM_WINDOWS
    return std::to_string(::_getpid());
#else
    return std::to_string(::getpid());
#endif
}

std::string unique_path(char const* const tag) {
#if NET_PLATFORM_WINDOWS
    char directory[MAX_PATH + 1] = {};
    ::GetTempPathA(MAX_PATH + 1, directory);
    return std::string{directory} + "net_local_" + tag + "_" + process_tag();
#else
    return "/tmp/net_local_" + std::string{tag} + "_" + process_tag();
#endif
}

struct path_guard {
    std::string path;
    ~path_guard() { std::remove(path.c_str()); }
};

bool file_exists(std::string const& path) {
#if NET_PLATFORM_WINDOWS
    return ::GetFileAttributesA(path.c_str()) != INVALID_FILE_ATTRIBUTES;
#else
    struct stat st{};
    return ::stat(path.c_str(), &st) == 0;
#endif
}


template <class T> T run_task(test_context& ctx, net::task<T> t) {
    T result{};
    net::run_async(ctx.get_executor(), [&](T v) { result = std::move(v); }, [](std::exception_ptr) { CHECK(false); })(std::move(t));
    ctx.run();
    ctx.restart();
    return result;
}

// ---- 流 ----

auto echo_server(net::local_stream_acceptor* acceptor, std::string* peer_path)
    CO2_BEG((net::task<std::error_code>), (acceptor, peer_path), net::io_result<net::local_stream_socket> accepted;
            net::local_stream_socket peer; char buf[512]; net::io_result<std::size_t> r; net::io_result<std::size_t> w;
            std::error_code ec;) {
    CO2_AWAIT_SET(accepted, acceptor->accept());
    if (accepted.ec) CO2_RETURN(accepted.ec);
    peer = std::move(accepted.value);
    *peer_path = peer.remote_endpoint(ec).path(); // 未绑定的客户端：未命名端点，空路径
    for (;;) {
        CO2_AWAIT_SET(r, peer.read_some(net::buffer(buf)));
        if (r.ec) break;
        CO2_AWAIT_SET(w, net::write(peer, net::buffer(buf, r.value)));
        if (w.ec) CO2_RETURN(w.ec);
    }
    CO2_RETURN(r.ec);
}
CO2_END

auto echo_client(net::local_stream_socket* client, net::local::stream_protocol::endpoint ep, std::string* echoed)
    CO2_BEG((net::task<std::error_code>), (client, ep, echoed), net::io_result<> c; net::io_result<std::size_t> w;
            net::io_result<std::size_t> r; char buf[512]; std::string const payload{"hello over AF_UNIX"};) {
    CO2_AWAIT_SET(c, client->connect(ep));
    if (c.ec) CO2_RETURN(c.ec);
    CO2_AWAIT_SET(w, net::write(*client, net::buffer(payload)));
    if (w.ec) CO2_RETURN(w.ec);
    CO2_AWAIT_SET(r, net::read(*client, net::buffer(buf, payload.size())));
    if (r.ec) CO2_RETURN(r.ec);
    echoed->assign(buf, r.value);
    client->shutdown(net::shutdown_type::send); // 服务端读到 eof 后退出
    CO2_AWAIT_SET(r, client->read_some(net::buffer(buf)));
    CO2_RETURN(r.ec); // 服务端关闭 → eof
}
CO2_END

void stream_echo_over_filesystem_path() {
    test_context ctx;
    path_guard guard{unique_path("stream")};
    net::local::stream_protocol::endpoint const ep{guard.path};
    CHECK_EQ(ep.path(), guard.path);
    net::local_stream_acceptor acceptor{ctx, ep};
    CHECK(file_exists(guard.path));
    std::error_code ec;
    CHECK_EQ(acceptor.local_endpoint(ec).path(), guard.path);
    CHECK(not ec);
    net::local_stream_socket client{ctx};
    std::string echoed;
    std::string peer_path = "unset";
    std::error_code server_ec;
    std::error_code client_ec;
    net::run_async(ctx.get_executor(), [&](std::error_code e) { server_ec = e; }, [](std::exception_ptr) { CHECK(false); })(
        echo_server(&acceptor, &peer_path));
    net::run_async(ctx.get_executor(), [&](std::error_code e) { client_ec = e; }, [](std::exception_ptr) { CHECK(false); })(
        echo_client(&client, ep, &echoed));
    ctx.run();
    CHECK(server_ec == net::error::eof);
    CHECK(client_ec == net::error::eof);
    CHECK_EQ(echoed, "hello over AF_UNIX");
    CHECK_EQ(peer_path, "");
    // 再建一个 acceptor 到同一路径：默认 unlink 已有文件，不会 EADDRINUSE
    acceptor.close();
    net::local_stream_acceptor again{ctx, ep};
    CHECK(again.is_open());
}

#if !NET_PLATFORM_WINDOWS
void stream_echo_over_abstract_namespace() {
    test_context ctx;
    std::string name = "net-local-abstract-" + process_tag();
    name.insert(name.begin(), '\0');
    net::local::stream_protocol::endpoint const ep{name};
    CHECK_EQ(ep.path().size(), name.size());
    CHECK(ep.path()[0] == '\0');
    net::local_stream_acceptor acceptor{ctx, ep};
    std::error_code ec;
    CHECK(acceptor.local_endpoint(ec) == ep); // 抽象名字按长度精确比较
    net::local_stream_socket client{ctx};
    std::string echoed;
    std::string peer_path;
    std::error_code server_ec;
    std::error_code client_ec;
    net::run_async(ctx.get_executor(), [&](std::error_code e) { server_ec = e; }, [](std::exception_ptr) { CHECK(false); })(
        echo_server(&acceptor, &peer_path));
    net::run_async(ctx.get_executor(), [&](std::error_code e) { client_ec = e; }, [](std::exception_ptr) { CHECK(false); })(
        echo_client(&client, ep, &echoed));
    ctx.run();
    CHECK(server_ec == net::error::eof);
    CHECK(client_ec == net::error::eof);
    CHECK_EQ(echoed, "hello over AF_UNIX");
}
#endif

auto connect_only(net::local_stream_socket* client, net::local::stream_protocol::endpoint ep)
    CO2_BEG((net::task<std::error_code>), (client, ep), net::io_result<> c;) {
    CO2_AWAIT_SET(c, client->connect(ep));
    CO2_RETURN(c.ec);
}
CO2_END

void connect_errors_are_reported() {
    test_context ctx;
    net::local_stream_socket client{ctx};
    auto const ec = run_task(ctx, connect_only(&client, net::local::stream_protocol::endpoint{"/nonexistent/dir/sock"}));
    // Windows 的 afunix 对不存在的路径报 WSAECONNREFUSED / ERROR_PATH_NOT_FOUND 之一
    CHECK(ec == std::errc::no_such_file_or_directory || ec == std::errc::connection_refused);
    // 路径过长
    auto threw = false;
    try {
        net::local::stream_protocol::endpoint const too_long{std::string(200U, 'x')};
    } catch (std::system_error const& e) {
        threw = e.code() == std::errc::filename_too_long;
    }
    CHECK(threw);
}

// WriteSink 定制点：write_eof → shutdown(send) → 对端 eof
auto sink_send(net::local_stream_socket* client, net::local::stream_protocol::endpoint ep, std::string payload)
    CO2_BEG((net::task<net::io_result<std::size_t>>), (client, ep, payload), net::io_result<> c;
            net::stream_write_sink<net::local_stream_socket> sink{*client}; net::io_result<std::size_t> r;) {
    CO2_AWAIT_SET(c, client->connect(ep));
    CHECK(not c.ec);
    CO2_AWAIT_SET(r, sink.write_eof(net::buffer(payload)));
    CO2_RETURN(r);
}
CO2_END

auto accept_and_drain(net::local_stream_acceptor* acceptor, std::string* out)
    CO2_BEG((net::task<std::error_code>), (acceptor, out), net::io_result<net::local_stream_socket> accepted;
            net::local_stream_socket peer; char buf[4096]; net::io_result<std::size_t> r;) {
    CO2_AWAIT_SET(accepted, acceptor->accept());
    if (accepted.ec) CO2_RETURN(accepted.ec);
    peer = std::move(accepted.value);
    for (;;) {
        CO2_AWAIT_SET(r, peer.read_some(net::buffer(buf)));
        if (r.ec) CO2_RETURN(r.ec);
        out->append(buf, r.value);
    }
}
CO2_END

void write_sink_eof_is_shutdown() {
    test_context ctx;
    path_guard guard{unique_path("sink")};
    net::local::stream_protocol::endpoint const ep{guard.path};
    net::local_stream_acceptor acceptor{ctx, ep};
    net::local_stream_socket client{ctx};
    std::string const payload(150U * 1024U, 'u');
    std::string received;
    std::error_code recv_ec;
    net::io_result<std::size_t> sent;
    net::run_async(ctx.get_executor(), [&](std::error_code e) { recv_ec = e; }, [](std::exception_ptr) { CHECK(false); })(
        accept_and_drain(&acceptor, &received));
    net::run_async(ctx.get_executor(), [&](net::io_result<std::size_t> v) { sent = v; }, [](std::exception_ptr) { CHECK(false); })(
        sink_send(&client, ep, payload));
    ctx.run();
    CHECK(not sent.ec);
    CHECK_EQ(sent.value, payload.size());
    CHECK(recv_ec == net::error::eof);
    CHECK(received == payload);
}

// ---- 数据报（POSIX） ----

#if !NET_PLATFORM_WINDOWS
auto dgram_exchange(net::local_datagram_socket* a, net::local_datagram_socket* b, net::local::datagram_protocol::endpoint b_ep,
                    std::string* seen_sender, std::string* got)
    CO2_BEG((net::task<std::error_code>), (a, b, b_ep, seen_sender, got), net::io_result<std::size_t> s; net::io_result<std::size_t> r;
            char buf[256]; net::local::datagram_protocol::endpoint sender; std::error_code ec;
            std::string const payload{"datagram over AF_UNIX"};) {
    CO2_AWAIT_SET(s, a->send_to(net::buffer(payload), b_ep));
    if (s.ec) CO2_RETURN(s.ec);
    CO2_AWAIT_SET(r, b->receive_from(net::buffer(buf), sender));
    if (r.ec) CO2_RETURN(r.ec);
    got->assign(buf, r.value);
    *seen_sender = sender.path();
    // 回送到发送方端点，对方 connect 后用 receive
    CO2_AWAIT_SET(s, b->send_to(net::buffer(got->data(), got->size()), sender));
    if (s.ec) CO2_RETURN(s.ec);
    CO2_AWAIT_SET(r, a->receive(net::buffer(buf)));
    if (r.ec) CO2_RETURN(r.ec);
    CHECK_EQ(std::string(buf, r.value), payload);
    CO2_RETURN(std::error_code{});
}
CO2_END

void datagram_send_to_and_receive_from() {
    test_context ctx;
    path_guard ga{unique_path("dgram_a")};
    path_guard gb{unique_path("dgram_b")};
    net::local::datagram_protocol::endpoint const a_ep{ga.path};
    net::local::datagram_protocol::endpoint const b_ep{gb.path};
    net::local_datagram_socket a{ctx, a_ep};
    net::local_datagram_socket b{ctx, b_ep};
    CHECK(file_exists(ga.path) && file_exists(gb.path));
    std::error_code ec;
    CHECK_EQ(a.local_endpoint(ec).path(), ga.path);
    std::string seen_sender;
    std::string got;
    auto const rc = run_task(ctx, dgram_exchange(&a, &b, b_ep, &seen_sender, &got));
    CHECK(not rc);
    CHECK_EQ(got, "datagram over AF_UNIX");
    CHECK_EQ(seen_sender, ga.path); // 内核报告的发送方地址长度决定路径
}

auto connected_dgram(net::local_datagram_socket* client, net::local_datagram_socket* server,
                     net::local::datagram_protocol::endpoint server_ep, std::string* got)
    CO2_BEG((net::task<std::error_code>), (client, server, server_ep, got), net::io_result<std::size_t> s;
            net::io_result<std::size_t> r; char buf[64]; net::local::datagram_protocol::endpoint sender;
            std::string const payload{"connected"};) {
    if (auto const ec = client->connect(server_ep)) CO2_RETURN(ec);
    CO2_AWAIT_SET(s, client->send(net::buffer(payload)));
    if (s.ec) CO2_RETURN(s.ec);
    CO2_AWAIT_SET(r, server->receive_from(net::buffer(buf), sender));
    if (r.ec) CO2_RETURN(r.ec);
    got->assign(buf, r.value);
    CHECK(sender.path().empty()); // 未绑定的客户端：未命名
    CO2_RETURN(std::error_code{});
}
CO2_END

void datagram_connect_send_receive() {
    test_context ctx;
    path_guard gs{unique_path("dgram_s")};
    net::local::datagram_protocol::endpoint const server_ep{gs.path};
    net::local_datagram_socket server{ctx, server_ep};
    net::local_datagram_socket client{ctx};
    std::string got;
    auto const rc = run_task(ctx, connected_dgram(&client, &server, server_ep, &got));
    CHECK(not rc);
    CHECK_EQ(got, "connected");
    std::error_code ec;
    CHECK_EQ(client.remote_endpoint(ec).path(), gs.path);
    CHECK(not ec);
}
#endif

} // namespace

int main() {
    stream_echo_over_filesystem_path();
    connect_errors_are_reported();
    write_sink_eof_is_shutdown();
#if !NET_PLATFORM_WINDOWS
    stream_echo_over_abstract_namespace();
    datagram_send_to_and_receive_from();
    datagram_connect_send_receive();
#endif
    std::cout << "local socket tests passed\n";
    return 0;
}
