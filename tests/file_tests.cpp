// 异步文件（Paper 10）：stream_file（隐式位置、满足 Stream）与 random_access_file（显式偏移）。
// 每个后端各编一份：io_uring 走 READV / WRITEV 带偏移的 SQE，就绪型后端走同步 preadv / pwritev。

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>


#include "net/any_stream.hpp"
#include "net/buffers.hpp"
#include "net/dynamic_buffer.hpp"
#include "net/error.hpp"
#include "net/file.hpp"
#include "net/io_context.hpp"
#include "net/run_async.hpp"
#include "net/source_sink.hpp"
#include "net/stream.hpp"
#include "net/task.hpp"
#include "net/tcp.hpp"
#include "net/when_all.hpp"

#include "check.hpp"
#include "platform.hpp"

namespace {

static_assert(net::is_read_stream<net::stream_file>::value, "");
static_assert(net::is_write_stream<net::stream_file>::value, "");
static_assert(net::is_stream<net::stream_file>::value, "");
static_assert(not net::is_read_stream<net::random_access_file>::value, ""); // 没有位置：不是 Stream

// 临时文件：构造时创建、析构时删除。
struct temp_path {
    std::string path;
    temp_path() : path{net_test::make_temp_file()} { CHECK(not path.empty()); }
    ~temp_path() { std::remove(path.c_str()); }
};

template <class T> T run_task(test_context& ctx, net::task<T> t) {
    T result{};
    net::run_async(ctx.get_executor(), [&](T v) { result = std::move(v); }, [](std::exception_ptr) { CHECK(false); })(std::move(t));
    ctx.run();
    ctx.restart();
    return result;
}

std::string pattern(std::size_t const n) {
    std::string s;
    s.reserve(n);
    for (auto i = std::size_t{}; i != n; ++i) s += static_cast<char>('a' + (i * 7U) % 26U);
    return s;
}

// ---- stream_file：写、读回、eof、位置 ----

auto write_all(net::stream_file* file, std::string const* payload)
    CO2_BEG((net::task<net::io_result<std::size_t>>), (file, payload), net::io_result<std::size_t> r;) {
    CO2_AWAIT_SET(r, net::write(*file, net::buffer(*payload)));
    CO2_RETURN(r);
}
CO2_END

auto read_to_eof(net::stream_file* file, std::string* out)
    CO2_BEG((net::task<std::error_code>), (file, out), char buf[3001]; net::io_result<std::size_t> r;) {
    for (;;) {
        CO2_AWAIT_SET(r, file->read_some(net::buffer(buf)));
        if (r.ec) CO2_RETURN(r.ec);
        out->append(buf, r.value);
    }
}
CO2_END

void stream_file_round_trip() {
    test_context ctx;
    temp_path tmp;
    auto const payload = pattern(100U * 1024U + 13U);
    {
        net::stream_file out{ctx};
        CHECK(not out.is_open());
        auto const ec = out.open(tmp.path, net::file_base::write_only | net::file_base::truncate);
        CHECK(not ec);
        CHECK(out.is_open());
        auto const w = run_task(ctx, write_all(&out, &payload));
        CHECK(not w.ec);
        CHECK_EQ(w.value, payload.size());
        CHECK_EQ(out.position(), payload.size());
        CHECK(not out.sync_all());
        std::error_code sec;
        CHECK_EQ(out.size(sec), payload.size());
        CHECK(not sec);
        CHECK(not out.close());
        CHECK(not out.is_open());
    }
    {
        net::stream_file in{ctx, tmp.path, net::file_base::read_only};
        std::string back;
        auto const ec = run_task(ctx, read_to_eof(&in, &back));
        CHECK(ec == net::error::eof);
        CHECK(back == payload);
        CHECK_EQ(in.position(), payload.size());
        // seek 回中间再读
        std::error_code sec;
        auto const pos = in.seek(-10, net::file_base::seek_basis::seek_end, sec);
        CHECK(not sec);
        CHECK_EQ(pos, payload.size() - 10U);
        std::string tail;
        auto const ec2 = run_task(ctx, read_to_eof(&in, &tail));
        CHECK(ec2 == net::error::eof);
        CHECK(tail == payload.substr(payload.size() - 10U));
        in.seek(0, net::file_base::seek_basis::seek_set, sec);
        CHECK_EQ(in.position(), 0U);
    }
}

// ---- stream_file 是 Stream：read_until、any_stream、as_buffer_source → TCP（"发送文件"） ----

auto read_lines(net::any_read_stream* file, std::vector<std::string>* lines)
    CO2_BEG((net::task<std::error_code>), (file, lines), std::string storage; net::io_result<std::size_t> r;
            net::dynamic_container_buffer<std::string> db{storage};) {
    for (;;) {
        CO2_AWAIT_SET(r, net::read_until(*file, db, "\n"));
        if (r.ec) { // eof：分隔符之后剩下的（没有换行的最后一行）还留在缓冲里
            if (db.size() != 0U) lines->push_back(storage.substr(0, db.size()));
            CO2_RETURN(r.ec);
        }
        lines->push_back(storage.substr(0, r.value - 1U));
        db.consume(r.value);
    }
}
CO2_END

void stream_file_works_with_stream_algorithms() {
    test_context ctx;
    temp_path tmp;
    {
        net::stream_file out{ctx, tmp.path, net::file_base::write_only | net::file_base::truncate};
        std::string const text = "first line\nsecond line\nthird without newline";
        auto const w = run_task(ctx, write_all(&out, &text));
        CHECK(not w.ec);
    }
    net::stream_file in{ctx, tmp.path, net::file_base::read_only};
    net::any_read_stream erased{&in};
    std::vector<std::string> lines;
    auto const ec = run_task(ctx, read_lines(&erased, &lines));
    CHECK(ec == net::error::eof);
    CHECK_EQ(lines.size(), 3U);
    CHECK_EQ(lines[0], "first line");
    CHECK_EQ(lines[1], "second line");
    CHECK_EQ(lines[2], "third without newline");
}

auto send_file(net::stream_file* file, net::tcp_socket* sock)
    CO2_BEG((net::task<net::io_result<std::size_t>>), (file, sock), net::stream_buffer_source<net::stream_file> source{*file, 4096U};
            net::io_result<std::size_t> r;) {
    CO2_AWAIT_SET(r, net::transfer_to_stream(source, *sock));
    sock->shutdown(net::shutdown_type::send);
    CO2_RETURN(r);
}
CO2_END

auto recv_all(net::tcp_socket* sock, std::string* out)
    CO2_BEG((net::task<std::error_code>), (sock, out), char buf[8192]; net::io_result<std::size_t> r;) {
    for (;;) {
        CO2_AWAIT_SET(r, sock->read_some(net::buffer(buf)));
        if (r.ec) CO2_RETURN(r.ec);
        out->append(buf, r.value);
    }
}
CO2_END

auto connect_pair(net::tcp_acceptor* acceptor, net::tcp_socket* client, net::tcp_socket* server)
    CO2_BEG((net::task<std::error_code>), (acceptor, client, server), net::io_result<> c; net::io_result<net::tcp_socket> a;
            std::error_code ec;) {
    CO2_AWAIT_SET(c, client->connect(acceptor->local_endpoint(ec)));
    if (c.ec) CO2_RETURN(c.ec);
    CO2_AWAIT_SET(a, acceptor->accept());
    if (a.ec) CO2_RETURN(a.ec);
    *server = std::move(a.value);
    CO2_RETURN(std::error_code{});
}
CO2_END

void file_as_buffer_source_feeds_a_socket() {
    test_context ctx;
    temp_path tmp;
    auto const payload = pattern(300U * 1024U + 7U);
    {
        net::stream_file out{ctx, tmp.path, net::file_base::write_only | net::file_base::truncate};
        auto const w = run_task(ctx, write_all(&out, &payload));
        CHECK(not w.ec);
    }
    net::tcp_acceptor acceptor{ctx, net::ip::tcp::endpoint{net::ip::address_v4::loopback(), 0}};
    net::tcp_socket client{ctx};
    net::tcp_socket server;
    auto const cec = run_task(ctx, connect_pair(&acceptor, &client, &server));
    CHECK(not cec);
    net::stream_file in{ctx, tmp.path, net::file_base::read_only};
    std::string received;
    std::error_code recv_ec;
    net::io_result<std::size_t> sent;
    net::run_async(ctx.get_executor(), [&](std::error_code e) { recv_ec = e; }, [](std::exception_ptr) { CHECK(false); })(
        recv_all(&server, &received));
    net::run_async(ctx.get_executor(), [&](net::io_result<std::size_t> v) { sent = v; }, [](std::exception_ptr) { CHECK(false); })(
        send_file(&in, &client));
    ctx.run();
    CHECK(not sent.ec);
    CHECK_EQ(sent.value, payload.size());
    CHECK(recv_ec == net::error::eof);
    CHECK(received == payload);
}

// ---- random_access_file：显式偏移、并发的两个读、resize ----

auto write_at(net::random_access_file* file, std::uint64_t offset, std::string payload)
    CO2_BEG((net::task<net::io_result<std::size_t>>), (file, offset, payload), net::io_result<std::size_t> r; std::size_t done{};) {
    while (done < payload.size()) {
        CO2_AWAIT_SET(r, file->write_some_at(offset + done, net::buffer(payload) + done));
        if (r.ec) CO2_RETURN((net::io_result<std::size_t>{r.ec, done}));
        done += r.value;
    }
    CO2_RETURN((net::io_result<std::size_t>{std::error_code{}, done}));
}
CO2_END

auto read_at(net::random_access_file* file, std::uint64_t offset, std::size_t n, std::string* out)
    CO2_BEG((net::task<net::io_result<std::size_t>>), (file, offset, n, out), net::io_result<std::size_t> r; std::size_t done{};) {
    out->assign(n, '\0');
    while (done < n) {
        CO2_AWAIT_SET(r, file->read_some_at(offset + done, net::buffer(&(*out)[done], n - done)));
        if (r.ec) {
            out->resize(done);
            CO2_RETURN((net::io_result<std::size_t>{r.ec, done}));
        }
        done += r.value;
    }
    CO2_RETURN((net::io_result<std::size_t>{std::error_code{}, done}));
}
CO2_END

void random_access_file_reads_and_writes_at_offsets() {
    test_context ctx;
    temp_path tmp;
    net::random_access_file file{ctx, tmp.path, net::file_base::read_write | net::file_base::truncate};
    auto const first = pattern(4096U);
    auto const second = std::string(1000U, 'Z');
    // 先写远处再写近处：中间是洞（零）
    auto const w2 = run_task(ctx, write_at(&file, 10000U, second));
    CHECK(not w2.ec);
    CHECK_EQ(w2.value, 1000U);
    auto const w1 = run_task(ctx, write_at(&file, 0U, first));
    CHECK(not w1.ec);
    std::error_code sec;
    CHECK_EQ(file.size(sec), 11000U);
    std::string back;
    auto const r1 = run_task(ctx, read_at(&file, 0U, 4096U, &back));
    CHECK(not r1.ec);
    CHECK(back == first);
    auto const r2 = run_task(ctx, read_at(&file, 10000U, 1000U, &back));
    CHECK(not r2.ec);
    CHECK(back == second);
    auto const hole = run_task(ctx, read_at(&file, 4096U, 100U, &back));
    CHECK(not hole.ec);
    CHECK(back == std::string(100U, '\0'));
    // 越过末尾：eof，读到的是末尾前的部分
    auto const past = run_task(ctx, read_at(&file, 10990U, 100U, &back));
    CHECK(past.ec == net::error::eof);
    CHECK_EQ(past.value, 10U);
    CHECK(back == std::string(10U, 'Z'));
    // resize 截断后 size 变化、读到 eof
    CHECK(not file.resize(500U));
    CHECK_EQ(file.size(sec), 500U);
    auto const gone = run_task(ctx, read_at(&file, 500U, 10U, &back));
    CHECK(gone.ec == net::error::eof);
    CHECK_EQ(gone.value, 0U);
    CHECK(not file.sync_data());
}

// 两个句柄打开同一文件，各自一段同时读：完成型后端两 SQE 同时在飞。
auto parallel_reads(net::random_access_file* a, net::random_access_file* b, std::string* out_a, std::string* out_b)
    CO2_BEG((net::task<std::error_code>), (a, b, out_a, out_b), net::io_result<std::size_t, std::size_t> both;) {
    CO2_AWAIT_SET(both, net::when_all(read_at(a, 0U, 5000U, out_a), read_at(b, 5000U, 5000U, out_b)));
    if (not both.ec) {
        CHECK_EQ(std::get<0>(both.values), 5000U);
        CHECK_EQ(std::get<1>(both.values), 5000U);
    }
    CO2_RETURN(both.ec);
}
CO2_END

void two_handles_read_concurrently() {
    test_context ctx;
    temp_path tmp;
    auto const payload = pattern(10000U);
    {
        net::stream_file out{ctx, tmp.path, net::file_base::write_only | net::file_base::truncate};
        auto const w = run_task(ctx, write_all(&out, &payload));
        CHECK(not w.ec);
    }
    net::random_access_file a{ctx, tmp.path, net::file_base::read_only};
    net::random_access_file b{ctx, tmp.path, net::file_base::read_only};
    std::string out_a;
    std::string out_b;
    auto const ec = run_task(ctx, parallel_reads(&a, &b, &out_a, &out_b));
    CHECK(not ec);
    CHECK(out_a + out_b == payload);
}

// ---- 注册缓冲区上的文件读写（io_uring：READ_FIXED / WRITE_FIXED；其它后端照常） ----

auto write_region(net::stream_file* file, unsigned char const* data, std::size_t n)
    CO2_BEG((net::task<net::io_result<std::size_t>>), (file, data, n), net::io_result<std::size_t> r;) {
    CO2_AWAIT_SET(r, net::write(*file, net::buffer(data, n))); // 单缓冲、落在注册区域内
    CO2_RETURN(r);
}
CO2_END

auto read_region_at(net::random_access_file* file, std::uint64_t offset, unsigned char* data, std::size_t n)
    CO2_BEG((net::task<net::io_result<std::size_t>>), (file, offset, data, n), net::io_result<std::size_t> r; std::size_t done{};) {
    while (done < n) {
        CO2_AWAIT_SET(r, file->read_some_at(offset + done, net::buffer(data + done, n - done)));
        if (r.ec) CO2_RETURN((net::io_result<std::size_t>{r.ec, done}));
        done += r.value;
    }
    CO2_RETURN((net::io_result<std::size_t>{std::error_code{}, done}));
}
CO2_END

void registered_buffer_file_io() {
    test_context ctx;
    temp_path tmp;
    constexpr auto half = std::size_t{64U * 1024U};
    std::vector<unsigned char> region(2U * half);
    auto const registered = ctx.register_buffer(net::buffer(region));
    static_cast<void>(registered); // 不支持 / 内存锁定额度不够的后端照常用普通路径
    for (auto i = std::size_t{}; i != half; ++i) region[i] = static_cast<unsigned char>(i % 251U);
    {
        net::stream_file out{ctx, tmp.path, net::file_base::write_only | net::file_base::truncate};
        auto const w = run_task(ctx, write_region(&out, region.data(), half));
        CHECK(not w.ec);
        CHECK_EQ(w.value, half);
    }
    net::random_access_file in{ctx, tmp.path, net::file_base::read_only};
    auto const r = run_task(ctx, read_region_at(&in, 0U, region.data() + half, half)); // 读进区域后半段
    CHECK(not r.ec);
    CHECK_EQ(r.value, half);
    CHECK(std::equal(region.begin(), region.begin() + static_cast<std::ptrdiff_t>(half), region.begin() + static_cast<std::ptrdiff_t>(half)));
    ctx.unregister_buffer(net::buffer(region));
    // 注销后同一块缓冲照常
    std::fill(region.begin() + static_cast<std::ptrdiff_t>(half), region.end(), 0);
    auto const again = run_task(ctx, read_region_at(&in, 0U, region.data() + half, half));
    CHECK(not again.ec);
    CHECK(std::equal(region.begin(), region.begin() + static_cast<std::ptrdiff_t>(half), region.begin() + static_cast<std::ptrdiff_t>(half)));
}

// ---- 打开标志与错误 ----

void open_flags_and_errors() {
    test_context ctx;
    temp_path tmp;
    net::stream_file f{ctx};
    // 不存在的文件
    auto ec = f.open("/nonexistent/dir/file.bin", net::file_base::read_only);
    CHECK(ec == std::errc::no_such_file_or_directory);
    CHECK(not f.is_open());
    // exclusive 对已有文件失败
    ec = f.open(tmp.path, net::file_base::write_only | net::file_base::create | net::file_base::exclusive);
    CHECK(ec == std::errc::file_exists);
    // append：写总是到末尾
    {
        net::stream_file w{ctx, tmp.path, net::file_base::write_only | net::file_base::truncate};
        std::string const head = "head-";
        CHECK(not run_task(ctx, write_all(&w, &head)).ec);
    }
    {
        net::stream_file w{ctx, tmp.path, net::file_base::write_only | net::file_base::append};
        std::string const tail = "tail";
        CHECK(not run_task(ctx, write_all(&w, &tail)).ec); // 位置 0，但 O_APPEND 使其追加
    }
    {
        net::stream_file r{ctx, tmp.path, net::file_base::read_only};
        std::string back;
        run_task(ctx, read_to_eof(&r, &back));
        CHECK_EQ(back, "head-tail");
    }
    // 只读文件上写：错误经 error_code 报告
    {
        net::stream_file r{ctx, tmp.path, net::file_base::read_only};
        std::string const x = "x";
        auto const w = run_task(ctx, write_all(&r, &x));
        CHECK(w.ec == std::errc::bad_file_descriptor || w.ec == std::errc::permission_denied); // Windows：ERROR_ACCESS_DENIED
    }
    // 未打开的文件：not_open
    {
        net::stream_file closed{ctx};
        std::string back;
        auto const rec = run_task(ctx, read_to_eof(&closed, &back));
        CHECK(rec == net::error::not_open);
        std::error_code sec;
        closed.size(sec);
        CHECK(sec == net::error::not_open);
    }
    // 接管 / 释放描述符
    {
        auto const fd = net_test::open_readonly_raw(tmp.path);
        CHECK(net::file_is_valid(fd));
        net::stream_file adopted{ctx};
        CHECK(not adopted.assign(fd));
        CHECK_EQ(adopted.native_handle(), fd);
        std::string back;
        run_task(ctx, read_to_eof(&adopted, &back));
        CHECK_EQ(back, "head-tail");
        auto const released = adopted.release();
        CHECK_EQ(released, fd);
        CHECK(not adopted.is_open());
        net_test::close_raw_file(fd);
    }
    // 构造函数形态在打开失败时抛
    auto threw = false;
    try {
        net::stream_file bad{ctx, "/nonexistent/x", net::file_base::read_only};
    } catch (std::system_error const&) {
        threw = true;
    }
    CHECK(threw);
    // 移动
    net::stream_file src{ctx, tmp.path, net::file_base::read_only};
    net::stream_file dst{std::move(src)};
    CHECK(dst.is_open());
    CHECK(not src.is_open());
}

} // namespace

int main() {
    stream_file_round_trip();
    stream_file_works_with_stream_algorithms();
    file_as_buffer_source_feeds_a_socket();
    random_access_file_reads_and_writes_at_offsets();
    two_handles_read_concurrently();
    registered_buffer_file_io();
    open_flags_and_errors();
    std::cout << "file tests passed\n";
    return 0;
}
