#include "net/tls/stream.hpp"

#include <cstring>
#include <vector>

#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509_vfy.h>

#include "co2/contract.hpp"

#include "net/continuation.hpp"
#include "net/error.hpp"
#include "net/io_env.hpp"
#include "net/ip.hpp"
#include "net/stream.hpp"
#include "net/tls/error.hpp"

#include "tls/context_impl.hpp"

// OpenSSL / BoringSSL 引擎 + 驱动协程。
//
// 引擎是 sans-I/O 的：SSL 对象挂两个内存 BIO——传输读到的密文写进输入 BIO，SSL 产生的密文
// 从输出 BIO 取出发给传输。每个 TLS 操作是一个驱动协程：调用引擎 → 把输出 BIO 里的密文
// 全部写到底层流 → 若引擎要输入则从底层流读一批喂进去 → 重复到完成。传输方向的写由一个
// 单线程门闩串行化（读操作也可能产生输出，例如 TLS 1.3 的 KeyUpdate 应答）。

namespace net {
namespace tls {
namespace detail {

namespace {

constexpr std::size_t staging_size = 17U * 1024U; // 一条 TLS 记录（16 KiB）+ 头部与 MAC 的余量

enum class op_kind : unsigned char { handshake, read, write, shutdown };

struct engine_step {
    std::error_code ec;
    std::size_t bytes = 0;
    bool completed = false;
    bool want_input = false;
};

// 单线程（同一 strand）的异步门闩：串行化对底层流的写。
struct write_gate {
    struct waiter {
        continuation cont;
        io_env const* env = nullptr;
        waiter* next = nullptr;
    };

    struct acquire_awaitable {
        write_gate* gate;
        waiter node;

        bool await_ready() noexcept {
            if (gate->locked) return false;
            gate->locked = true;
            return true;
        }

        coroutine_handle<> await_suspend(coroutine_handle<> const h, io_env const* const env) noexcept {
            node.cont.h = h;
            node.env = env;
            node.next = nullptr;
            if (gate->tail != nullptr)
                gate->tail->next = &node;
            else
                gate->head = &node;
            gate->tail = &node;
            return noop_coroutine();
        }

        void await_resume() noexcept {}
    };

    acquire_awaitable acquire() noexcept { return acquire_awaitable{this, waiter{}}; }

    // 有等待者：所有权直接转交（locked 保持为真），经它的执行器恢复它。
    void release() noexcept {
        if (head == nullptr) {
            locked = false;
            return;
        }
        auto* const next = head;
        head = next->next;
        if (head == nullptr) tail = nullptr;
        next->env->executor.post(next->cont);
    }

    bool locked = false;
    waiter* head = nullptr;
    waiter* tail = nullptr;
};

} // namespace

struct openssl_stream_impl final : detail::stream_hooks {
    openssl_stream_impl(any_stream* const next_layer, context const& ctx_) : next{next_layer}, ctx{ctx_} {
        inbuf.resize(staging_size);
        outbuf.resize(staging_size);
    }

    openssl_stream_impl(openssl_stream_impl const&) = delete;
    openssl_stream_impl& operator=(openssl_stream_impl const&) = delete;

    ~openssl_stream_impl() { destroy_ssl(); }

    // 创建 SSL 与两个内存 BIO；SSL 拥有 BIO。
    std::error_code create_ssl() noexcept {
        destroy_ssl();
        ::ERR_clear_error();
        ssl = ::SSL_new(static_cast<SSL_CTX*>(ctx.native_handle()));
        if (ssl == nullptr) return take_provider_error(std::make_error_code(std::errc::not_enough_memory));
        auto* const in = ::BIO_new(::BIO_s_mem());
        auto* const out = ::BIO_new(::BIO_s_mem());
        if (in == nullptr || out == nullptr) {
            if (in != nullptr) ::BIO_free(in);
            if (out != nullptr) ::BIO_free(out);
            destroy_ssl();
            return std::make_error_code(std::errc::not_enough_memory);
        }
        ::SSL_set_bio(ssl, in, out);
        input = in;
        output = out;
        ::SSL_set_ex_data(ssl, detail::stream_ex_data_index(), static_cast<detail::stream_hooks*>(this));
        return {};
    }

    // ---- stream_hooks（context 的回调经 SSL 的 ex_data 找回本对象） ----
    void on_new_session(SSL_SESSION* const owned) noexcept override { latest_session = detail::session_access::make(owned); }
    bool is_client() const noexcept override { return current_role == role::client; }
    void set_ocsp_error(std::error_code const ec) noexcept override { ocsp_error = ec; }

    void destroy_ssl() noexcept {
        if (ssl != nullptr) ::SSL_free(ssl); // 连同 BIO
        ssl = nullptr;
        input = nullptr;
        output = nullptr;
        handshake_started = false;
    }

    std::error_code apply_hostname(role const r) noexcept {
        if (r != role::client || hostname.empty()) return {};
        std::error_code ec;
        auto const literal = ip::make_address(hostname, ec);
        static_cast<void>(literal);
        auto* const param = ::SSL_get0_param(ssl);
        if (not ec) {
            // IP 字面量：只匹配证书的 iPAddress，不发 SNI（RFC 6066）。
            if (::X509_VERIFY_PARAM_set1_ip_asc(param, hostname.c_str()) != 1)
                return take_provider_error(std::make_error_code(std::errc::invalid_argument));
            return {};
        }
#if defined(OPENSSL_IS_BORINGSSL)
        auto const sni_ok = ::SSL_set_tlsext_host_name(ssl, hostname.c_str()) == 1;
#else
        // OpenSSL 的 SSL_set_tlsext_host_name 是带 C 风格转换的宏；直接调用底层 ctrl。
        auto const sni_ok = ::SSL_ctrl(ssl, SSL_CTRL_SET_TLSEXT_HOSTNAME, TLSEXT_NAMETYPE_host_name,
                                       const_cast<char*>(hostname.c_str())) == 1;
#endif
        if (not sni_ok) return take_provider_error(std::make_error_code(std::errc::invalid_argument));
        if (::SSL_set1_host(ssl, hostname.c_str()) != 1)
            return take_provider_error(std::make_error_code(std::errc::invalid_argument));
        return {};
    }

    std::size_t pending_output() const noexcept {
        return output != nullptr ? ::BIO_ctrl_pending(output) : 0U;
    }

    // 把输出 BIO 里最多 outbuf.size() 字节取到 outbuf。
    std::size_t take_output() noexcept {
        auto const n = ::BIO_read(output, outbuf.data(), static_cast<int>(outbuf.size()));
        return n > 0 ? static_cast<std::size_t>(n) : 0U;
    }

    void feed_input(std::size_t const n) noexcept {
        if (n != 0U) ::BIO_write(input, inbuf.data(), static_cast<int>(n));
    }

    // 调用引擎一步并解释结果。
    engine_step run_engine(op_kind const kind, mutable_buffer_array<>& read_buffers,
                           const_buffer_array<>& write_buffers) noexcept {
        engine_step step;
        ::ERR_clear_error();
        auto ret = 0;
        switch (kind) {
        case op_kind::handshake: ret = ::SSL_do_handshake(ssl); break;
        case op_kind::read: {
            if (read_buffers.empty()) {
                step.completed = true;
                return step;
            }
            auto const b = read_buffers[0];
            ret = ::SSL_read(ssl, b.data(), static_cast<int>(b.size()));
            break;
        }
        case op_kind::write: {
            if (write_buffers.empty()) {
                step.completed = true;
                return step;
            }
            auto const b = write_buffers[0];
            ret = ::SSL_write(ssl, b.data(), static_cast<int>(b.size()));
            break;
        }
        case op_kind::shutdown:
            ret = ::SSL_shutdown(ssl);
            if (ret == 0) {
                // 已发出 close_notify；再调一次会等待对端的 close_notify（WANT_READ）。
                ret = ::SSL_shutdown(ssl);
            }
            break;
        }

        if (ret > 0) {
            step.completed = true;
            step.bytes = kind == op_kind::read || kind == op_kind::write ? static_cast<std::size_t>(ret) : 0U;
            return step;
        }

        switch (::SSL_get_error(ssl, ret)) {
        case SSL_ERROR_WANT_READ:
            step.want_input = true;
            return step;
        case SSL_ERROR_WANT_WRITE:
            return step; // 先冲输出再重试
        case SSL_ERROR_ZERO_RETURN:
            step.completed = true;
            // 对端的 close_notify：读是干净的 EOF；shutdown 视为完成。
            step.ec = kind == op_kind::read ? make_error_code(error::eof) : std::error_code{};
            return step;
        case SSL_ERROR_SSL: {
            step.completed = true;
            if (ocsp_error) { // OCSP 回调让握手失败：报它而不是提供者的通用错误
                ::ERR_clear_error();
                step.ec = ocsp_error;
                return step;
            }
            auto const verify = ::SSL_get_verify_result(ssl);
            if (verify != X509_V_OK) {
                ::ERR_clear_error();
                step.ec = std::error_code{static_cast<int>(verify), verify_category()};
            } else {
                step.ec = take_provider_error(std::make_error_code(std::errc::protocol_error));
            }
            return step;
        }
        case SSL_ERROR_SYSCALL:
        default:
            step.completed = true;
            step.ec = take_provider_error(ret == 0 ? make_error_code(error::stream_truncated)
                                                   : std::make_error_code(std::errc::io_error));
            return step;
        }
    }

    any_stream* next;
    context ctx;
    SSL* ssl = nullptr;
    BIO* input = nullptr;
    BIO* output = nullptr;
    std::vector<unsigned char> inbuf;
    std::vector<unsigned char> outbuf;
    std::string hostname;
    write_gate gate;
    bool handshake_started = false;
    role current_role = role::client;
    session resume_session; // set_session：下一次客户端握手尝试恢复
    session latest_session; // new-session 回调交来的可复用会话
    std::error_code ocsp_error; // OCSP 回调判定的失败原因（握手错误时优先报它）
};

namespace {

// 驱动协程：调用引擎 → 冲输出 → 需要时读输入 → 重复到完成。
auto run_op(openssl_stream_impl* self, op_kind kind, mutable_buffer_array<> read_buffers,
            const_buffer_array<> write_buffers)
    CO2_BEG((task<io_result<std::size_t>>), (self, kind, read_buffers, write_buffers), engine_step step;
            io_result<std::size_t> io; std::size_t chunk{};) {
    for (;;) {
        step = self->run_engine(kind, read_buffers, write_buffers);
        if (self->pending_output() != 0U) {
            CO2_AWAIT(self->gate.acquire());
            while (self->pending_output() != 0U) {
                chunk = self->take_output();
                CO2_AWAIT_SET(io, net::write(*self->next, net::buffer(self->outbuf.data(), chunk)));
                if (io.ec) {
                    self->gate.release();
                    CO2_RETURN((io_result<std::size_t>{io.ec, 0U}));
                }
            }
            self->gate.release();
        }
        if (step.completed) CO2_RETURN((io_result<std::size_t>{step.ec, step.bytes}));
        if (step.want_input) {
            CO2_AWAIT_SET(io, self->next->read_some(net::buffer(self->inbuf)));
            if (io.ec) {
                // 传输在 close_notify 之前结束：与截断攻击不可区分。
                CO2_RETURN((io_result<std::size_t>{io.ec == error::eof ? make_error_code(error::stream_truncated) : io.ec, 0U}));
            }
            self->feed_input(io.value);
        }
    }
}
CO2_END

auto fail(std::error_code ec) CO2_BEG((task<io_result<>>), (ec)) { CO2_RETURN((io_result<>{ec})); }
CO2_END

auto to_void(task<io_result<std::size_t>> inner) CO2_BEG((task<io_result<>>), (inner), io_result<std::size_t> r;) {
    CO2_AWAIT_SET(r, std::move(inner));
    CO2_RETURN((io_result<>{r.ec}));
}
CO2_END

} // namespace

void openssl_stream_impl_deleter::operator()(openssl_stream_impl* const impl) const noexcept { delete impl; }

} // namespace detail

// ---- openssl_stream ----

void openssl_stream::init(context const& ctx) {
    impl_.reset(new detail::openssl_stream_impl{&stream_, ctx});
}

openssl_stream::openssl_stream(openssl_stream&& other) noexcept
    : stream_{std::move(other.stream_)}, impl_{std::move(other.impl_)} {
    if (impl_) impl_->next = &stream_;
}

openssl_stream& openssl_stream::operator=(openssl_stream&& other) noexcept {
    if (this == &other) return *this;
    stream_ = std::move(other.stream_);
    impl_ = std::move(other.impl_);
    if (impl_) impl_->next = &stream_;
    return *this;
}

openssl_stream::~openssl_stream() = default;

void* openssl_stream::native_handle() const noexcept { return impl_->ssl; }

void openssl_stream::set_hostname(std::string hostname) { impl_->hostname = std::move(hostname); }

void openssl_stream::set_session(session const& s) { impl_->resume_session = s; }

session openssl_stream::current_session() const { return impl_->latest_session; }

bool openssl_stream::session_reused() const noexcept { return impl_->ssl != nullptr && ::SSL_session_reused(impl_->ssl) == 1; }

std::string openssl_stream::servername() const {
    if (impl_->ssl == nullptr) return {};
    auto const* const name = ::SSL_get_servername(impl_->ssl, TLSEXT_NAMETYPE_host_name);
    return name != nullptr ? std::string{name} : std::string{};
}

std::string openssl_stream::ocsp_response() const {
    if (impl_->ssl == nullptr) return {};
#if defined(OPENSSL_IS_BORINGSSL)
    std::uint8_t const* data = nullptr;
    std::size_t length = 0;
    ::SSL_get0_ocsp_response(impl_->ssl, &data, &length);
    return data != nullptr ? std::string{reinterpret_cast<char const*>(data), length} : std::string{};
#else
    unsigned char const* data = nullptr;
    auto const length = ::SSL_get_tlsext_status_ocsp_resp(impl_->ssl, &data);
    return data != nullptr && length > 0 ? std::string{reinterpret_cast<char const*>(data), static_cast<std::size_t>(length)}
                                         : std::string{};
#endif
}

void openssl_stream::reset() { impl_->destroy_ssl(); }

std::string openssl_stream::alpn_selected() const {
    if (impl_->ssl == nullptr) return {};
    unsigned char const* data = nullptr;
    unsigned length = 0;
    ::SSL_get0_alpn_selected(impl_->ssl, &data, &length);
    if (data == nullptr || length == 0U) return {};
    return std::string{reinterpret_cast<char const*>(data), length};
}

task<io_result<>> openssl_stream::handshake(role const r) {
    auto& impl = *impl_;
    // 每次握手都是全新的会话：之前的尝试（成功或失败）不保留。
    impl.current_role = r;
    impl.ocsp_error = std::error_code{};
    auto ec = impl.create_ssl();
    if (not ec) ec = impl.apply_hostname(r);
    if (not ec && r == role::client && impl.resume_session)
        if (::SSL_set_session(impl.ssl, static_cast<SSL_SESSION*>(impl.resume_session.native_handle())) != 1)
            ec = take_provider_error(std::make_error_code(std::errc::invalid_argument));
    if (ec) return detail::fail(ec);
    if (r == role::client)
        ::SSL_set_connect_state(impl.ssl);
    else
        ::SSL_set_accept_state(impl.ssl);
    impl.handshake_started = true;
    return detail::to_void(detail::run_op(&impl, detail::op_kind::handshake, mutable_buffer_array<>{}, const_buffer_array<>{}));
}

task<io_result<>> openssl_stream::shutdown() {
    if (impl_->ssl == nullptr || not impl_->handshake_started) return detail::fail(make_error_code(error::not_open));
    return detail::to_void(detail::run_op(impl_.get(), detail::op_kind::shutdown, mutable_buffer_array<>{}, const_buffer_array<>{}));
}

task<io_result<std::size_t>> openssl_stream::do_read_some(mutable_buffer_array<> buffers) {
    CO2_CONTRACT_CHECK(impl_->ssl != nullptr && impl_->handshake_started);
    return detail::run_op(impl_.get(), detail::op_kind::read, std::move(buffers), const_buffer_array<>{});
}

task<io_result<std::size_t>> openssl_stream::do_write_some(const_buffer_array<> buffers) {
    CO2_CONTRACT_CHECK(impl_->ssl != nullptr && impl_->handshake_started);
    return detail::run_op(impl_.get(), detail::op_kind::write, mutable_buffer_array<>{}, std::move(buffers));
}

} // namespace tls
} // namespace net
