#include "detail/iocp/iocp_backend.hpp"

#include <chrono>
#include <cstring>

#include "co2/contract.hpp"

#include "net/error.hpp"
#include "net/io_context.hpp"

#include "detail/heap_timer.hpp"
#include "detail/iocp/iocp_file.hpp"
#include "detail/iocp/iocp_socket.hpp"

namespace net {
namespace detail {

namespace {

// 完成键：0 是 I/O 操作（OVERLAPPED 非空），wake_key 是 interrupt() 投的空包。
constexpr ULONG_PTR io_key = 0;
constexpr ULONG_PTR wake_key = 1;

std::error_code last_win32_error() noexcept { return std::error_code{static_cast<int>(::GetLastError()), std::system_category()}; }

} // namespace

std::error_code iocp_error(DWORD const error, bool const accept_path) noexcept {
    switch (error) {
    case 0: return {};
    case ERROR_OPERATION_ABORTED: return make_error_code(error::operation_aborted); // 995：CancelIoEx / closesocket
    case ERROR_NETNAME_DELETED: // 64：对端 RST（本地关闭已由 995 覆盖）
        return std::make_error_code(accept_path ? std::errc::connection_aborted : std::errc::connection_reset);
    case WSAECONNRESET: return std::make_error_code(std::errc::connection_reset);
    case WSAECONNREFUSED:
    case ERROR_CONNECTION_REFUSED: return std::make_error_code(std::errc::connection_refused);
    case WSAECONNABORTED:
    case ERROR_CONNECTION_ABORTED: return std::make_error_code(std::errc::connection_aborted);
    case WSAENETUNREACH:
    case ERROR_NETWORK_UNREACHABLE: return std::make_error_code(std::errc::network_unreachable);
    case WSAEHOSTUNREACH:
    case ERROR_HOST_UNREACHABLE: return std::make_error_code(std::errc::host_unreachable);
    case WSAETIMEDOUT:
    case ERROR_SEM_TIMEOUT: return std::make_error_code(std::errc::timed_out);
    case WSAEBADF:
    case ERROR_INVALID_HANDLE: return std::make_error_code(std::errc::bad_file_descriptor);
    case WSAEADDRINUSE: return std::make_error_code(std::errc::address_in_use);
    case WSAEADDRNOTAVAIL: return std::make_error_code(std::errc::address_not_available);
    case WSAEACCES: return std::make_error_code(std::errc::permission_denied);
    case WSAEMSGSIZE: return std::make_error_code(std::errc::message_size);
    case WSAENOTCONN: return std::make_error_code(std::errc::not_connected);
    case WSAEISCONN: return std::make_error_code(std::errc::already_connected);
    case WSAEINVAL: return std::make_error_code(std::errc::invalid_argument);
    default: return std::error_code{static_cast<int>(error), std::system_category()};
    }
}

// ---- 生命周期 ----

iocp_backend::iocp_backend(execution_context& context) : context_{static_cast<io_context*>(&context)} {
    ensure_networking_initialized();
    // NumberOfConcurrentThreads = 0：允许与处理器数相同的线程同时活跃；io_context 让所有 run() 线程进来。
    port_ = ::CreateIoCompletionPort(INVALID_HANDLE_VALUE, nullptr, 0, 0);
    if (port_ == nullptr) throw std::system_error{last_win32_error(), "CreateIoCompletionPort"};
}

iocp_backend::~iocp_backend() {
    if (socket_is_valid(signal_pump_.socket)) ::closesocket(signal_pump_.socket);
    if (port_ != nullptr) ::CloseHandle(port_);
}

void iocp_backend::shutdown() {
    std::lock_guard<std::mutex> lock{mutex_};
    timers_.clear();
}

std::error_code iocp_backend::associate(HANDLE const handle) noexcept {
    if (::CreateIoCompletionPort(handle, port_, io_key, 0) == nullptr) return last_win32_error();
    return {};
}

std::unique_ptr<socket_impl> iocp_backend::create_socket(io_context& context) {
    return std::unique_ptr<socket_impl>{new iocp_socket{context, *this}};
}

std::unique_ptr<timer_impl> iocp_backend::create_timer(io_context& context) {
    return std::unique_ptr<timer_impl>{new heap_timer{context, *this}};
}

std::unique_ptr<file_impl> iocp_backend::create_file(io_context& context) {
    return std::unique_ptr<file_impl>{new iocp_file{context, *this}};
}

// ---- 信号泵 ----

std::error_code iocp_backend::register_signal_reader(native_socket_type const read_end, void (*const deliver)(int)) noexcept {
    if (socket_is_valid(signal_pump_.socket)) return {};
    auto const ec = associate(reinterpret_cast<HANDLE>(read_end));
    if (ec) return ec;
    signal_pump_.socket = read_end;
    signal_pump_.deliver = deliver;
    signal_pump_.counts_as_work = false;
    if (not signal_pump_.arm()) return iocp_error(signal_pump_.last_error);
    return {};
}

bool iocp_backend::signal_pump::arm() noexcept {
    reset_overlapped();
    wsabuf.buf = buffer + leftover;
    wsabuf.len = static_cast<ULONG>(sizeof(buffer) - leftover);
    DWORD flags = 0;
    DWORD transferred = 0;
    if (::WSARecv(socket, &wsabuf, 1U, &transferred, &flags, overlapped(), nullptr) == SOCKET_ERROR) {
        auto const error = static_cast<DWORD>(::WSAGetLastError());
        if (error != WSA_IO_PENDING) {
            last_error = error;
            return false;
        }
    }
    return true;
}

void iocp_backend::signal_pump::on_complete(DWORD const error, DWORD const bytes) noexcept {
    last_error = error;
    received = error == 0 ? bytes : 0;
}

void iocp_backend::signal_pump::complete() noexcept {
    auto const total = static_cast<std::size_t>(leftover + received);
    auto const count = total / sizeof(int);
    for (auto i = std::size_t{}; i != count; ++i) {
        int value = 0;
        std::memcpy(&value, buffer + i * sizeof(int), sizeof(int));
        deliver(value);
    }
    leftover = static_cast<DWORD>(total % sizeof(int));
    if (leftover != 0) std::memmove(buffer, buffer + count * sizeof(int), leftover);
    if (last_error == 0 && received != 0) arm(); // 0 字节 = 写端关闭；出错（端口关闭等）不再武装
}

// ---- 定时器 ----

bool iocp_backend::add_timer(timer_op& op) noexcept {
    auto wake = false;
    {
        std::lock_guard<std::mutex> lock{mutex_};
        CO2_CONTRACT_CHECK(op.heap_index == timer_op::not_queued);
        if (op.cancel_requested) {
            op.cancel_requested = false;
            op.ec = make_error_code(error::operation_aborted);
            return true;
        }
        context_->get_executor().on_work_started(); // 发布之前
        timers_.push(op);
        wake = waiting_ != 0U && timers_.front() == &op; // 最早到期变了：叫醒一个等待线程重算超时
    }
    if (wake) interrupt();
    return false;
}

bool iocp_backend::cancel_timer(timer_op& op, bool const from_stop_token) noexcept {
    {
        std::lock_guard<std::mutex> lock{mutex_};
        if (op.heap_index == timer_op::not_queued) {
            if (from_stop_token) op.cancel_requested = true;
            return false;
        }
        timers_.remove(op.heap_index);
        op.ec = make_error_code(error::operation_aborted);
    }
    op.complete();
    context_->get_executor().on_work_finished();
    return true;
}

DWORD iocp_backend::wait_timeout_ms_locked(long const limit_ms) const noexcept {
    auto timeout = limit_ms < 0 ? INFINITE : static_cast<DWORD>(limit_ms);
    if (not timers_.empty()) {
        auto const now = std::chrono::steady_clock::now();
        auto const expiry = timers_.front()->expiry;
        if (expiry <= now) return 0;
        auto const ms = std::chrono::duration_cast<std::chrono::milliseconds>(expiry - now).count() + 1; // 向上取整
        auto const until = ms > static_cast<long long>(INFINITE - 1) ? INFINITE - 1 : static_cast<DWORD>(ms);
        if (until < timeout) timeout = until;
    }
    return timeout;
}

// ---- 事件循环 ----

void iocp_backend::interrupt() noexcept { ::PostQueuedCompletionStatus(port_, 0, wake_key, nullptr); }

void iocp_backend::run(long const timeout_ms) {
    // 多个线程可能同时在这里：每个线程自己的一批完成与到期定时器放在线程局部的向量里。
    static thread_local std::vector<completed> completed_;
    static thread_local std::vector<timer_op*> expired_;
    DWORD timeout = 0;
    {
        std::lock_guard<std::mutex> lock{mutex_};
        timeout = wait_timeout_ms_locked(timeout_ms);
        if (timeout != 0) ++waiting_;
    }
    completed_.clear();
    expired_.clear();
    // 第一次按超时阻塞，之后用零超时把已到达的包都取出来（一轮最多 64 个）。
    for (auto round = 0; round != 64; ++round) {
        DWORD bytes = 0;
        ULONG_PTR key = 0;
        LPOVERLAPPED overlapped = nullptr;
        ::SetLastError(0);
        auto const ok = ::GetQueuedCompletionStatus(port_, &bytes, &key, &overlapped, round == 0 ? timeout : 0);
        auto const error = ok ? DWORD{} : ::GetLastError();
        if (round == 0 && timeout != 0) {
            std::lock_guard<std::mutex> lock{mutex_};
            --waiting_;
        }
        if (overlapped == nullptr) {
            if (ok) continue;               // wake_key：只是叫醒
            if (error == WAIT_TIMEOUT) break; // 超时 / 已排空
            throw std::system_error{std::error_code{static_cast<int>(error), std::system_category()}, "GetQueuedCompletionStatus"};
        }
        static_cast<void>(key);
        auto* const op = iocp_op::from_overlapped(overlapped);
        op->on_complete(error, bytes);
        completed_.push_back(completed{op, op->counts_as_work});
    }
    {
        std::lock_guard<std::mutex> lock{mutex_};
        timers_.pop_expired(std::chrono::steady_clock::now(), expired_);
    }
    auto const executor = context_->get_executor();
    for (auto const& entry : completed_) {
        entry.op->complete(); // complete() 之后不再触碰 op
        if (entry.counts_as_work) executor.on_work_finished();
    }
    for (auto* const op : expired_) {
        op->complete();
        executor.on_work_finished();
    }
    completed_.clear();
    expired_.clear();
}

} // namespace detail
} // namespace net
