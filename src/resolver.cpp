#include "net/resolver.hpp"

#include <atomic>
#include <condition_variable>
#include <cstring>
#include <mutex>
#include <thread>

#include "co2/contract.hpp"

#include "net/continuation.hpp"
#include "net/detail/socket_types.hpp"
#include "net/detail/storage.hpp"
#include "net/error.hpp"
#include "net/execution_context.hpp"
#include "net/io_context.hpp"

#include "detail/backend.hpp"

namespace net {
namespace detail {

namespace {

std::error_code translate_gai(int const code) noexcept {
    switch (code) {
    case 0: return {};
    case EAI_AGAIN: return make_error_code(error::host_not_found_try_again);
    case EAI_FAIL: return make_error_code(error::no_recovery);
    case EAI_NONAME: return make_error_code(error::host_not_found);
    case EAI_SERVICE: return make_error_code(error::service_not_found);
    case EAI_SOCKTYPE: return make_error_code(error::socket_type_not_supported);
    case EAI_MEMORY: return std::make_error_code(std::errc::not_enough_memory);
#if defined(EAI_SYSTEM)
    case EAI_SYSTEM: return std::error_code{errno, std::system_category()};
#endif
#if defined(EAI_NODATA) && EAI_NODATA != EAI_NONAME
    case EAI_NODATA: return make_error_code(error::no_data);
#endif
    default: return make_error_code(error::host_not_found);
    }
}

} // namespace

struct resolver_service;

struct cancel_resolve {
    resolver_impl* impl;
    void operator()() const noexcept;
};

struct resolver_impl {
    explicit resolver_impl(io_context& context_);
    ~resolver_impl() { CO2_CONTRACT_CHECK(not pending); }

    // 工作线程调用：post 之后协程可能立刻在 io 线程上恢复、结束并销毁本对象，所以先取执行器。
    void complete() noexcept {
        auto const executor = context->get_executor();
        env->executor.post(cont);
        executor.on_work_finished();
    }

    io_context* context;
    resolver_service* service;

    // 作业
    bool reverse = false;
    std::string host;
    std::string service_name;
    int socktype = 0;
    int protocol = 0;
    int flags = 0;
    sockaddr_storage reverse_address{};
    socklen_t reverse_length = 0;

    // 结果
    std::vector<raw_endpoint> results;
    std::error_code ec;

    continuation cont;
    io_env const* env = nullptr;
    bool pending = false;
    std::atomic<bool> cancelled{false};
    late_init<stop_callback<cancel_resolve>> stop_cb;
    resolver_impl* next = nullptr; // 服务队列
};

// 每个 io_context 一个解析服务：一条惰性启动的工作线程串行执行阻塞的 getaddrinfo。
struct resolver_service final : execution_context::service {
    explicit resolver_service(execution_context&) { ensure_networking_initialized(); }

    ~resolver_service() override { stop_worker(); }

    void shutdown() override { stop_worker(); }

    void enqueue(resolver_impl& job) {
        std::lock_guard<std::mutex> lock{mutex_};
        job.next = nullptr;
        if (tail_ != nullptr)
            tail_->next = &job;
        else
            head_ = &job;
        tail_ = &job;
        if (not worker_.joinable() && not stopping_) worker_ = std::thread{[this] { worker_main(); }};
        cv_.notify_one();
    }

    // 仍在队列里则摘下，返回 true。
    bool dequeue(resolver_impl& job) noexcept {
        std::lock_guard<std::mutex> lock{mutex_};
        resolver_impl* previous = nullptr;
        for (auto* current = head_; current != nullptr; previous = current, current = current->next) {
            if (current != &job) continue;
            if (previous != nullptr)
                previous->next = current->next;
            else
                head_ = current->next;
            if (tail_ == current) tail_ = previous;
            current->next = nullptr;
            return true;
        }
        return false;
    }

  private:
    void stop_worker() {
        {
            std::lock_guard<std::mutex> lock{mutex_};
            stopping_ = true;
            head_ = nullptr;
            tail_ = nullptr;
        }
        cv_.notify_all();
        if (worker_.joinable()) worker_.join();
    }

    void worker_main() {
        std::unique_lock<std::mutex> lock{mutex_};
        for (;;) {
            while (head_ == nullptr && not stopping_)
                cv_.wait(lock);
            if (stopping_) return;
            auto* const job = head_;
            head_ = job->next;
            if (head_ == nullptr) tail_ = nullptr;
            job->next = nullptr;
            lock.unlock();
            run_job(*job);
            job->complete();
            lock.lock();
        }
    }

    static void run_job(resolver_impl& job) noexcept {
        job.results.clear();
        if (job.cancelled.load(std::memory_order_acquire)) {
            job.ec = make_error_code(error::operation_aborted);
            return;
        }
        job.ec = job.reverse ? run_reverse(job) : run_forward(job);
        if (job.cancelled.load(std::memory_order_acquire)) {
            job.results.clear();
            job.ec = make_error_code(error::operation_aborted);
        }
    }

    static std::error_code run_forward(resolver_impl& job) noexcept {
        addrinfo hints{};
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = job.socktype;
        hints.ai_protocol = job.protocol;
        hints.ai_flags = job.flags;
        addrinfo* list = nullptr;
        auto const code = ::getaddrinfo(job.host.empty() ? nullptr : job.host.c_str(),
                                        job.service_name.empty() ? nullptr : job.service_name.c_str(),
                                        &hints, &list);
        if (code != 0) return translate_gai(code);
        for (auto* entry = list; entry != nullptr; entry = entry->ai_next) {
            if (entry->ai_addrlen > sizeof(sockaddr_storage)) continue;
            raw_endpoint raw{};
            std::memcpy(&raw.storage, entry->ai_addr, entry->ai_addrlen);
            raw.length = static_cast<socklen_t>(entry->ai_addrlen);
            raw.host_name = entry->ai_canonname != nullptr ? entry->ai_canonname : job.host;
            raw.service_name = job.service_name;
            job.results.push_back(std::move(raw));
        }
        ::freeaddrinfo(list);
        return {};
    }

    static std::error_code run_reverse(resolver_impl& job) noexcept {
        char host[NI_MAXHOST];
        char service[NI_MAXSERV];
        auto const code = ::getnameinfo(reinterpret_cast<sockaddr const*>(&job.reverse_address),
                                        job.reverse_length, host, sizeof(host), service,
                                        sizeof(service), job.socktype == SOCK_DGRAM ? NI_DGRAM : 0);
        if (code != 0) return translate_gai(code);
        raw_endpoint raw{};
        std::memcpy(&raw.storage, &job.reverse_address, static_cast<std::size_t>(job.reverse_length));
        raw.length = job.reverse_length;
        raw.host_name = host;
        raw.service_name = service;
        job.results.push_back(std::move(raw));
        return {};
    }

    std::mutex mutex_;
    std::condition_variable cv_;
    resolver_impl* head_ = nullptr;
    resolver_impl* tail_ = nullptr;
    std::thread worker_;
    bool stopping_ = false;
};

resolver_impl::resolver_impl(io_context& context_)
    : context{&context_}, service{&context_.use_service<resolver_service>()} {}

void cancel_resolve::operator()() const noexcept { resolver_access::cancel(*impl); }

// ---- resolver_access ----

resolver_impl* resolver_access::create(io_context& context) { return new resolver_impl{context}; }

void resolver_access::destroy(resolver_impl* const impl) noexcept { delete impl; }

io_context& resolver_access::context(resolver_impl& impl) noexcept { return *impl.context; }

void resolver_access::start_forward(resolver_impl& impl, std::string host, std::string service,
                                    int const socktype, int const protocol, int const flags) {
    CO2_CONTRACT_CHECK(not impl.pending);
    impl.reverse = false;
    impl.host = std::move(host);
    impl.service_name = std::move(service);
    impl.socktype = socktype;
    impl.protocol = protocol;
    impl.flags = flags;
    impl.cancelled.store(false, std::memory_order_relaxed);
}

void resolver_access::start_reverse(resolver_impl& impl, sockaddr const* const address,
                                    socklen_t const length, int const socktype) {
    CO2_CONTRACT_CHECK(not impl.pending);
    impl.reverse = true;
    std::memcpy(&impl.reverse_address, address, static_cast<std::size_t>(length));
    impl.reverse_length = length;
    impl.socktype = socktype;
    impl.cancelled.store(false, std::memory_order_relaxed);
}

coroutine_handle<> resolver_access::suspend(resolver_impl& impl, coroutine_handle<> const h,
                                            io_env const* const env) noexcept {
    impl.cont.h = h;
    impl.env = env;
    impl.pending = true;
    if (env->stop_token.stop_requested()) {
        impl.results.clear();
        impl.ec = make_error_code(error::operation_aborted);
        return h;
    }
    if (env->stop_token.stop_possible())
        impl.stop_cb.emplace(env->stop_token, cancel_resolve{&impl});
    impl.context->get_executor().on_work_started();
    impl.service->enqueue(impl);
    return noop_coroutine();
}

std::error_code resolver_access::finish(resolver_impl& impl, std::vector<raw_endpoint>& out) noexcept {
    impl.stop_cb.reset();
    impl.pending = false;
    impl.env = nullptr;
    out = std::move(impl.results);
    impl.results.clear();
    return impl.ec;
}

void resolver_access::cancel(resolver_impl& impl) noexcept {
    if (not impl.pending) return;
    impl.cancelled.store(true, std::memory_order_release);
    // 还没被工作线程取走：立即以 operation_aborted 完成；否则工作线程结束后完成。
    if (impl.service->dequeue(impl)) {
        impl.results.clear();
        impl.ec = make_error_code(error::operation_aborted);
        impl.complete();
    }
}

} // namespace detail
} // namespace net
