#pragma once

#include <cstddef>
#include <cstring>
#include <memory>
#include <string>
#include <system_error>
#include <vector>

#include <netdb.h>
#include <sys/socket.h>

#include "net/coroutine.hpp"
#include "net/io_env.hpp"
#include "net/io_result.hpp"
#include "net/ip.hpp"

// DNS（P4100R1 §8.8 Paper 12）：resolver.resolve(host, service) 正向解析，resolve(endpoint)
// 反向解析。getaddrinfo 是阻塞调用，在 io_context 的解析服务线程上执行，完成后经调用方
// 的执行器恢复协程。取消（cancel / stop_token）不能中断进行中的 getaddrinfo：结果到达后以
// operation_aborted 完成。
//
//   auto [ec, results] = co_await resolver.resolve("example.com", "80");
//   for (auto const& entry : results) { entry.endpoint ... }

namespace net {

struct io_context;

namespace detail {

struct resolver_impl;

struct raw_endpoint {
    sockaddr_storage storage;
    socklen_t length;
    std::string host_name;
    std::string service_name;
};

struct resolver_access {
    static resolver_impl* create(io_context& context);
    static void destroy(resolver_impl* impl) noexcept;
    static io_context& context(resolver_impl& impl) noexcept;
    static void start_forward(resolver_impl& impl, std::string host, std::string service,
                              int socktype, int protocol, int flags);
    static void start_reverse(resolver_impl& impl, sockaddr const* address, socklen_t length,
                              int socktype);
    static coroutine_handle<> suspend(resolver_impl& impl, coroutine_handle<> h,
                                      io_env const* env) noexcept;
    static std::error_code finish(resolver_impl& impl, std::vector<raw_endpoint>& out) noexcept;
    static void cancel(resolver_impl& impl) noexcept;
};

struct resolver_impl_deleter {
    void operator()(resolver_impl* const impl) const noexcept { resolver_access::destroy(impl); }
};

} // namespace detail

namespace ip {

enum class resolver_flags : int {
    none = 0,
    passive = AI_PASSIVE,
    canonical_name = AI_CANONNAME,
    numeric_host = AI_NUMERICHOST,
    numeric_service = AI_NUMERICSERV,
    v4_mapped = AI_V4MAPPED,
    all_matching = AI_ALL,
    address_configured = AI_ADDRCONFIG,
};

inline resolver_flags operator|(resolver_flags const a, resolver_flags const b) noexcept {
    return static_cast<resolver_flags>(static_cast<int>(a) | static_cast<int>(b));
}

inline resolver_flags operator&(resolver_flags const a, resolver_flags const b) noexcept {
    return static_cast<resolver_flags>(static_cast<int>(a) & static_cast<int>(b));
}

template <class Protocol> struct basic_resolver_entry {
    using protocol_type = Protocol;
    using endpoint_type = typename Protocol::endpoint;

    endpoint_type endpoint;
    std::string host_name;
    std::string service_name;
};

template <class Protocol> struct basic_resolver_results {
    using value_type = basic_resolver_entry<Protocol>;
    using const_iterator = typename std::vector<value_type>::const_iterator;
    using iterator = const_iterator;

    basic_resolver_results() = default;
    explicit basic_resolver_results(std::vector<value_type> entries) : entries_(std::move(entries)) {}

    std::size_t size() const noexcept { return entries_.size(); }
    bool empty() const noexcept { return entries_.empty(); }
    const_iterator begin() const noexcept { return entries_.begin(); }
    const_iterator end() const noexcept { return entries_.end(); }
    value_type const& operator[](std::size_t const index) const noexcept { return entries_[index]; }

  private:
    std::vector<value_type> entries_;
};

template <class Protocol> struct basic_resolver_awaitable {
    using results_type = basic_resolver_results<Protocol>;
    using endpoint_type = typename Protocol::endpoint;

    detail::resolver_impl* impl;

    bool await_ready() const noexcept { return false; }

    coroutine_handle<> await_suspend(coroutine_handle<> const h, io_env const* const env) noexcept {
        return detail::resolver_access::suspend(*impl, h, env);
    }

    io_result<results_type> await_resume() {
        std::vector<detail::raw_endpoint> raw;
        auto result = io_result<results_type>{detail::resolver_access::finish(*impl, raw), results_type{}};
        if (result.ec) return result;
        std::vector<basic_resolver_entry<Protocol>> entries;
        entries.reserve(raw.size());
        for (auto& entry : raw) {
            auto endpoint = endpoint_type{};
            if (entry.length <= endpoint.capacity()) {
                std::memcpy(endpoint.data(), &entry.storage, entry.length);
            }
            entries.push_back(basic_resolver_entry<Protocol>{endpoint, std::move(entry.host_name),
                                                             std::move(entry.service_name)});
        }
        result.value = results_type{std::move(entries)};
        return result;
    }
};

template <class Protocol> struct basic_resolver {
    using protocol_type = Protocol;
    using endpoint_type = typename Protocol::endpoint;
    using results_type = basic_resolver_results<Protocol>;
    using flags = resolver_flags;

    explicit basic_resolver(io_context& context) : impl_{detail::resolver_access::create(context)} {}

    basic_resolver(basic_resolver&&) noexcept = default;
    basic_resolver& operator=(basic_resolver&&) noexcept = default;

    io_context& context() const noexcept { return detail::resolver_access::context(*impl_); }

    basic_resolver_awaitable<Protocol> resolve(std::string host, std::string service,
                                               resolver_flags const options = resolver_flags::none) {
        auto const protocol = Protocol::v4();
        detail::resolver_access::start_forward(*impl_, std::move(host), std::move(service),
                                               protocol.type(), protocol.protocol(),
                                               static_cast<int>(options));
        return basic_resolver_awaitable<Protocol>{impl_.get()};
    }

    basic_resolver_awaitable<Protocol> resolve(endpoint_type const& endpoint) {
        detail::resolver_access::start_reverse(*impl_, endpoint.data(),
                                               static_cast<socklen_t>(endpoint.size()),
                                               Protocol::v4().type());
        return basic_resolver_awaitable<Protocol>{impl_.get()};
    }

    // 未完成的解析以 operation_aborted 完成（进行中的系统调用结束之后）。
    void cancel() noexcept { detail::resolver_access::cancel(*impl_); }

  private:
    std::unique_ptr<detail::resolver_impl, detail::resolver_impl_deleter> impl_;
};

} // namespace ip
} // namespace net
