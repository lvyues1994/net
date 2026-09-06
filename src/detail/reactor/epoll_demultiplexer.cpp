#include <cerrno>
#include <cstdint>
#include <system_error>

#include <sys/epoll.h>
#include <sys/syscall.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include "detail/reactor/demultiplexer.hpp"

// Linux epoll：边沿触发一次注册，所有兴趣位始终登记；就绪位由反应器维护。中断用一个
// eventfd（data.ptr 为空作标记）。

namespace net {
namespace detail {

namespace {

constexpr int max_events = 128;

unsigned ready_bits_of(std::uint32_t const events) noexcept {
    auto bits = 0U;
    if (events & (EPOLLIN | EPOLLRDHUP | EPOLLERR | EPOLLHUP)) bits |= read_ready_bit;
    if (events & (EPOLLOUT | EPOLLERR | EPOLLHUP)) bits |= write_ready_bit;
    return bits;
}

struct epoll_demultiplexer final : demultiplexer {
    epoll_demultiplexer() {
        epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
        if (epoll_fd_ < 0) throw std::system_error{errno, std::system_category(), "epoll_create1"};
        event_fd_ = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
        if (event_fd_ < 0) {
            auto const saved = errno;
            ::close(epoll_fd_);
            throw std::system_error{saved, std::system_category(), "eventfd"};
        }
        epoll_event event{};
        event.events = EPOLLIN | EPOLLET;
        event.data.ptr = nullptr;
        if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, event_fd_, &event) != 0) {
            auto const saved = errno;
            ::close(event_fd_);
            ::close(epoll_fd_);
            throw std::system_error{saved, std::system_category(), "epoll_ctl"};
        }
    }

    ~epoll_demultiplexer() override {
        ::close(event_fd_);
        ::close(epoll_fd_);
    }

    char const* name() const noexcept override { return "epoll"; }
    bool edge_triggered() const noexcept override { return true; }

    std::error_code add(descriptor_state& state, unsigned) noexcept override {
        epoll_event event{};
        event.events = EPOLLIN | EPOLLOUT | EPOLLRDHUP | EPOLLET;
        event.data.ptr = &state;
        if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, state.fd, &event) != 0)
            return std::error_code{errno, std::system_category()};
        return {};
    }

    void update(descriptor_state&, unsigned) noexcept override {}

    void remove(descriptor_state& state) noexcept override {
        epoll_event event{};
        ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, state.fd, &event);
    }

    std::error_code wait(long long const timeout_ns, event_sink& sink) noexcept override {
        epoll_event events[max_events];
        timespec ts{};
        auto const* const timeout = timespec_of(timeout_ns, ts);
        auto count = -1;
#if defined(__NR_epoll_pwait2)
        // 5.11+：纳秒精度。老内核 ENOSYS 后退回毫秒的 epoll_wait（向上取整）。
        if (have_pwait2_) {
            count = static_cast<int>(::syscall(__NR_epoll_pwait2, epoll_fd_, events, max_events, timeout, nullptr, 0));
            if (count < 0 && errno == ENOSYS) have_pwait2_ = false;
        }
        if (not have_pwait2_)
#endif
        {
            auto const ms = timeout_ns < 0 ? -1 : static_cast<int>((timeout_ns + 999999LL) / 1000000LL);
            count = ::epoll_wait(epoll_fd_, events, max_events, ms);
        }
        if (count < 0) {
            if (errno == EINTR) return {};
            return std::error_code{errno, std::system_category()};
        }
        for (auto index = 0; index < count; ++index) {
            auto* const state = static_cast<descriptor_state*>(events[index].data.ptr);
            if (state == nullptr) {
                std::uint64_t drained = 0;
                while (::read(event_fd_, &drained, sizeof(drained)) > 0) {}
                continue;
            }
            sink.on_ready(*state, ready_bits_of(events[index].events));
        }
        return {};
    }

    void interrupt() noexcept override {
        std::uint64_t const one = 1U;
        static_cast<void>(::write(event_fd_, &one, sizeof(one)));
    }

  private:
    int epoll_fd_ = -1;
    bool have_pwait2_ = true;
    int event_fd_ = -1;
};

} // namespace

std::unique_ptr<demultiplexer> make_epoll_demultiplexer() {
    return std::unique_ptr<demultiplexer>{new epoll_demultiplexer{}};
}

} // namespace detail
} // namespace net
