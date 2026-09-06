#include <atomic>
#include <cerrno>
#include <cstdint>
#include <mutex>
#include <system_error>
#include <vector>

#include <sys/eventfd.h>
#include <sys/select.h>
#include <unistd.h>

#include "detail/reactor/demultiplexer.hpp"

// POSIX select(2)：电平触发，描述符号必须小于 FD_SETSIZE（超出以 EMFILE 拒绝注册）。
// 注册表按 fd 索引；每次 wait 在锁内从兴趣位重建三个 fd_set 再阻塞，兴趣变化时写
// eventfd 唤醒重建。

namespace net {
namespace detail {

namespace {

struct select_demultiplexer final : demultiplexer {
    select_demultiplexer() : by_fd_(FD_SETSIZE, nullptr), interest_(FD_SETSIZE, 0U) {
        event_fd_ = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
        if (event_fd_ < 0) throw std::system_error{errno, std::system_category(), "eventfd"};
        if (event_fd_ >= FD_SETSIZE) {
            ::close(event_fd_);
            throw std::system_error{EMFILE, std::system_category(), "select: eventfd >= FD_SETSIZE"};
        }
    }

    ~select_demultiplexer() override { ::close(event_fd_); }

    char const* name() const noexcept override { return "select"; }
    bool edge_triggered() const noexcept override { return false; }

    std::error_code add(descriptor_state& state, unsigned const interest) noexcept override {
        if (state.fd < 0 || state.fd >= FD_SETSIZE) return std::error_code{EMFILE, std::system_category()};
        std::lock_guard<std::mutex> lock{mutex_};
        auto const fd = static_cast<std::size_t>(state.fd);
        by_fd_[fd] = &state;
        interest_[fd] = interest;
        state.demux_index = fd;
        if (interest != 0U && waiting_.load(std::memory_order_acquire)) interrupt();
        return {};
    }

    void update(descriptor_state& state, unsigned const interest) noexcept override {
        std::lock_guard<std::mutex> lock{mutex_};
        if (state.demux_index == descriptor_state::no_index) return;
        interest_[state.demux_index] = interest;
        if (interest != 0U && waiting_.load(std::memory_order_acquire)) interrupt();
    }

    void remove(descriptor_state& state) noexcept override {
        std::lock_guard<std::mutex> lock{mutex_};
        if (state.demux_index == descriptor_state::no_index) return;
        if (by_fd_[state.demux_index] == &state) {
            by_fd_[state.demux_index] = nullptr;
            interest_[state.demux_index] = 0U;
        }
        state.demux_index = descriptor_state::no_index;
    }

    std::error_code wait(long long const timeout_ns, event_sink& sink) noexcept override {
        fd_set read_set;
        fd_set write_set;
        fd_set error_set;
        FD_ZERO(&read_set);
        FD_ZERO(&write_set);
        FD_ZERO(&error_set);
        auto max_fd = event_fd_;
        {
            std::lock_guard<std::mutex> lock{mutex_};
            FD_SET(event_fd_, &read_set);
            for (auto fd = 0; fd < FD_SETSIZE; ++fd) {
                auto const interest = interest_[static_cast<std::size_t>(fd)];
                if (interest == 0U || by_fd_[static_cast<std::size_t>(fd)] == nullptr) continue;
                if (interest & read_ready_bit) FD_SET(fd, &read_set);
                if (interest & write_ready_bit) FD_SET(fd, &write_set);
                FD_SET(fd, &error_set);
                if (fd > max_fd) max_fd = fd;
            }
            waiting_.store(true, std::memory_order_release);
        }
        timespec ts{};
        auto const count = ::pselect(max_fd + 1, &read_set, &write_set, &error_set, timespec_of(timeout_ns, ts), nullptr);
        waiting_.store(false, std::memory_order_release);
        if (count < 0) {
            if (errno == EINTR) return {};
            return std::error_code{errno, std::system_category()};
        }
        if (count == 0) return {};
        if (FD_ISSET(event_fd_, &read_set)) {
            std::uint64_t drained = 0;
            while (::read(event_fd_, &drained, sizeof(drained)) > 0) {}
        }
        std::lock_guard<std::mutex> lock{mutex_};
        for (auto fd = 0; fd <= max_fd; ++fd) {
            if (fd == event_fd_) continue;
            auto bits = 0U;
            if (FD_ISSET(fd, &read_set)) bits |= read_ready_bit;
            if (FD_ISSET(fd, &write_set)) bits |= write_ready_bit;
            if (FD_ISSET(fd, &error_set)) bits |= read_ready_bit | write_ready_bit;
            if (bits == 0U) continue;
            auto* const state = by_fd_[static_cast<std::size_t>(fd)];
            if (state == nullptr) continue;
            sink.on_ready(*state, bits);
        }
        return {};
    }

    void interrupt() noexcept override {
        std::uint64_t const one = 1U;
        static_cast<void>(::write(event_fd_, &one, sizeof(one)));
    }

  private:
    int event_fd_ = -1;
    std::mutex mutex_;
    std::vector<descriptor_state*> by_fd_;
    std::vector<unsigned> interest_;
    std::atomic<bool> waiting_{false};
};

} // namespace

std::unique_ptr<demultiplexer> make_select_demultiplexer() {
    return std::unique_ptr<demultiplexer>{new select_demultiplexer{}};
}

} // namespace detail
} // namespace net
