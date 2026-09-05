#include <atomic>
#include <cerrno>
#include <cstdint>
#include <mutex>
#include <system_error>
#include <vector>

#include <poll.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include "detail/reactor/demultiplexer.hpp"

// POSIX poll(2)：电平触发。注册表是一个 pollfd 数组（下标 0 是中断用的 eventfd），每次
// wait 在锁内复制一份快照再阻塞，兴趣变化时写 eventfd 唤醒重建。兴趣为 0 的描述符把
// fd 取负——poll 会忽略它，否则 POLLHUP/POLLERR 仍会被报告而造成忙转。

namespace net {
namespace detail {

namespace {

short events_of(unsigned const interest) noexcept {
    auto events = short{};
    if (interest & read_ready_bit) events |= POLLIN | POLLRDHUP;
    if (interest & write_ready_bit) events |= POLLOUT;
    return events;
}

unsigned ready_bits_of(short const revents) noexcept {
    auto bits = 0U;
    if (revents & (POLLIN | POLLRDHUP | POLLHUP | POLLERR | POLLNVAL)) bits |= read_ready_bit;
    if (revents & (POLLOUT | POLLHUP | POLLERR | POLLNVAL)) bits |= write_ready_bit;
    return bits;
}

struct poll_demultiplexer final : demultiplexer {
    poll_demultiplexer() {
        event_fd_ = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
        if (event_fd_ < 0) throw std::system_error{errno, std::system_category(), "eventfd"};
        fds_.push_back(pollfd{event_fd_, POLLIN, 0});
        states_.push_back(nullptr);
    }

    ~poll_demultiplexer() override { ::close(event_fd_); }

    char const* name() const noexcept override { return "poll"; }
    bool edge_triggered() const noexcept override { return false; }

    std::error_code add(descriptor_state& state, unsigned const interest) noexcept override {
        std::lock_guard<std::mutex> lock{mutex_};
        state.demux_index = fds_.size();
        fds_.push_back(pollfd{interest != 0U ? state.fd : -state.fd - 1, events_of(interest), 0});
        states_.push_back(&state);
        if (state.fd >= 0) {
            if (static_cast<std::size_t>(state.fd) >= by_fd_.size())
                by_fd_.resize(static_cast<std::size_t>(state.fd) + 1U, nullptr);
            by_fd_[static_cast<std::size_t>(state.fd)] = &state;
        }
        if (interest != 0U && waiting_.load(std::memory_order_acquire)) interrupt();
        return {};
    }

    void update(descriptor_state& state, unsigned const interest) noexcept override {
        std::lock_guard<std::mutex> lock{mutex_};
        if (state.demux_index == descriptor_state::no_index) return;
        auto& entry = fds_[state.demux_index];
        entry.fd = interest != 0U ? state.fd : -state.fd - 1;
        entry.events = events_of(interest);
        // 兴趣增加时唤醒阻塞中的 wait 让它重建快照；缩小不必（多余的事件无害）。
        if (interest != 0U && waiting_.load(std::memory_order_acquire)) interrupt();
    }

    void remove(descriptor_state& state) noexcept override {
        std::lock_guard<std::mutex> lock{mutex_};
        auto const index = state.demux_index;
        if (index == descriptor_state::no_index) return;
        auto const last = fds_.size() - 1U;
        if (index != last) {
            fds_[index] = fds_[last];
            states_[index] = states_[last];
            states_[index]->demux_index = index;
        }
        fds_.pop_back();
        states_.pop_back();
        state.demux_index = descriptor_state::no_index;
        if (state.fd >= 0 && static_cast<std::size_t>(state.fd) < by_fd_.size() &&
            by_fd_[static_cast<std::size_t>(state.fd)] == &state)
            by_fd_[static_cast<std::size_t>(state.fd)] = nullptr;
    }

    std::error_code wait(long const timeout_ms, event_sink& sink) noexcept override {
        {
            std::lock_guard<std::mutex> lock{mutex_};
            snapshot_ = fds_;
            waiting_.store(true, std::memory_order_release);
        }
        auto const count = ::poll(snapshot_.data(), static_cast<nfds_t>(snapshot_.size()),
                                  timeout_ms < 0 ? -1 : static_cast<int>(timeout_ms));
        waiting_.store(false, std::memory_order_release);
        if (count < 0) {
            if (errno == EINTR) return {};
            return std::error_code{errno, std::system_category()};
        }
        if (count == 0) return {};
        if (snapshot_[0].revents != 0) {
            std::uint64_t drained = 0;
            while (::read(event_fd_, &drained, sizeof(drained)) > 0) {}
        }
        std::lock_guard<std::mutex> lock{mutex_};
        for (auto index = std::size_t{1}; index < snapshot_.size(); ++index) {
            auto const& entry = snapshot_[index];
            if (entry.revents == 0 || entry.fd < 0) continue;
            // 快照下标可能已过期（摘除会换位），按 fd 找当前登记的状态。
            auto const fd = static_cast<std::size_t>(entry.fd);
            auto* const state = fd < by_fd_.size() ? by_fd_[fd] : nullptr;
            if (state == nullptr) continue;
            sink.on_ready(*state, ready_bits_of(entry.revents));
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
    std::vector<pollfd> fds_;
    std::vector<descriptor_state*> states_;
    std::vector<descriptor_state*> by_fd_;
    std::vector<pollfd> snapshot_; // 只有运行 wait 的线程触碰
    std::atomic<bool> waiting_{false};
};

} // namespace

std::unique_ptr<demultiplexer> make_poll_demultiplexer() {
    return std::unique_ptr<demultiplexer>{new poll_demultiplexer{}};
}

} // namespace detail
} // namespace net
