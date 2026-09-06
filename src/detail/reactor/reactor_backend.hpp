#pragma once

#include <chrono>
#include <memory>
#include <mutex>
#include <unordered_set>
#include <utility>
#include <vector>

#include "net/execution_context.hpp"

#include "detail/backend.hpp"
#include "detail/reactor/demultiplexer.hpp"
#include "detail/reactor/reactor_op.hpp"

// 就绪型 io_backend：描述符表 + 就绪位 + 每方向一个操作的排队 + 定时器二叉堆 + 信号泵。
// 等待机制由注入的 demultiplexer 提供。作为服务注册在 io_context 里（键 io_backend）。
//
// 工作计数：每个排队中的操作与定时器持有 io_context 的一份工作（counts_as_work 为假的
// 常驻监听除外）。

namespace net {
namespace detail {

struct reactor_backend final : execution_context::service, io_backend, event_sink {
    using key_type = io_backend;

    reactor_backend(execution_context& context, std::unique_ptr<demultiplexer> demux);
    ~reactor_backend() override;

    // ---- io_backend ----
    void run(long timeout_ms) override;
    void interrupt() noexcept override;
    std::unique_ptr<socket_impl> create_socket(io_context& context) override;
    std::unique_ptr<timer_impl> create_timer(io_context& context) override;
    std::error_code register_signal_reader(int read_fd,
                                           void (*deliver)(int signal_number)) noexcept override;
    char const* name() const noexcept override { return demux_->name(); }

    // ---- 就绪型内部协议（reactor_socket / reactor_timer 使用） ----

    io_context& context() noexcept { return *context_; }

    std::error_code register_descriptor(descriptor_state& state, int fd) noexcept;
    // 从解复用器摘除并取消两个方向的操作（以 operation_aborted 完成）。
    void deregister_descriptor(descriptor_state& state) noexcept;

    // 启动操作。就绪位命中时先在调用线程上 perform()：完成则返回 true（调用方自己恢复
    // 协程，不会再调用 complete()）；否则排队并返回 false。
    bool start_op(descriptor_state& state, op_direction direction, reactor_op& op) noexcept;
    // 丢掉尚未消费的就绪位（connect 之前：未连接套接字的初始"可写"是过期的）。
    void clear_ready(descriptor_state& state, unsigned bits) noexcept;
    // 仍排队时摘下并以 operation_aborted 完成，返回 true；否则返回 false。
    bool cancel_op(descriptor_state& state, op_direction direction, reactor_op& op) noexcept;
    void cancel_ops(descriptor_state& state) noexcept;

    bool add_timer(timer_op& op) noexcept; // 返回 true：已因停止请求同步以 aborted 完成
    bool cancel_timer(timer_op& op, bool from_stop_token = false) noexcept;

  protected:
    void shutdown() override;

  private:
    struct pending_event {
        descriptor_state* state;
        unsigned bits;
    };

    struct completed_op {
        reactor_op* op;
        bool counts_as_work;
    };

    struct signal_pump final : reactor_op {
        bool perform() noexcept override;
        void complete() noexcept override {}
        int fd = -1;
        void (*deliver)(int) = nullptr;
    };

    // event_sink：wait 内（锁外）只记录。
    void on_ready(descriptor_state& state, unsigned ready_bits) noexcept override;

    // 锁内。
    void process_event(descriptor_state& state, unsigned ready, std::vector<completed_op>& completed) noexcept;
    void refresh_interest(descriptor_state& state) noexcept;
    void detach_ops(descriptor_state& state, reactor_op* (&cancelled)[2]) noexcept;
    long long wait_timeout_ns(long limit_ms) const noexcept;
    void arm_timer_fd_locked() noexcept;
    void pop_expired_timers(std::vector<timer_op*>& expired) noexcept;
    void heap_push(timer_op& op) noexcept;
    void heap_remove(std::size_t index) noexcept;
    void heap_up(std::size_t index) noexcept;
    void heap_down(std::size_t index) noexcept;
    void heap_swap(std::size_t a, std::size_t b) noexcept;

    void finish_cancelled(reactor_op* const (&cancelled)[2]) noexcept;

    io_context* context_;
    std::unique_ptr<demultiplexer> demux_;
    std::mutex mutex_;
    std::unordered_set<descriptor_state*> registered_;
    std::vector<timer_op*> timers_;
    std::vector<pending_event> events_;   // 只有运行 run 的线程触碰
    std::vector<completed_op> completed_; // 同上（复用，避免每轮分配）
    std::vector<timer_op*> expired_;      // 同上
    descriptor_state signal_state_;
    signal_pump signal_pump_;
    bool shut_down_ = false;
    bool waiting_ = false; // 有线程阻塞在 demux_->wait 里（锁内读写）

    // 定时器堆最早到期用 timerfd 送进解复用器：timerfd 是 hrtimer、不受线程 timer slack
    //（默认 50 µs）影响，而 epoll_pwait2 / ppoll / pselect 的超时会被 slack 拉长。只在最早到期
    // 变化时 timerfd_settime。
    int timer_fd_ = -1;
    descriptor_state timer_state_;
    std::chrono::steady_clock::time_point armed_expiry_{};
    bool armed_ = false;
};

} // namespace detail
} // namespace net
