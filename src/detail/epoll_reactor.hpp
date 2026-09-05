#pragma once

#include <mutex>
#include <unordered_set>
#include <vector>

#include "net/execution_context.hpp"
#include "net/io_context.hpp"

#include "detail/reactor.hpp"

// Linux epoll 反应器：作为服务注册在 io_context 里。边沿触发（EPOLLET）一次注册；
// 就绪事件到达而没有排队操作时记入 descriptor_state::ready，下一次 start_op 先消费它，
// 避免 ET 下的丢失唤醒。定时器是按到期时间排序的二叉堆，epoll_wait 的超时取最早到期。

namespace net {
namespace detail {

struct epoll_reactor final : execution_context::service, reactor {
    explicit epoll_reactor(execution_context& context);
    ~epoll_reactor() override;

    // ---- reactor ----
    std::error_code register_descriptor(descriptor_state& state, int fd) noexcept override;
    void deregister_descriptor(descriptor_state& state) noexcept override;
    bool start_op(descriptor_state& state, op_direction direction,
                  reactor_op& op) noexcept override;
    bool cancel_op(descriptor_state& state, op_direction direction,
                   reactor_op& op) noexcept override;
    void cancel_ops(descriptor_state& state) noexcept override;
    void add_timer(timer_op& op) noexcept override;
    bool cancel_timer(timer_op& op) noexcept override;

    // ---- io_context 调用 ----

    // 等待并处理一批事件；timeout_ms < 0 表示只受定时器限制。完成的操作在锁外 complete()。
    void run(long timeout_ms);
    // 唤醒阻塞在 epoll_wait 里的线程。
    void interrupt() noexcept;

  protected:
    void shutdown() override;

  private:
    static constexpr unsigned read_ready = 1U;
    static constexpr unsigned write_ready = 2U;

    struct completed_op {
        reactor_op* op;
        bool counts_as_work;
    };

    void process_descriptor(descriptor_state& state, unsigned ready,
                            std::vector<completed_op>& completed) noexcept;
    long timer_timeout_ms(long limit) const noexcept;
    void pop_expired_timers(std::vector<timer_op*>& expired) noexcept;

    // 堆操作（锁内）
    void heap_push(timer_op& op) noexcept;
    void heap_remove(std::size_t index) noexcept;
    void heap_up(std::size_t index) noexcept;
    void heap_down(std::size_t index) noexcept;
    void heap_swap(std::size_t a, std::size_t b) noexcept;

    io_context* context_;
    int epoll_fd_ = -1;
    int event_fd_ = -1;
    std::mutex mutex_;
    std::unordered_set<descriptor_state*> registered_;
    std::vector<timer_op*> timers_;
    bool shut_down_ = false;
};

} // namespace detail
} // namespace net
