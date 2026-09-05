#pragma once

#include <memory>
#include <system_error>

#include "detail/reactor/reactor_op.hpp"

// 就绪型后端的策略点：事件解复用器。reactor_backend 拥有描述符表、就绪位、操作排队与
// 定时器堆（所有后端相同），只把"如何等待一批描述符就绪"委托给这里。epoll / poll /
// select 各是一个实现；kqueue 会是第四个。这对应 Corosio 的 epoll_scheduler /
// select_scheduler / kqueue_scheduler 在 reactor_* 模板之下所扮演的角色。
//
// 线程模型：add / update / remove 在反应器锁内被调用（可能来自不同线程）；wait 只由
// 当前运行事件循环的那一个线程在锁外调用；interrupt 任意线程可调。电平触发的实现
// 必须自己保护注册表（wait 在锁外读它），并在兴趣变化时唤醒可能阻塞中的 wait。

namespace net {
namespace detail {

struct event_sink {
    virtual ~event_sink() = default;
    // 在 wait 内（锁外）被调用；实现只应记录 (state, bits)，处理留给持锁阶段。
    virtual void on_ready(descriptor_state& state, unsigned ready_bits) noexcept = 0;
};

struct demultiplexer {
    demultiplexer() = default;
    demultiplexer(demultiplexer const&) = delete;
    demultiplexer& operator=(demultiplexer const&) = delete;
    virtual ~demultiplexer() = default;

    virtual char const* name() const noexcept = 0;

    // 边沿触发：注册一次即可，反应器维护就绪位、不再调整兴趣。电平触发：反应器在每次
    // 排队/摘除后以 update 调整兴趣（否则一个可读而无人读的描述符会让 wait 忙转）。
    virtual bool edge_triggered() const noexcept = 0;

    virtual std::error_code add(descriptor_state& state, unsigned interest) noexcept = 0;
    virtual void update(descriptor_state& state, unsigned interest) noexcept = 0;
    virtual void remove(descriptor_state& state) noexcept = 0;

    // 阻塞至多 timeout_ms（< 0 无限）等待事件，就绪的描述符逐个交给 sink。
    virtual std::error_code wait(long timeout_ms, event_sink& sink) noexcept = 0;
    virtual void interrupt() noexcept = 0;
};

std::unique_ptr<demultiplexer> make_epoll_demultiplexer();
std::unique_ptr<demultiplexer> make_poll_demultiplexer();
std::unique_ptr<demultiplexer> make_select_demultiplexer();

} // namespace detail
} // namespace net
