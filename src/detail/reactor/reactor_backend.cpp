#include "detail/reactor/reactor_backend.hpp"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdlib>

#include <sys/timerfd.h>
#include <unistd.h>

#include "co2/contract.hpp"

#include "net/error.hpp"
#include "net/io_context.hpp"

#include "detail/reactor/reactor_socket.hpp"
#include "detail/reactor/reactor_file.hpp"
#include "detail/heap_timer.hpp"

namespace net {
namespace detail {

reactor_backend::reactor_backend(execution_context& context, std::unique_ptr<demultiplexer> demux)
    : context_{static_cast<io_context*>(&context)}, demux_{std::move(demux)} {
    CO2_CONTRACT_CHECK(demux_ != nullptr);
    force_dispatch_ = std::getenv("NET_REACTOR_FORCE_DISPATCH") != nullptr;
    events_.reserve(128U);
    completed_.reserve(128U);
    expired_.reserve(32U);
    timer_fd_ = ::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
    if (timer_fd_ < 0) throw std::system_error{errno, std::system_category(), "timerfd_create"};
    timer_state_.fd = timer_fd_;
    timer_state_.registered = true;
    if (auto const ec = demux_->add(timer_state_, read_ready_bit)) {
        ::close(timer_fd_);
        throw std::system_error{ec, "register timerfd"};
    }
}

reactor_backend::~reactor_backend() {
    if (timer_fd_ >= 0) ::close(timer_fd_);
}

void reactor_backend::shutdown() {
    // 未完成的操作被放弃：不再调用 complete()（等待它们的协程不会恢复）。
    std::lock_guard<std::mutex> lock{mutex_};
    shut_down_ = true;
    for (auto* const state : registered_) {
        state->ops[0] = nullptr;
        state->ops[1] = nullptr;
        state->registered = false;
        demux_->remove(*state);
    }
    registered_.clear();
    timers_.clear(); // 关闭时堆里不该还有定时器（销毁契约）；清掉 heap_index 让迟到的 cancel 成为空操作
    demux_->remove(timer_state_);
}

// ---- 工厂 ----

std::unique_ptr<socket_impl> reactor_backend::create_socket(io_context& context) {
    return std::unique_ptr<socket_impl>{new reactor_socket{context, *this}};
}

std::unique_ptr<timer_impl> reactor_backend::create_timer(io_context& context) {
    return std::unique_ptr<timer_impl>{new heap_timer{context, *this}};
}

std::unique_ptr<file_impl> reactor_backend::create_file(io_context& context) {
    return std::unique_ptr<file_impl>{new reactor_file{context}};
}

// ---- 信号泵 ----

bool reactor_backend::signal_pump::perform(int const pipe_fd) noexcept {
    for (;;) {
        int value = 0;
        auto const n = ::read(pipe_fd, &value, sizeof(value));
        if (n == static_cast<ssize_t>(sizeof(value))) {
            deliver(value);
            continue;
        }
        if (n < 0 && errno == EINTR) continue;
        return false; // EAGAIN：保持排队等待下一次可读
    }
}

std::error_code reactor_backend::register_signal_reader(int const read_fd,
                                                        void (*const deliver)(int)) noexcept {
    if (signal_state_.registered) return {};
    signal_pump_.fd = read_fd;
    signal_pump_.deliver = deliver;
    signal_pump_.counts_as_work = false;
    auto const ec = register_descriptor(signal_state_, read_fd);
    if (ec) return ec;
    start_op(signal_state_, op_direction::read, signal_pump_);
    return {};
}

// ---- 描述符 ----

std::error_code reactor_backend::register_descriptor(descriptor_state& state, int const fd) noexcept {
    std::lock_guard<std::mutex> lock{mutex_};
    if (shut_down_) return make_error_code(error::operation_aborted);
    CO2_CONTRACT_CHECK(not state.registered);
    state.fd = fd;
    state.ops[0] = nullptr;
    state.ops[1] = nullptr;
    state.ready = 0U;
    state.interest = 0U;
    state.demux_index = descriptor_state::no_index;
    ++state.generation;
    // 边沿触发：一次登记全部兴趣；电平触发：从无兴趣开始，排队时再加。
    auto const initial = demux_->edge_triggered() ? (read_ready_bit | write_ready_bit) : 0U;
    auto const ec = demux_->add(state, initial);
    if (ec) return ec;
    state.interest = initial;
    state.registered = true;
    registered_.insert(&state);
    return {};
}

void reactor_backend::detach_ops(descriptor_state& state, reactor_op* (&cancelled)[2]) noexcept {
    // 锁内。已派发的操作留给执行它的线程：只记取消标记，由它以 operation_aborted 收尾（它看到标记、或看到
    // 描述符已注销时）。
    for (auto direction = 0; direction != 2; ++direction) {
        auto* const op = state.ops[direction];
        cancelled[direction] = nullptr;
        if (op == nullptr) continue;
        if (op->state == reactor_op::state_type::dispatched) {
            op->cancel_requested = true;
            continue;
        }
        cancelled[direction] = op;
        state.ops[direction] = nullptr;
        op->state = reactor_op::state_type::idle;
        op->ec = make_error_code(error::operation_aborted);
    }
}

void reactor_backend::finish_cancelled(reactor_op* const (&cancelled)[2]) noexcept {
    // 锁外。
    auto const executor = context_->get_executor();
    for (auto* const op : cancelled) {
        if (op == nullptr) continue;
        auto const counts = op->counts_as_work; // complete() 之后不再触碰 op
        op->complete();
        if (counts) executor.on_work_finished();
    }
}

void reactor_backend::deregister_descriptor(descriptor_state& state) noexcept {
    reactor_op* cancelled[2] = {nullptr, nullptr};
    {
        std::unique_lock<std::mutex> lock{mutex_};
        if (not state.registered) return;
        demux_->remove(state);
        registered_.erase(&state);
        state.registered = false;
        ++state.generation;
        detach_ops(state, cancelled);
        state.ready = 0U;
        state.interest = 0U;
        // 派发执行的线程可能正在这个 fd 上做系统调用（非阻塞，微秒级）：等它做完再让调用方关闭 / 交出 fd，
        // 否则 fd 号被复用后它会读写到别的描述符上。在系统调用里的线程不需要这把锁以外的任何东西，不会死锁；
        // 单线程时这里恒为零（系统调用只在别的线程上进行）。
        syscall_done_.wait(lock, [&state] { return state.in_syscall == 0U; });
    }
    finish_cancelled(cancelled);
}

void reactor_backend::refresh_interest(descriptor_state& state) noexcept {
    // 锁内。边沿触发的后端不需要。
    if (demux_->edge_triggered()) return;
    auto const wanted = state.wanted();
    if (wanted == state.interest) return;
    state.interest = wanted;
    demux_->update(state, wanted);
}

bool reactor_backend::start_op(descriptor_state& state, op_direction const direction,
                               reactor_op& op) noexcept {
    auto const index = static_cast<unsigned>(direction);
    auto const bit = direction == op_direction::read ? read_ready_bit : write_ready_bit;
    {
        std::lock_guard<std::mutex> lock{mutex_};
        CO2_CONTRACT_CHECK(state.registered);
        CO2_CONTRACT_CHECK(state.ops[index] == nullptr);
        op.backend = this;
        op.descriptor = &state;
        op.direction_index = index;
        op.generation = state.generation;
        if (op.cancel_requested) {
            // 停止请求先到了：不排队，同步以 aborted 完成。
            op.cancel_requested = false;
            op.ec = make_error_code(error::operation_aborted);
            op.bytes_transferred = 0U;
            return true;
        }
        if (state.ready & bit) {
            // 上一次就绪已经到达且尚未被消费：先试一次。
            state.ready &= ~bit;
            if (op.perform(state.fd)) return true;
        }
        // 工作计数在发布之前、锁内加：发布之后别的线程可能立刻完成它并 on_work_finished——
        // 计数先减后加会把 outstanding_work 打到 0（run() 提前返回 / 契约违规），而且那时 op
        // 可能已随套接字销毁。
        if (op.counts_as_work) context_->get_executor().on_work_started();
        op.state = reactor_op::state_type::queued;
        state.ops[index] = &op;
        refresh_interest(state);
    }
    return false;
}

void reactor_backend::clear_ready(descriptor_state& state, unsigned const bits) noexcept {
    std::lock_guard<std::mutex> lock{mutex_};
    state.ready &= ~bits;
}

bool reactor_backend::cancel_op(descriptor_state& state, op_direction const direction,
                                reactor_op& op) noexcept {
    auto const index = static_cast<unsigned>(direction);
    {
        std::lock_guard<std::mutex> lock{mutex_};
        if (state.ops[index] != &op || op.state == reactor_op::state_type::dispatched) {
            // 尚未 start_op（或已完成、协程尚未恢复——finish 会清掉这个过期标记），或已派发给别的线程执行
            //（它看到标记就以 operation_aborted 收尾；系统调用已经成功则照常完成）。
            op.cancel_requested = true;
            return false;
        }
        state.ops[index] = nullptr;
        op.state = reactor_op::state_type::idle;
        op.ec = make_error_code(error::operation_aborted);
        refresh_interest(state);
    }
    auto const counts = op.counts_as_work; // complete() 之后不再触碰 op
    op.complete();
    if (counts) context_->get_executor().on_work_finished();
    return true;
}

void reactor_backend::cancel_ops(descriptor_state& state) noexcept {
    reactor_op* cancelled[2] = {nullptr, nullptr};
    {
        std::lock_guard<std::mutex> lock{mutex_};
        detach_ops(state, cancelled);
        if (state.registered) refresh_interest(state);
    }
    finish_cancelled(cancelled);
}

// ---- 事件 ----

void reactor_backend::on_ready(descriptor_state& state, unsigned const ready_bits) noexcept {
    events_.push_back(pending_event{&state, ready_bits});
}

void reactor_backend::process_event(descriptor_state& state, unsigned const ready, bool const distribute,
                                    std::vector<completed_op>& completed) noexcept {
    // 锁内。就绪的方向：有排队操作就执行（或派发给别的线程执行），完成则摘下交给完成列表；没有操作、或操作
    // 已派发还没执行完，就记下就绪位（执行它的线程 EAGAIN 后看到会再试）。
    unsigned const bits[2] = {read_ready_bit, write_ready_bit};
    auto changed = false;
    for (auto direction = 0U; direction != 2U; ++direction) {
        if ((ready & bits[direction]) == 0U) continue;
        auto* const op = state.ops[direction];
        if (op == nullptr || op->state == reactor_op::state_type::dispatched) {
            state.ready |= bits[direction];
            continue;
        }
        if (distribute && op->counts_as_work) {
            op->state = reactor_op::state_type::dispatched;
            op->dispatch_frame.set(&on_dispatched, op);
            op->dispatch_cont.h = op->dispatch_frame.handle();
            dispatched_.push_back(op);
            changed = true;
            continue;
        }
        if (not op->perform(state.fd)) continue; // 仍是 EAGAIN：保持排队
        state.ops[direction] = nullptr;
        op->state = reactor_op::state_type::idle;
        completed.push_back(completed_op{op, op->counts_as_work});
        changed = true;
    }
    if (changed) refresh_interest(state);
}

// ---- 定时器 ----

bool reactor_backend::add_timer(timer_op& op) noexcept {
    auto became_earliest = false;
    {
        std::lock_guard<std::mutex> lock{mutex_};
        CO2_CONTRACT_CHECK(op.heap_index == timer_op::not_queued);
        if (op.cancel_requested) {
            op.cancel_requested = false;
            op.ec = make_error_code(error::operation_aborted);
            return true;
        }
        context_->get_executor().on_work_started(); // 发布之前（见 start_op）
        timers_.push(op);
        // 成为最早到期且有线程正阻塞在解复用器里：重新武装 timerfd，它会在新到期时刻叫醒那个
        // 线程（timerfd 在解复用器的集合里，跨线程 settime 立即生效）。没有线程在等时留给 run()
        // 进入等待前统一武装，省一次系统调用。
        became_earliest = waiting_ && timers_.front() == &op;
        if (became_earliest) arm_timer_fd_locked();
    }
    return false;
}

bool reactor_backend::cancel_timer(timer_op& op, bool const from_stop_token) noexcept {
    {
        std::lock_guard<std::mutex> lock{mutex_};
        if (op.heap_index == timer_op::not_queued) {
            // 只有 stop_token 路径会在"装好回调、尚未 add_timer"的窗口里到达；用户的 cancel()
            // 对没在等的定时器不能留下标记，否则下一次 wait() 会被误中止。
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

long long reactor_backend::wait_timeout_ns(long const limit_ms) const noexcept {
    // 锁内。定时器到期由 timerfd 负责；这里只有 io_context 给的上限（run_for 的截止），以及
    //"已有定时器到期"时的 0（不必进内核等待）。
    if (not timers_.empty() && timers_.front()->expiry <= std::chrono::steady_clock::now()) return 0;
    return limit_ms < 0 ? -1LL : static_cast<long long>(limit_ms) * 1000000LL;
}

void reactor_backend::arm_timer_fd_locked() noexcept {
    // 锁内。timerfd 只需保证"不晚于堆顶到期"叫醒：已经武装在一个不晚于堆顶的时刻就不动它——过早醒来一次
    // 是空转，重新武装是一次系统调用。每次读都套超时的服务器里堆顶每趟往返都往后挪，原先每趟一次
    // timerfd_settime，现在稳态下每个超时周期一次。堆空时也不解除武装：留着的那次到期最多带来一次空唤醒
    //（到期时 run() 读掉它、armed_ 置假），下一次有定时器再按堆顶武装。
    if (timers_.empty()) return;
    auto const expiry = timers_.front()->expiry;
    if (armed_ && armed_expiry_ <= expiry) return;
    // steady_clock 在 Linux/libstdc++ 上就是 CLOCK_MONOTONIC：直接用绝对时间。
    auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(expiry.time_since_epoch()).count();
    if (ns <= 0) ns = 1; // it_value 全零表示解除武装；过去的时刻立刻到期
    itimerspec spec{};
    spec.it_value.tv_sec = static_cast<time_t>(ns / 1000000000LL);
    spec.it_value.tv_nsec = static_cast<long>(ns % 1000000000LL);
    ::timerfd_settime(timer_fd_, TFD_TIMER_ABSTIME, &spec, nullptr);
    armed_ = true;
    armed_expiry_ = expiry;
}

void reactor_backend::interrupt() noexcept { demux_->interrupt(); }

void reactor_backend::run(long const timeout_ms) {
    auto timeout = 0LL;
    {
        std::lock_guard<std::mutex> lock{mutex_};
        arm_timer_fd_locked();
        timeout = wait_timeout_ns(timeout_ms);
        waiting_ = timeout != 0; // 与武装在同一把锁内：之后加入的更早定时器会重新武装 timerfd
    }
    events_.clear();
    auto const wait_error = demux_->wait(timeout, *this);
    {
        std::lock_guard<std::mutex> lock{mutex_};
        waiting_ = false;
    }
    if (wait_error) throw std::system_error{wait_error, demux_->name()};

    completed_.clear();
    expired_.clear();
    dispatched_.clear();
    // 有空闲线程在等活时才派发：让它们并行做这批系统调用。单线程（或别的线程都在忙）时就地执行更省——
    // 派发要多拿两次锁、多走一次队列。
    auto const distribute = force_dispatch_ || (events_.size() > 1U && io_context_access::has_idle_threads(*context_));
    {
        std::lock_guard<std::mutex> lock{mutex_};
        for (auto const& event : events_) {
            if (event.state == &timer_state_) {
                std::uint64_t expirations = 0;
                static_cast<void>(::read(timer_fd_, &expirations, sizeof(expirations)));
                armed_ = false; // 已触发：下一轮按堆顶重新武装
                continue;
            }
            if (registered_.count(event.state) == 0U) continue; // 已注销：迟到的事件
            process_event(*event.state, event.bits, distribute, completed_);
        }
        timers_.pop_expired(std::chrono::steady_clock::now(), expired_);
    }
    events_.clear();

    auto const executor = context_->get_executor();
    for (auto* const op : dispatched_) executor.post(op->dispatch_cont); // 本线程在 backend.run() 里：进私有队列
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
    dispatched_.clear();
}

// ---- 派发执行 ----

coroutine_handle<> reactor_backend::on_dispatched(void* const user) noexcept {
    auto& op = *static_cast<reactor_op*>(user);
    return op.backend->run_dispatched(op);
}

void reactor_backend::abandon_dispatched_locked(reactor_op& op) noexcept {
    auto& state = *op.descriptor;
    if (state.ops[op.direction_index] == &op) state.ops[op.direction_index] = nullptr;
    op.state = reactor_op::state_type::idle;
    op.ec = make_error_code(error::operation_aborted);
    op.bytes_transferred = 0U;
    if (state.registered && state.generation == op.generation) refresh_interest(state);
}

coroutine_handle<> reactor_backend::run_dispatched(reactor_op& op) noexcept {
    // op 登记在 descriptor_state::ops 里、状态 dispatched、持有一份工作计数。descriptor 与 op 都活着：
    // 操作未完成，拥有它们的套接字按契约不能销毁。
    auto& state = *op.descriptor;
    auto const index = op.direction_index;
    auto const bit = index == 0U ? read_ready_bit : write_ready_bit;
    for (;;) {
        auto fd = -1;
        {
            std::lock_guard<std::mutex> lock{mutex_};
            CO2_CONTRACT_CHECK(op.state == reactor_op::state_type::dispatched);
            if (op.cancel_requested || not state.registered || state.generation != op.generation) {
                abandon_dispatched_locked(op); // 排队期间被取消、关闭或关闭后重开
                break;
            }
            state.ready &= ~bit; // 这次执行会看到到目前为止的全部就绪
            fd = state.fd;
            ++state.in_syscall;
        }
        auto const done = op.perform(fd); // 锁外：多个线程并行做各自描述符上的系统调用
        {
            std::lock_guard<std::mutex> lock{mutex_};
            if (--state.in_syscall == 0U) syscall_done_.notify_all();
            auto const stale = not state.registered || state.generation != op.generation;
            if (done) {
                // 系统调用已经成功：照实完成，close() / cancel() 算作在它之后到达。改报 aborted 会丢掉读到的
                // 字节，accept 出来的描述符也没人关闭。注销在等 in_syscall 归零，ops 里仍是这个操作。
                state.ops[index] = nullptr;
                op.state = reactor_op::state_type::idle;
                if (not stale) refresh_interest(state);
                break;
            }
            if (stale || op.cancel_requested) {
                abandon_dispatched_locked(op);
                break;
            }
            // EAGAIN（伪就绪）：执行期间又来了就绪就再试一次，否则回到排队等下一次就绪，工作计数照旧持有。
            if ((state.ready & bit) == 0U) {
                op.state = reactor_op::state_type::queued;
                refresh_interest(state);
                return coroutine_handle<>{};
            }
        }
    }
    // 完成或被放弃：经操作的执行器恢复协程（同一上下文里就对称转移过去）。complete_here 之后不再碰 op。
    auto const counts = op.counts_as_work;
    auto const executor = context_->get_executor();
    auto const next = op.complete_here();
    if (counts) executor.on_work_finished();
    return next;
}

} // namespace detail
} // namespace net
