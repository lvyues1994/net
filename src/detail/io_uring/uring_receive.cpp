#include "detail/io_uring/uring_receive.hpp"

#include <cerrno>
#include <cstdlib>
#include <cstring>

#include <unistd.h>

#include "co2/contract.hpp"

#include "net/error.hpp"
#include "net/io_context.hpp"

#include "detail/io_uring/uring_backend.hpp"

#ifndef IORING_RECV_MULTISHOT
#define IORING_RECV_MULTISHOT (1U << 1)
#endif
#ifndef IORING_CQE_F_BUFFER
#define IORING_CQE_F_BUFFER (1U << 0)
#endif
#ifndef IORING_CQE_BUFFER_SHIFT
#define IORING_CQE_BUFFER_SHIFT 16
#endif

namespace net {
namespace detail {

namespace {

constexpr std::size_t buf_entry_size = 16U; // struct io_uring_buf：addr u64, len u32, bid u16, resv u16
constexpr std::size_t ring_tail_offset = 14U; // 叠在 bufs[0].resv 上

unsigned round_up_pow2(unsigned v) noexcept {
    auto p = 1U;
    while (p < v) p <<= 1U;
    return p;
}

std::size_t page_size() noexcept {
    static std::size_t const size = static_cast<std::size_t>(::sysconf(_SC_PAGESIZE));
    return size;
}

} // namespace

// ---- 操作 ----

uring_multishot_recv_op::uring_multishot_recv_op(uring_backend& backend, int const fd, int const file_slot, unsigned const entries,
                                                 std::size_t const buffer_size, unsigned short const group)
    : backend_{&backend}, fd_{fd}, file_slot_{file_slot}, entries_{entries}, mask_{entries - 1U}, buffer_size_{buffer_size},
      group_{group} {
    persistent = true;
    counts_as_work = false; // 用户可见的工作按每次 pull 计
    auto const ring_bytes = (static_cast<std::size_t>(entries) * buf_entry_size + page_size() - 1U) / page_size() * page_size();
    void* ring = nullptr;
    if (::posix_memalign(&ring, page_size(), ring_bytes) != 0) return;
    std::memset(ring, 0, ring_bytes);
    void* storage = nullptr;
    if (::posix_memalign(&storage, page_size(), static_cast<std::size_t>(entries) * buffer_size) != 0) {
        std::free(ring);
        return;
    }
    if (backend_->ring().register_buffer_ring(ring, entries, group) != 0) {
        std::free(ring);
        std::free(storage);
        return;
    }
    ring_registered_ = true;
    ring_ = static_cast<unsigned char*>(ring);
    storage_ = static_cast<unsigned char*>(storage);
    for (auto bid = 0U; bid != entries; ++bid)
        give_back(static_cast<unsigned short>(bid));
    publish();
}

uring_multishot_recv_op::~uring_multishot_recv_op() {
    if (ring_registered_) backend_->ring().unregister_buffer_ring(group_);
    std::free(ring_);
    std::free(storage_);
}

void uring_multishot_recv_op::give_back(unsigned short const bid) noexcept {
    auto* const entry = ring_ + static_cast<std::size_t>((local_tail_ + pending_adds_) & mask_) * buf_entry_size;
    auto const addr = reinterpret_cast<std::uint64_t>(buffer_at(bid));
    auto const len = static_cast<std::uint32_t>(buffer_size_);
    std::memcpy(entry, &addr, sizeof(addr));
    std::memcpy(entry + 8U, &len, sizeof(len));
    std::memcpy(entry + 12U, &bid, sizeof(bid));
    ++pending_adds_;
}

void uring_multishot_recv_op::publish() noexcept {
    if (pending_adds_ == 0U) return;
    local_tail_ = static_cast<unsigned short>(local_tail_ + pending_adds_);
    pending_adds_ = 0U;
    __atomic_store_n(reinterpret_cast<unsigned short*>(ring_ + ring_tail_offset), local_tail_, __ATOMIC_RELEASE);
}

void uring_multishot_recv_op::prepare(io_uring_sqe& sqe) noexcept {
    sqe.opcode = IORING_OP_RECV;
    if (file_slot_ >= 0) {
        sqe.fd = file_slot_;
        sqe.flags |= IOSQE_FIXED_FILE;
    } else {
        sqe.fd = fd_;
    }
    sqe.flags |= IOSQE_BUFFER_SELECT;
    sqe.buf_group = group_;
    sqe.ioprio = IORING_RECV_MULTISHOT;
    sqe.addr = 0U;
    sqe.len = 0U;
    sqe.msg_flags = 0U;
}

// 环锁内：on_complete 之后（请求不再武装时）后端问是否重新提交。
bool uring_multishot_recv_op::rearm() noexcept {
    if (owner == nullptr) return false;
    std::lock_guard<std::mutex> lock{owner->mutex()};
    return owner->take_rearm_request();
}

// 环锁内。
void uring_multishot_recv_op::on_complete(int const res, unsigned const flags) noexcept {
    auto const more = (flags & IORING_CQE_F_MORE) != 0U;
    if (owner == nullptr) { // 已退役：数据丢弃；终止 CQE 之后后端删除本对象
        return;
    }
    std::lock_guard<std::mutex> lock{owner->mutex()};
    if (res > 0) {
        if ((flags & IORING_CQE_F_BUFFER) != 0U)
            owner->on_chunk(static_cast<unsigned short>(flags >> IORING_CQE_BUFFER_SHIFT), static_cast<std::size_t>(res));
        if (not more) owner->on_disarmed(false);
        return;
    }
    if (res == 0) {
        owner->on_terminal(make_error_code(error::eof), true);
        return;
    }
    if (res == -ENOBUFS) {
        owner->on_disarmed(true);
        return;
    }
    if (res == -ECANCELED) {
        owner->on_disarmed(false);
        owner->on_terminal(make_error_code(error::operation_aborted), false);
        return;
    }
    owner->on_terminal(std::error_code{-res, std::system_category()}, false);
}

// ---- 流 ----

void cancel_uring_receive::operator()() const noexcept {
    std::lock_guard<std::mutex> lock{stream->mutex()};
    stream->on_terminal(make_error_code(error::operation_aborted), false);
}

uring_receive_stream::uring_receive_stream(io_context& context, uring_backend& backend, int const fd, int const file_slot,
                                           std::size_t const buffer_count, std::size_t const buffer_size)
    : context_{&context}, backend_{&backend} {
    auto const group = backend.allocate_buffer_group();
    if (group < 0) return;
    auto const entries = round_up_pow2(static_cast<unsigned>(buffer_count));
    op_.reset(new uring_multishot_recv_op{backend, fd, file_slot, entries, buffer_size, static_cast<unsigned short>(group)});
    op_->owner = this;
    if (not op_->valid()) {
        op_.reset();
        backend.release_buffer_group(group);
    }
}

uring_receive_stream::~uring_receive_stream() {
    CO2_CONTRACT_CHECK(not waiting_);
    if (not op_) return;
    {
        std::lock_guard<std::mutex> lock{backend_->mutex()};
        op_->owner = nullptr;
    }
    backend_->retire(std::move(op_)); // 在飞的话取消；终止 CQE 后删除（注销环、释放存储）
}

void uring_receive_stream::arm() noexcept {
    // 流锁外调用（要拿环锁）。多次 submit 同一个在飞的操作是契约违规：armed_ 在锁内先置真。
    backend_->submit(*op_);
}

bool uring_receive_stream::pull_ready() noexcept {
    auto need_arm = false;
    {
        std::lock_guard<std::mutex> lock{mutex_};
        if (head_ != chunks_.size() || eof_ || error_ || aborted_once_) return true;
        if (not armed_ && not starved_) {
            armed_ = true;
            need_arm = true;
        }
    }
    if (need_arm) arm();
    return false;
}

coroutine_handle<> uring_receive_stream::pull_suspend(coroutine_handle<> const h, io_env const* const env) noexcept {
    if (env->stop_token.stop_requested()) {
        std::lock_guard<std::mutex> lock{mutex_};
        aborted_once_ = true;
        return h;
    }
    if (env->stop_token.stop_possible()) stop_cb_.emplace(env->stop_token, cancel_uring_receive{this});
    {
        std::lock_guard<std::mutex> lock{mutex_};
        if (head_ != chunks_.size() || eof_ || error_ || aborted_once_) { // ready 与 suspend 之间到了
            stop_cb_.reset();
            return h;
        }
        cont_.h = h;
        env_ = env;
        waiting_ = true;
        context_->get_executor().on_work_started();
    }
    return noop_coroutine(); // 之后不再碰 this 的等待者字段（完成可能在别的线程）
}

io_result<span<const_buffer>> uring_receive_stream::pull_finish(span<const_buffer> const dest) noexcept {
    stop_cb_.reset(); // 锁外：回调本身要拿流锁，析构会等它跑完
    std::lock_guard<std::mutex> lock{mutex_};
    if (aborted_once_) {
        aborted_once_ = false;
        return io_result<span<const_buffer>>{make_error_code(error::operation_aborted), span<const_buffer>{}};
    }
    if (head_ == chunks_.size()) {
        auto const ec = eof_ ? make_error_code(error::eof) : error_;
        return io_result<span<const_buffer>>{ec, span<const_buffer>{}};
    }
    auto count = std::size_t{};
    for (auto i = head_; i != chunks_.size() && count != dest.size(); ++i, ++count) {
        auto const& c = chunks_[i];
        auto const skip = i == head_ ? first_offset_ : 0U;
        dest[count] = const_buffer{op_->buffer_at(c.bid) + skip, c.length - skip};
    }
    return io_result<span<const_buffer>>{std::error_code{}, span<const_buffer>{dest.data(), count}};
}

void uring_receive_stream::consume(std::size_t n) noexcept {
    auto need_arm = false;
    {
        std::lock_guard<std::mutex> lock{mutex_};
        auto returned = 0U;
        while (n != 0U && head_ != chunks_.size()) {
            auto& c = chunks_[head_];
            auto const remaining = c.length - first_offset_;
            if (n < remaining) {
                first_offset_ += n;
                break;
            }
            n -= remaining;
            op_->give_back(c.bid);
            ++returned;
            first_offset_ = 0U;
            ++head_;
        }
        if (head_ == chunks_.size()) {
            chunks_.clear();
            head_ = 0U;
        }
        if (returned != 0U) {
            op_->publish();
            if (starved_ && not armed_ && not eof_ && not error_) {
                starved_ = false;
                armed_ = true;
                need_arm = true;
            }
        }
    }
    if (need_arm) arm();
}

void uring_receive_stream::cancel() noexcept {
    std::lock_guard<std::mutex> lock{mutex_};
    on_terminal(make_error_code(error::operation_aborted), false);
}

// ---- 环锁 + 流锁内 ----

void uring_receive_stream::on_chunk(unsigned short const bid, std::size_t const length) noexcept {
    chunks_.push_back(chunk{bid, length});
    wake_locked();
}

void uring_receive_stream::on_terminal(std::error_code const ec, bool const eof) noexcept {
    if (ec == error::operation_aborted) {
        if (waiting_) { // 取消只影响正在等的 pull；多发请求仍然可以在飞
            aborted_once_ = true;
            wake_locked();
        }
        return;
    }
    if (eof)
        eof_ = true;
    else if (not error_)
        error_ = ec;
    armed_ = false;
    wake_locked();
}

void uring_receive_stream::on_disarmed(bool const starved) noexcept {
    starved_ = starved;
    // 请求结束但没出错也没 eof：有人在等且缓冲还有就让后端立刻重新提交（rearm() 取走请求），否则等
    // 下一次 pull_ready / consume 再武装。armed_ 保持为真挡住并发的 pull_ready 重复提交。
    if (not starved && not eof_ && not error_ && waiting_) {
        rearm_requested_ = true;
        armed_ = true;
        return;
    }
    armed_ = false;
}

bool uring_receive_stream::take_rearm_request() noexcept {
    auto const requested = rearm_requested_;
    rearm_requested_ = false;
    if (not requested) armed_ = false;
    return requested;
}

void uring_receive_stream::wake_locked() noexcept {
    if (not waiting_) return;
    waiting_ = false;
    env_->executor.post(cont_);
    context_->get_executor().on_work_finished();
}

} // namespace detail
} // namespace net
