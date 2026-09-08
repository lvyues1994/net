#include "net/receive_source.hpp"

#include <vector>

#include "co2/contract.hpp"

#include "net/error.hpp"
#include "net/io_context.hpp"

#include "detail/backend.hpp"

namespace net {

namespace detail {

// 回退实现：自己的缓冲池 + 套接字读方向的三步协议。每次 pull 最多发起一个 read_some 到下一块空闲
// 缓冲；块队列按到达顺序交出。
struct fallback_receive_stream final : receive_stream_impl {
    fallback_receive_stream(socket_impl& impl, std::size_t const count, std::size_t const size)
        : impl_{&impl}, size_{size}, storage_(count * size) {
        free_.reserve(count);
        for (auto i = count; i-- > 0;) free_.push_back(i);
    }

    bool pull_ready() noexcept override {
        if (head_ != chunks_.size() || terminal_ || transient_) return true;
        if (free_.empty()) { // 调用方还没归还任何块
            transient_ec_ = std::make_error_code(std::errc::no_buffer_space);
            transient_ = true;
            return true;
        }
        current_ = free_.back();
        free_.pop_back();
        mutable_buffer const target{storage_.data() + current_ * size_, size_};
        impl_->begin_read(span<mutable_buffer const>{&target, 1U});
        reading_ = true;
        if (impl_->ready(op_direction::read)) {
            finish_read();
            return true;
        }
        return false;
    }

    coroutine_handle<> pull_suspend(coroutine_handle<> const h, io_env const* const env) noexcept override {
        return impl_->suspend(op_direction::read, h, env);
    }

    io_result<const_buffer_span> pull_finish(const_buffer_span const dest) noexcept override {
        if (reading_) finish_read();
        if (transient_) {
            transient_ = false;
            return io_result<const_buffer_span>{transient_ec_, const_buffer_span{}};
        }
        if (head_ == chunks_.size()) return io_result<const_buffer_span>{terminal_ec_, const_buffer_span{}};
        auto count = std::size_t{};
        for (auto i = head_; i != chunks_.size() && count != dest.size(); ++i, ++count) {
            auto const& c = chunks_[i];
            auto const skip = i == head_ ? first_offset_ : 0U;
            dest[count] = const_buffer{storage_.data() + c.index * size_ + skip, c.length - skip};
        }
        return io_result<const_buffer_span>{std::error_code{}, const_buffer_span{dest.data(), count}};
    }

    void consume(std::size_t n) noexcept override {
        while (n != 0U && head_ != chunks_.size()) {
            auto& c = chunks_[head_];
            auto const remaining = c.length - first_offset_;
            if (n < remaining) {
                first_offset_ += n;
                return;
            }
            n -= remaining;
            free_.push_back(c.index);
            first_offset_ = 0U;
            ++head_;
        }
        if (head_ == chunks_.size()) {
            chunks_.clear();
            head_ = 0U;
        }
    }

    void cancel() noexcept override { impl_->cancel(); }
    bool has_pending() const noexcept override { return reading_; }

  private:
    struct chunk {
        std::size_t index;
        std::size_t length;
    };

    void finish_read() noexcept {
        auto const r = impl_->finish_transfer(op_direction::read);
        reading_ = false;
        if (r.ec) {
            free_.push_back(current_);
            if (r.ec == error::operation_aborted) { // 取消：报一次，之后还能继续 pull
                transient_ec_ = r.ec;
                transient_ = true;
                return;
            }
            terminal_ = true; // eof 与其它错误：之后每次 pull 都报同样的结果
            terminal_ec_ = r.ec;
            return;
        }
        chunks_.push_back(chunk{current_, r.value});
    }

    socket_impl* impl_;
    std::size_t size_;
    std::vector<unsigned char> storage_;
    std::vector<std::size_t> free_;
    std::vector<chunk> chunks_;
    std::size_t head_ = 0;
    std::size_t first_offset_ = 0;
    std::size_t current_ = 0;
    bool reading_ = false;
    bool terminal_ = false;
    bool transient_ = false;
    std::error_code terminal_ec_;
    std::error_code transient_ec_;
};

} // namespace detail

receive_source::receive_source(socket_base& socket, std::size_t const buffer_count, std::size_t const buffer_size) {
    CO2_CONTRACT_CHECK(buffer_count != 0U && buffer_size != 0U);
    auto* const impl = detail::socket_access::impl(socket);
    CO2_CONTRACT_CHECK(impl != nullptr);
    impl_ = impl->create_receive_stream(buffer_count, buffer_size);
    kernel_owned_ = impl_ != nullptr;
    if (not impl_) impl_.reset(new detail::fallback_receive_stream{*impl, buffer_count, buffer_size});
}

receive_source::~receive_source() { CO2_CONTRACT_CHECK(not impl_->has_pending()); }

void receive_source::consume(std::size_t const n) noexcept { impl_->consume(n); }

void receive_source::cancel() noexcept { impl_->cancel(); }

bool receive_pull_awaitable::await_ready() noexcept { return self->impl_->pull_ready(); }

coroutine_handle<> receive_pull_awaitable::await_suspend(coroutine_handle<> const h, io_env const* const env) noexcept {
    return self->impl_->pull_suspend(h, env);
}

io_result<const_buffer_span> receive_pull_awaitable::await_resume() noexcept { return self->impl_->pull_finish(dest); }

} // namespace net
