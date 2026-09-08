#include "net/file.hpp"

#include "co2/contract.hpp"

#include "net/error.hpp"
#include "net/io_context.hpp"

#include "detail/backend.hpp"
#include "detail/file_ops.hpp"

namespace net {

// ---- awaiter ----

bool file_read_awaitable::await_ready() noexcept { return impl->ready(detail::op_direction::read); }

coroutine_handle<> file_read_awaitable::await_suspend(coroutine_handle<> const h, io_env const* const env) noexcept {
    return impl->suspend(detail::op_direction::read, h, env);
}

io_result<std::size_t> file_read_awaitable::await_resume() noexcept {
    auto const r = impl->finish_transfer(detail::op_direction::read);
    if (advance != nullptr && not r.ec) *advance += r.value;
    return r;
}

bool file_write_awaitable::await_ready() noexcept { return impl->ready(detail::op_direction::write); }

coroutine_handle<> file_write_awaitable::await_suspend(coroutine_handle<> const h, io_env const* const env) noexcept {
    return impl->suspend(detail::op_direction::write, h, env);
}

io_result<std::size_t> file_write_awaitable::await_resume() noexcept {
    auto const r = impl->finish_transfer(detail::op_direction::write);
    if (advance != nullptr && not r.ec) *advance += r.value;
    return r;
}

// ---- basic_file ----

void basic_file::impl_deleter::operator()(detail::file_impl* const impl) const noexcept { delete impl; }

basic_file::basic_file() noexcept = default;

basic_file::basic_file(io_context& context) : impl_{detail::io_context_access::backend(context).create_file(context).release()} {}

basic_file::basic_file(basic_file&& other) noexcept = default;
basic_file& basic_file::operator=(basic_file&& other) noexcept = default;

basic_file::~basic_file() {
    if (impl_) CO2_CONTRACT_CHECK(not impl_->has_pending()); // 带着未完成的操作销毁是契约违规
}

io_context& basic_file::context() const noexcept { return impl_->context(); }

std::error_code basic_file::open(std::string const& path, flags const mode) noexcept {
    if (not impl_) return make_error_code(error::not_open);
    if (is_open()) {
        auto const ec = close();
        if (ec) return ec;
    }
    std::error_code open_ec;
    auto const fd = detail::fileops::open_file(path, mode, open_ec);
    if (open_ec) return open_ec;
    auto const ec = impl_->assign(fd);
    if (ec) detail::fileops::close_file(fd);
    return ec;
}

std::error_code basic_file::assign(native_handle_type const fd) noexcept {
    if (not impl_) return make_error_code(error::not_open);
    return impl_->assign(fd);
}

std::error_code basic_file::close() noexcept {
    if (not impl_) return {};
    return impl_->close();
}

basic_file::native_handle_type basic_file::release() noexcept { return impl_ ? impl_->release() : invalid_file_value(); }

bool basic_file::is_open() const noexcept { return impl_ && file_is_valid(impl_->native_handle()); }

basic_file::native_handle_type basic_file::native_handle() const noexcept {
    return impl_ ? impl_->native_handle() : invalid_file_value();
}

void basic_file::cancel() noexcept {
    if (impl_) impl_->cancel();
}

std::uint64_t basic_file::size(std::error_code& ec) const noexcept {
    if (not is_open()) {
        ec = make_error_code(error::not_open);
        return 0U;
    }
    return detail::fileops::file_size(native_handle(), ec);
}

std::error_code basic_file::resize(std::uint64_t const new_size) noexcept {
    if (not is_open()) return make_error_code(error::not_open);
    return detail::fileops::file_resize(native_handle(), new_size);
}

std::error_code basic_file::sync_data() noexcept {
    if (not is_open()) return make_error_code(error::not_open);
    return detail::fileops::file_sync_data(native_handle());
}

std::error_code basic_file::sync_all() noexcept {
    if (not is_open()) return make_error_code(error::not_open);
    return detail::fileops::file_sync_all(native_handle());
}

// ---- stream_file ----

stream_file::stream_file(io_context& context, std::string const& path, flags const mode) : basic_file{context} {
    auto const ec = open(path, mode);
    if (ec) throw std::system_error{ec, "stream_file::open"};
}

std::uint64_t stream_file::seek(std::int64_t const offset, seek_basis const origin, std::error_code& ec) noexcept {
    ec.clear();
    auto base = std::int64_t{};
    switch (origin) {
    case seek_basis::seek_set: base = 0; break;
    case seek_basis::seek_cur: base = static_cast<std::int64_t>(position_); break;
    case seek_basis::seek_end: {
        auto const end = size(ec);
        if (ec) return position_;
        base = static_cast<std::int64_t>(end);
        break;
    }
    }
    auto const target = base + offset;
    if (target < 0) {
        ec = std::make_error_code(std::errc::invalid_argument);
        return position_;
    }
    position_ = static_cast<std::uint64_t>(target);
    return position_;
}

file_read_awaitable stream_file::start_read(mutable_buffer_array<> const& buffers) noexcept {
    impl()->begin_read(position_, buffers.to_span());
    return file_read_awaitable{impl(), &position_};
}

file_write_awaitable stream_file::start_write(const_buffer_array<> const& buffers) noexcept {
    impl()->begin_write(position_, buffers.to_span());
    return file_write_awaitable{impl(), &position_};
}

// ---- random_access_file ----

random_access_file::random_access_file(io_context& context, std::string const& path, flags const mode) : basic_file{context} {
    auto const ec = open(path, mode);
    if (ec) throw std::system_error{ec, "random_access_file::open"};
}

file_read_awaitable random_access_file::start_read_at(std::uint64_t const offset, mutable_buffer_array<> const& buffers) noexcept {
    impl()->begin_read(offset, buffers.to_span());
    return file_read_awaitable{impl(), nullptr};
}

file_write_awaitable random_access_file::start_write_at(std::uint64_t const offset, const_buffer_array<> const& buffers) noexcept {
    impl()->begin_write(offset, buffers.to_span());
    return file_write_awaitable{impl(), nullptr};
}

} // namespace net
