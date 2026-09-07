#include "detail/reactor/reactor_file.hpp"

#include <cerrno>

#include <unistd.h>

#include "co2/contract.hpp"

#include "net/error.hpp"

#include "detail/posix/file_ops.hpp"

namespace net {
namespace detail {

reactor_file::~reactor_file() { close(); }

std::error_code reactor_file::assign(int const fd) noexcept {
    if (fd_ >= 0) return make_error_code(error::already_open);
    fd_ = fd;
    return {};
}

std::error_code reactor_file::close() noexcept {
    if (fd_ < 0) return {};
    auto const fd = fd_;
    fd_ = -1;
    if (::close(fd) != 0) return std::error_code{errno, std::system_category()};
    return {};
}

int reactor_file::release() noexcept {
    auto const fd = fd_;
    fd_ = -1;
    return fd;
}

void reactor_file::begin_read(std::uint64_t const offset, span<mutable_buffer const> const buffers) noexcept {
    read_.offset = offset;
    read_.read_buffers = mutable_buffer_array<>{buffers};
}

void reactor_file::begin_write(std::uint64_t const offset, span<const_buffer const> const buffers) noexcept {
    write_.offset = offset;
    write_.write_buffers = const_buffer_array<>{buffers};
}

bool reactor_file::ready(op_direction const direction) noexcept {
    auto& o = op_for(direction);
    if (fd_ < 0) {
        o.ec = make_error_code(error::not_open);
        o.bytes = 0U;
        return true;
    }
    auto const r = direction == op_direction::read ? posix::preadv_at(fd_, o.offset, o.read_buffers.to_span())
                                                   : posix::pwritev_at(fd_, o.offset, o.write_buffers.to_span());
    o.ec = r.ec;
    o.bytes = r.bytes;
    return true; // 同步完成
}

coroutine_handle<> reactor_file::suspend(op_direction, coroutine_handle<> const h, io_env const*) noexcept {
    CO2_CONTRACT_CHECK(false && "reactor_file completes synchronously in ready()");
    return h;
}

io_result<std::size_t> reactor_file::finish_transfer(op_direction const direction) noexcept {
    auto& o = op_for(direction);
    return io_result<std::size_t>{o.ec, o.bytes};
}

} // namespace detail
} // namespace net
