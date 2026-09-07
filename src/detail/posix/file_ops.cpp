#include "detail/posix/file_ops.hpp"

#include <cerrno>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <unistd.h>

#include "net/error.hpp"

namespace net {
namespace detail {
namespace posix {

namespace {

std::error_code errno_code() noexcept { return std::error_code{errno, std::system_category()}; }

template <class Buffer> std::size_t fill_iovec(iovec (&vectors)[max_iovec], span<Buffer const> const buffers) noexcept {
    auto count = std::size_t{};
    for (auto const& b : buffers) {
        if (count == max_iovec) break;
        vectors[count].iov_base = const_cast<void*>(static_cast<void const*>(b.data()));
        vectors[count].iov_len = b.size();
        ++count;
    }
    return count;
}

} // namespace

int open_file(std::string const& path, file_base::flags const mode) noexcept {
    auto oflags = O_CLOEXEC;
    auto const access = mode & file_base::read_write;
    if (access == file_base::read_write)
        oflags |= O_RDWR;
    else if (access == file_base::write_only)
        oflags |= O_WRONLY;
    else
        oflags |= O_RDONLY;
    if (mode & file_base::append) oflags |= O_APPEND;
    if (mode & file_base::create) oflags |= O_CREAT;
    if (mode & file_base::exclusive) oflags |= O_EXCL;
    if (mode & file_base::truncate) oflags |= O_TRUNC;
    if (mode & file_base::sync_all_on_write) oflags |= O_SYNC;
    for (;;) {
        auto const fd = ::open(path.c_str(), oflags, 0666);
        if (fd >= 0 || errno != EINTR) return fd;
    }
}

file_transfer preadv_at(int const fd, std::uint64_t const offset, span<mutable_buffer const> const buffers) noexcept {
    iovec vectors[max_iovec];
    auto const count = fill_iovec(vectors, buffers);
    if (count == 0U) return file_transfer{{}, 0U};
    for (;;) {
        auto const n = ::preadv(fd, vectors, static_cast<int>(count), static_cast<off_t>(offset));
        if (n > 0) return file_transfer{{}, static_cast<std::size_t>(n)};
        if (n == 0) return file_transfer{make_error_code(error::eof), 0U};
        if (errno == EINTR) continue;
        return file_transfer{errno_code(), 0U};
    }
}

file_transfer pwritev_at(int const fd, std::uint64_t const offset, span<const_buffer const> const buffers) noexcept {
    iovec vectors[max_iovec];
    auto const count = fill_iovec(vectors, buffers);
    if (count == 0U) return file_transfer{{}, 0U};
    for (;;) {
        auto const n = ::pwritev(fd, vectors, static_cast<int>(count), static_cast<off_t>(offset));
        if (n >= 0) return file_transfer{{}, static_cast<std::size_t>(n)};
        if (errno == EINTR) continue;
        return file_transfer{errno_code(), 0U};
    }
}

std::uint64_t file_size(int const fd, std::error_code& ec) noexcept {
    struct stat st{};
    if (::fstat(fd, &st) != 0) {
        ec = errno_code();
        return 0U;
    }
    ec.clear();
    return static_cast<std::uint64_t>(st.st_size);
}

std::error_code file_resize(int const fd, std::uint64_t const size) noexcept {
    if (::ftruncate(fd, static_cast<off_t>(size)) != 0) return errno_code();
    return {};
}

std::error_code file_sync_data(int const fd) noexcept {
    if (::fdatasync(fd) != 0) return errno_code();
    return {};
}

std::error_code file_sync_all(int const fd) noexcept {
    if (::fsync(fd) != 0) return errno_code();
    return {};
}

} // namespace posix
} // namespace detail
} // namespace net
