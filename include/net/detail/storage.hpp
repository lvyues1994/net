#pragma once

#include "co2/detail/late_init.hpp"
#include "co2/detail/result_storage.hpp"

// net 复用 co2 的两个构件（同一作者维护的姊妹库）：
//   result_storage<T>：可空的结果格，T 为 void 时退化为完成标志；
//   late_init<T>：稍后就地构造一次、载荷可不可移动（stop_callback）。
// 集中在这里别名，其它头文件不直接拼写 co2::detail。

namespace net {
namespace detail {

template <class T> using result_storage = co2::detail::ResultStorage<T>;
template <class T> using late_init = co2::detail::LateInit<T>;

} // namespace detail
} // namespace net
