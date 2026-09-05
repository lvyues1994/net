// 契约违规：带着未完成的操作销毁 I/O 对象。等待它的协程永远不能恢复——这是未定义行为，
// net 通过契约处理器报告。程序在违规点打印 "contract violation" 并以 86 退出。

#include <cstdio>
#include <cstdlib>
#include <memory>

#include "co2/contract.hpp"

#include "net/io_context.hpp"
#include "net/ip.hpp"
#include "net/run_async.hpp"
#include "net/task.hpp"
#include "net/udp.hpp"

namespace {

auto blocked_receive(net::udp_socket* sock) CO2_BEG(net::task<>, (sock), char buf[8]; net::io_result<std::size_t> r;) {
    CO2_AWAIT_SET(r, sock->receive(net::buffer(buf)));
}
CO2_END

} // namespace

int main() {
    co2::setContractViolationHandler([](co2::ContractViolation const& violation) {
        std::printf("contract violation: %s (%s:%u)\n", violation.condition, violation.file, violation.line);
        std::fflush(stdout);
        std::_Exit(86);
    });

    net::io_context ctx;
    std::unique_ptr<net::udp_socket> sock{
        new net::udp_socket{ctx, net::ip::udp::endpoint{net::ip::address_v4::loopback(), 0}}};
    net::run_async(ctx.get_executor())(blocked_receive(sock.get()));
    ctx.poll(); // 协程挂起在 receive 上
    sock.reset(); // 违规：操作仍未完成
    std::printf("unreachable\n");
    return 1;
}
