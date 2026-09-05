# 普通测试：可执行文件名与源文件名一一对应，成功即退出码 0。
set(net_test_sources
    task_tests.cpp
    executor_tests.cpp
    buffer_tests.cpp
    stream_tests.cpp
    combinator_tests.cpp
    timer_tests.cpp
    tcp_tests.cpp
    udp_tests.cpp
    resolver_tests.cpp
    signal_tests.cpp
    header_self_containment.cpp)

function(net_configure_test_target target)
    target_link_libraries(${target} PRIVATE net::net)
    set_target_properties(
        ${target}
        PROPERTIES
            CXX_STANDARD 14
            CXX_STANDARD_REQUIRED YES
            CXX_EXTENSIONS NO)
    if(NOT MSVC)
        target_compile_options(
            ${target}
            PRIVATE
                -Wall -Wextra -Wpedantic -Werror
                -Wconversion -Wsign-conversion
                -Wnon-virtual-dtor -Wold-style-cast -Wzero-as-null-pointer-constant
                -Werror=return-type -Werror=uninitialized -Werror=suggest-override)
    endif()
endfunction()

foreach(source IN LISTS net_test_sources)
    get_filename_component(name "${source}" NAME_WE)
    add_executable(${name} ${source})
    net_configure_test_target(${name})
    add_test(NAME ${name} COMMAND ${name})
    set_tests_properties(${name} PROPERTIES TIMEOUT 120)
endforeach()

# 契约违规测试：程序在预期的失败点经由契约处理器以退出码 86 结束。
set(net_contract_violation_test_sources
    socket_destroy_pending_contract_violation_test.cpp)

foreach(source IN LISTS net_contract_violation_test_sources)
    get_filename_component(name "${source}" NAME_WE)
    add_executable(${name} ${source})
    net_configure_test_target(${name})
    add_test(NAME ${name} COMMAND ${name})
    # 设置 PASS_REGULAR_EXPRESSION 后 ctest 忽略退出码，只看输出。
    set_tests_properties(${name} PROPERTIES PASS_REGULAR_EXPRESSION "contract violation" TIMEOUT 60)
endforeach()
