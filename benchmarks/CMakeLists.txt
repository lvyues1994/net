# 性能基准（-DNET_BUILD_BENCHMARKS=ON，建议 Release）。每个可执行文件打印一张表；
# `--quick` 把迭代数缩到 1/20 供 CI 用。

include(CheckCXXCompilerFlag)
find_package(Threads REQUIRED)
check_cxx_compiler_flag(-Wmismatched-new-delete NET_COMPILER_HAS_WMISMATCHED_NEW_DELETE)

set(net_benchmark_sources
    bench_core.cpp
    bench_net.cpp)
if(net_has_tls)
    list(APPEND net_benchmark_sources bench_tls.cpp)
endif()

foreach(source IN LISTS net_benchmark_sources)
    get_filename_component(name "${source}" NAME_WE)
    add_executable(${name} ${source} bench_alloc.cpp)
    target_link_libraries(${name} PRIVATE net::net)
    set_target_properties(
        ${name}
        PROPERTIES
            CXX_STANDARD 14
            CXX_STANDARD_REQUIRED YES
            CXX_EXTENSIONS NO)
    if(NOT MSVC)
        target_compile_options(${name} PRIVATE -Wall -Wextra -Wpedantic)
    endif()
    if(NET_COMPILER_HAS_WMISMATCHED_NEW_DELETE)
        target_compile_options(${name} PRIVATE -Wno-mismatched-new-delete)
    endif()
endforeach()

# 与 Boost.Asio 的对照（可选，需要 Boost ≥ 1.75 的头文件；awaitable 需要 C++20）。
option(NET_BENCH_ASIO "Build the Boost.Asio comparison benchmark" ON)
if(NET_BENCH_ASIO)
    find_package(Boost 1.75 QUIET)
    if(Boost_FOUND)
        add_executable(bench_asio bench_asio.cpp bench_alloc.cpp)
        target_include_directories(bench_asio PRIVATE ${Boost_INCLUDE_DIRS})
        target_link_libraries(bench_asio PRIVATE Threads::Threads)
        set_target_properties(bench_asio PROPERTIES CXX_STANDARD 20 CXX_STANDARD_REQUIRED YES CXX_EXTENSIONS NO)
        if(NOT MSVC)
            target_compile_options(bench_asio PRIVATE -Wall -Wextra)
        endif()
        if(NET_COMPILER_HAS_WMISMATCHED_NEW_DELETE)
            target_compile_options(bench_asio PRIVATE -Wno-mismatched-new-delete)
        endif()
        message(STATUS "net: benchmarks compare against Boost.Asio ${Boost_VERSION}")
    else()
        message(STATUS "net: Boost not found, skipping bench_asio")
    endif()
endif()

# 一键运行全部基准（quick 模式）。
set(net_benchmark_commands COMMAND bench_core --quick COMMAND bench_net --quick)
if(net_has_tls)
    list(APPEND net_benchmark_commands COMMAND bench_tls --quick)
endif()
add_custom_target(run_benchmarks
    ${net_benchmark_commands}
    DEPENDS bench_core bench_net $<$<BOOL:${net_has_tls}>:bench_tls>
    USES_TERMINAL)
