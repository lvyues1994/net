cmake_minimum_required(VERSION 3.14)

project(net VERSION 0.1.0 LANGUAGES CXX)

include(GNUInstallDirs)
find_package(Threads REQUIRED)

# ---- 协程核心：co2（C++14 无栈协程） ----
#
# 优先使用已安装的 co2 包；找不到时把 NET_CO2_DIR 指向的源码树作为子项目加入。默认
# 路径指向与本仓库并列的 ../../coro/coro。
set(NET_CO2_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../../coro/coro"
    CACHE PATH "co2 源码目录（find_package 失败时用 add_subdirectory 加入）")

if(NOT TARGET co2::co2)
    find_package(co2 CONFIG QUIET)
    if(NOT TARGET co2::co2)
        if(EXISTS "${NET_CO2_DIR}/CMakeLists.txt")
            set(CO2_BUILD_TESTS OFF CACHE BOOL "" FORCE)
            set(CO2_BUILD_BENCHMARKS OFF CACHE BOOL "" FORCE)
            add_subdirectory("${NET_CO2_DIR}" "${CMAKE_CURRENT_BINARY_DIR}/co2" EXCLUDE_FROM_ALL)
        else()
            message(FATAL_ERROR
                "co2 not found: install it or set NET_CO2_DIR to the co2 source tree")
        endif()
    endif()
endif()

# ---- 库 ----
#
# 抽象层（IoAwaitable 协议、task、执行器、缓冲区、流、组合子）仅含头文件；平台层
# （io_context 的 epoll 反应器、套接字、定时器、DNS、信号）编译进 libnet。
add_library(net STATIC
    src/error.cpp
    src/execution_context.cpp
    src/memory_resource.cpp
    src/io_context.cpp
    src/detail/epoll_reactor.cpp
    src/thread_pool.cpp
    src/ip.cpp
    src/socket_base.cpp
    src/tcp.cpp
    src/udp.cpp
    src/timer.cpp
    src/resolver.cpp
    src/signal_set.cpp)
add_library(net::net ALIAS net)

target_compile_features(net PUBLIC cxx_std_14)
set_target_properties(net PROPERTIES CXX_EXTENSIONS OFF)
target_link_libraries(net PUBLIC co2::co2 Threads::Threads)
target_include_directories(
    net
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src)

if(NOT MSVC)
    target_compile_options(
        net
        PRIVATE
            -Wall -Wextra -Wpedantic
            -Wconversion -Wsign-conversion
            -Wnon-virtual-dtor -Wold-style-cast -Wzero-as-null-pointer-constant
            -Werror=return-type -Werror=uninitialized -Werror=suggest-override)
endif()

set(NET_BUILD_TESTS_DEFAULT OFF)
set(NET_BUILD_EXAMPLES_DEFAULT OFF)
if(CMAKE_SOURCE_DIR STREQUAL PROJECT_SOURCE_DIR)
    set(NET_BUILD_TESTS_DEFAULT ON)
    set(NET_BUILD_EXAMPLES_DEFAULT ON)
endif()
option(NET_BUILD_TESTS "Build net tests" ${NET_BUILD_TESTS_DEFAULT})
option(NET_BUILD_EXAMPLES "Build net examples" ${NET_BUILD_EXAMPLES_DEFAULT})

if(NET_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

if(NET_BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()

install(TARGETS net EXPORT netTargets
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(EXPORT netTargets FILE netTargets.cmake NAMESPACE net::
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/net)
