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

set(NET_CO2_GIT_REPOSITORY "https://github.com/lvyues1994/coro.git"
    CACHE STRING "本地找不到 co2 时用 FetchContent 拉取的仓库")
set(NET_CO2_GIT_TAG "main" CACHE STRING "拉取 co2 时的分支 / 标签")

if(NOT TARGET co2::co2)
    find_package(co2 CONFIG QUIET)
    if(NOT TARGET co2::co2)
        set(CO2_BUILD_TESTS OFF CACHE BOOL "" FORCE)
        set(CO2_BUILD_BENCHMARKS OFF CACHE BOOL "" FORCE)
        if(EXISTS "${NET_CO2_DIR}/CMakeLists.txt")
            add_subdirectory("${NET_CO2_DIR}" "${CMAKE_CURRENT_BINARY_DIR}/co2" EXCLUDE_FROM_ALL)
        else()
            message(STATUS "net: co2 not found at ${NET_CO2_DIR}, fetching ${NET_CO2_GIT_REPOSITORY}@${NET_CO2_GIT_TAG}")
            include(FetchContent)
            FetchContent_Declare(co2
                GIT_REPOSITORY ${NET_CO2_GIT_REPOSITORY}
                GIT_TAG ${NET_CO2_GIT_TAG}
                GIT_SHALLOW TRUE)
            FetchContent_MakeAvailable(co2)
        endif()
    endif()
endif()

# ---- 库 ----
#
# 抽象层（IoAwaitable 协议、task、执行器、缓冲区、流、组合子）仅含头文件；平台层编译进
# libnet，分三部分：
#   - 具体层：io_context / 套接字 / 定时器 / DNS / 信号，只依赖 src/detail/backend.hpp 的抽象接口；
#   - 就绪型后端族（src/detail/reactor）：共享的反应器 + epoll / poll / select 解复用器；
#   - POSIX 公共部分（src/detail/posix）：系统调用封装。
#   - 完成型后端 io_uring（src/detail/io_uring）：裸系统调用实现的环，不依赖 liburing；
#   - 完成型后端 IOCP（src/detail/iocp）：Windows 完成端口，WSARecv / WSASend / AcceptEx / ConnectEx /
#     ReadFile / WriteFile 重叠 I/O。Windows 上只编译具体层 + iocp；Linux 上编译 posix + reactor + io_uring。
set(net_concrete_sources
    src/error.cpp
    src/execution_context.cpp
    src/memory_resource.cpp
    src/thread_pool.cpp
    src/io_context.cpp
    src/ip.cpp
    src/socket_base.cpp
    src/tcp.cpp
    src/udp.cpp
    src/timer.cpp
    src/file.cpp
    src/resolver.cpp
    src/signal_set.cpp
    src/local.cpp
    src/receive_source.cpp
    src/detail/timer_heap.cpp
    src/detail/heap_timer.cpp)

set(net_posix_sources
    src/detail/posix/socket_ops.cpp
    src/detail/posix/file_ops.cpp)

set(net_iocp_sources
    src/detail/iocp/iocp_backend.cpp
    src/detail/iocp/iocp_socket.cpp
    src/detail/iocp/iocp_file.cpp
    src/detail/iocp/win_file_ops.cpp)

set(net_reactor_sources
    src/detail/reactor/reactor_backend.cpp
    src/detail/reactor/reactor_socket.cpp
    src/detail/reactor/reactor_file.cpp
    src/detail/reactor/epoll_demultiplexer.cpp
    src/detail/reactor/poll_demultiplexer.cpp
    src/detail/reactor/select_demultiplexer.cpp)

set(net_io_uring_sources
    src/detail/io_uring/uring.cpp
    src/detail/io_uring/uring_backend.cpp
    src/detail/io_uring/uring_socket.cpp
    src/detail/io_uring/uring_timer.cpp
    src/detail/io_uring/uring_file.cpp
    src/detail/io_uring/uring_receive.cpp)

# ---- TLS 提供者 ----
#
# tls::context / tls::stream 的实现建立在 OpenSSL API 子集之上，OpenSSL 与 BoringSSL 共用同
# 一份源码，构建期选择链接哪一个。BoringSSL 可以给现成的安装根目录，或用 FetchContent
# 从源码构建（约 400 个目标，不需要 Go / Perl）。
set(NET_TLS_PROVIDER "OpenSSL" CACHE STRING "TLS provider: OpenSSL | BoringSSL | OFF")
set_property(CACHE NET_TLS_PROVIDER PROPERTY STRINGS OpenSSL BoringSSL OFF)
set(NET_BORINGSSL_ROOT "" CACHE PATH "Prebuilt BoringSSL root containing include/ and lib/ (empty: FetchContent)")
set(NET_BORINGSSL_GIT_TAG "0.20250701.0" CACHE STRING "BoringSSL tag fetched when NET_BORINGSSL_ROOT is empty")

set(net_tls_sources)
set(net_tls_libraries)
set(net_has_tls OFF)
if(NET_TLS_PROVIDER STREQUAL "OpenSSL")
    find_package(OpenSSL REQUIRED)
    set(net_tls_libraries OpenSSL::SSL OpenSSL::Crypto)
    set(net_has_tls ON)
elseif(NET_TLS_PROVIDER STREQUAL "BoringSSL")
    if(NET_BORINGSSL_ROOT)
        add_library(net_boringssl_crypto STATIC IMPORTED)
        set_target_properties(net_boringssl_crypto PROPERTIES
            IMPORTED_LOCATION "${NET_BORINGSSL_ROOT}/lib/libcrypto.a"
            INTERFACE_INCLUDE_DIRECTORIES "${NET_BORINGSSL_ROOT}/include")
        add_library(net_boringssl_ssl STATIC IMPORTED)
        set_target_properties(net_boringssl_ssl PROPERTIES
            IMPORTED_LOCATION "${NET_BORINGSSL_ROOT}/lib/libssl.a"
            INTERFACE_INCLUDE_DIRECTORIES "${NET_BORINGSSL_ROOT}/include"
            INTERFACE_LINK_LIBRARIES net_boringssl_crypto)
        set(net_tls_libraries net_boringssl_ssl net_boringssl_crypto)
    else()
        include(FetchContent)
        FetchContent_Declare(boringssl
            GIT_REPOSITORY https://github.com/google/boringssl.git
            GIT_TAG ${NET_BORINGSSL_GIT_TAG}
            GIT_SHALLOW TRUE)
        set(BUILD_TESTING OFF CACHE BOOL "" FORCE)
        FetchContent_MakeAvailable(boringssl)
        set(net_tls_libraries ssl crypto)
    endif()
    set(net_has_tls ON)
elseif(NOT NET_TLS_PROVIDER STREQUAL "OFF")
    message(FATAL_ERROR "NET_TLS_PROVIDER must be OpenSSL, BoringSSL or OFF (got '${NET_TLS_PROVIDER}')")
endif()

if(net_has_tls)
    set(net_tls_sources
        src/tls/error.cpp
        src/tls/context.cpp
        src/tls/openssl_stream.cpp)
endif()

if(WIN32)
    set(net_platform_sources ${net_iocp_sources})
else()
    set(net_platform_sources ${net_posix_sources} ${net_reactor_sources} ${net_io_uring_sources})
endif()

add_library(net STATIC
    ${net_concrete_sources}
    ${net_platform_sources}
    ${net_tls_sources})
add_library(net::net ALIAS net)

if(WIN32)
    target_link_libraries(net PUBLIC ws2_32 mswsock)
    target_compile_definitions(net PUBLIC _WIN32_WINNT=0x0A00 WIN32_LEAN_AND_MEAN NOMINMAX)
    if(net_has_tls)
        target_link_libraries(net PUBLIC crypt32) # tls::context::add_os_certificates：系统证书库
    endif()
endif()

if(net_has_tls)
    target_link_libraries(net PUBLIC ${net_tls_libraries})
    target_compile_definitions(net PUBLIC NET_HAS_TLS=1 "NET_TLS_PROVIDER_NAME=\"${NET_TLS_PROVIDER}\"")
    message(STATUS "net: TLS provider ${NET_TLS_PROVIDER}")
else()
    message(STATUS "net: TLS disabled")
endif()

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

# MSVC：/permissive- 让 not / and / or 成为关键字并开启两阶段查找；/Zc:preprocessor 是 co2 协程 DSL
# 需要的标准预处理器；/utf-8 因为源码里有 UTF-8 注释；/bigobj 因为宏展开的协程状态机让目标文件很大。
# 这些是 PUBLIC 的：消费方的翻译单元包含本库的头时同样需要。
if(MSVC)
    target_compile_options(net PUBLIC /permissive- /Zc:__cplusplus /Zc:preprocessor /EHsc /utf-8 /bigobj)
    target_compile_options(net PRIVATE /W4 /wd4324)
    target_compile_definitions(net PRIVATE _CRT_SECURE_NO_WARNINGS)
else()
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
# 默认帧分配器。glibc 的 malloc 自带每线程免锁缓存（tcache），基准里比无锁的回收式分配器还
# 快约 3 ns/帧；在 malloc 较慢的平台（MSVC、macOS）或需要有界内存池时选 recycling。
set(NET_DEFAULT_FRAME_ALLOCATOR "new_delete" CACHE STRING "Default frame allocator: new_delete | recycling")
set_property(CACHE NET_DEFAULT_FRAME_ALLOCATOR PROPERTY STRINGS new_delete recycling)
if(NET_DEFAULT_FRAME_ALLOCATOR STREQUAL "recycling")
    target_compile_definitions(net PUBLIC NET_DEFAULT_FRAME_ALLOCATOR_RECYCLING=1)
elseif(NOT NET_DEFAULT_FRAME_ALLOCATOR STREQUAL "new_delete")
    message(FATAL_ERROR "NET_DEFAULT_FRAME_ALLOCATOR must be new_delete or recycling")
endif()

option(NET_BUILD_TESTS "Build net tests" ${NET_BUILD_TESTS_DEFAULT})
option(NET_BUILD_EXAMPLES "Build net examples" ${NET_BUILD_EXAMPLES_DEFAULT})
option(NET_BUILD_BENCHMARKS "Build net performance benchmarks" OFF)

if(NET_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

if(NET_BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()

if(NET_BUILD_BENCHMARKS)
    add_subdirectory(benchmarks)
endif()

install(TARGETS net EXPORT netTargets
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(EXPORT netTargets FILE netTargets.cmake NAMESPACE net::
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/net)
