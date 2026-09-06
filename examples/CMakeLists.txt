set(net_example_sources
    echo_server.cpp
    echo_client.cpp
    http_get.cpp
    timers.cpp)
if(net_has_tls)
    list(APPEND net_example_sources https_get.cpp)
endif()

foreach(source IN LISTS net_example_sources)
    get_filename_component(name "${source}" NAME_WE)
    add_executable(example_${name} ${source})
    target_link_libraries(example_${name} PRIVATE net::net)
    set_target_properties(
        example_${name}
        PROPERTIES
            CXX_STANDARD 14
            CXX_STANDARD_REQUIRED YES
            CXX_EXTENSIONS NO
            OUTPUT_NAME ${name})
    if(NOT MSVC)
        target_compile_options(example_${name} PRIVATE -Wall -Wextra -Wpedantic)
    endif()
endforeach()
