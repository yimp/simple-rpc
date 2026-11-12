cmake_minimum_required(VERSION 3.16)
project(simple_rpc VERSION 0.3.0 LANGUAGES C)

find_package(Threads REQUIRED)

add_library(simple_rpc STATIC
    include/rpc_common.h
    include/rpc_client.h
    include/rpc_server.h
    include/rpc.h
    src/rpc_internal.h
    src/buffer.c
    src/protocol.c
    src/io.c
    src/request.c
    src/connection.c
    src/server_stream.c
    src/server.c
    src/client.c
    src/client_stream.c
)
target_link_libraries(simple_rpc PUBLIC Threads::Threads)
target_include_directories(simple_rpc PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_compile_features(simple_rpc PUBLIC c_std_11)
target_compile_options(simple_rpc PRIVATE -Wall -Wextra -Wpedantic)

add_executable(echo_server examples/echo_server.c)
target_link_libraries(echo_server PRIVATE simple_rpc)
add_executable(async_server examples/async_server.c)
target_link_libraries(async_server PRIVATE simple_rpc)
add_executable(stream_server examples/stream_server.c)
target_link_libraries(stream_server PRIVATE simple_rpc)
add_executable(stream_client examples/stream_client.c)
target_link_libraries(stream_client PRIVATE simple_rpc)
add_executable(echo_client examples/echo_client.c)
target_link_libraries(echo_client PRIVATE simple_rpc)

include(CTest)
if(BUILD_TESTING)
    add_executable(rpc_tests tests/test_rpc.c)
    target_link_libraries(rpc_tests PRIVATE simple_rpc Threads::Threads)
    add_executable(stream_tests tests/test_stream.c)
    target_link_libraries(stream_tests PRIVATE simple_rpc Threads::Threads)
    add_test(NAME rpc_streaming COMMAND stream_tests)
    add_test(NAME rpc_integration COMMAND rpc_tests)
    set_tests_properties(rpc_integration rpc_streaming PROPERTIES TIMEOUT 30)
endif()
