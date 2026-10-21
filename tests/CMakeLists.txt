include(GoogleTest)

# tstream_add_test(<name> LABEL <label> [TIMEOUT <seconds>] SOURCES <sources...>)
# One GoogleTest executable per call, linked against the core library and
# registered with CTest under <label> (unit, stress, integration, ...), so a
# group can be run with `ctest --preset <p> -L <label>`. TIMEOUT (default 60 s)
# turns a hang into a failure. Discovery runs at test time so sanitizer runtime
# options from the test presets apply.
function(tstream_add_test name)
  cmake_parse_arguments(ARG "" "LABEL;TIMEOUT" "SOURCES" ${ARGN})
  if(NOT ARG_TIMEOUT)
    set(ARG_TIMEOUT 60)
  endif()
  add_executable(${name} ${ARG_SOURCES})
  target_link_libraries(${name} PRIVATE tstream::core GTest::gtest_main GTest::gmock)
  target_include_directories(${name} PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/support")
  tstream_set_warnings(${name})
  gtest_discover_tests(${name}
    DISCOVERY_MODE PRE_TEST
    PROPERTIES LABELS ${ARG_LABEL} TIMEOUT ${ARG_TIMEOUT})
endfunction()

tstream_add_test(spsc_ring_test LABEL unit SOURCES unit/spsc_ring_test.cpp)
tstream_add_test(spsc_ring_stress_test LABEL stress TIMEOUT 600
  SOURCES stress/spsc_ring_stress_test.cpp)
tstream_add_test(config_test LABEL unit SOURCES unit/config_test.cpp)
tstream_add_test(sources_test LABEL unit SOURCES unit/sources_test.cpp)
tstream_add_test(batcher_test LABEL unit SOURCES unit/batcher_test.cpp)
tstream_add_test(pipeline_test LABEL unit SOURCES unit/pipeline_test.cpp)

tstream_add_test(cli_test LABEL integration TIMEOUT 300 SOURCES integration/cli_test.cpp)
target_compile_definitions(cli_test PRIVATE
  TSTREAM_BINARY="$<TARGET_FILE:tstream>"
  TSTREAM_SOURCE_DIR="${PROJECT_SOURCE_DIR}")
add_dependencies(cli_test tstream)
tstream_add_test(framing_test LABEL unit SOURCES unit/framing_test.cpp)
tstream_add_test(encoder_test LABEL unit SOURCES unit/encoder_test.cpp)
tstream_add_test(tcp_transport_test LABEL unit SOURCES unit/tcp_transport_test.cpp)
tstream_add_test(receiver_test LABEL unit SOURCES unit/receiver_test.cpp)
tstream_add_test(stats_test LABEL unit SOURCES unit/stats_test.cpp)
tstream_add_test(fault_test LABEL unit SOURCES unit/fault_test.cpp)
tstream_add_test(health_test LABEL unit SOURCES unit/health_test.cpp)
tstream_add_test(monitors_test LABEL unit SOURCES unit/monitors_test.cpp)

# Replaces global operator new, so it must be its own executable (SPEC §9).
# Not built with sanitizers: they ship their own operator new (clang's static
# runtimes then fail to link, and ASan flags new/free mixes), and the point is
# to measure our code with the real allocator anyway.
if(TSTREAM_SANITIZER STREQUAL "none")
  tstream_add_test(encoder_alloc_test LABEL alloc SOURCES alloc/encoder_alloc_test.cpp)
endif()

# Both run the real sender and receiver (integration/e2e_fixture.hpp).
foreach(test e2e_test)
  tstream_add_test(${test} LABEL integration TIMEOUT 300 SOURCES integration/${test}.cpp)
  target_compile_definitions(${test} PRIVATE
    TSTREAM_BINARY="$<TARGET_FILE:tstream>"
    TSTREAM_RECV_BINARY="$<TARGET_FILE:tstream-recv>"
    TSTREAM_SOURCE_DIR="${PROJECT_SOURCE_DIR}")
  add_dependencies(${test} tstream tstream-recv)
endforeach()
