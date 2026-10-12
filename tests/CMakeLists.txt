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
  tstream_set_warnings(${name})
  gtest_discover_tests(${name}
    DISCOVERY_MODE PRE_TEST
    PROPERTIES LABELS ${ARG_LABEL} TIMEOUT ${ARG_TIMEOUT})
endfunction()

tstream_add_test(toolchain_smoke_test LABEL unit SOURCES unit/toolchain_smoke_test.cpp)
tstream_add_test(spsc_ring_test LABEL unit SOURCES unit/spsc_ring_test.cpp)
