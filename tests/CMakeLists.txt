include(GoogleTest)

# tstream_add_test(<name> <sources...>)
# One GoogleTest executable per call, linked against the core library and
# registered with CTest. Discovery runs at test time so sanitizer runtime
# options from the test presets apply.
function(tstream_add_test name)
  add_executable(${name} ${ARGN})
  target_link_libraries(${name} PRIVATE tstream::core GTest::gtest_main GTest::gmock)
  tstream_set_warnings(${name})
  gtest_discover_tests(${name} DISCOVERY_MODE PRE_TEST)
endfunction()

tstream_add_test(toolchain_smoke_test unit/toolchain_smoke_test.cpp)
