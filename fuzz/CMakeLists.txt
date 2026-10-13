# libFuzzer targets. Clang only; best combined with a sanitizer preset, e.g.
#   cmake --preset clang-asan -DTSTREAM_BUILD_FUZZ=ON
if(NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  message(FATAL_ERROR "TSTREAM_BUILD_FUZZ requires clang (libFuzzer)")
endif()

add_executable(framing_fuzz framing_fuzz.cpp)
target_link_libraries(framing_fuzz PRIVATE tstream::core)
target_compile_options(framing_fuzz PRIVATE -fsanitize=fuzzer)
target_link_options(framing_fuzz PRIVATE -fsanitize=fuzzer)
tstream_set_warnings(framing_fuzz)
