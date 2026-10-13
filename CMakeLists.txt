cmake_minimum_required(VERSION 3.20)

project(tstream
  VERSION 0.1.0
  DESCRIPTION "Real-time C++20 telemetry pipeline"
  LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Debug CACHE STRING "Build type" FORCE)
endif()

option(TSTREAM_BUILD_TESTS "Build unit, stress, and integration tests" ON)
option(TSTREAM_BUILD_BENCH "Build Google Benchmark targets" OFF)
option(TSTREAM_WARNINGS_AS_ERRORS "Treat compiler warnings as errors" OFF)
option(TSTREAM_BUILD_FUZZ "Build libFuzzer targets (clang only)" OFF)
set(TSTREAM_SANITIZER "none" CACHE STRING "Sanitizer to build with: none, address, thread")
set_property(CACHE TSTREAM_SANITIZER PROPERTY STRINGS none address thread)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
include(CompilerWarnings)
include(Sanitizers)

# Dependencies (SPEC §11): Ubuntu system packages only, see docs/design-decisions.md ADR-001.
find_package(Protobuf REQUIRED)
find_package(nlohmann_json 3.11 REQUIRED)
find_package(spdlog REQUIRED)
find_package(Threads REQUIRED)

# Wire schema (tstream_proto), then the core library (tstream_core) and executables.
add_subdirectory(proto)
add_subdirectory(src)
add_subdirectory(tools)

if(TSTREAM_BUILD_TESTS)
  enable_testing()
  find_package(GTest REQUIRED)
  add_subdirectory(tests)
endif()

if(TSTREAM_BUILD_BENCH)
  find_package(benchmark REQUIRED)
  add_subdirectory(bench)
endif()

if(TSTREAM_BUILD_FUZZ)
  add_subdirectory(fuzz)
endif()
