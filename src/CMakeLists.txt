# Everything except main.cpp, so tests and benchmarks link the same code the
# executable runs.
add_library(tstream_core STATIC
  config.cpp
  encoder.cpp
  framing.cpp
  pipeline.cpp
  sample_ring.cpp
  stdout_sink.cpp
  tcp_transport.cpp
  sources/fixed_rate_source.cpp
  sources/sim_models.cpp)
add_library(tstream::core ALIAS tstream_core)
target_include_directories(tstream_core PUBLIC "${PROJECT_SOURCE_DIR}/include")
target_compile_features(tstream_core PUBLIC cxx_std_20)
target_link_libraries(tstream_core PUBLIC
  tstream_proto
  Threads::Threads
  nlohmann_json::nlohmann_json
  spdlog::spdlog)
tstream_set_warnings(tstream_core)

add_executable(tstream main.cpp)
target_link_libraries(tstream PRIVATE tstream::core)
tstream_set_warnings(tstream)
