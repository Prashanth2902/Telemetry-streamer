# tstream_add_bench(<name> <sources...>)
function(tstream_add_bench name)
  add_executable(${name} ${ARGN})
  target_link_libraries(${name} PRIVATE tstream::core benchmark::benchmark_main)
  tstream_set_warnings(${name})
endfunction()

# Placeholder so the benchmark dependency and the CI smoke run exist from M0.
tstream_add_bench(bench_smoke bench_smoke.cpp)

tstream_add_bench(bench_spsc bench_spsc.cpp)
