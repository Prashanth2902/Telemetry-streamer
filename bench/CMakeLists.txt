# tstream_add_bench(<name> <sources...>)
function(tstream_add_bench name)
  add_executable(${name} ${ARGN})
  target_link_libraries(${name} PRIVATE tstream::core benchmark::benchmark_main)
  tstream_set_warnings(${name})
endfunction()

# Placeholder so the benchmark dependency and the CI smoke run exist from M0.
# bench_spsc (M1), bench_encode, and bench_e2e (M6) are added alongside it.
tstream_add_bench(bench_smoke bench_smoke.cpp)
