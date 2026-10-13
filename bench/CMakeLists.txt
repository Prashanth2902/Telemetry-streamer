# tstream_add_bench(<name> <sources...>)
function(tstream_add_bench name)
  add_executable(${name} ${ARGN})
  target_link_libraries(${name} PRIVATE tstream::core benchmark::benchmark_main)
  tstream_set_warnings(${name})
endfunction()

tstream_add_bench(bench_spsc bench_spsc.cpp)
