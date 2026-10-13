// M0 placeholder benchmark: proves Google Benchmark links and runs.

#include <atomic>
#include <cstdint>

#include <benchmark/benchmark.h>

namespace {

// Cost of one relaxed atomic increment: proves Google Benchmark links and runs.
void BM_AtomicIncrement(benchmark::State& state) {
  std::atomic<std::uint64_t> counter{0};
  for (auto _ : state) {
    counter.fetch_add(1, std::memory_order_relaxed);
  }
  benchmark::DoNotOptimize(counter.load(std::memory_order_relaxed));
}
BENCHMARK(BM_AtomicIncrement);

}  // namespace
