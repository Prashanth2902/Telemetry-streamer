// SpscRing vs mutex baselines (SPEC §5, §14.1).
//
//   BM_Throughput: producer thread streams uint64 values to a consumer thread.
//                  Reported as items/s; each iteration moves kItemsPerIter items
//                  and waits until all are consumed.
//   BM_PingPong:   one value bounces between two queues. Time per iteration is
//                  one round trip; one-way handoff latency is about half of it.
//
// Thread placement: set TSTREAM_BENCH_CPUS=<producer>,<consumer> to pin the two
// threads (e.g. two SMT siblings vs two physical cores). Unset = unpinned.
// Methodology and results: docs/benchmarks.md.

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <utility>

#include <benchmark/benchmark.h>
#include <pthread.h>
#include <sched.h>

#include "tstream/spsc_ring.hpp"

#include "baseline_queues.hpp"

namespace {

using tstream::SpscRing;
using tstream::bench::MutexDequeQueue;
using tstream::bench::MutexRingQueue;

constexpr std::size_t kCapacity = 1024;
constexpr std::uint64_t kItemsPerIter = 1 << 16;

inline void cpu_relax() {
#if defined(__x86_64__) || defined(__i386__)
  __builtin_ia32_pause();
#endif
}

// Parsed once from TSTREAM_BENCH_CPUS="<producer>,<consumer>".
std::optional<std::pair<std::size_t, std::size_t>> bench_cpus() {
  static const auto cpus = []() -> std::optional<std::pair<std::size_t, std::size_t>> {
    const char* env = std::getenv("TSTREAM_BENCH_CPUS");
    if (env == nullptr) {
      return std::nullopt;
    }
    const std::string spec(env);
    const auto comma = spec.find(',');
    if (comma == std::string::npos) {
      return std::nullopt;
    }
    return std::pair{static_cast<std::size_t>(std::stoul(spec.substr(0, comma))),
                     static_cast<std::size_t>(std::stoul(spec.substr(comma + 1)))};
  }();
  return cpus;
}

void pin_current_thread(std::size_t cpu) {
  cpu_set_t set;
  CPU_ZERO(&set);
  CPU_SET(cpu, &set);
  pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
}

void pin_producer() {
  if (const auto cpus = bench_cpus()) {
    pin_current_thread(cpus->first);
  }
}

void pin_consumer() {
  if (const auto cpus = bench_cpus()) {
    pin_current_thread(cpus->second);
  }
}

template <typename Queue>
void BM_Throughput(benchmark::State& state) {
  // Heap-allocated: queues hold kCapacity slots and may be over-aligned.
  auto queue = std::make_unique<Queue>();
  std::atomic<bool> stop{false};
  // Written by the consumer once per kItemsPerIter items, not per item, so the
  // progress counter doesn't add its own cache-line traffic to the measurement.
  std::atomic<std::uint64_t> consumed{0};

  std::thread consumer([&] {
    pin_consumer();
    std::uint64_t count = 0;
    std::uint64_t value = 0;
    while (true) {
      if (queue->try_pop(value)) {
        benchmark::DoNotOptimize(value);
        if (++count % kItemsPerIter == 0) {
          consumed.store(count, std::memory_order_release);
        }
      } else if (stop.load(std::memory_order_acquire)) {
        break;
      } else {
        cpu_relax();
      }
    }
  });

  pin_producer();
  std::uint64_t next = 0;
  for (auto _ : state) {
    const std::uint64_t target = next + kItemsPerIter;
    for (; next < target; ++next) {
      while (!queue->try_push(next)) {
        cpu_relax();
      }
    }
    while (consumed.load(std::memory_order_acquire) < target) {
      cpu_relax();
    }
  }

  stop.store(true, std::memory_order_release);
  consumer.join();
  state.SetItemsProcessed(static_cast<std::int64_t>(next));
}

template <typename Queue>
void BM_PingPong(benchmark::State& state) {
  auto to_echo = std::make_unique<Queue>();
  auto from_echo = std::make_unique<Queue>();
  std::atomic<bool> stop{false};

  std::thread echo([&] {
    pin_consumer();
    std::uint64_t value = 0;
    while (!stop.load(std::memory_order_acquire)) {
      if (to_echo->try_pop(value)) {
        while (!from_echo->try_push(value)) {
          cpu_relax();
        }
      } else {
        cpu_relax();
      }
    }
  });

  pin_producer();
  std::uint64_t value = 0;
  for (auto _ : state) {
    while (!to_echo->try_push(value)) {
      cpu_relax();
    }
    std::uint64_t reply = 0;
    while (!from_echo->try_pop(reply)) {
      cpu_relax();
    }
    benchmark::DoNotOptimize(reply);
    ++value;
  }

  stop.store(true, std::memory_order_release);
  echo.join();
  state.counters["round_trips"] =
      benchmark::Counter(static_cast<double>(state.iterations()), benchmark::Counter::kIsRate);
}

using Spsc = SpscRing<std::uint64_t, kCapacity>;
using MutexRing = MutexRingQueue<std::uint64_t, kCapacity>;
using MutexDeque = MutexDequeQueue<std::uint64_t, kCapacity>;

BENCHMARK_TEMPLATE(BM_Throughput, Spsc)->UseRealTime();
BENCHMARK_TEMPLATE(BM_Throughput, MutexRing)->UseRealTime();
BENCHMARK_TEMPLATE(BM_Throughput, MutexDeque)->UseRealTime();

BENCHMARK_TEMPLATE(BM_PingPong, Spsc)->UseRealTime();
BENCHMARK_TEMPLATE(BM_PingPong, MutexRing)->UseRealTime();
BENCHMARK_TEMPLATE(BM_PingPong, MutexDeque)->UseRealTime();

// Variants (SPEC §14.1): with/without cached indices, with/without cache-line
// padding, and 64 vs 128 alignment. These need SpscRing to expose those knobs
// (its design is the author's call), so they get registered here once it does:
//   BENCHMARK_TEMPLATE(BM_Throughput, <variant type>)->UseRealTime();

}  // namespace
