// Concurrency stress test for SpscRing (SPEC §5): one producer pushes a
// strictly increasing sequence while one consumer checks it arrives complete,
// in order, and without duplicates.
//
// Item count: 100M in optimized non-sanitizer builds (SPEC §5). Smaller by
// default in Debug and sanitizer builds so CI stays fast. Override with the
// TSTREAM_STRESS_COUNT environment variable, e.g. TSTREAM_STRESS_COUNT=100000000.

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <string>
#include <thread>

#include <gtest/gtest.h>

#include "tstream/spsc_ring.hpp"

#if defined(__SANITIZE_THREAD__) || defined(__SANITIZE_ADDRESS__)
#define TSTREAM_SANITIZED 1
#elif defined(__has_feature)
#if __has_feature(thread_sanitizer) || __has_feature(address_sanitizer)
#define TSTREAM_SANITIZED 1
#endif
#endif

namespace {

using tstream::BasicSpscRing;
using tstream::SpscRing;
using tstream::SpscRingOptions;

std::uint64_t stress_count() {
  // getenv is only unsafe alongside setenv, which nothing here calls; and this
  // runs on the test thread before the producer and consumer start.
  if (const char* env = std::getenv("TSTREAM_STRESS_COUNT")) {  // NOLINT(concurrency-mt-unsafe)
    return std::stoull(env);
  }
#if defined(TSTREAM_SANITIZED)
  return 2'000'000;
#elif !defined(NDEBUG)
  return 10'000'000;
#else
  return 100'000'000;
#endif
}

// Result of one consumer run, checked on the main thread after join so a
// failure reports the first bad value instead of millions of assertion lines.
struct ConsumerResult {
  std::uint64_t received = 0;
  std::uint64_t first_bad_index = 0;
  std::uint64_t first_bad_value = 0;
  bool ok = true;
};

void check_value(ConsumerResult& r, std::uint64_t value) {
  if (r.ok && value != r.received) {
    r.ok = false;
    r.first_bad_index = r.received;
    r.first_bad_value = value;
  }
  ++r.received;
}

// The producer sets this (release) after its last push. A consumer that sees
// it (acquire) and then finds the ring empty knows no more items are coming,
// so a lost item ends the run with a short count instead of hanging forever.
struct Done {
  std::atomic<bool> flag{false};
};

template <typename Ring>
void produce(Ring& ring, std::uint64_t count, Done& done) {
  for (std::uint64_t i = 0; i < count; ++i) {
    while (!ring.try_push(i)) {
      std::this_thread::yield();
    }
  }
  done.flag.store(true, std::memory_order_release);
}

template <typename Ring>
ConsumerResult consume_single(Ring& ring, std::uint64_t count, const Done& done) {
  ConsumerResult r;
  std::uint64_t value = 0;
  while (r.received < count) {
    if (ring.try_pop(value)) {
      check_value(r, value);
    } else if (done.flag.load(std::memory_order_acquire)) {
      if (!ring.try_pop(value)) {
        break;  // producer finished and ring drained: anything missing is lost
      }
      check_value(r, value);
    } else {
      std::this_thread::yield();
    }
  }
  return r;
}

template <typename Ring, std::size_t BatchSize>
ConsumerResult consume_bulk(Ring& ring, std::uint64_t count, const Done& done) {
  ConsumerResult r;
  std::array<std::uint64_t, BatchSize> batch{};
  while (r.received < count) {
    std::size_t n = ring.try_pop_bulk(batch.data(), batch.size());
    if (n == 0) {
      if (!done.flag.load(std::memory_order_acquire)) {
        std::this_thread::yield();
        continue;
      }
      n = ring.try_pop_bulk(batch.data(), batch.size());
      if (n == 0) {
        break;  // producer finished and ring drained: anything missing is lost
      }
    }
    EXPECT_LE(n, BatchSize);
    for (std::size_t i = 0; i < n; ++i) {
      check_value(r, batch[i]);
    }
  }
  return r;
}

void expect_complete_and_ordered(const ConsumerResult& r, std::uint64_t count) {
  EXPECT_TRUE(r.ok) << "item #" << r.first_bad_index << " had value " << r.first_bad_value
                    << " (expected " << r.first_bad_index << ")";
  EXPECT_EQ(r.received, count);
}

template <typename Ring>
void run_single_pop(std::uint64_t count) {
  Ring ring;
  Done done;
  ConsumerResult result;
  std::thread consumer([&] { result = consume_single(ring, count, done); });
  std::thread producer([&] { produce(ring, count, done); });
  producer.join();
  consumer.join();
  expect_complete_and_ordered(result, count);
  EXPECT_EQ(ring.size_approx(), 0U);
}

template <typename Ring, std::size_t BatchSize>
void run_bulk_pop(std::uint64_t count) {
  Ring ring;
  Done done;
  ConsumerResult result;
  std::thread consumer([&] { result = consume_bulk<Ring, BatchSize>(ring, count, done); });
  std::thread producer([&] { produce(ring, count, done); });
  producer.join();
  consumer.join();
  expect_complete_and_ordered(result, count);
  EXPECT_EQ(ring.size_approx(), 0U);
}

// A tiny ring flips between full and empty constantly, which exercises the
// cached-index refresh paths on both sides.
TEST(SpscRingStress, SmallRingSinglePop) {
  run_single_pop<SpscRing<std::uint64_t, 16>>(stress_count());
}

TEST(SpscRingStress, LargeRingSinglePop) {
  run_single_pop<SpscRing<std::uint64_t, 4096>>(stress_count());
}

// Batch smaller than capacity, so bulk pops regularly straddle the wrap point.
TEST(SpscRingStress, BulkPop) {
  run_bulk_pop<SpscRing<std::uint64_t, 1024>, 100>(stress_count());
}

// The non-default configurations that bench_spsc measures must be correct
// too, or their numbers mean nothing. A tenth of the count keeps CI fast.
using NoCachedIndices = BasicSpscRing<std::uint64_t, 16, SpscRingOptions{.cache_indices = false}>;
using NoPadding = BasicSpscRing<std::uint64_t, 16,
                                SpscRingOptions{.alignment = alignof(std::atomic<std::size_t>)}>;
using Padded64 =
    BasicSpscRing<std::uint64_t, 16, SpscRingOptions{.alignment = tstream::kCacheLine}>;

TEST(SpscRingStress, VariantWithoutCachedIndices) {
  run_single_pop<NoCachedIndices>(stress_count() / 10);
  run_bulk_pop<NoCachedIndices, 7>(stress_count() / 10);
}

TEST(SpscRingStress, VariantWithoutPadding) {
  run_single_pop<NoPadding>(stress_count() / 10);
  run_bulk_pop<NoPadding, 7>(stress_count() / 10);
}

TEST(SpscRingStress, VariantPaddedTo64) {
  run_single_pop<Padded64>(stress_count() / 10);
  run_bulk_pop<Padded64, 7>(stress_count() / 10);
}

}  // namespace
