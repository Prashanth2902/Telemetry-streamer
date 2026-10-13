// Zero-allocation check for the encoder (SPEC §9).
//
// This binary replaces the global operator new/delete with versions that count
// calls, so it must stay a separate executable: the override applies to the
// whole program. After a warmup, encoding batches must allocate nothing.

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <new>
#include <vector>

#include <gtest/gtest.h>

#include "tstream/encoder.hpp"

#include "telemetry.pb.h"

namespace {

std::atomic<std::uint64_t> g_allocations{0};

// malloc-backed allocation that counts every call.
void* counted_alloc(std::size_t size) {
  g_allocations.fetch_add(1, std::memory_order_relaxed);
  if (void* p = std::malloc(size == 0 ? 1 : size)) {
    return p;
  }
  throw std::bad_alloc();
}

// Aligned variant for over-aligned types.
void* counted_aligned_alloc(std::size_t size, std::align_val_t align) {
  g_allocations.fetch_add(1, std::memory_order_relaxed);
  const auto alignment = static_cast<std::size_t>(align);
  const std::size_t rounded = (size + alignment - 1) / alignment * alignment;
  if (void* p = std::aligned_alloc(alignment, rounded == 0 ? alignment : rounded)) {
    return p;
  }
  throw std::bad_alloc();
}

}  // namespace

// Replacement global allocation functions: every form counts.
void* operator new(std::size_t size) {
  return counted_alloc(size);
}
void* operator new[](std::size_t size) {
  return counted_alloc(size);
}
void* operator new(std::size_t size, std::align_val_t align) {
  return counted_aligned_alloc(size, align);
}
void* operator new[](std::size_t size, std::align_val_t align) {
  return counted_aligned_alloc(size, align);
}
void operator delete(void* p) noexcept {
  std::free(p);
}
void operator delete[](void* p) noexcept {
  std::free(p);
}
void operator delete(void* p, std::size_t /*size*/) noexcept {
  std::free(p);
}
void operator delete[](void* p, std::size_t /*size*/) noexcept {
  std::free(p);
}
void operator delete(void* p, std::align_val_t /*align*/) noexcept {
  std::free(p);
}
void operator delete[](void* p, std::align_val_t /*align*/) noexcept {
  std::free(p);
}
void operator delete(void* p, std::size_t /*size*/, std::align_val_t /*align*/) noexcept {
  std::free(p);
}
void operator delete[](void* p, std::size_t /*size*/, std::align_val_t /*align*/) noexcept {
  std::free(p);
}

namespace {

using tstream::BatchView;
using tstream::Sample;
using tstream::SampleKind;

// `count` samples cycling through every kind, so consecutive batches put
// different oneof cases in the same repeated-field slots (the ADR-005 case).
std::vector<Sample> mixed_samples(std::size_t count, std::size_t rotate) {
  std::vector<Sample> samples(count);
  for (std::size_t i = 0; i < count; ++i) {
    Sample& s = samples[i];
    s.source_id = static_cast<std::uint16_t>(1 + (i + rotate) % 5);
    s.seq = static_cast<std::uint32_t>(i);
    s.t_capture_ns = static_cast<std::int64_t>(i) * 1000;
    switch ((i + rotate) % 5) {
      case 0:
        s.kind = SampleKind::Imu;
        s.imu = {.ax = 1, .ay = 2, .az = 3, .gx = 4, .gy = 5, .gz = 6};
        break;
      case 1:
        s.kind = SampleKind::Gps;
        s.gps = {.lat = 1, .lon = 2, .alt_m = 3, .speed_mps = 4, .fix = 3};
        break;
      case 2:
        s.kind = SampleKind::Battery;
        s.bat = {.voltage_v = 1, .current_a = 2, .soc_pct = 3, .temp_c = 4};
        break;
      case 3:
        s.kind = SampleKind::Can;
        s.can = {.can_id = 7, .dlc = 8, .data = {1, 2, 3, 4, 5, 6, 7, 8}};
        break;
      default:
        s.kind = SampleKind::Health;
        s.health = {.source_id = 1, .monitor = 2, .severity = 3, .detail = 4};
        break;
    }
  }
  return samples;
}

// Encodes `rounds` batches of varying size and kind mix with the counter
// running; returns how many allocations happened.
std::uint64_t allocations_while_encoding(std::size_t max_samples, int rounds) {
  tstream::Encoder encoder(max_samples);
  // Prepared up front: building test data allocates, encoding must not.
  std::vector<std::vector<Sample>> inputs;
  for (std::size_t r = 0; r < 7; ++r) {
    const std::size_t size = r == 0 ? max_samples : 1 + (max_samples * r) / 7;
    inputs.push_back(mixed_samples(size, r));
  }

  std::size_t total_bytes = 0;
  for (const auto& samples : inputs) {  // warmup
    total_bytes +=
        encoder.encode_frame(BatchView{.batch_seq = 0, .t_flush_ns = 0, .samples = samples}, 1)
            .size();
  }

  g_allocations.store(0, std::memory_order_relaxed);
  for (int round = 0; round < rounds; ++round) {
    const auto& samples = inputs[static_cast<std::size_t>(round) % inputs.size()];
    total_bytes += encoder
                       .encode_frame(BatchView{.batch_seq = static_cast<std::uint64_t>(round),
                                               .t_flush_ns = 0,
                                               .samples = samples},
                                     round)
                       .size();
  }
  const std::uint64_t allocations = g_allocations.load(std::memory_order_relaxed);
  EXPECT_GT(total_bytes, 0U);  // keeps the loop from being optimized away
  return allocations;
}

TEST(EncoderAllocation, SteadyStateEncodingDoesNotAllocate) {
  EXPECT_EQ(allocations_while_encoding(256, 1000), 0U);
}

TEST(EncoderAllocation, LargeBatchesDoNotAllocate) {
  EXPECT_EQ(allocations_while_encoding(4096, 200), 0U);
}

// Sanity check that the counter actually sees allocations.
TEST(EncoderAllocation, CounterCountsAllocations) {
  g_allocations.store(0, std::memory_order_relaxed);
  // A direct call, not a new-expression: compilers may elide `new int` at -O3
  // (C++14 [expr.new]), but not a call to the replaceable function itself.
  void* p = ::operator new(16);
  EXPECT_GE(g_allocations.load(std::memory_order_relaxed), 1U);
  ::operator delete(p);
}

// Negative control for the zero results above, and the reason for ADR-005:
// a reused heap-allocated Batch whose slots switch oneof case allocates on
// every batch. If this saw 0, the counter would be missing libprotobuf's
// allocations and the zero-allocation tests would prove nothing.
TEST(EncoderAllocation, ReusedHeapBatchWithChangingOneofsAllocates) {
  const auto a = mixed_samples(64, 0);
  const auto b = mixed_samples(64, 1);  // same slots, different kinds
  tstream::v1::Batch batch;
  std::vector<std::uint8_t> out(1 << 16);
  // Fills `batch` the way a naive encoder would.
  auto encode = [&](const std::vector<Sample>& samples) {
    batch.Clear();
    for (const Sample& s : samples) {
      auto* m = batch.add_samples();
      m->set_seq(s.seq);
      if (s.kind == SampleKind::Imu) {
        m->mutable_imu()->set_ax(s.imu.ax);
      } else {
        m->mutable_gps()->set_lat(1.0);
      }
    }
    static_cast<void>(batch.SerializeToArray(out.data(), static_cast<int>(out.size())));
  };
  encode(a);
  encode(b);  // warmup: repeated-field capacity is now allocated

  g_allocations.store(0, std::memory_order_relaxed);
  for (int i = 0; i < 10; ++i) {
    encode(i % 2 == 0 ? a : b);
  }
  EXPECT_GT(g_allocations.load(std::memory_order_relaxed), 0U);
}

}  // namespace
