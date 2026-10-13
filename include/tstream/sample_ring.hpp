// Runtime-sized handoff queue for Samples.
//
// SpscRing takes its capacity as a template parameter (SPEC §5), but ring
// capacity comes from the config file. SampleRing is a small interface over
// SpscRing<Sample, N>, and make_sample_ring() picks the N matching the config
// at startup. Each push/pop costs one virtual call on top of SpscRing; see
// docs/design-decisions.md ADR-011.

#pragma once

#include <cstddef>
#include <memory>

#include "tstream/sample.hpp"

namespace tstream {

class SampleRing {
 public:
  // Used through pointers to the interface: not copyable or movable.
  SampleRing() = default;
  SampleRing(const SampleRing&) = delete;
  SampleRing& operator=(const SampleRing&) = delete;
  SampleRing(SampleRing&&) = delete;
  SampleRing& operator=(SampleRing&&) = delete;
  // Virtual: rings are owned and deleted as std::unique_ptr<SampleRing>.
  virtual ~SampleRing() = default;

  // Same contracts as SpscRing: one producer thread, one consumer thread.

  // Producer only. False, without blocking, if the ring is full.
  [[nodiscard]] virtual bool try_push(const Sample& sample) = 0;
  // Consumer only. Pops up to max samples into out; returns how many.
  [[nodiscard]] virtual std::size_t try_pop_bulk(Sample* out, std::size_t max) = 0;
  // Fill level; exact only when nothing pushes or pops concurrently.
  [[nodiscard]] virtual std::size_t size_approx() const = 0;
  // The capacity this ring was built with.
  [[nodiscard]] virtual std::size_t capacity() const = 0;
};

// Capacity must be a power of two in [kMinRingCapacity, kMaxRingCapacity]
// (config.hpp); Config validation guarantees this. Throws
// std::invalid_argument otherwise.
std::unique_ptr<SampleRing> make_sample_ring(std::size_t capacity);

}  // namespace tstream
