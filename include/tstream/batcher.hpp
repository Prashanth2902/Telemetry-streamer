// Accumulates samples into one batch and decides when to flush (SPEC §3).
//
// A batch is flushed when it is full, or max_delay after its first sample
// entered it. Storage is allocated once in the constructor; the pipeline pops
// samples straight into free_slots(), so steady-state batching neither copies
// twice nor allocates. Pure logic with explicit timestamps, so it is tested
// without threads or sleeps.

#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <stdexcept>
#include <vector>

#include "tstream/sample.hpp"

namespace tstream {

class Batcher {
 public:
  // Allocates room for max_samples once; a batch stays open at most max_delay_ns.
  Batcher(std::size_t max_samples, std::int64_t max_delay_ns)
      : samples_(max_samples), max_delay_ns_(max_delay_ns) {
    if (max_samples == 0) {
      throw std::invalid_argument("Batcher needs max_samples > 0");
    }
  }

  // First unused slot; the pipeline pops samples straight into it.
  [[nodiscard]] Sample* free_slots() { return samples_.data() + size_; }
  // How many more samples fit before the batch is full.
  [[nodiscard]] std::size_t free_count() const { return samples_.size() - size_; }

  // Marks n more slots as filled. The first commit into an empty batch starts
  // its max-delay timer.
  void commit(std::size_t n, std::int64_t now_ns) {
    if (n == 0) {
      return;
    }
    if (size_ == 0) {
      opened_ns_ = now_ns;
    }
    size_ += n;
  }

  // True if the open batch holds no samples.
  [[nodiscard]] bool empty() const { return size_ == 0; }
  // True once max_samples are waiting; the pipeline flushes then.
  [[nodiscard]] bool full() const { return size_ == samples_.size(); }
  // Samples currently in the open batch.
  [[nodiscard]] std::size_t size() const { return size_; }

  // True once max_delay has passed since the open batch's first commit.
  [[nodiscard]] bool due(std::int64_t now_ns) const {
    return size_ > 0 && now_ns - opened_ns_ >= max_delay_ns_;
  }
  // When the open batch becomes due. Only meaningful if !empty().
  [[nodiscard]] std::int64_t deadline_ns() const { return opened_ns_ + max_delay_ns_; }

  // Read-only view of the committed samples, oldest first.
  [[nodiscard]] std::span<const Sample> samples() const { return {samples_.data(), size_}; }

  // Empties the batch after a flush; keeps the storage.
  void clear() { size_ = 0; }

 private:
  std::vector<Sample> samples_;
  std::size_t size_ = 0;
  std::int64_t opened_ns_ = 0;
  std::int64_t max_delay_ns_;
};

}  // namespace tstream
