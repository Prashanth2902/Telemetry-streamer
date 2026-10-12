// The pipeline thread (SPEC §3): drains every source's ring, batches, and
// hands finished batches to a sink. Single thread, no locks on the hot path.
//
// - Round-robin over the rings, starting one ring later each pass, taking at
//   most kMaxDrainPerRing samples per ring per turn, so a 1 kHz source can't
//   delay a 1 Hz one by more than one bounded drain.
// - Samples are popped straight into the batch's free slots.
// - When a pass finds nothing, the thread sleeps until the open batch's
//   deadline, but at most kIdleSleep, rather than spinning a core (ADR-013).
// - stop() asks the thread to finish: it drains every ring until empty,
//   flushes the final partial batch, and joins. Stop the sources first, so the
//   rings stop filling (SPEC §3.2 shutdown order).

#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <thread>
#include <vector>

#include "tstream/batcher.hpp"
#include "tstream/clock.hpp"
#include "tstream/config.hpp"
#include "tstream/sample_ring.hpp"
#include "tstream/sink.hpp"

namespace tstream {

class Pipeline {
 public:
  static constexpr std::size_t kMaxDrainPerRing = 64;
  static constexpr Clock::duration kIdleSleep = std::chrono::microseconds(100);

  // The pipeline is the single consumer of every ring in `rings`. Rings and
  // sink must outlive it.
  Pipeline(std::vector<SampleRing*> rings, const BatchConfig& batch, BatchSink& sink);
  Pipeline(const Pipeline&) = delete;
  Pipeline& operator=(const Pipeline&) = delete;
  Pipeline(Pipeline&&) = delete;
  Pipeline& operator=(Pipeline&&) = delete;
  ~Pipeline();

  // Call once.
  void start();
  // Final drain + flush, then join. Safe to call more than once.
  void stop();

  // Samples taken from rings[i] so far. Exact once stop() has returned.
  [[nodiscard]] std::uint64_t delivered(std::size_t ring_index) const;
  [[nodiscard]] std::uint64_t batches() const;

 private:
  void run();
  std::size_t drain_pass(std::size_t first_ring);
  void flush();
  void idle_wait() const;

  std::vector<SampleRing*> rings_;
  BatchSink& sink_;
  Batcher batcher_;
  std::uint64_t next_batch_seq_ = 0;

  std::unique_ptr<std::atomic<std::uint64_t>[]> delivered_;
  std::atomic<std::uint64_t> batches_{0};
  std::atomic<bool> stop_requested_{false};
  std::thread thread_;
};

}  // namespace tstream
