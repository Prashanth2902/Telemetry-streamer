// The pipeline thread (SPEC §3): drains every source's ring, batches, and
// hands finished batches to a sink. Single thread, no locks on the hot path.
//
// - Round-robin over the rings, starting one ring later each pass, taking at
//   most kMaxDrainPerRing samples per ring per turn, so a 1 kHz source can't
//   delay a 1 Hz one by more than one bounded drain.
// - Samples are popped straight into the batch's free slots.
// - When a pass finds nothing, the thread sleeps until the open batch's
//   deadline, but at most kIdleSleep, rather than spinning a core (ADR-013).
// - Health monitors (SPEC §7) see every sample as it enters the batch and get
//   a tick each loop; their events go into the batch right after, as Health
//   samples from source 0, so the receiver sees them in order with the data.
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
#include "tstream/health.hpp"
#include "tstream/sample_ring.hpp"
#include "tstream/sink.hpp"

namespace tstream {

class Pipeline {
 public:
  static constexpr std::size_t kMaxDrainPerRing = 64;
  static constexpr Clock::duration kIdleSleep = std::chrono::microseconds(100);

  // The pipeline is the single consumer of every ring in `rings`. Rings, sink,
  // and monitors must outlive it; the monitors are then used by its thread only.
  Pipeline(std::vector<SampleRing*> rings, const BatchConfig& batch, BatchSink& sink,
           std::vector<HealthMonitor*> monitors = {});
  // Owns a thread that captures `this`: not copyable or movable.
  Pipeline(const Pipeline&) = delete;
  Pipeline& operator=(const Pipeline&) = delete;
  Pipeline(Pipeline&&) = delete;
  Pipeline& operator=(Pipeline&&) = delete;
  // Stops (final drain and flush) if still running.
  ~Pipeline();

  // Call once.
  void start();
  // Final drain + flush, then join. Safe to call more than once.
  void stop();

  // Samples from rings[i] handed to the sink so far. Exact once stop() has
  // returned.
  [[nodiscard]] std::uint64_t delivered(std::size_t ring_index) const;
  // Batches handed to the sink so far.
  [[nodiscard]] std::uint64_t batches() const;
  // Health samples put into batches so far (counted when injected).
  [[nodiscard]] std::uint64_t health_events() const;

 private:
  // Thread body: drain, flush when due, idle when quiet, final drain on stop.
  void run();
  // One round-robin pass over all rings from first_ring; returns samples taken.
  std::size_t drain_pass(std::size_t first_ring);
  // Ticks every monitor, then moves their events into the batch.
  void run_monitors();
  // Sends the open batch to the sink, credits delivered counts, and clears it.
  void flush();
  // Sleeps until the open batch is due, or kIdleSleep at most.
  void idle_wait() const;

  std::vector<SampleRing*> rings_;
  BatchSink& sink_;
  std::vector<HealthMonitor*> monitors_;
  Batcher batcher_;
  std::uint64_t next_batch_seq_ = 0;
  std::uint32_t next_health_seq_ = 0;  // seq of source 0's samples
  // Per ring: samples in the open batch, credited to delivered_ on flush.
  // Pipeline thread only.
  std::vector<std::uint64_t> in_open_batch_;

  std::unique_ptr<std::atomic<std::uint64_t>[]> delivered_;
  std::atomic<std::uint64_t> batches_{0};
  std::atomic<std::uint64_t> health_events_{0};
  std::atomic<bool> stop_requested_{false};
  std::thread thread_;
};

}  // namespace tstream
