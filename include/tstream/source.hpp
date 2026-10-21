// Data sources (SPEC §3.1). Each source runs on its own thread and is the
// single producer for its SampleRing.

#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <thread>

#include "tstream/clock.hpp"
#include "tstream/config.hpp"
#include "tstream/sample.hpp"
#include "tstream/sample_ring.hpp"
#include "tstream/sim_models.hpp"

namespace tstream {

struct SourceCounters {
  std::uint64_t generated = 0;        // samples created (each consumed one seq)
  std::uint64_t dropped_newest = 0;   // drop_newest: rejected because the ring was full
  std::uint64_t dropped_timeout = 0;  // block_with_timeout: still full after timeout_us
  std::uint64_t blocked = 0;          // block_with_timeout: waited, then got in
  std::uint64_t overruns = 0;         // times the thread fell a full period behind

  // All samples the ring rejected, whatever the policy.
  [[nodiscard]] std::uint64_t dropped() const { return dropped_newest + dropped_timeout; }
};

// What a source does when its ring is full (SPEC §6).
struct PushPolicy {
  BackpressurePolicy policy = BackpressurePolicy::DropNewest;
  Clock::duration timeout{};  // block_with_timeout only
};

class Source {
 public:
  // Owns a thread: not copyable or movable.
  Source() = default;
  Source(const Source&) = delete;
  Source& operator=(const Source&) = delete;
  Source(Source&&) = delete;
  Source& operator=(Source&&) = delete;
  // Virtual: sources are owned as std::unique_ptr<Source>.
  virtual ~Source() = default;

  // Call once. A restarted source would push from a new thread, which the
  // ring's single-producer check rejects in debug builds.
  virtual void start() = 0;
  // Stops producing and joins the thread. Safe to call more than once.
  virtual void stop() = 0;
  // The configured source id (1-65535).
  [[nodiscard]] virtual std::uint16_t id() const = 0;
  // Source type name, e.g. "sim_imu".
  [[nodiscard]] virtual std::string_view name() const = 0;
  // Relaxed snapshot; exact once stop() has returned.
  [[nodiscard]] virtual SourceCounters counters() const = 0;
};

// Produces one sample per period from a SampleModel.
//
// - seq is assigned when the sample is created, before try_push, so a sample
//   rejected by a full ring still consumes its seq and shows up downstream as
//   a gap (SPEC §4.1).
// - Pacing uses sleep_until on steady_clock against an absolute schedule, so
//   timing errors don't accumulate. If the thread falls more than one period
//   behind it skips ahead instead of bursting to catch up, and counts an
//   overrun.
// - The sleep is split into slices of at most kStopPollInterval so stop() is
//   prompt even for a 1 Hz source, without a mutex or condition variable on
//   the producer path (ADR-012).
// - A full ring is handled by the PushPolicy: drop_newest drops at once;
//   block_with_timeout spins briefly, then yields, until the sample fits or
//   the timeout passes (ADR-017). Blocking gives up early on stop(), so a long
//   timeout can't hold up shutdown.
class FixedRateSource final : public Source {
 public:
  static constexpr Clock::duration kStopPollInterval = std::chrono::milliseconds(10);
  // block_with_timeout: busy-wait this many times before yielding the CPU.
  static constexpr int kSpinsBeforeYield = 64;

  // Produces into `ring` at rate_hz using `model`, handling a full ring per
  // `push_policy`; nothing runs until start().
  FixedRateSource(std::uint16_t id, std::string_view name, double rate_hz, SampleRing& ring,
                  std::unique_ptr<SampleModel> model, PushPolicy push_policy = {});
  // Stops and joins the thread if still running.
  ~FixedRateSource() override;

  // Spawns the producer thread.
  void start() override;
  // Requests stop and joins the producer thread.
  void stop() override;
  // See Source::id().
  [[nodiscard]] std::uint16_t id() const override { return id_; }
  // See Source::name().
  [[nodiscard]] std::string_view name() const override { return name_; }
  // See Source::counters().
  [[nodiscard]] SourceCounters counters() const override;

 private:
  // Thread body: create, stamp, and push one sample per period.
  void run();
  // Pushes per the policy; false (and counted) if the sample was dropped.
  bool push(const Sample& sample);
  // Sleeps until deadline in kStopPollInterval slices; returns early on stop.
  void sleep_until_or_stop(Clock::time_point deadline) const;

  const std::uint16_t id_;
  const std::string name_;
  const double rate_hz_;
  const Clock::duration period_;
  SampleRing& ring_;
  std::unique_ptr<SampleModel> model_;
  const PushPolicy push_policy_;

  std::atomic<bool> stop_requested_{false};
  std::atomic<std::uint64_t> generated_{0};
  std::atomic<std::uint64_t> dropped_newest_{0};
  std::atomic<std::uint64_t> dropped_timeout_{0};
  std::atomic<std::uint64_t> blocked_{0};
  std::atomic<std::uint64_t> overruns_{0};
  std::thread thread_;
};

// Builds the source described by `config`, producing into `ring`. The model's
// noise is seeded from (seed, config.id), so runs are reproducible.
std::unique_ptr<Source> make_source(const SourceConfig& config, SampleRing& ring,
                                    std::uint64_t seed);

}  // namespace tstream
