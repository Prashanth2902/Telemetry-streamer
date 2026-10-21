// The health monitors (SPEC §7). Each one keeps fixed per-source state, sized
// from the configured sources at construction, and a fixed event queue, so
// on_sample / on_tick / poll_event never allocate. Samples from ids that
// weren't configured (including the pipeline's own health samples) are
// ignored. Timestamps are the pipeline's steady_clock "now", in nanoseconds.

#pragma once

#include <array>
#include <cstdint>
#include <optional>
#include <span>
#include <vector>

#include "tstream/health.hpp"
#include "tstream/source_index.hpp"

namespace tstream {

// Critical when a source has sent nothing for its stale threshold; Cleared
// when it sends again. Detail: ms since the last sample (raise) or the length
// of the outage (clear). Measured from start_ns, so a source that never
// produces anything goes stale too.
class StaleMonitor final : public HealthMonitor {
 public:
  StaleMonitor(std::span<const SourceHealthThresholds> thresholds, std::int64_t start_ns);
  void on_sample(const Sample& sample, std::int64_t now_ns) override;
  void on_tick(std::int64_t now_ns) override;
  std::optional<HealthData> poll_event() override { return events_.pop(); }

 private:
  struct State {
    std::int64_t stale_ns = 0;
    std::int64_t last_seen_ns = 0;
    bool stale = false;
  };
  std::vector<std::uint16_t> ids_;  // by slot, for event source ids
  SourceIndex index_;
  std::vector<State> state_;  // by slot; [0] ("other") unused
  HealthEventQueue events_;
};

// Warning when the rate over the source's sliding window is outside
// ±rate_tolerance_pct of the configured rate; Cleared when it's back inside.
// The window is kBuckets equal buckets; each completed bucket slides it on
// and re-checks. Nothing is judged until one full window has passed. Detail:
// the measured rate in millihertz.
class RateMonitor final : public HealthMonitor {
 public:
  static constexpr std::size_t kBuckets = 10;

  RateMonitor(std::span<const SourceHealthThresholds> thresholds, std::int64_t start_ns);
  void on_sample(const Sample& sample, std::int64_t now_ns) override;
  void on_tick(std::int64_t now_ns) override;
  std::optional<HealthData> poll_event() override { return events_.pop(); }

 private:
  struct State {
    double expected_hz = 0;
    double tolerance_pct = 0;
    std::int64_t window_ns = 0;
    std::int64_t bucket_ns = 0;
    std::int64_t bucket_start_ns = 0;            // start of the bucket being filled
    std::uint32_t filling = 0;                   // samples in the bucket being filled
    std::array<std::uint32_t, kBuckets> done{};  // the last kBuckets completed buckets
    std::size_t next = 0;                        // where the next completed bucket goes
    std::size_t completed = 0;                   // completed buckets, up to kBuckets
    bool deviating = false;
  };
  // Closes every bucket that ended by now_ns, checking the window after each.
  void advance(std::size_t slot, std::int64_t now_ns);

  std::vector<std::uint16_t> ids_;
  SourceIndex index_;
  std::vector<State> state_;
  HealthEventQueue events_;
};

// Warning when a source's seq jumps, meaning samples were dropped before the
// pipeline (SPEC §4.1). Gaps are added up and reported at most once per
// kReportInterval per source, so a burst of drops is one event, not
// thousands. Detail: samples missing since the last report.
class SeqGapMonitor final : public HealthMonitor {
 public:
  static constexpr std::int64_t kReportIntervalNs = 100'000'000;  // 100 ms

  explicit SeqGapMonitor(std::span<const SourceHealthThresholds> thresholds);
  void on_sample(const Sample& sample, std::int64_t now_ns) override;
  void on_tick(std::int64_t now_ns) override;
  std::optional<HealthData> poll_event() override { return events_.pop(); }

 private:
  struct State {
    bool seen = false;
    std::uint32_t expected = 0;
    std::uint64_t pending_missing = 0;
    std::int64_t last_report_ns = 0;
  };
  std::vector<std::uint16_t> ids_;
  SourceIndex index_;
  std::vector<State> state_;
  HealthEventQueue events_;
};

// Warning when a source's payload is unchanged for frozen_count samples in a
// row (a stuck sensor); Cleared on the first change. Detail: the run length
// when raised.
class FrozenMonitor final : public HealthMonitor {
 public:
  explicit FrozenMonitor(std::span<const SourceHealthThresholds> thresholds);
  void on_sample(const Sample& sample, std::int64_t now_ns) override;
  // Purely sample-driven; nothing to do on ticks.
  void on_tick(std::int64_t /*now_ns*/) override {}
  std::optional<HealthData> poll_event() override { return events_.pop(); }

 private:
  struct State {
    std::uint32_t frozen_count = 0;
    bool has_last = false;
    Sample last{};
    std::uint32_t run = 0;  // samples in a row equal to `last`, including it
    bool frozen = false;
  };
  std::vector<std::uint16_t> ids_;
  SourceIndex index_;
  std::vector<State> state_;
  HealthEventQueue events_;
};

// True if the two samples carry the same kind and payload values.
bool same_payload(const Sample& a, const Sample& b);

}  // namespace tstream
