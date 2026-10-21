// Health monitoring building blocks (SPEC §7): the pluggable monitor
// interface, event kinds, a fixed-capacity event queue, and per-source
// thresholds derived from the config.
//
// Monitors run on the pipeline thread. They raise an event when a condition
// starts (Warning or Critical) and a Cleared event when it ends, so the
// stream carries the state changes rather than one event per sample.

#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>
#include <vector>

#include "tstream/config.hpp"
#include "tstream/sample.hpp"

namespace tstream {

// Which monitor raised an event (HealthData::monitor).
enum class HealthMonitorKind : std::uint8_t { Stale = 1, Rate = 2, SeqGap = 3, Frozen = 4 };

// How bad it is (HealthData::severity). Cleared means the condition ended.
enum class HealthSeverity : std::uint8_t { Cleared = 0, Warning = 1, Critical = 2 };

// Short names for logs and receiver output, e.g. "stale", "warning".
std::string_view monitor_name(std::uint8_t monitor);
std::string_view severity_name(std::uint8_t severity);

// SPEC §7: a pluggable monitor. on_sample sees every sample the pipeline
// drains, on_tick runs on every pipeline loop iteration for time-based checks,
// and poll_event hands back queued events without allocating.
struct HealthMonitor {
  virtual ~HealthMonitor() = default;
  virtual void on_sample(const Sample& sample, std::int64_t now_ns) = 0;
  virtual void on_tick(std::int64_t now_ns) = 0;
  virtual std::optional<HealthData> poll_event() = 0;
};

// Fixed-capacity FIFO of events, allocated once. When full, new events are
// dropped and counted rather than growing the queue on the hot path.
class HealthEventQueue {
 public:
  // Room for `capacity` events.
  explicit HealthEventQueue(std::size_t capacity = 64) : events_(capacity) {}

  // Adds an event; false (and counted) if the queue is full.
  bool push(const HealthData& event);
  // The oldest event, if any.
  std::optional<HealthData> pop();
  // Events dropped because the queue was full.
  [[nodiscard]] std::uint64_t overflowed() const { return overflowed_; }

 private:
  std::vector<HealthData> events_;
  std::size_t head_ = 0;
  std::size_t count_ = 0;
  std::uint64_t overflowed_ = 0;
};

// Effective health thresholds for one source (SPEC §7, §10).
struct SourceHealthThresholds {
  std::uint16_t source_id = 0;
  double expected_rate_hz = 0;
  std::int64_t stale_ns = 0;        // max(stale_min_ms, stale_periods x period), or override
  std::int64_t rate_window_ns = 0;  // max(rate_window_ms, rate_min_samples x period), or override
  double rate_tolerance_pct = 0;    // global, or override
  std::uint32_t frozen_count = 0;   // identical payloads in a row before "frozen"
};

// One entry per configured source, in config order.
std::vector<SourceHealthThresholds> health_thresholds(const Config& config);

}  // namespace tstream
