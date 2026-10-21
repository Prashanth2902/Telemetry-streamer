#include "tstream/health.hpp"

#include <algorithm>
#include <cmath>

#include "tstream/clock.hpp"

namespace tstream {

namespace {}  // namespace

// Unknown values read as "unknown" rather than failing: they come off the wire.
std::string_view monitor_name(std::uint8_t monitor) {
  switch (static_cast<HealthMonitorKind>(monitor)) {
    case HealthMonitorKind::Stale:
      return "stale";
    case HealthMonitorKind::Rate:
      return "rate";
    case HealthMonitorKind::SeqGap:
      return "seq_gap";
    case HealthMonitorKind::Frozen:
      return "frozen";
  }
  return "unknown";
}

// Unknown values read as "unknown" rather than failing: they come off the wire.
std::string_view severity_name(std::uint8_t severity) {
  switch (static_cast<HealthSeverity>(severity)) {
    case HealthSeverity::Cleared:
      return "cleared";
    case HealthSeverity::Warning:
      return "warning";
    case HealthSeverity::Critical:
      return "critical";
  }
  return "unknown";
}

// Ring buffer over the preallocated vector.
bool HealthEventQueue::push(const HealthData& event) {
  if (count_ == events_.size()) {
    ++overflowed_;
    return false;
  }
  events_[(head_ + count_) % events_.size()] = event;
  ++count_;
  return true;
}

// FIFO order.
std::optional<HealthData> HealthEventQueue::pop() {
  if (count_ == 0) {
    return std::nullopt;
  }
  const HealthData event = events_[head_];
  head_ = (head_ + 1) % events_.size();
  --count_;
  return event;
}

// SPEC §7 formulas; a source's own health block overrides with absolute values.
std::vector<SourceHealthThresholds> health_thresholds(const Config& config) {
  const HealthConfig& h = config.health;
  std::vector<SourceHealthThresholds> out;
  out.reserve(config.sources.size());
  for (const SourceConfig& s : config.sources) {
    const double period_ms = 1000.0 / s.rate_hz;
    SourceHealthThresholds t;
    t.source_id = s.id;
    t.expected_rate_hz = s.rate_hz;
    t.stale_ns =
        ms_to_ns(s.health.stale_ms.value_or(std::max(h.stale_min_ms, h.stale_periods * period_ms)));
    t.rate_window_ns = ms_to_ns(s.health.rate_window_ms.value_or(
        std::max(h.rate_window_ms, static_cast<double>(h.rate_min_samples) * period_ms)));
    t.rate_tolerance_pct = s.health.rate_tolerance_pct.value_or(h.rate_tolerance_pct);
    t.frozen_count = static_cast<std::uint32_t>(h.frozen_count);
    out.push_back(t);
  }
  return out;
}

}  // namespace tstream
