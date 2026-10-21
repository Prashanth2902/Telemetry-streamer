#include "tstream/clock.hpp"
#include "tstream/monitors.hpp"

#include "monitor_util.hpp"

namespace tstream {

// Every source starts "last seen" at start_ns, so silence from the start counts.
StaleMonitor::StaleMonitor(std::span<const SourceHealthThresholds> thresholds,
                           std::int64_t start_ns)
    : ids_(detail::slot_ids(thresholds)), index_(detail::configured(ids_)), state_(index_.size()) {
  for (std::size_t i = 0; i < thresholds.size(); ++i) {
    state_[i + 1] = {.stale_ns = thresholds[i].stale_ns, .last_seen_ns = start_ns, .stale = false};
  }
}

// A sample from a stale source ends the outage.
void StaleMonitor::on_sample(const Sample& sample, std::int64_t now_ns) {
  const std::size_t slot = index_(sample.source_id);
  if (slot == 0) {
    return;
  }
  State& s = state_[slot];
  if (s.stale) {
    s.stale = false;
    static_cast<void>(events_.push(
        detail::make_event(ids_[slot], HealthMonitorKind::Stale, HealthSeverity::Cleared,
                           detail::clamp_detail((now_ns - s.last_seen_ns) / kNsPerMs))));
  }
  s.last_seen_ns = now_ns;
}

// O(sources) per tick; raises once per outage.
void StaleMonitor::on_tick(std::int64_t now_ns) {
  for (std::size_t slot = 1; slot < state_.size(); ++slot) {
    State& s = state_[slot];
    if (!s.stale && now_ns - s.last_seen_ns > s.stale_ns) {
      s.stale = true;
      static_cast<void>(events_.push(
          detail::make_event(ids_[slot], HealthMonitorKind::Stale, HealthSeverity::Critical,
                             detail::clamp_detail((now_ns - s.last_seen_ns) / kNsPerMs))));
    }
  }
}

}  // namespace tstream
