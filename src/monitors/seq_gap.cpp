#include "tstream/monitors.hpp"

#include "monitor_util.hpp"

namespace tstream {

// No per-source thresholds apply; only the ids matter.
SeqGapMonitor::SeqGapMonitor(std::span<const SourceHealthThresholds> thresholds)
    : ids_(detail::slot_ids(thresholds)), index_(detail::configured(ids_)), state_(index_.size()) {}

// Wraparound-safe: a jump of less than 2^31 is a gap, anything else is a
// late or repeated sample (which can't happen through an SPSC ring, so it's
// ignored rather than reported).
void SeqGapMonitor::on_sample(const Sample& sample, std::int64_t /*now_ns*/) {
  const std::size_t slot = index_(sample.source_id);
  if (slot == 0) {
    return;
  }
  State& s = state_[slot];
  if (!s.seen) {
    s.seen = true;
    s.expected = sample.seq + 1;
    return;
  }
  const std::uint32_t ahead = sample.seq - s.expected;
  if (ahead != 0 && ahead < 0x8000'0000U) {
    s.pending_missing += ahead;
  }
  if (ahead < 0x8000'0000U) {
    s.expected = sample.seq + 1;
  }
}

// Rate-limited per source: one event per kReportIntervalNs at most.
void SeqGapMonitor::on_tick(std::int64_t now_ns) {
  for (std::size_t slot = 1; slot < state_.size(); ++slot) {
    State& s = state_[slot];
    if (s.pending_missing > 0 && now_ns - s.last_report_ns >= kReportIntervalNs) {
      report(slot, now_ns);
    }
  }
}

// Shutdown: nothing waits for the interval, so the counts add up exactly.
void SeqGapMonitor::on_stop(std::int64_t now_ns) {
  for (std::size_t slot = 1; slot < state_.size(); ++slot) {
    if (state_[slot].pending_missing > 0) {
      report(slot, now_ns);
    }
  }
}

// Clamped to the 32-bit detail field.
void SeqGapMonitor::report(std::size_t slot, std::int64_t now_ns) {
  State& s = state_[slot];
  static_cast<void>(events_.push(
      detail::make_event(ids_[slot], HealthMonitorKind::SeqGap, HealthSeverity::Warning,
                         detail::clamp_detail(static_cast<std::int64_t>(s.pending_missing)))));
  s.pending_missing = 0;
  s.last_report_ns = now_ns;
}

}  // namespace tstream
