#include <algorithm>
#include <cmath>
#include <numeric>

#include "tstream/monitors.hpp"

#include "monitor_util.hpp"

namespace tstream {

// Buckets start at start_ns; the first verdict comes one full window later.
RateMonitor::RateMonitor(std::span<const SourceHealthThresholds> thresholds, std::int64_t start_ns)
    : ids_(detail::slot_ids(thresholds)), index_(detail::configured(ids_)), state_(index_.size()) {
  for (std::size_t i = 0; i < thresholds.size(); ++i) {
    State& s = state_[i + 1];
    s.expected_hz = thresholds[i].expected_rate_hz;
    s.tolerance_pct = thresholds[i].rate_tolerance_pct;
    s.bucket_ns = std::max<std::int64_t>(
        thresholds[i].rate_window_ns / static_cast<std::int64_t>(kBuckets), 1);
    s.bucket_start_ns = start_ns;
  }
}

// Counts into the current bucket, after closing any that already ended.
void RateMonitor::on_sample(const Sample& sample, std::int64_t now_ns) {
  const std::size_t slot = index_(sample.source_id);
  if (slot == 0) {
    return;
  }
  advance(slot, now_ns);
  ++state_[slot].filling;
}

// Time passing alone closes buckets, so a silent source drops to rate 0.
void RateMonitor::on_tick(std::int64_t now_ns) {
  for (std::size_t slot = 1; slot < state_.size(); ++slot) {
    advance(slot, now_ns);
  }
}

// The window is the last kBuckets completed buckets, so it covers exactly
// kBuckets x bucket_ns. Raise or clear only on a change of state.
void RateMonitor::advance(std::size_t slot, std::int64_t now_ns) {
  State& s = state_[slot];
  while (now_ns >= s.bucket_start_ns + s.bucket_ns) {
    s.done[s.next] = s.filling;  // overwrites the bucket that just left the window
    s.next = (s.next + 1) % kBuckets;
    s.filling = 0;
    s.bucket_start_ns += s.bucket_ns;
    if (s.completed < kBuckets) {
      ++s.completed;
    }
    if (s.completed < kBuckets) {
      continue;  // not a full window yet
    }
    const std::uint64_t in_window = std::accumulate(s.done.begin(), s.done.end(), std::uint64_t{0});
    const double window_s =
        static_cast<double>(s.bucket_ns * static_cast<std::int64_t>(kBuckets)) / 1e9;
    const double rate_hz = static_cast<double>(in_window) / window_s;
    const double deviation_pct = std::abs(rate_hz - s.expected_hz) / s.expected_hz * 100.0;
    const bool deviating = deviation_pct > s.tolerance_pct;
    if (deviating != s.deviating) {
      s.deviating = deviating;
      static_cast<void>(events_.push(
          detail::make_event(ids_[slot], HealthMonitorKind::Rate,
                             deviating ? HealthSeverity::Warning : HealthSeverity::Cleared,
                             detail::clamp_detail(std::llround(rate_hz * 1000.0)))));
    }
  }
}

}  // namespace tstream
