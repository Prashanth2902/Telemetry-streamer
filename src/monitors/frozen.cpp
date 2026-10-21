#include <algorithm>

#include "tstream/monitors.hpp"

#include "monitor_util.hpp"

namespace tstream {

// Field by field, per kind: comparing raw bytes would also compare padding.
bool same_payload(const Sample& a, const Sample& b) {
  if (a.kind != b.kind) {
    return false;
  }
  switch (a.kind) {
    case SampleKind::Imu:
      return a.imu.ax == b.imu.ax && a.imu.ay == b.imu.ay && a.imu.az == b.imu.az &&
             a.imu.gx == b.imu.gx && a.imu.gy == b.imu.gy && a.imu.gz == b.imu.gz;
    case SampleKind::Gps:
      return a.gps.lat == b.gps.lat && a.gps.lon == b.gps.lon && a.gps.alt_m == b.gps.alt_m &&
             a.gps.speed_mps == b.gps.speed_mps && a.gps.fix == b.gps.fix;
    case SampleKind::Battery:
      return a.bat.voltage_v == b.bat.voltage_v && a.bat.current_a == b.bat.current_a &&
             a.bat.soc_pct == b.bat.soc_pct && a.bat.temp_c == b.bat.temp_c;
    case SampleKind::Can:
      return a.can.can_id == b.can.can_id && a.can.dlc == b.can.dlc &&
             std::equal(std::begin(a.can.data), std::end(a.can.data), std::begin(b.can.data));
    case SampleKind::Health:
      return a.health.source_id == b.health.source_id && a.health.monitor == b.health.monitor &&
             a.health.severity == b.health.severity && a.health.detail == b.health.detail;
  }
  return false;
}

// Each source gets the configured run length.
FrozenMonitor::FrozenMonitor(std::span<const SourceHealthThresholds> thresholds)
    : ids_(detail::slot_ids(thresholds)), index_(detail::configured(ids_)), state_(index_.size()) {
  for (std::size_t i = 0; i < thresholds.size(); ++i) {
    state_[i + 1].frozen_count = thresholds[i].frozen_count;
  }
}

// Raises once when the run reaches frozen_count; clears on the first change.
void FrozenMonitor::on_sample(const Sample& sample, std::int64_t /*now_ns*/) {
  const std::size_t slot = index_(sample.source_id);
  if (slot == 0) {
    return;
  }
  State& s = state_[slot];
  if (s.has_last && same_payload(sample, s.last)) {
    ++s.run;
    if (!s.frozen && s.run >= s.frozen_count) {
      s.frozen = true;
      static_cast<void>(events_.push(detail::make_event(ids_[slot], HealthMonitorKind::Frozen,
                                                        HealthSeverity::Warning, s.run)));
    }
  } else {
    if (s.frozen) {
      s.frozen = false;
      static_cast<void>(events_.push(detail::make_event(ids_[slot], HealthMonitorKind::Frozen,
                                                        HealthSeverity::Cleared, s.run)));
    }
    s.run = 1;
  }
  s.last = sample;
  s.has_last = true;
}

}  // namespace tstream
