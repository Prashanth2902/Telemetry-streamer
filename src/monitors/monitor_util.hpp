// Internal helpers shared by the monitor implementations.

#pragma once

#include <cstdint>
#include <span>
#include <utility>
#include <vector>

#include "tstream/health.hpp"

namespace tstream::detail {

// Source id per SourceIndex slot: slot 0 is "other" (id 0), slot i+1 is
// thresholds[i]. Monitors keep this to put the right id in their events.
inline std::vector<std::uint16_t> slot_ids(std::span<const SourceHealthThresholds> thresholds) {
  std::vector<std::uint16_t> ids{0};
  ids.reserve(thresholds.size() + 1);
  for (const auto& t : thresholds) {
    ids.push_back(t.source_id);
  }
  return ids;
}

// The configured ids alone (slots 1..N), for building a SourceIndex.
inline std::span<const std::uint16_t> configured(const std::vector<std::uint16_t>& slot_ids) {
  return std::span(slot_ids).subspan(1);
}

// One event with the monitor's kind filled in.
inline HealthData make_event(std::uint16_t source_id, HealthMonitorKind monitor,
                             HealthSeverity severity, std::uint32_t detail) {
  return {.source_id = source_id,
          .monitor = static_cast<std::uint8_t>(monitor),
          .severity = static_cast<std::uint8_t>(severity),
          .detail = detail};
}

// Clamps a count or duration into the 32-bit detail field.
inline std::uint32_t clamp_detail(std::int64_t value) {
  if (value < 0) {
    return 0;
  }
  return std::cmp_greater(value, 0xFFFF'FFFFU) ? 0xFFFF'FFFFU : static_cast<std::uint32_t>(value);
}

}  // namespace tstream::detail
