#pragma once

#include <chrono>
#include <cstdint>

namespace tstream {

// All timestamps are steady_clock nanoseconds (SPEC §4.4). On Linux this is
// CLOCK_MONOTONIC, shared by every process on the host, so a receiver on the
// same machine can subtract a sender's timestamp directly.
using Clock = std::chrono::steady_clock;

// Nanoseconds since the steady_clock epoch.
inline std::int64_t to_ns(Clock::time_point t) {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(t.time_since_epoch()).count();
}

// Current steady_clock time in nanoseconds: the timestamp used everywhere.
inline std::int64_t now_ns() {
  return to_ns(Clock::now());
}

}  // namespace tstream
