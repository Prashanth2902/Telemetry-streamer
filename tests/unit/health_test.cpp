// Health building blocks: names, the event queue, SourceIndex, and the
// per-source thresholds from SPEC §7 / §10.

#include "tstream/health.hpp"

#include <cstdint>
#include <vector>

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "tstream/config.hpp"
#include "tstream/source_index.hpp"

namespace {

using tstream::HealthData;
using tstream::HealthEventQueue;
using tstream::HealthMonitorKind;
using tstream::HealthSeverity;
using tstream::SourceIndex;

// An event with `detail`, so FIFO order is checkable.
HealthData event(std::uint32_t detail) {
  return {.source_id = 1, .monitor = 1, .severity = 1, .detail = detail};
}

TEST(HealthNames, MonitorAndSeverityNames) {
  EXPECT_EQ(tstream::monitor_name(static_cast<std::uint8_t>(HealthMonitorKind::Stale)), "stale");
  EXPECT_EQ(tstream::monitor_name(static_cast<std::uint8_t>(HealthMonitorKind::Rate)), "rate");
  EXPECT_EQ(tstream::monitor_name(static_cast<std::uint8_t>(HealthMonitorKind::SeqGap)), "seq_gap");
  EXPECT_EQ(tstream::monitor_name(static_cast<std::uint8_t>(HealthMonitorKind::Frozen)), "frozen");
  EXPECT_EQ(tstream::monitor_name(99), "unknown");
  EXPECT_EQ(tstream::severity_name(static_cast<std::uint8_t>(HealthSeverity::Cleared)), "cleared");
  EXPECT_EQ(tstream::severity_name(static_cast<std::uint8_t>(HealthSeverity::Critical)),
            "critical");
  EXPECT_EQ(tstream::severity_name(7), "unknown");
}

TEST(HealthEventQueue, FifoAndBoundedWithOverflowCount) {
  HealthEventQueue queue(3);
  EXPECT_FALSE(queue.pop().has_value());
  for (std::uint32_t i = 0; i < 5; ++i) {
    static_cast<void>(queue.push(event(i)));
  }
  EXPECT_EQ(queue.overflowed(), 2U);
  for (std::uint32_t i = 0; i < 3; ++i) {
    if (const auto e = queue.pop()) {
      EXPECT_EQ(e->detail, i);
    } else {
      ADD_FAILURE() << "queue empty at " << i;
    }
  }
  EXPECT_FALSE(queue.pop().has_value());
  EXPECT_TRUE(queue.push(event(9)));  // room again after draining
}

TEST(SourceIndex, MapsConfiguredIdsAndOther) {
  const std::vector<std::uint16_t> ids{7, 3, 65535};
  const SourceIndex index(ids);
  EXPECT_EQ(index.size(), 4U);
  EXPECT_EQ(index(7), 1U);
  EXPECT_EQ(index(3), 2U);
  EXPECT_EQ(index(65535), 3U);
  EXPECT_EQ(index(0), 0U);  // pipeline health samples: "other"
  EXPECT_EQ(index(42), 0U);
}

// The SPEC §10 example: thresholds per source from their periods, with the
// battery's stale override.
TEST(HealthThresholds, DerivedFromPeriodsWithOverrides) {
  const auto config = tstream::parse_config(R"({
    "sources": [
      { "id": 1, "type": "sim_imu",     "rate_hz": 1000, "ring_capacity": 4096, "policy": "drop_newest" },
      { "id": 2, "type": "sim_gps",     "rate_hz": 10,   "ring_capacity": 256,  "policy": "drop_newest" },
      { "id": 3, "type": "sim_battery", "rate_hz": 1,    "ring_capacity": 64,   "policy": "drop_newest",
        "health": { "stale_ms": 5000, "rate_tolerance_pct": 50 } }
    ],
    "batch": { "max_samples": 256, "max_delay_us": 2000 },
    "transport": { "host": "127.0.0.1", "port": 9000, "max_pending_batches": 64 },
    "health": { "stale_periods": 3, "stale_min_ms": 20, "rate_tolerance_pct": 20,
                "rate_window_ms": 1000, "rate_min_samples": 10, "frozen_count": 25 },
    "seed": 42
  })");
  const auto t = tstream::health_thresholds(config);
  ASSERT_EQ(t.size(), 3U);

  EXPECT_EQ(t[0].source_id, 1);
  EXPECT_EQ(t[0].stale_ns, 20'000'000);  // 3 x 1 ms is below the 20 ms floor
  EXPECT_EQ(t[0].rate_window_ns, 1'000'000'000);
  EXPECT_DOUBLE_EQ(t[0].rate_tolerance_pct, 20);
  EXPECT_EQ(t[0].frozen_count, 25U);

  EXPECT_EQ(t[1].stale_ns, 300'000'000);          // 3 x 100 ms
  EXPECT_EQ(t[1].rate_window_ns, 1'000'000'000);  // 10 x 100 ms = the 1 s default

  EXPECT_EQ(t[2].stale_ns, 5'000'000'000);         // override
  EXPECT_EQ(t[2].rate_window_ns, 10'000'000'000);  // 10 samples x 1 s
  EXPECT_DOUBLE_EQ(t[2].rate_tolerance_pct, 50);   // override
  EXPECT_DOUBLE_EQ(t[2].expected_rate_hz, 1);
}

}  // namespace
