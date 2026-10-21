// Each health monitor driven with synthetic timestamps (SPEC §13 Unit):
// no threads, no sleeps, exact expectations.

#include "tstream/monitors.hpp"

#include <algorithm>
#include <cstdint>
#include <vector>

#include <gtest/gtest.h>

namespace {

using tstream::FrozenMonitor;
using tstream::HealthData;
using tstream::HealthMonitor;
using tstream::HealthMonitorKind;
using tstream::HealthSeverity;
using tstream::RateMonitor;
using tstream::Sample;
using tstream::SampleKind;
using tstream::SeqGapMonitor;
using tstream::SourceHealthThresholds;
using tstream::StaleMonitor;

constexpr std::int64_t kMs = 1'000'000;
constexpr std::int64_t kSec = 1'000'000'000;

// Source 1 at 100 Hz: stale after 50 ms, 1 s rate window, ±20%, frozen at 5.
// Source 2 at 1 Hz: stale after 3 s, 10 s window.
std::vector<SourceHealthThresholds> thresholds() {
  return {{.source_id = 1,
           .expected_rate_hz = 100,
           .stale_ns = 50 * kMs,
           .rate_window_ns = kSec,
           .rate_tolerance_pct = 20,
           .frozen_count = 5},
          {.source_id = 2,
           .expected_rate_hz = 1,
           .stale_ns = 3 * kSec,
           .rate_window_ns = 10 * kSec,
           .rate_tolerance_pct = 20,
           .frozen_count = 5}};
}

// An IMU sample from `source` with seq and a payload derived from `value`.
Sample sample(std::uint16_t source, std::uint32_t seq, float value = 0) {
  Sample s{};
  s.source_id = source;
  s.kind = SampleKind::Imu;
  s.seq = seq;
  s.imu = {.ax = value, .ay = 0, .az = 9.81F, .gx = 0, .gy = 0, .gz = 0};
  return s;
}

// Everything the monitor has queued.
std::vector<HealthData> drain(HealthMonitor& monitor) {
  std::vector<HealthData> out;
  while (const auto e = monitor.poll_event()) {
    out.push_back(*e);
  }
  return out;
}

// Checks one event's fields.
void expect_event(const HealthData& e, std::uint16_t source, HealthMonitorKind monitor,
                  HealthSeverity severity) {
  EXPECT_EQ(e.source_id, source);
  EXPECT_EQ(e.monitor, static_cast<std::uint8_t>(monitor));
  EXPECT_EQ(e.severity, static_cast<std::uint8_t>(severity));
}

// Feeds `count` samples from `source` every `period_ns` from `start`, ticking
// after each; returns the time after the last one.
std::int64_t feed(HealthMonitor& m, std::uint16_t source, std::uint32_t first_seq,
                  std::uint32_t count, std::int64_t start, std::int64_t period_ns) {
  std::int64_t t = start;
  for (std::uint32_t i = 0; i < count; ++i) {
    m.on_sample(sample(source, first_seq + i, static_cast<float>(i)), t);
    m.on_tick(t);
    t += period_ns;
  }
  return t;
}

// --- Stale --------------------------------------------------------------------

TEST(StaleMonitor, RaisesOnceAfterThresholdAndClearsOnResume) {
  const auto t = thresholds();
  StaleMonitor m(t, 0);
  m.on_sample(sample(1, 0), 0);
  m.on_sample(sample(2, 0), 0);
  m.on_tick(50 * kMs);  // exactly the threshold: not yet
  EXPECT_TRUE(drain(m).empty());
  m.on_tick(51 * kMs);
  auto events = drain(m);
  ASSERT_EQ(events.size(), 1U);
  expect_event(events[0], 1, HealthMonitorKind::Stale, HealthSeverity::Critical);
  EXPECT_EQ(events[0].detail, 51U);  // ms since the last sample

  m.on_tick(500 * kMs);  // still stale: no repeat
  EXPECT_TRUE(drain(m).empty());

  m.on_sample(sample(1, 1), 800 * kMs);
  events = drain(m);
  ASSERT_EQ(events.size(), 1U);
  expect_event(events[0], 1, HealthMonitorKind::Stale, HealthSeverity::Cleared);
  EXPECT_EQ(events[0].detail, 800U);  // the outage
}

TEST(StaleMonitor, SourceThatNeverStartsGoesStale) {
  const auto t = thresholds();
  StaleMonitor m(t, 10 * kSec);
  m.on_tick(10 * kSec + 49 * kMs);
  EXPECT_TRUE(drain(m).empty());
  m.on_tick(10 * kSec + 51 * kMs);
  const auto events = drain(m);
  ASSERT_EQ(events.size(), 1U);
  expect_event(events[0], 1, HealthMonitorKind::Stale, HealthSeverity::Critical);
}

TEST(StaleMonitor, ThresholdsArePerSource) {
  const auto t = thresholds();
  StaleMonitor m(t, 0);
  // Source 1 keeps sending; source 2 (1 Hz, 3 s threshold) sends once.
  m.on_sample(sample(2, 0), 0);
  const std::int64_t end = feed(m, 1, 0, 250, 0, 10 * kMs);  // 2.5 s at 100 Hz
  EXPECT_TRUE(drain(m).empty()) << "a 1 Hz source isn't stale after 2.5 s";
  m.on_sample(sample(1, 250), end);
  m.on_tick(3 * kSec + kMs);
  const auto events = drain(m);
  ASSERT_EQ(events.size(), 2U);  // both: source 1 stopped at 2.5 s too
  EXPECT_EQ(events[0].source_id, 1);
  EXPECT_EQ(events[1].source_id, 2);
}

TEST(StaleMonitor, IgnoresUnconfiguredSources) {
  const auto t = thresholds();
  StaleMonitor m(t, 0);
  m.on_sample(sample(0, 0), 0);  // a health sample
  m.on_sample(sample(9, 0), 0);
  m.on_sample(sample(1, 0), 0);
  m.on_sample(sample(2, 0), 0);
  m.on_tick(10 * kMs);
  EXPECT_TRUE(drain(m).empty());
}

// --- Rate ---------------------------------------------------------------------

TEST(RateMonitor, QuietAtExpectedRateWithJitter) {
  const auto t = thresholds();
  RateMonitor m(t, 0);
  // 3 s of source 1 at 100 Hz, every 4th sample 3 ms late.
  std::int64_t time = 0;
  for (std::uint32_t i = 0; i < 300; ++i) {
    const std::int64_t jitter = i % 4 == 0 ? 3 * kMs : 0;
    m.on_sample(sample(1, i), time + jitter);
    m.on_tick(time + jitter);
    time += 10 * kMs;
  }
  for (const auto& e : drain(m)) {
    EXPECT_NE(e.source_id, 1) << "source 1 is at its expected rate";
  }
}

TEST(RateMonitor, NoVerdictBeforeOneFullWindow) {
  const auto t = thresholds();
  RateMonitor m(t, 0);
  feed(m, 1, 0, 40, 0, 20 * kMs);  // 50 Hz for 0.8 s: below a full 1 s window
  for (const auto& e : drain(m)) {
    EXPECT_NE(e.source_id, 1);
  }
}

TEST(RateMonitor, RaisesOnHalfRateAndClearsOnRecovery) {
  const auto t = thresholds();
  RateMonitor m(t, 0);
  std::int64_t time = feed(m, 1, 0, 100, 0, 10 * kMs);  // 1 s at 100 Hz
  time = feed(m, 1, 100, 100, time, 20 * kMs);          // 2 s at 50 Hz
  auto events = drain(m);
  const auto raised =
      std::ranges::find_if(events, [](const HealthData& e) { return e.source_id == 1; });
  ASSERT_NE(raised, events.end());
  expect_event(*raised, 1, HealthMonitorKind::Rate, HealthSeverity::Warning);
  // Raised as the sliding window first leaves ±20%, on its way down to 50 Hz.
  EXPECT_LT(raised->detail, 80'000U);  // millihertz
  EXPECT_GE(raised->detail, 50'000U);

  feed(m, 1, 200, 200, time, 10 * kMs);  // back to 100 Hz for 2 s
  events = drain(m);
  ASSERT_FALSE(events.empty());
  expect_event(events.back(), 1, HealthMonitorKind::Rate, HealthSeverity::Cleared);
}

// A source that stops entirely has rate 0: ticks alone close the buckets.
TEST(RateMonitor, SilenceAloneRaises) {
  const auto t = thresholds();
  RateMonitor m(t, 0);
  const std::int64_t time = feed(m, 1, 0, 100, 0, 10 * kMs);
  m.on_tick(time + 2 * kSec);
  bool raised = false;
  for (const auto& e : drain(m)) {
    if (e.source_id == 1 && e.severity == static_cast<std::uint8_t>(HealthSeverity::Warning)) {
      raised = true;
      EXPECT_LT(e.detail, 80'000U);  // first window below the band
    }
  }
  EXPECT_TRUE(raised);
}

// --- Seq gap ------------------------------------------------------------------

TEST(SeqGapMonitor, ReportsMissingCountExactly) {
  const auto t = thresholds();
  SeqGapMonitor m(t);
  for (const std::uint32_t seq : {0U, 1U, 2U, 7U, 8U}) {  // 3..6 missing
    m.on_sample(sample(1, seq), 0);
  }
  m.on_tick(200 * kMs);
  const auto events = drain(m);
  ASSERT_EQ(events.size(), 1U);
  expect_event(events[0], 1, HealthMonitorKind::SeqGap, HealthSeverity::Warning);
  EXPECT_EQ(events[0].detail, 4U);
}

TEST(SeqGapMonitor, CoalescesGapsWithinTheReportInterval) {
  const auto t = thresholds();
  SeqGapMonitor m(t);
  m.on_sample(sample(1, 0), 0);
  m.on_sample(sample(1, 5), 0);  // 4 missing
  m.on_tick(200 * kMs);
  ASSERT_EQ(drain(m).size(), 1U);

  m.on_sample(sample(1, 10), 210 * kMs);  // 4 more
  m.on_tick(210 * kMs);
  m.on_sample(sample(1, 20), 250 * kMs);  // 9 more
  m.on_tick(250 * kMs);
  EXPECT_TRUE(drain(m).empty()) << "within 100 ms of the last report";
  m.on_tick(300 * kMs);
  const auto events = drain(m);
  ASSERT_EQ(events.size(), 1U);
  EXPECT_EQ(events[0].detail, 13U);
}

TEST(SeqGapMonitor, StopReportsWhatTheIntervalHeldBack) {
  const auto t = thresholds();
  SeqGapMonitor m(t);
  m.on_sample(sample(1, 0), 0);
  m.on_sample(sample(1, 3), 0);
  m.on_tick(200 * kMs);
  ASSERT_EQ(drain(m).size(), 1U);
  m.on_sample(sample(1, 10), 210 * kMs);  // 6 more, 10 ms after the report
  m.on_stop(220 * kMs);
  const auto events = drain(m);
  ASSERT_EQ(events.size(), 1U);
  EXPECT_EQ(events[0].detail, 6U);
}

TEST(SeqGapMonitor, NoEventWithoutGapsAndHandlesWraparound) {
  const auto t = thresholds();
  SeqGapMonitor m(t);
  for (const std::uint32_t seq : {0xFFFF'FFFEU, 0xFFFF'FFFFU, 0U, 1U}) {
    m.on_sample(sample(1, seq), 0);
  }
  m.on_sample(sample(0, 5), 0);  // health samples have their own seq: ignored
  m.on_sample(sample(0, 9), 0);
  m.on_tick(kSec);
  EXPECT_TRUE(drain(m).empty());
}

// --- Frozen -------------------------------------------------------------------

TEST(FrozenMonitor, RaisesAfterFrozenCountIdenticalPayloads) {
  const auto t = thresholds();
  FrozenMonitor m(t);
  for (std::uint32_t i = 0; i < 4; ++i) {
    m.on_sample(sample(1, i, 1.5F), 0);  // run of 4: below 5
  }
  EXPECT_TRUE(drain(m).empty());
  m.on_sample(sample(1, 4, 1.5F), 0);  // run of 5
  auto events = drain(m);
  ASSERT_EQ(events.size(), 1U);
  expect_event(events[0], 1, HealthMonitorKind::Frozen, HealthSeverity::Warning);
  EXPECT_EQ(events[0].detail, 5U);

  for (std::uint32_t i = 5; i < 20; ++i) {
    m.on_sample(sample(1, i, 1.5F), 0);  // still frozen: no repeat
  }
  EXPECT_TRUE(drain(m).empty());

  m.on_sample(sample(1, 20, 2.0F), 0);
  events = drain(m);
  ASSERT_EQ(events.size(), 1U);
  expect_event(events[0], 1, HealthMonitorKind::Frozen, HealthSeverity::Cleared);
  EXPECT_EQ(events[0].detail, 20U);  // how long it was frozen
}

TEST(FrozenMonitor, ChangingPayloadNeverRaises) {
  const auto t = thresholds();
  FrozenMonitor m(t);
  for (std::uint32_t i = 0; i < 100; ++i) {
    m.on_sample(sample(1, i, static_cast<float>(i % 3)), 0);
  }
  EXPECT_TRUE(drain(m).empty());
}

TEST(SamePayload, ComparesKindAndValues) {
  EXPECT_TRUE(tstream::same_payload(sample(1, 0, 1), sample(2, 9, 1)));  // header ignored
  EXPECT_FALSE(tstream::same_payload(sample(1, 0, 1), sample(1, 0, 2)));
  Sample gps = sample(1, 0, 1);
  gps.kind = SampleKind::Gps;
  EXPECT_FALSE(tstream::same_payload(sample(1, 0, 1), gps));
}

}  // namespace
