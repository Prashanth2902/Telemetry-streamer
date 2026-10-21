// Sender stats: the lost-sample accounting and the stats/summary formats.

#include "tstream/stats.hpp"

#include <string>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace {

using ::testing::HasSubstr;
using ::testing::Not;
using tstream::SenderStats;
using tstream::SourceStats;
using tstream::TransportCounters;

// A source whose numbers add up: 100 generated = 3 + 2 ring drops + 95 delivered,
// and over TCP the 95 delivered = 90 sent + 5 dropped by the transport.
SourceStats balanced_source(std::uint16_t id) {
  SourceStats s;
  s.id = id;
  s.name = "sim_imu";
  s.counters.generated = 100;
  s.counters.dropped_newest = 3;
  s.counters.dropped_timeout = 2;
  s.delivered = 95;
  s.tx_sent = 90;
  s.tx_dropped = 5;
  return s;
}

TEST(Stats, NothingLostWhenEverySampleIsAccounted) {
  const SourceStats s = balanced_source(1);
  EXPECT_EQ(tstream::lost(s, /*has_transport=*/true), 0U);
  EXPECT_EQ(tstream::lost(s, /*has_transport=*/false), 0U);
}

TEST(Stats, LostCountsMissingSamples) {
  SourceStats s = balanced_source(1);
  s.tx_sent = 80;  // 10 neither sent nor dropped by the transport
  EXPECT_EQ(tstream::lost(s, true), 10U);
  s.delivered = 90;  // and 5 never reached the sink
  EXPECT_EQ(tstream::lost(s, false), 5U);
}

// Mid-run snapshots can briefly show more accounted than generated; that
// must read as 0, not wrap around.
TEST(Stats, LostSaturatesAtZero) {
  SourceStats s = balanced_source(1);
  s.counters.generated = 50;
  EXPECT_EQ(tstream::lost(s, true), 0U);
}

// One source over-accounted, one short: the shortfall must still show.
TEST(Stats, TotalLostIsPerSourceSoErrorsCantCancel) {
  SenderStats stats;
  stats.transport = TransportCounters{};
  SourceStats surplus = balanced_source(1);
  surplus.tx_sent = 100;  // 10 more than possible
  SourceStats shortfall = balanced_source(2);
  shortfall.tx_sent = 80;  // 10 missing
  stats.sources = {surplus, shortfall};
  EXPECT_EQ(tstream::total_lost(stats), 10U);
}

TEST(Stats, StatsLineShowsTotalsRateAndTransport) {
  SenderStats previous;
  previous.t_s = 1.0;
  previous.sources = {balanced_source(1)};
  SenderStats now = previous;
  now.t_s = 2.0;
  now.sources[0].counters.generated = 1100;  // 1000 more in 1 s
  now.batches = 7;
  TransportCounters t;
  t.queued = 4;
  t.queue_high_water = 9;
  now.transport = t;

  const std::string line = tstream::format_stats_line(now, previous);
  EXPECT_THAT(line, HasSubstr("stats t=2.0s generated=1100 rate=1000/s"));
  EXPECT_THAT(line, HasSubstr("batches=7"));
  EXPECT_THAT(line, HasSubstr("queued=4 queue_high_water=9"));

  now.transport.reset();
  EXPECT_THAT(tstream::format_stats_line(now, previous), Not(HasSubstr("queued=")));
}

TEST(Stats, SummaryHasPerSourceTotalAndTransportLines) {
  SenderStats stats;
  stats.sources = {balanced_source(1), balanced_source(2)};
  stats.batches = 12;
  stats.transport = TransportCounters{};

  const auto lines = tstream::format_summary(stats);
  ASSERT_EQ(lines.size(), 4U);
  EXPECT_THAT(lines[0], HasSubstr("summary source=1 type=sim_imu generated=100"));
  EXPECT_THAT(lines[0], HasSubstr("dropped_newest=3 dropped_timeout=2"));
  EXPECT_THAT(lines[0], HasSubstr("tx_sent=90 tx_dropped=5 lost=0"));
  EXPECT_THAT(lines[2], HasSubstr("summary total generated=200 dropped=10 delivered=190"));
  EXPECT_THAT(lines[2], HasSubstr("lost=0 batches=12"));
  EXPECT_THAT(lines[3], HasSubstr("summary transport "));

  stats.transport.reset();  // stdout sink: no transport line
  EXPECT_EQ(tstream::format_summary(stats).size(), 3U);
}

}  // namespace
