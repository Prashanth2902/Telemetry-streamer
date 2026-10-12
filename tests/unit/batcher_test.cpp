// Batcher flush rules with synthetic timestamps (no threads, no sleeps).

#include <cstdint>
#include <stdexcept>

#include <gtest/gtest.h>

#include "tstream/batcher.hpp"

namespace {

using tstream::Batcher;
using tstream::Sample;

constexpr std::int64_t kDelayNs = 2'000'000;  // 2 ms

void add(Batcher& b, std::uint32_t seq, std::int64_t now_ns) {
  ASSERT_GT(b.free_count(), 0U);
  b.free_slots()->seq = seq;
  b.commit(1, now_ns);
}

TEST(Batcher, StartsEmptyAndNeverDue) {
  const Batcher b(4, kDelayNs);
  EXPECT_TRUE(b.empty());
  EXPECT_FALSE(b.full());
  EXPECT_EQ(b.free_count(), 4U);
  EXPECT_FALSE(b.due(1'000'000'000));
}

TEST(Batcher, IsFullAtMaxSamples) {
  Batcher b(3, kDelayNs);
  add(b, 0, 0);
  add(b, 1, 0);
  EXPECT_FALSE(b.full());
  add(b, 2, 0);
  EXPECT_TRUE(b.full());
  EXPECT_EQ(b.free_count(), 0U);
}

TEST(Batcher, BecomesDueMaxDelayAfterFirstSample) {
  Batcher b(100, kDelayNs);
  add(b, 0, 1000);
  add(b, 1, 1000 + kDelayNs - 1);  // later samples don't reset the timer
  EXPECT_FALSE(b.due(1000 + kDelayNs - 1));
  EXPECT_TRUE(b.due(1000 + kDelayNs));
  EXPECT_EQ(b.deadline_ns(), 1000 + kDelayNs);
}

TEST(Batcher, ClearResetsSizeAndTimer) {
  Batcher b(4, kDelayNs);
  add(b, 0, 0);
  b.clear();
  EXPECT_TRUE(b.empty());
  EXPECT_FALSE(b.due(10 * kDelayNs));
  add(b, 1, 5 * kDelayNs);  // new batch, new timer
  EXPECT_FALSE(b.due(5 * kDelayNs + 1));
  EXPECT_TRUE(b.due(6 * kDelayNs));
}

TEST(Batcher, SamplesViewHoldsCommittedSamplesInOrder) {
  Batcher b(4, kDelayNs);
  add(b, 10, 0);
  add(b, 11, 0);
  const auto samples = b.samples();
  ASSERT_EQ(samples.size(), 2U);
  EXPECT_EQ(samples[0].seq, 10U);
  EXPECT_EQ(samples[1].seq, 11U);
}

TEST(Batcher, CommitOfZeroDoesNotStartTimer) {
  Batcher b(4, kDelayNs);
  b.commit(0, 0);
  EXPECT_TRUE(b.empty());
  add(b, 0, 10 * kDelayNs);
  EXPECT_FALSE(b.due(10 * kDelayNs + 1));
}

TEST(Batcher, RejectsZeroCapacity) { EXPECT_THROW(Batcher(0, kDelayNs), std::invalid_argument); }

}  // namespace
