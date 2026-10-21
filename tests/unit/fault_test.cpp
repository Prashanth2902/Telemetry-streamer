// Fault syntax and timing (SPEC §8).

#include "tstream/fault.hpp"

#include <stdexcept>
#include <string>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace {

using ::testing::HasSubstr;
using tstream::Fault;
using tstream::FaultKind;
using tstream::parse_fault;

constexpr std::int64_t kMs = 1'000'000;

// Expect parse_fault to throw std::invalid_argument containing `needle`.
void expect_invalid(const std::string& text, const std::string& needle) {
  try {
    static_cast<void>(parse_fault(text));
    FAIL() << "expected \"" << text << "\" to be rejected";
  } catch (const std::invalid_argument& e) {
    EXPECT_THAT(e.what(), HasSubstr(needle)) << text;
  }
}

TEST(ParseFault, ParsesEachKind) {
  const Fault stall = parse_fault("stall:1:500@2000");
  EXPECT_EQ(stall.kind, FaultKind::Stall);
  EXPECT_EQ(stall.source_id, 1);
  EXPECT_EQ(stall.arg, 500);
  EXPECT_EQ(stall.start_ns, 2000 * kMs);

  const Fault burst = parse_fault("burst:2:10000");
  EXPECT_EQ(burst.kind, FaultKind::Burst);
  EXPECT_EQ(burst.arg, 10000);
  EXPECT_EQ(burst.start_ns, 0);

  const Fault rate = parse_fault("rate:1:0.5@1000+3000");
  EXPECT_EQ(rate.kind, FaultKind::Rate);
  EXPECT_EQ(rate.arg, 0.5);
  EXPECT_EQ(rate.start_ns, 1000 * kMs);
  EXPECT_EQ(rate.duration_ns, 3000 * kMs);

  const Fault freeze = parse_fault("freeze:3@500");
  EXPECT_EQ(freeze.kind, FaultKind::Freeze);
  EXPECT_EQ(freeze.source_id, 3);
  EXPECT_FALSE(freeze.duration_ns.has_value());

  const Fault slow = parse_fault("slow_consumer:25");
  EXPECT_EQ(slow.kind, FaultKind::SlowConsumer);
  EXPECT_EQ(slow.source_id, 0);
  EXPECT_EQ(slow.arg, 25);
}

TEST(ParseFault, RejectsMalformedText) {
  expect_invalid("explode:1:5", "unknown kind");
  expect_invalid("stall", "source id");
  expect_invalid("stall:0:100", "source id");
  expect_invalid("stall:70000:100", "source id");
  expect_invalid("stall:1.5:100", "integer");
  expect_invalid("stall:1", "stall ms");
  expect_invalid("stall:1:abc", "stall ms");
  expect_invalid("stall:1:100:9", "stall ms");
  expect_invalid("burst:1:2.5", "integer");
  expect_invalid("rate:1:0", "rate factor");
  expect_invalid("rate:1:1000", "rate factor");
  expect_invalid("freeze:1:7", "no argument");
  expect_invalid("stall:1:100@", "start ms");
  expect_invalid("stall:1:100@-5", "start ms");
  expect_invalid("stall:1:100@0+50", "only applies to rate and freeze");
  expect_invalid("rate:1:2@0+", "duration ms");
  expect_invalid("freeze:1:", "empty field");
  expect_invalid("stall:1:100:", "empty field");
  expect_invalid("slow_consumer:1.5", "integer");
  expect_invalid("stall:1:0", "[1, 3600000]");
  expect_invalid("slow_consumer", "slow_consumer:<ms>");
  expect_invalid("slow_consumer:5@100", "slow_consumer:<ms>");
}

TEST(Fault, StallIsActiveForItsLength) {
  const Fault f = parse_fault("stall:1:100@50");
  EXPECT_FALSE(f.active_at(49 * kMs));
  EXPECT_TRUE(f.active_at(50 * kMs));
  EXPECT_TRUE(f.active_at(149 * kMs));
  EXPECT_FALSE(f.active_at(150 * kMs));
  EXPECT_EQ(f.end_ns(), 150 * kMs);
}

TEST(Fault, RateAndFreezeLastUntilDurationOrForever) {
  const Fault bounded = parse_fault("rate:1:2@100+200");
  EXPECT_TRUE(bounded.active_at(299 * kMs));
  EXPECT_FALSE(bounded.active_at(300 * kMs));

  const Fault forever = parse_fault("freeze:1@100");
  EXPECT_FALSE(forever.active_at(99 * kMs));
  EXPECT_TRUE(forever.active_at(1'000'000 * kMs));
  EXPECT_FALSE(forever.end_ns().has_value());
}

TEST(Fault, BurstIsNeverActive) {
  const Fault f = parse_fault("burst:1:10@0");
  EXPECT_FALSE(f.active_at(0));
  EXPECT_FALSE(f.active_at(kMs));
}

}  // namespace
