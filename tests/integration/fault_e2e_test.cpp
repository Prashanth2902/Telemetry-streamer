// One test per SPEC §8 fault, end to end: the real tstream with --fault, the
// real tstream-recv, and the health event or drop counter each fault must
// produce (M5 acceptance).

#include <chrono>
#include <cstdint>
#include <string>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "e2e_fixture.hpp"
#include "process.hpp"

namespace {

using namespace std::chrono_literals;
using ::testing::HasSubstr;
using tstream::testing::ChildProcess;
using tstream::testing::field;
using tstream::testing::FieldMap;
using tstream::testing::find_line;
using tstream::testing::health_lines;
using tstream::testing::key_values;
using tstream::testing::kReceiver;
using tstream::testing::kSender;
using tstream::testing::per_source;
using tstream::testing::read_file;

class FaultE2eTest : public tstream::testing::SenderReceiverTest {
 protected:
  // Runs the sender with `config`, each of `faults` as a --fault flag, for
  // `seconds`; returns its exit code.
  int run_with_faults(const nlohmann::json& config, const std::vector<std::string>& faults,
                      const std::string& seconds) {
    std::vector<std::string> args{"--config", write_config(config).string(), "--duration", seconds};
    for (const std::string& fault : faults) {
      args.insert(args.end(), {"--fault", fault});
    }
    ChildProcess sender(kSender, args, dir_ / "send.out", dir_ / "send.err");
    return sender.wait(120s);
  }

  // Starts a receiver, runs the sender with `faults`, and returns the
  // receiver's stdout once everything has arrived.
  std::string run_scenario(nlohmann::json config, const std::vector<std::string>& faults,
                           const std::string& seconds) {
    std::unique_ptr<ChildProcess> receiver;
    const std::uint16_t port = start_receiver(receiver, 0);
    config["transport"]["port"] = port;
    EXPECT_EQ(run_with_faults(config, faults, seconds), 0) << read_file(dir_ / "send.err");
    std::this_thread::sleep_for(300ms);  // let the last bytes arrive
    return stop_receiver(*receiver);
  }
};

// The health events about `source` from `monitor`, in arrival order.
std::vector<FieldMap> events_for(const std::string& recv_out, const std::string& source,
                                 const std::string& monitor) {
  std::vector<FieldMap> out;
  for (auto& e : health_lines(recv_out)) {
    if (e["source"] == source && e["monitor"] == monitor) {
      out.push_back(e);
    }
  }
  return out;
}

// Raised exactly once, then cleared, in that order.
void expect_raised_then_cleared(const std::vector<FieldMap>& events, const std::string& severity) {
  ASSERT_EQ(events.size(), 2U);
  EXPECT_EQ(events[0].at("severity"), severity);
  EXPECT_EQ(events[1].at("severity"), "cleared");
}

// GPS (10 Hz, 300 ms stale threshold) stops for 1 s.
TEST_F(FaultE2eTest, StallRaisesStaleAndClearsOnResume) {
  const std::string out = run_scenario(default_config(0), {"stall:2:1000@500"}, "3");
  const auto stale = events_for(out, "2", "stale");
  expect_raised_then_cleared(stale, "critical");
  ASSERT_EQ(stale.size(), 2U);
  EXPECT_GE(field(stale[0], "detail"), 300U);  // ms since the last sample
  EXPECT_GE(field(stale[1], "detail"), 900U);  // the whole outage
  EXPECT_EQ(field(per_source(out, "recv source=").at("2"), "gaps"), 0U)
      << "a stalled source skips no seqs";
}

// 20,000 IMU samples at once into a 256-slot drop_newest ring.
TEST_F(FaultE2eTest, BurstDropsAndRaisesSeqGap) {
  auto config = default_config(0);
  config["sources"][0]["ring_capacity"] = 256;
  const std::string out = run_scenario(config, {"burst:1:20000@500"}, "3");

  const auto sent = per_source(read_file(dir_ / "send.out"), "summary source=").at("1");
  const std::uint64_t dropped = field(sent, "dropped_newest");
  ASSERT_GT(dropped, 0U) << "the burst didn't overflow the ring";

  std::uint64_t reported_missing = 0;
  for (const auto& e : events_for(out, "1", "seq_gap")) {
    EXPECT_EQ(e.at("severity"), "warning");
    reported_missing += field(e, "detail");
  }
  // The sender's monitor, the receiver's tracker, and the drop counter agree.
  EXPECT_EQ(reported_missing, dropped);
  EXPECT_EQ(field(per_source(out, "recv source=").at("1"), "gaps"), dropped);
}

// GPS halves its rate from 0.5 s on; the 1 s window sees ~5 Hz against 10.
TEST_F(FaultE2eTest, RateFactorRaisesRateWarning) {
  const std::string out = run_scenario(default_config(0), {"rate:2:0.5@500"}, "4");
  const auto rate = events_for(out, "2", "rate");
  ASSERT_FALSE(rate.empty()) << out;
  EXPECT_EQ(rate[0].at("severity"), "warning");
  EXPECT_LT(field(rate[0], "detail"), 8000U);  // millihertz: outside 10 Hz ±20%
}

// IMU payload stops changing for 0.5 s (~500 samples; frozen_count is 20).
TEST_F(FaultE2eTest, FreezeRaisesFrozenAndClears) {
  const std::string out = run_scenario(default_config(0), {"freeze:1@500+500"}, "2.5");
  const auto frozen = events_for(out, "1", "frozen");
  expect_raised_then_cleared(frozen, "warning");
  ASSERT_EQ(frozen.size(), 2U);
  EXPECT_EQ(field(frozen[0], "detail"), 20U);   // raised as the run reaches frozen_count
  EXPECT_GE(field(frozen[1], "detail"), 100U);  // how long the run got
}

// The receiver-side fault: it reads 1 KiB per second, so the sender's queue
// fills and drops batches, but the sender keeps running and finishes on time.
TEST_F(FaultE2eTest, SlowConsumerDropsWithoutBlockingSender) {
  std::unique_ptr<ChildProcess> receiver;
  const std::uint16_t port = start_receiver(receiver, 0, "recv", {"--fault", "slow_consumer:1000"});
  ASSERT_NE(port, 0);
  auto config = default_config(port);
  config["transport"]["socket_send_buffer_bytes"] = 8192;
  config["transport"]["max_pending_batches"] = 16;

  const auto started = std::chrono::steady_clock::now();
  ASSERT_EQ(run_with_faults(config, {}, "3"), 0) << read_file(dir_ / "send.err");
  EXPECT_LT(std::chrono::steady_clock::now() - started, 3s + 2s + 2s);
  stop_receiver(*receiver);

  const std::string send_out = read_file(dir_ / "send.out");
  const auto total = key_values(find_line(send_out, "summary total "));
  const auto transport = key_values(find_line(send_out, "summary transport "));
  EXPECT_EQ(field(total, "lost"), 0U);
  EXPECT_GT(field(transport, "batches_dropped"), 0U);
  EXPECT_GT(field(transport, "samples_dropped"), 0U);
}

TEST_F(FaultE2eTest, BadFaultsAreRejected) {
  auto config = default_config(1);
  EXPECT_EQ(run_with_faults(config, {"stall:9:100"}, "1"), 2);
  EXPECT_THAT(read_file(dir_ / "send.err"), HasSubstr("no source with id 9"));
  EXPECT_EQ(run_with_faults(config, {"slow_consumer:10"}, "1"), 2);
  EXPECT_THAT(read_file(dir_ / "send.err"), HasSubstr("receiver fault"));

  // tstream-recv takes only slow_consumer.
  ChildProcess receiver(kReceiver, {"--fault", "stall:1:100"}, dir_ / "r.out", dir_ / "r.err");
  EXPECT_EQ(receiver.wait(), 1);
}

}  // namespace
