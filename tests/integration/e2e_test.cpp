// tstream -> TCP -> tstream-recv over loopback (SPEC §13 "Integration", M3
// acceptance). Both programs are the real executables.

#include <algorithm>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <filesystem>
#include <map>
#include <sstream>
#include <string>
#include <thread>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "process.hpp"
#include "sockets.hpp"

namespace {

using namespace std::chrono_literals;
namespace fs = std::filesystem;
using ::testing::HasSubstr;
using tstream::testing::ChildProcess;
using tstream::testing::field;
using tstream::testing::find_line;
using tstream::testing::free_port;
using tstream::testing::key_values;
using tstream::testing::read_file;

const fs::path kSender = TSTREAM_BINARY;
const fs::path kReceiver = TSTREAM_RECV_BINARY;
const fs::path kDefaultConfig = fs::path(TSTREAM_SOURCE_DIR) / "configs" / "default.json";

class E2eTest : public tstream::testing::TempDirTest {
 protected:
  // Starts tstream-recv on `port` (0 = any) with `extra` flags and returns the
  // port it bound, once it has printed its "listening on" line. `name` keeps
  // output files apart.
  std::uint16_t start_receiver(std::unique_ptr<ChildProcess>& receiver, std::uint16_t port,
                               const std::string& name = "recv",
                               const std::vector<std::string>& extra = {}) {
    std::vector<std::string> args{"--port", std::to_string(port), "--interval", "1"};
    args.insert(args.end(), extra.begin(), extra.end());
    receiver = std::make_unique<ChildProcess>(kReceiver, args, dir_ / (name + ".out"),
                                              dir_ / (name + ".err"));
    const auto deadline = std::chrono::steady_clock::now() + 10s;
    while (std::chrono::steady_clock::now() < deadline) {
      const std::string line = find_line(read_file(dir_ / (name + ".out")), "listening on ");
      if (!line.empty()) {
        return static_cast<std::uint16_t>(std::stoul(line.substr(line.rfind(':') + 1)));
      }
      std::this_thread::sleep_for(20ms);
    }
    ADD_FAILURE() << "receiver never printed its port";
    return 0;
  }

  // Writes configs/default.json aimed at `port`, with `transport` keys
  // overridden, and returns its path.
  fs::path config_for(std::uint16_t port,
                      const nlohmann::json& transport = nlohmann::json::object()) {
    auto config = nlohmann::json::parse(read_file(kDefaultConfig));
    config["transport"]["port"] = port;
    for (const auto& [key, value] : transport.items()) {
      config["transport"][key] = value;
    }
    const fs::path path = dir_ / "config.json";
    std::ofstream(path) << config.dump(2);
    return path;
  }

  // Runs the sender for `seconds` against `port`; returns its exit code.
  int run_sender(std::uint16_t port, const std::string& seconds) {
    ChildProcess sender(kSender, {"--config", config_for(port).string(), "--duration", seconds},
                        dir_ / "send.out", dir_ / "send.err");
    return sender.wait(120s);
  }

  // Stops a receiver with SIGINT and returns its stdout.
  std::string stop_receiver(ChildProcess& receiver, const std::string& name = "recv") {
    receiver.signal(SIGINT);
    EXPECT_EQ(receiver.wait(), 0) << read_file(dir_ / (name + ".err"));
    return read_file(dir_ / (name + ".out"));
  }
};

// The M3 acceptance test: 10 s with zero gaps, and p50/p99 latency reported.
TEST_F(E2eTest, TenSecondRunHasZeroGapsAndReportsLatency) {
  std::unique_ptr<ChildProcess> receiver;
  const std::uint16_t port = start_receiver(receiver, 0);
  ASSERT_NE(port, 0);

  ASSERT_EQ(run_sender(port, "10"), 0) << read_file(dir_ / "send.err");
  std::this_thread::sleep_for(300ms);  // let the last bytes arrive
  const std::string recv_out = stop_receiver(*receiver);
  const std::string send_out = read_file(dir_ / "send.out");

  const auto sent = key_values(find_line(send_out, "summary total "));
  const auto transport = key_values(find_line(send_out, "summary transport "));
  const auto recv = key_values(find_line(recv_out, "recv summary "));
  ASSERT_FALSE(recv.empty()) << recv_out;

  EXPECT_EQ(field(sent, "lost"), 0U);
  EXPECT_EQ(field(transport, "batches_dropped"), 0U);
  EXPECT_EQ(field(transport, "connects"), 1U);
  EXPECT_GT(field(transport, "samples_sent"), 5000U);  // ~10,000 from the 1 kHz IMU
  EXPECT_EQ(field(recv, "samples"), field(transport, "samples_sent"));
  EXPECT_EQ(field(recv, "batches"), field(transport, "batches_sent"));
  EXPECT_EQ(field(recv, "gaps"), 0U);
  EXPECT_EQ(field(recv, "out_of_order"), 0U);
  EXPECT_EQ(field(recv, "batch_seq_gaps"), 0U);
  EXPECT_EQ(field(recv, "frame_errors"), 0U);
  EXPECT_EQ(field(recv, "decode_errors"), 0U);
  EXPECT_EQ(field(recv, "invalid_samples"), 0U);

  const std::string summary = find_line(recv_out, "recv summary ");
  EXPECT_THAT(summary, HasSubstr("e2e_us p50="));
  EXPECT_THAT(summary, HasSubstr(" p99="));
  EXPECT_THAT(summary, HasSubstr("pipeline_us p50="));
  EXPECT_THAT(summary, ::testing::Not(HasSubstr("unavailable")));    // loopback: same clock
  EXPECT_GE(std::count(recv_out.begin(), recv_out.end(), '\n'), 10)  // per-second stats lines
      << recv_out;
}

// Per-source "summary source=" (sender) or "recv source=" (receiver) lines, by id.
std::map<std::string, std::map<std::string, std::string>> per_source(const std::string& out,
                                                                     const std::string& prefix) {
  std::map<std::string, std::map<std::string, std::string>> sources;
  std::istringstream in(out);
  std::string line;
  while (std::getline(in, line)) {
    if (line.starts_with(prefix)) {
      auto fields = key_values(line);
      sources[fields["source"]] = std::move(fields);
    }
  }
  return sources;
}

// The sender starts first and keeps retrying; once the receiver appears it
// connects. Batches sent before that are dropped and counted, not lost.
TEST_F(E2eTest, SenderConnectsWhenReceiverStartsLater) {
  const std::uint16_t port = free_port();
  ChildProcess sender(kSender, {"--config", config_for(port).string(), "--duration", "3"},
                      dir_ / "send.out", dir_ / "send.err");
  std::this_thread::sleep_for(1s);
  std::unique_ptr<ChildProcess> receiver;
  ASSERT_EQ(start_receiver(receiver, port), port);
  ASSERT_EQ(sender.wait(), 0) << read_file(dir_ / "send.err");
  std::this_thread::sleep_for(300ms);
  const std::string recv_out = stop_receiver(*receiver);
  const std::string send_out = read_file(dir_ / "send.out");

  const auto sent = key_values(find_line(send_out, "summary total "));
  const auto transport = key_values(find_line(send_out, "summary transport "));
  const auto recv = key_values(find_line(recv_out, "recv summary "));
  EXPECT_EQ(field(sent, "lost"), 0U);
  EXPECT_GT(field(transport, "batches_dropped"), 0U);
  EXPECT_GE(field(transport, "connects"), 1U);
  // The transport also carries the pipeline's health samples (source 0).
  EXPECT_EQ(field(transport, "samples_sent") + field(transport, "samples_dropped"),
            field(sent, "delivered") + field(sent, "health_events"));
  EXPECT_GT(field(recv, "samples"), 0U);
  EXPECT_EQ(field(recv, "samples"), field(transport, "samples_sent"));
  // The first K batches waited in the queue and arrived; the ones after them
  // were dropped until the connection opened. Those drops are exactly the
  // receiver's gaps (SPEC §13 reconciliation), source by source. Source 0
  // is left out: a dropped trailing health sample is never seen as a gap.
  const auto receiver_sources = per_source(recv_out, "recv source=");
  for (const auto& [id, source] : per_source(send_out, "summary source=")) {
    SCOPED_TRACE("source " + id);
    ASSERT_TRUE(receiver_sources.contains(id));
    EXPECT_EQ(field(receiver_sources.at(id), "gaps"), field(source, "tx_dropped"));
  }
  EXPECT_EQ(field(recv, "batch_seq_gaps"), field(transport, "batches_dropped"));
}

// SPEC §13: receiver restart triggers reconnect.
TEST_F(E2eTest, ReceiverRestartTriggersReconnect) {
  std::unique_ptr<ChildProcess> first;
  const std::uint16_t port = start_receiver(first, 0, "recv1");
  ASSERT_NE(port, 0);
  ChildProcess sender(kSender, {"--config", config_for(port).string(), "--duration", "4"},
                      dir_ / "send.out", dir_ / "send.err");

  std::this_thread::sleep_for(1500ms);
  const auto first_recv = key_values(find_line(stop_receiver(*first, "recv1"), "recv summary "));
  std::unique_ptr<ChildProcess> second;
  ASSERT_EQ(start_receiver(second, port, "recv2"), port);

  ASSERT_EQ(sender.wait(), 0) << read_file(dir_ / "send.err");
  std::this_thread::sleep_for(300ms);
  const auto second_recv = key_values(find_line(stop_receiver(*second, "recv2"), "recv summary "));
  const std::string send_out = read_file(dir_ / "send.out");
  const auto sent = key_values(find_line(send_out, "summary total "));
  const auto transport = key_values(find_line(send_out, "summary transport "));

  EXPECT_EQ(field(sent, "lost"), 0U);
  EXPECT_GE(field(transport, "disconnects"), 1U);
  EXPECT_GE(field(transport, "connects"), 2U);
  EXPECT_GT(field(first_recv, "samples"), 0U);
  EXPECT_GT(field(second_recv, "samples"), 0U);
  // Everything the transport reports as sent reached one of the two receivers,
  // except a frame or two in flight when the first one was stopped.
  const std::uint64_t received = field(first_recv, "samples") + field(second_recv, "samples");
  EXPECT_LE(received, field(transport, "samples_sent"));
  EXPECT_GE(received + 600, field(transport, "samples_sent"));
}

// Small socket buffers and a short queue, so a slow receiver backs the sender
// up within a second instead of after megabytes.
const nlohmann::json kTightTransport = {{"socket_send_buffer_bytes", 8192},
                                        {"max_pending_batches", 16}};

// The M4 acceptance test (SPEC §8 slow_consumer, §13): the receiver is slow
// for its first 3 s, then catches up. The sender must keep running, drop and
// count whole batches while backed up, and every drop must show up at the
// receiver as a gap of exactly the same size, per source.
TEST_F(E2eTest, SlowConsumerDropsReconcilePerSource) {
  std::unique_ptr<ChildProcess> receiver;
  const std::uint16_t port =
      start_receiver(receiver, 0, "recv", {"--slow-consumer-ms", "50", "--slow-for", "3"});
  ASSERT_NE(port, 0);

  const auto started = std::chrono::steady_clock::now();
  ChildProcess sender(kSender,
                      {"--config", config_for(port, kTightTransport).string(), "--duration", "7"},
                      dir_ / "send.out", dir_ / "send.err");
  ASSERT_EQ(sender.wait(60s), 0) << read_file(dir_ / "send.err");
  const auto sender_runtime = std::chrono::steady_clock::now() - started;
  std::this_thread::sleep_for(500ms);
  const std::string recv_out = stop_receiver(*receiver);
  const std::string send_out = read_file(dir_ / "send.out");

  // Never blocked indefinitely: 7 s of running plus at most the 2 s close timeout.
  EXPECT_LT(sender_runtime, 7s + 2s + 2s);

  const auto total = key_values(find_line(send_out, "summary total "));
  const auto transport = key_values(find_line(send_out, "summary transport "));
  const auto recv = key_values(find_line(recv_out, "recv summary "));
  EXPECT_EQ(field(total, "lost"), 0U);
  ASSERT_GT(field(transport, "batches_dropped"), 0U) << "receiver never fell behind:\n" << send_out;
  EXPECT_EQ(field(recv, "samples"), field(transport, "samples_sent"));
  EXPECT_EQ(field(recv, "batch_seq_gaps"), field(transport, "batches_dropped"));
  EXPECT_EQ(field(recv, "frame_errors"), 0U);

  const auto sender_sources = per_source(send_out, "summary source=");
  const auto receiver_sources = per_source(recv_out, "recv source=");
  ASSERT_EQ(sender_sources.size(), 3U);
  for (const auto& [id, sent] : sender_sources) {
    SCOPED_TRACE("source " + id);
    ASSERT_TRUE(receiver_sources.contains(id));
    const auto& got = receiver_sources.at(id);
    EXPECT_EQ(field(got, "samples"), field(sent, "tx_sent"));
    const std::uint64_t sender_drops =
        field(sent, "dropped_newest") + field(sent, "dropped_timeout") + field(sent, "tx_dropped");
    EXPECT_EQ(field(got, "gaps"), sender_drops);
  }
}

// A receiver that barely reads at all: the sender still finishes on time,
// and everything it couldn't deliver is counted, not lost.
TEST_F(E2eTest, StalledReceiverDoesNotBlockSender) {
  std::unique_ptr<ChildProcess> receiver;
  const std::uint16_t port = start_receiver(receiver, 0, "recv", {"--slow-consumer-ms", "1000"});
  ASSERT_NE(port, 0);

  const auto started = std::chrono::steady_clock::now();
  ChildProcess sender(kSender,
                      {"--config", config_for(port, kTightTransport).string(), "--duration", "3"},
                      dir_ / "send.out", dir_ / "send.err");
  ASSERT_EQ(sender.wait(60s), 0) << read_file(dir_ / "send.err");
  EXPECT_LT(std::chrono::steady_clock::now() - started, 3s + 2s + 2s);
  stop_receiver(*receiver);

  const std::string send_out = read_file(dir_ / "send.out");
  const auto total = key_values(find_line(send_out, "summary total "));
  const auto transport = key_values(find_line(send_out, "summary transport "));
  EXPECT_EQ(field(total, "lost"), 0U);
  EXPECT_GT(field(transport, "batches_dropped"), 0U);
  EXPECT_EQ(field(transport, "samples_sent") + field(transport, "samples_dropped"),
            field(total, "delivered") + field(total, "health_events"));
}

}  // namespace
