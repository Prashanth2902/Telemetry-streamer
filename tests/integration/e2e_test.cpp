// tstream -> TCP -> tstream-recv over loopback (SPEC §13 "Integration", M3
// acceptance). Both programs are the real executables.

#include <algorithm>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <nlohmann/json.hpp>
#include <sys/socket.h>
#include <unistd.h>

#include "process.hpp"

namespace {

using namespace std::chrono_literals;
namespace fs = std::filesystem;
using ::testing::HasSubstr;
using tstream::testing::ChildProcess;
using tstream::testing::field;
using tstream::testing::find_line;
using tstream::testing::key_values;
using tstream::testing::read_file;

const fs::path kSender = TSTREAM_BINARY;
const fs::path kReceiver = TSTREAM_RECV_BINARY;
const fs::path kDefaultConfig = fs::path(TSTREAM_SOURCE_DIR) / "configs" / "default.json";

// An ephemeral TCP port nothing is listening on (bound, then released).
std::uint16_t free_port() {
  const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  EXPECT_EQ(::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);
  socklen_t len = sizeof(addr);
  ::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len);
  ::close(fd);
  return ntohs(addr.sin_port);
}

class E2eTest : public ::testing::Test {
 protected:
  // A fresh temporary directory per test.
  void SetUp() override {
    const auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
    dir_ = fs::temp_directory_path() /
           ("tstream_e2e_" + std::string(info->name()) + "_" + std::to_string(::getpid()));
    fs::create_directories(dir_);
  }
  // Removes the test's temporary directory.
  void TearDown() override { fs::remove_all(dir_); }

  // Starts tstream-recv on `port` (0 = any) and returns the port it bound,
  // once it has printed its "listening on" line. `name` keeps output files apart.
  std::uint16_t start_receiver(std::unique_ptr<ChildProcess>& receiver, std::uint16_t port,
                               const std::string& name = "recv") {
    receiver = std::make_unique<ChildProcess>(
        kReceiver, std::vector<std::string>{"--port", std::to_string(port), "--interval", "1"},
        dir_ / (name + ".out"), dir_ / (name + ".err"));
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

  // Writes configs/default.json aimed at `port` and returns its path.
  fs::path config_for(std::uint16_t port) {
    auto config = nlohmann::json::parse(read_file(kDefaultConfig));
    config["transport"]["port"] = port;
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

  fs::path dir_;
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
  EXPECT_EQ(field(transport, "samples_sent") + field(transport, "samples_dropped"),
            field(sent, "delivered"));
  EXPECT_GT(field(recv, "samples"), 0U);
  EXPECT_EQ(field(recv, "samples"), field(transport, "samples_sent"));
  EXPECT_EQ(field(recv, "gaps"), 0U);  // it joined mid-stream, then nothing was lost
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

}  // namespace
