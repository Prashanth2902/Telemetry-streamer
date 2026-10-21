// Shared fixture for tests that run the real tstream and tstream-recv
// executables over loopback. The including target defines TSTREAM_BINARY,
// TSTREAM_RECV_BINARY, and TSTREAM_SOURCE_DIR.

#pragma once

#include <chrono>
#include <csignal>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "process.hpp"

namespace tstream::testing {

inline const std::filesystem::path kSender = TSTREAM_BINARY;
inline const std::filesystem::path kReceiver = TSTREAM_RECV_BINARY;
inline const std::filesystem::path kDefaultConfig =
    std::filesystem::path(TSTREAM_SOURCE_DIR) / "configs" / "default.json";

using FieldMap = std::map<std::string, std::string>;

// Every line starting with `prefix`, keyed by its "source" field.
inline std::map<std::string, FieldMap> per_source(const std::string& out,
                                                  const std::string& prefix) {
  std::map<std::string, FieldMap> sources;
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

// The fields of every "health ..." line tstream-recv printed, in order.
inline std::vector<FieldMap> health_lines(const std::string& recv_out) {
  std::vector<FieldMap> events;
  std::istringstream in(recv_out);
  std::string line;
  while (std::getline(in, line)) {
    if (line.starts_with("health ")) {
      events.push_back(key_values(line));
    }
  }
  return events;
}

class SenderReceiverTest : public TempDirTest {
 protected:
  // Starts tstream-recv on `port` (0 = any) with `extra` flags and returns the
  // port it bound, once it has printed its "listening on" line. `name` keeps
  // output files apart.
  std::uint16_t start_receiver(std::unique_ptr<ChildProcess>& receiver, std::uint16_t port,
                               const std::string& name = "recv",
                               const std::vector<std::string>& extra = {}) {
    using namespace std::chrono_literals;
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

  // configs/default.json aimed at `port`, as a mutable JSON object.
  static nlohmann::json default_config(std::uint16_t port) {
    auto config = nlohmann::json::parse(read_file(kDefaultConfig));
    config["transport"]["port"] = port;
    return config;
  }

  // Writes `config` into the temp directory and returns its path.
  std::filesystem::path write_config(const nlohmann::json& config) {
    const std::filesystem::path path = dir_ / "config.json";
    std::ofstream(path) << config.dump(2);
    return path;
  }

  // Writes configs/default.json aimed at `port`, with `transport` keys
  // overridden, and returns its path.
  std::filesystem::path config_for(std::uint16_t port,
                                   const nlohmann::json& transport = nlohmann::json::object()) {
    auto config = default_config(port);
    for (const auto& [key, value] : transport.items()) {
      config["transport"][key] = value;
    }
    return write_config(config);
  }

  // Runs the sender for `seconds` against `port`; returns its exit code.
  int run_sender(std::uint16_t port, const std::string& seconds) {
    using namespace std::chrono_literals;
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

}  // namespace tstream::testing
