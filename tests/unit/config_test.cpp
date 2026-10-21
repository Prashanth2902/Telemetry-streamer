// Config parsing and validation (SPEC §10).

#include "tstream/config.hpp"

#include <string>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

namespace {

using nlohmann::json;
using ::testing::HasSubstr;
using tstream::BackpressurePolicy;
using tstream::ConfigError;
using tstream::parse_config;
using tstream::SourceType;

// The SPEC §10 example, as a mutable JSON object.
json example() {
  return json::parse(R"({
    "sources": [
      { "id": 1, "type": "sim_imu",     "rate_hz": 1000, "ring_capacity": 4096, "policy": "drop_newest" },
      { "id": 2, "type": "sim_gps",     "rate_hz": 10,   "ring_capacity": 256,  "policy": "block_with_timeout", "timeout_us": 500 },
      { "id": 3, "type": "sim_battery", "rate_hz": 1,    "ring_capacity": 64,   "policy": "block_with_timeout", "timeout_us": 500,
        "health": { "stale_ms": 5000 } }
    ],
    "batch": { "max_samples": 256, "max_delay_us": 2000 },
    "transport": { "host": "127.0.0.1", "port": 9000, "max_pending_batches": 64, "max_frame_bytes": 1048576 },
    "health": { "stale_periods": 3, "stale_min_ms": 20, "rate_tolerance_pct": 20, "rate_window_ms": 1000, "rate_min_samples": 10 },
    "seed": 42
  })");
}

// Expect parse_config to throw a ConfigError whose message contains `needle`.
void expect_error(const json& config, const std::string& needle) {
  try {
    parse_config(config.dump());
    FAIL() << "expected ConfigError containing \"" << needle << "\"";
  } catch (const ConfigError& e) {
    EXPECT_THAT(e.what(), HasSubstr(needle));
  }
}

TEST(Config, ParsesSpecExample) {
  const auto config = parse_config(example().dump());

  ASSERT_EQ(config.sources.size(), 3U);
  EXPECT_EQ(config.sources[0].id, 1);
  EXPECT_EQ(config.sources[0].type, SourceType::SimImu);
  EXPECT_DOUBLE_EQ(config.sources[0].rate_hz, 1000);
  EXPECT_EQ(config.sources[0].ring_capacity, 4096U);
  EXPECT_EQ(config.sources[0].policy, BackpressurePolicy::DropNewest);
  EXPECT_FALSE(config.sources[0].timeout_us.has_value());

  EXPECT_EQ(config.sources[1].type, SourceType::SimGps);
  EXPECT_EQ(config.sources[1].policy, BackpressurePolicy::BlockWithTimeout);
  EXPECT_EQ(config.sources[1].timeout_us, 500U);

  EXPECT_EQ(config.sources[2].type, SourceType::SimBattery);
  EXPECT_EQ(config.sources[2].health.stale_ms, 5000.0);
  EXPECT_FALSE(config.sources[2].health.rate_window_ms.has_value());

  EXPECT_EQ(config.batch.max_samples, 256U);
  EXPECT_EQ(config.batch.max_delay_us, 2000U);
  EXPECT_EQ(config.transport.host, "127.0.0.1");
  EXPECT_EQ(config.transport.port, 9000);
  EXPECT_EQ(config.transport.max_pending_batches, 64U);
  EXPECT_EQ(config.transport.max_frame_bytes, 1048576U);
  EXPECT_DOUBLE_EQ(config.health.stale_periods, 3);
  EXPECT_DOUBLE_EQ(config.health.stale_min_ms, 20);
  EXPECT_EQ(config.health.rate_min_samples, 10U);
  EXPECT_EQ(config.seed, 42U);
}

TEST(Config, HealthBlockIsOptional) {
  auto j = example();
  j.erase("health");
  const auto config = parse_config(j.dump());
  EXPECT_DOUBLE_EQ(config.health.stale_periods, 3);
  EXPECT_DOUBLE_EQ(config.health.rate_window_ms, 1000);
}

TEST(Config, RejectsInvalidJson) {
  EXPECT_THROW(parse_config("{ not json"), ConfigError);
}

TEST(Config, RejectsNonPowerOfTwoRingCapacity) {
  auto j = example();
  j["sources"][0]["ring_capacity"] = 1000;
  expect_error(j, "power of two");
}

TEST(Config, RejectsRingCapacityOutsideSupportedRange) {
  auto j = example();
  j["sources"][0]["ring_capacity"] = 8;
  expect_error(j, "ring_capacity");
  j["sources"][0]["ring_capacity"] = 131072;
  expect_error(j, "ring_capacity");
}

TEST(Config, RejectsNonPositiveRate) {
  auto j = example();
  j["sources"][0]["rate_hz"] = 0;
  expect_error(j, "rate_hz: must be positive");
  j["sources"][0]["rate_hz"] = -5;
  expect_error(j, "rate_hz: must be positive");
}

TEST(Config, RejectsRateAboveMaximum) {
  auto j = example();
  j["sources"][0]["rate_hz"] = 1e7;
  expect_error(j, "rate_hz: must be at most");
}

TEST(Config, RejectsPortOutOfRange) {
  auto j = example();
  j["transport"]["port"] = 0;
  expect_error(j, "transport.port");
  j["transport"]["port"] = 70000;
  expect_error(j, "transport.port");
}

TEST(Config, RejectsDuplicateSourceIds) {
  auto j = example();
  j["sources"][1]["id"] = 1;
  expect_error(j, "duplicate source id 1");
}

TEST(Config, RejectsReservedSourceIdZero) {
  auto j = example();
  j["sources"][0]["id"] = 0;
  expect_error(j, "sources[0].id");
}

TEST(Config, BlockWithTimeoutRequiresTimeout) {
  auto j = example();
  j["sources"][1].erase("timeout_us");
  expect_error(j, "block_with_timeout requires timeout_us");
}

TEST(Config, RejectsUnknownSourceTypeAndPolicy) {
  auto j = example();
  j["sources"][0]["type"] = "sim_lidar";
  expect_error(j, "unknown source type \"sim_lidar\"");
  j = example();
  j["sources"][0]["policy"] = "drop_oldest";
  expect_error(j, "unknown policy \"drop_oldest\"");
}

// A typo must not silently fall back to a default.
TEST(Config, RejectsUnknownKeys) {
  auto j = example();
  j["sources"][0]["rate_hx"] = 100;
  expect_error(j, "unknown key \"rate_hx\"");
  j = example();
  j["batch"]["max_delay_ms"] = 2;
  expect_error(j, "unknown key \"max_delay_ms\"");
  j = example();
  j["seeed"] = 1;
  expect_error(j, "unknown key \"seeed\"");
}

TEST(Config, RejectsMissingRequiredKeys) {
  for (const char* key : {"sources", "batch", "transport", "seed"}) {
    auto j = example();
    j.erase(key);
    expect_error(j, std::string("missing required key \"") + key + "\"");
  }
  auto j = example();
  j["sources"][0].erase("rate_hz");
  expect_error(j, "missing required key \"rate_hz\"");
}

TEST(Config, RejectsWrongTypes) {
  auto j = example();
  j["sources"][0]["rate_hz"] = "fast";
  expect_error(j, "rate_hz: must be a number");
  j = example();
  j["batch"]["max_samples"] = 2.5;
  expect_error(j, "max_samples: must be an integer");
  j = example();
  j["sources"] = json::array();
  expect_error(j, "sources: must be a non-empty array");
}

// SPEC §9: max_samples * 61 bytes + overhead must fit in max_frame_bytes.
TEST(Config, RejectsBatchThatCannotFitInOneFrame) {
  auto j = example();
  j["batch"]["max_samples"] = 65536;  // ~4 MB worst case
  j["transport"]["max_frame_bytes"] = 1048576;
  expect_error(j, "more than transport.max_frame_bytes");

  j["transport"]["max_frame_bytes"] = 8 * 1024 * 1024;  // now it fits
  EXPECT_NO_THROW(parse_config(j.dump()));
}

TEST(Config, ParsesOptionalSocketSendBuffer) {
  auto j = example();
  EXPECT_EQ(parse_config(j.dump()).transport.socket_send_buffer_bytes, 0U);
  j["transport"]["socket_send_buffer_bytes"] = 65536;
  EXPECT_EQ(parse_config(j.dump()).transport.socket_send_buffer_bytes, 65536U);
  j["transport"]["socket_send_buffer_bytes"] = 100;
  expect_error(j, "socket_send_buffer_bytes");
}

// SPEC §6: K preallocated worst-case frames must stay within kMaxQueueBytes.
TEST(Config, RejectsPendingQueueThatNeedsTooMuchMemory) {
  auto j = example();
  j["batch"]["max_samples"] = 16384;  // ~1 MB frames
  j["transport"]["max_pending_batches"] = 4096;
  expect_error(j, "transport.max_pending_batches");
}

TEST(Config, LoadConfigReportsMissingFile) {
  try {
    tstream::load_config("/nonexistent/tstream.json");
    FAIL() << "expected ConfigError";
  } catch (const ConfigError& e) {
    EXPECT_THAT(e.what(), HasSubstr("cannot open config file"));
  }
}

TEST(Config, ParsesFaults) {
  auto j = example();
  j["faults"] = {"stall:1:500@1000", "rate:3:2"};
  const auto config = parse_config(j.dump());
  ASSERT_EQ(config.faults.size(), 2U);
  EXPECT_EQ(config.faults[0].kind, tstream::FaultKind::Stall);
  EXPECT_EQ(config.faults[1].source_id, 3);
  EXPECT_TRUE(parse_config(example().dump()).faults.empty());
}

TEST(Config, RejectsBadFaults) {
  auto j = example();
  j["faults"] = "stall:1:5";
  expect_error(j, "faults: must be an array");
  j["faults"] = {5};
  expect_error(j, "faults[0]: must be a string");
  j["faults"] = {"stall:1:5", "stall:9:5"};
  expect_error(j, "faults[1]: fault \"stall:9:5\": no source with id 9");
  j["faults"] = {"explode:1:5"};
  expect_error(j, "unknown kind");
  j["faults"] = {"slow_consumer:10"};
  expect_error(j, "receiver fault");
  j["sources"][0]["rate_hz"] = 5000;
  j["faults"] = {"rate:1:50"};  // 5 kHz x 50 > kMaxRateHz
  expect_error(j, "above 100000 Hz");
}

TEST(Config, AddFaultAppendsAfterLoading) {
  auto config = parse_config(example().dump());
  tstream::add_fault(config, "freeze:1@100+50");
  ASSERT_EQ(config.faults.size(), 1U);
  EXPECT_EQ(config.faults[0].kind, tstream::FaultKind::Freeze);
  EXPECT_THROW(tstream::add_fault(config, "freeze:4"), ConfigError);
  EXPECT_EQ(config.faults.size(), 1U);
}

}  // namespace
