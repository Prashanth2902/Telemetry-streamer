// M0 smoke test: proves the toolchain, dependencies, and sanitizer runtimes
// are wired up. Replaced by real unit tests from M1 onward.

#include <atomic>
#include <cstdint>
#include <string>
#include <thread>

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include <spdlog/fmt/fmt.h>

namespace {

TEST(ToolchainSmoke, UsesCpp20) {
  EXPECT_GE(__cplusplus, 202002L);
}

TEST(ToolchainSmoke, ParsesJson) {
  const auto cfg = nlohmann::json::parse(R"({"seed": 42, "sources": [1, 2, 3]})");
  EXPECT_EQ(cfg.at("seed").get<int>(), 42);
  EXPECT_EQ(cfg.at("sources").size(), 3U);
}

TEST(ToolchainSmoke, FormatsWithFmt) {
  EXPECT_EQ(fmt::format("{}-{:02}", "m", 0), "m-00");
}

// A release/acquire handoff between two threads. Correct code, so TSan must
// stay quiet; the point is that the TSan runtime starts and runs at all.
TEST(ToolchainSmoke, ThreadHandoffRunsUnderSanitizers) {
  std::uint64_t payload = 0;
  std::atomic<bool> ready{false};

  std::thread producer([&] {
    payload = 0x7473;
    ready.store(true, std::memory_order_release);
  });

  while (!ready.load(std::memory_order_acquire)) {
    std::this_thread::yield();
  }
  EXPECT_EQ(payload, 0x7473U);
  producer.join();
}

}  // namespace
