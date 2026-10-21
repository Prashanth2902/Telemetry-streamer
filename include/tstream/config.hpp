// Runtime configuration loaded from JSON (SPEC §10).
//
// Validation is strict and happens once at startup: an invalid file is a
// ConfigError with a message naming the offending field, and the program exits
// nonzero. Unknown keys are errors too, so a typo can't silently fall back to
// a default.

#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace tstream {

class ConfigError : public std::runtime_error {
 public:
  using std::runtime_error::runtime_error;
};

enum class SourceType : std::uint8_t { SimImu, SimGps, SimBattery };

// Backpressure policy when a source's ring is full (SPEC §6); see PushPolicy.
enum class BackpressurePolicy : std::uint8_t { DropNewest, BlockWithTimeout };

// Per-source absolute overrides of the global health defaults (SPEC §7, §10).
struct SourceHealthOverrides {
  std::optional<double> stale_ms;
  std::optional<double> rate_window_ms;
  std::optional<double> rate_tolerance_pct;
};

struct SourceConfig {
  std::uint16_t id = 0;
  SourceType type = SourceType::SimImu;
  double rate_hz = 0;
  std::size_t ring_capacity = 0;
  BackpressurePolicy policy = BackpressurePolicy::DropNewest;
  std::optional<std::uint32_t> timeout_us;
  SourceHealthOverrides health;
};

struct BatchConfig {
  std::size_t max_samples = 0;
  std::uint32_t max_delay_us = 0;
};

struct TransportConfig {
  std::string host;
  std::uint16_t port = 0;
  std::size_t max_pending_batches = 0;
  std::size_t max_frame_bytes = 1 << 20;
};

// Defaults expressed relative to each source's period (SPEC §7).
struct HealthConfig {
  double stale_periods = 3;
  double stale_min_ms = 20;
  double rate_tolerance_pct = 20;
  double rate_window_ms = 1000;
  std::size_t rate_min_samples = 10;
};

struct Config {
  std::vector<SourceConfig> sources;
  BatchConfig batch;
  TransportConfig transport;
  HealthConfig health;
  std::uint64_t seed = 0;
};

// Ring capacities SampleRing can be built with (powers of two in this range).
inline constexpr std::size_t kMinRingCapacity = 16;
inline constexpr std::size_t kMaxRingCapacity = 65536;
inline constexpr double kMaxRateHz = 100'000;

// Parse and validate JSON text. Throws ConfigError.
Config parse_config(std::string_view json_text);

// Read a file and parse it. Throws ConfigError (including for I/O errors).
Config load_config(const std::string& path);

// Config spelling of a source type, e.g. "sim_imu".
std::string_view source_type_name(SourceType type);
// Config spelling of a policy, e.g. "drop_newest".
std::string_view policy_name(BackpressurePolicy policy);

}  // namespace tstream
