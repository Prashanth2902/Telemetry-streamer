#include "tstream/config.hpp"

#include <cmath>
#include <fstream>
#include <initializer_list>
#include <limits>
#include <set>
#include <sstream>
#include <string>

#include <nlohmann/json.hpp>

namespace tstream {

namespace {

using nlohmann::json;

[[noreturn]] void fail(const std::string& where, const std::string& what) {
  throw ConfigError(where + ": " + what);
}

void require_object(const json& j, const std::string& where) {
  if (!j.is_object()) {
    fail(where, "must be an object");
  }
}

// Reject keys outside `allowed`, so typos fail loudly instead of defaulting.
void reject_unknown_keys(const json& j, const std::string& where,
                         std::initializer_list<std::string_view> allowed) {
  for (const auto& item : j.items()) {
    bool known = false;
    for (const auto name : allowed) {
      known = known || item.key() == name;
    }
    if (!known) {
      fail(where, "unknown key \"" + item.key() + "\"");
    }
  }
}

const json& require_key(const json& j, const std::string& where, const char* key) {
  const auto it = j.find(key);
  if (it == j.end()) {
    fail(where, std::string("missing required key \"") + key + "\"");
  }
  return *it;
}

double get_number(const json& j, const std::string& where) {
  if (!j.is_number()) {
    fail(where, "must be a number");
  }
  const double value = j.get<double>();
  if (!std::isfinite(value)) {
    fail(where, "must be finite");
  }
  return value;
}

double get_positive(const json& j, const std::string& where) {
  const double value = get_number(j, where);
  if (value <= 0) {
    fail(where, "must be positive");
  }
  return value;
}

// Integer in [min, max]. Rejects fractional and negative values.
std::uint64_t get_integer(const json& j, const std::string& where, std::uint64_t min,
                          std::uint64_t max) {
  if (j.is_number_unsigned()) {
    const auto value = j.get<std::uint64_t>();
    if (value < min || value > max) {
      fail(where, "must be in [" + std::to_string(min) + ", " + std::to_string(max) + "]");
    }
    return value;
  }
  if (j.is_number_integer()) {  // signed, so negative here
    fail(where, "must be in [" + std::to_string(min) + ", " + std::to_string(max) + "]");
  }
  fail(where, "must be an integer");
}

std::string get_string(const json& j, const std::string& where) {
  if (!j.is_string()) {
    fail(where, "must be a string");
  }
  return j.get<std::string>();
}

bool is_power_of_two(std::uint64_t n) { return n != 0 && (n & (n - 1)) == 0; }

SourceType parse_source_type(const std::string& name, const std::string& where) {
  if (name == "sim_imu") {
    return SourceType::SimImu;
  }
  if (name == "sim_gps") {
    return SourceType::SimGps;
  }
  if (name == "sim_battery") {
    return SourceType::SimBattery;
  }
  fail(where, "unknown source type \"" + name + "\" (expected sim_imu, sim_gps, or sim_battery)");
}

BackpressurePolicy parse_policy(const std::string& name, const std::string& where) {
  if (name == "drop_newest") {
    return BackpressurePolicy::DropNewest;
  }
  if (name == "block_with_timeout") {
    return BackpressurePolicy::BlockWithTimeout;
  }
  fail(where, "unknown policy \"" + name + "\" (expected drop_newest or block_with_timeout)");
}

SourceHealthOverrides parse_source_health(const json& j, const std::string& where) {
  require_object(j, where);
  reject_unknown_keys(j, where, {"stale_ms", "rate_window_ms", "rate_tolerance_pct"});
  SourceHealthOverrides h;
  if (j.contains("stale_ms")) {
    h.stale_ms = get_positive(j.at("stale_ms"), where + ".stale_ms");
  }
  if (j.contains("rate_window_ms")) {
    h.rate_window_ms = get_positive(j.at("rate_window_ms"), where + ".rate_window_ms");
  }
  if (j.contains("rate_tolerance_pct")) {
    h.rate_tolerance_pct = get_positive(j.at("rate_tolerance_pct"), where + ".rate_tolerance_pct");
  }
  return h;
}

SourceConfig parse_source(const json& j, const std::string& where) {
  require_object(j, where);
  reject_unknown_keys(j, where,
                      {"id", "type", "rate_hz", "ring_capacity", "policy", "timeout_us", "health"});
  SourceConfig s;
  // 0 is reserved for the pipeline's own health samples (SPEC §4.1).
  s.id = static_cast<std::uint16_t>(
      get_integer(require_key(j, where, "id"), where + ".id", 1, 65535));
  s.type = parse_source_type(get_string(require_key(j, where, "type"), where + ".type"),
                             where + ".type");

  s.rate_hz = get_positive(require_key(j, where, "rate_hz"), where + ".rate_hz");
  if (s.rate_hz > kMaxRateHz) {
    fail(where + ".rate_hz", "must be at most " + std::to_string(static_cast<int>(kMaxRateHz)));
  }

  const auto capacity = get_integer(require_key(j, where, "ring_capacity"),
                                    where + ".ring_capacity", kMinRingCapacity, kMaxRingCapacity);
  if (!is_power_of_two(capacity)) {
    fail(where + ".ring_capacity", "must be a power of two (got " + std::to_string(capacity) + ")");
  }
  s.ring_capacity = static_cast<std::size_t>(capacity);

  s.policy = parse_policy(get_string(require_key(j, where, "policy"), where + ".policy"),
                          where + ".policy");
  if (j.contains("timeout_us")) {
    s.timeout_us = static_cast<std::uint32_t>(get_integer(
        j.at("timeout_us"), where + ".timeout_us", 1, std::numeric_limits<std::uint32_t>::max()));
  }
  if (s.policy == BackpressurePolicy::BlockWithTimeout && !s.timeout_us) {
    fail(where, "policy block_with_timeout requires timeout_us");
  }
  if (j.contains("health")) {
    s.health = parse_source_health(j.at("health"), where + ".health");
  }
  return s;
}

BatchConfig parse_batch(const json& j) {
  const std::string where = "batch";
  require_object(j, where);
  reject_unknown_keys(j, where, {"max_samples", "max_delay_us"});
  BatchConfig b;
  b.max_samples = static_cast<std::size_t>(
      get_integer(require_key(j, where, "max_samples"), where + ".max_samples", 1, 65536));
  b.max_delay_us = static_cast<std::uint32_t>(get_integer(
      require_key(j, where, "max_delay_us"), where + ".max_delay_us", 1, 10'000'000));
  return b;
}

TransportConfig parse_transport(const json& j) {
  const std::string where = "transport";
  require_object(j, where);
  reject_unknown_keys(j, where, {"host", "port", "max_pending_batches", "max_frame_bytes"});
  TransportConfig t;
  t.host = get_string(require_key(j, where, "host"), where + ".host");
  if (t.host.empty()) {
    fail(where + ".host", "must not be empty");
  }
  t.port =
      static_cast<std::uint16_t>(get_integer(require_key(j, where, "port"), where + ".port", 1, 65535));
  t.max_pending_batches = static_cast<std::size_t>(get_integer(
      require_key(j, where, "max_pending_batches"), where + ".max_pending_batches", 1, 4096));
  if (j.contains("max_frame_bytes")) {
    t.max_frame_bytes = static_cast<std::size_t>(get_integer(
        j.at("max_frame_bytes"), where + ".max_frame_bytes", 1024, std::size_t{256} << 20));
  }
  return t;
}

HealthConfig parse_health(const json& j) {
  const std::string where = "health";
  require_object(j, where);
  reject_unknown_keys(j, where,
                      {"stale_periods", "stale_min_ms", "rate_tolerance_pct", "rate_window_ms",
                       "rate_min_samples"});
  HealthConfig h;
  if (j.contains("stale_periods")) {
    h.stale_periods = get_positive(j.at("stale_periods"), where + ".stale_periods");
  }
  if (j.contains("stale_min_ms")) {
    h.stale_min_ms = get_positive(j.at("stale_min_ms"), where + ".stale_min_ms");
  }
  if (j.contains("rate_tolerance_pct")) {
    h.rate_tolerance_pct = get_positive(j.at("rate_tolerance_pct"), where + ".rate_tolerance_pct");
  }
  if (j.contains("rate_window_ms")) {
    h.rate_window_ms = get_positive(j.at("rate_window_ms"), where + ".rate_window_ms");
  }
  if (j.contains("rate_min_samples")) {
    h.rate_min_samples = static_cast<std::size_t>(
        get_integer(j.at("rate_min_samples"), where + ".rate_min_samples", 1, 1'000'000));
  }
  return h;
}

}  // namespace

Config parse_config(std::string_view json_text) {
  json root;
  try {
    root = json::parse(json_text);
  } catch (const json::parse_error& e) {
    throw ConfigError(std::string("invalid JSON: ") + e.what());
  }

  const std::string top = "config";
  require_object(root, top);
  reject_unknown_keys(root, top, {"sources", "batch", "transport", "health", "seed"});

  Config config;
  const json& sources = require_key(root, top, "sources");
  if (!sources.is_array() || sources.empty()) {
    fail("sources", "must be a non-empty array");
  }
  std::set<std::uint16_t> ids;
  for (std::size_t i = 0; i < sources.size(); ++i) {
    const std::string where = "sources[" + std::to_string(i) + "]";
    SourceConfig source = parse_source(sources[i], where);
    if (!ids.insert(source.id).second) {
      fail(where + ".id", "duplicate source id " + std::to_string(source.id));
    }
    config.sources.push_back(source);
  }

  config.batch = parse_batch(require_key(root, top, "batch"));
  config.transport = parse_transport(require_key(root, top, "transport"));
  if (root.contains("health")) {
    config.health = parse_health(root.at("health"));
  }
  config.seed = get_integer(require_key(root, top, "seed"), "seed", 0,
                            std::numeric_limits<std::uint64_t>::max());
  return config;
}

Config load_config(const std::string& path) {
  std::ifstream in(path);
  if (!in) {
    throw ConfigError("cannot open config file \"" + path + "\"");
  }
  std::ostringstream text;
  text << in.rdbuf();
  return parse_config(text.str());
}

std::string_view source_type_name(SourceType type) {
  switch (type) {
    case SourceType::SimImu:
      return "sim_imu";
    case SourceType::SimGps:
      return "sim_gps";
    case SourceType::SimBattery:
      return "sim_battery";
  }
  return "unknown";
}

std::string_view policy_name(BackpressurePolicy policy) {
  switch (policy) {
    case BackpressurePolicy::DropNewest:
      return "drop_newest";
    case BackpressurePolicy::BlockWithTimeout:
      return "block_with_timeout";
  }
  return "unknown";
}

}  // namespace tstream
