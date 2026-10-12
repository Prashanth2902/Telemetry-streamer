// In-memory sample that flows through the hot path (SPEC §4.1).
//
// Trivially copyable on purpose: it is copied by value through SpscRing and
// into batches, never heap-allocated. The wire format (Protobuf, M3) is
// separate and built from this at encode time.

#pragma once

#include <cstdint>
#include <string_view>
#include <type_traits>

namespace tstream {

enum class SampleKind : std::uint8_t { Imu, Gps, Battery, Can, Health };

struct ImuData {
  float ax, ay, az;  // m/s^2
  float gx, gy, gz;  // rad/s
};

struct GpsData {
  double lat, lon;  // degrees
  float alt_m;
  float speed_mps;
  std::uint8_t fix;  // 0 = none, 2 = 2D, 3 = 3D
};

struct BatteryData {
  float voltage_v;
  float current_a;  // negative = discharging
  float soc_pct;
  float temp_c;
};

struct CanData {
  std::uint32_t can_id;
  std::uint8_t dlc;
  std::uint8_t data[8];
};

struct HealthData {
  std::uint16_t source_id;  // the source the event is about
  std::uint8_t monitor;
  std::uint8_t severity;
  std::uint32_t detail;
};

struct Sample {
  std::uint16_t source_id;
  SampleKind kind;
  std::uint32_t seq;          // per-source, assigned before try_push (§4.1)
  std::int64_t t_capture_ns;  // steady_clock at creation
  union {
    ImuData imu;
    GpsData gps;
    BatteryData bat;
    CanData can;
    HealthData health;
  };
};
static_assert(std::is_trivially_copyable_v<Sample>);
static_assert(sizeof(Sample) == 48, "Sample layout changed; check ring and batch sizing");

// Source id 0 belongs to the pipeline itself (health samples, §4.1).
inline constexpr std::uint16_t kPipelineSourceId = 0;

constexpr std::string_view kind_name(SampleKind kind) {
  switch (kind) {
    case SampleKind::Imu:
      return "imu";
    case SampleKind::Gps:
      return "gps";
    case SampleKind::Battery:
      return "battery";
    case SampleKind::Can:
      return "can";
    case SampleKind::Health:
      return "health";
  }
  return "unknown";
}

}  // namespace tstream
