// Deterministic simulated sensor models.
//
// A model fills one sample's payload from the simulation time (seq / rate, so
// the data doesn't depend on wall-clock jitter) plus seeded Gaussian noise.
// The same (seed, source id) always produces the same sequence. Models know
// nothing about threads or timing; FixedRateSource drives them.

#pragma once

#include <cstdint>
#include <random>

#include "tstream/sample.hpp"

namespace tstream {

class SampleModel {
 public:
  // Owned through std::unique_ptr: not copyable or movable.
  SampleModel() = default;
  SampleModel(const SampleModel&) = delete;
  SampleModel& operator=(const SampleModel&) = delete;
  SampleModel(SampleModel&&) = delete;
  SampleModel& operator=(SampleModel&&) = delete;
  // Virtual: models are deleted through the base class.
  virtual ~SampleModel() = default;

  // The SampleKind this model fills in.
  [[nodiscard]] virtual SampleKind kind() const = 0;
  // Writes the payload for the sample at simulation time t_s. Called in
  // order of increasing t_s; models may keep state between calls.
  virtual void fill(Sample& sample, double t_s) = 0;
};

// Mixes the global seed with the source id so sources don't share noise.
std::uint64_t model_seed(std::uint64_t seed, std::uint16_t source_id);

// Vehicle IMU: gravity on z plus gentle longitudinal/lateral motion.
class SimImuModel final : public SampleModel {
 public:
  // seed: use model_seed(config seed, source id).
  explicit SimImuModel(std::uint64_t seed) : rng_(seed) {}
  // Always SampleKind::Imu.
  [[nodiscard]] SampleKind kind() const override { return SampleKind::Imu; }
  // Accel: gravity on z plus slow motion; gyro: slow yaw. All with noise.
  void fill(Sample& sample, double t_s) override;

 private:
  std::mt19937_64 rng_;
  std::normal_distribution<float> accel_noise_{0.0F, 0.05F};
  std::normal_distribution<float> gyro_noise_{0.0F, 0.01F};
};

// GPS on a vehicle driving north-east at ~15 m/s from a fixed start point.
class SimGpsModel final : public SampleModel {
 public:
  static constexpr double kStartLat = 42.3398;
  static constexpr double kStartLon = -71.0892;
  static constexpr double kSpeedMps = 15.0;

  // seed: use model_seed(config seed, source id).
  explicit SimGpsModel(std::uint64_t seed) : rng_(seed) {}
  // Always SampleKind::Gps.
  [[nodiscard]] SampleKind kind() const override { return SampleKind::Gps; }
  // Position along the north-east track at t_s, plus speed, altitude, and a 3D fix.
  void fill(Sample& sample, double t_s) override;

 private:
  std::mt19937_64 rng_;
  std::normal_distribution<double> position_noise_m_{0.0, 2.0};
  std::normal_distribution<float> speed_noise_{0.0F, 0.2F};
  std::normal_distribution<float> alt_noise_{0.0F, 1.0F};
};

// 96-cell (~360 V nominal) traction battery discharging at ~40 A.
class SimBatteryModel final : public SampleModel {
 public:
  static constexpr double kStartSocPct = 80.0;
  static constexpr double kCapacityAh = 208.0;
  static constexpr int kCells = 96;

  // seed: use model_seed(config seed, source id).
  explicit SimBatteryModel(std::uint64_t seed) : rng_(seed) {}
  // Always SampleKind::Battery.
  [[nodiscard]] SampleKind kind() const override { return SampleKind::Battery; }
  // Voltage, current, SOC, and temperature at t_s.
  void fill(Sample& sample, double t_s) override;

 private:
  std::mt19937_64 rng_;
  std::normal_distribution<float> current_noise_{0.0F, 0.5F};
  std::normal_distribution<float> voltage_noise_{0.0F, 0.2F};
  std::normal_distribution<float> temp_noise_{0.0F, 0.1F};
};

}  // namespace tstream
