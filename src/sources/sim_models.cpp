#include "tstream/sim_models.hpp"

#include <algorithm>
#include <cmath>
#include <numbers>

namespace tstream {

namespace {

constexpr double kTwoPi = 2.0 * std::numbers::pi;
constexpr double kMetersPerDegreeLat = 111'320.0;
constexpr float kGravity = 9.81F;

double wave(double amplitude, double freq_hz, double t_s) {
  return amplitude * std::sin(kTwoPi * freq_hz * t_s);
}

}  // namespace

std::uint64_t model_seed(std::uint64_t seed, std::uint16_t source_id) {
  // splitmix64 finalizer: nearby inputs give unrelated seeds.
  std::uint64_t z = seed + 0x9E3779B97F4A7C15ULL * (std::uint64_t{source_id} + 1);
  z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
  z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
  return z ^ (z >> 31);
}

void SimImuModel::fill(Sample& sample, double t_s) {
  sample.imu.ax = static_cast<float>(wave(0.5, 0.10, t_s)) + accel_noise_(rng_);
  sample.imu.ay = static_cast<float>(wave(0.3, 0.05, t_s)) + accel_noise_(rng_);
  sample.imu.az = kGravity + accel_noise_(rng_);
  sample.imu.gx = gyro_noise_(rng_);
  sample.imu.gy = gyro_noise_(rng_);
  sample.imu.gz = static_cast<float>(wave(0.05, 0.05, t_s)) + gyro_noise_(rng_);
}

void SimGpsModel::fill(Sample& sample, double t_s) {
  constexpr double kHeading = std::numbers::pi / 4;  // north-east
  const double distance_m = kSpeedMps * t_s;
  const double north_m = distance_m * std::cos(kHeading) + position_noise_m_(rng_);
  const double east_m = distance_m * std::sin(kHeading) + position_noise_m_(rng_);
  const double meters_per_degree_lon =
      kMetersPerDegreeLat * std::cos(kStartLat * std::numbers::pi / 180.0);

  sample.gps.lat = kStartLat + north_m / kMetersPerDegreeLat;
  sample.gps.lon = kStartLon + east_m / meters_per_degree_lon;
  sample.gps.alt_m = 20.0F + alt_noise_(rng_);
  sample.gps.speed_mps = static_cast<float>(kSpeedMps) + speed_noise_(rng_);
  sample.gps.fix = 3;
}

void SimBatteryModel::fill(Sample& sample, double t_s) {
  constexpr double kMeanCurrentA = -40.0;  // discharging
  constexpr double kInternalResistanceOhm = 0.05;

  const double current_a =
      kMeanCurrentA + wave(10.0, 0.02, t_s) + static_cast<double>(current_noise_(rng_));
  // Coulomb counting at the mean current keeps SOC monotonic and smooth.
  const double used_ah = -kMeanCurrentA * t_s / 3600.0;
  const double soc_pct = std::max(0.0, kStartSocPct - 100.0 * used_ah / kCapacityAh);
  // Open-circuit voltage from 3.0 V (empty) to 4.2 V (full) per cell, minus
  // the IR drop under load.
  const double ocv_v = kCells * (3.0 + 1.2 * soc_pct / 100.0);
  const double voltage_v = ocv_v + current_a * kInternalResistanceOhm;

  sample.bat.voltage_v = static_cast<float>(voltage_v) + voltage_noise_(rng_);
  sample.bat.current_a = static_cast<float>(current_a);
  sample.bat.soc_pct = static_cast<float>(soc_pct);
  sample.bat.temp_c =
      static_cast<float>(25.0 + 10.0 * (1.0 - std::exp(-t_s / 600.0))) + temp_noise_(rng_);
}

}  // namespace tstream
