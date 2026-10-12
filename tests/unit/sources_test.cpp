// SampleRing factory, simulated sensor models, and FixedRateSource.

#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <numbers>
#include <stdexcept>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "tstream/clock.hpp"
#include "tstream/sample_ring.hpp"
#include "tstream/sim_models.hpp"
#include "tstream/source.hpp"

namespace {

using namespace std::chrono_literals;
using tstream::FixedRateSource;
using tstream::Sample;
using tstream::SampleKind;
using tstream::SampleModel;
using tstream::SimBatteryModel;
using tstream::SimGpsModel;
using tstream::SimImuModel;

// --- SampleRing -------------------------------------------------------------

TEST(SampleRing, BuildsEverySupportedCapacity) {
  for (std::size_t capacity = 16; capacity <= 65536; capacity *= 2) {
    const auto ring = tstream::make_sample_ring(capacity);
    ASSERT_NE(ring, nullptr);
    EXPECT_EQ(ring->capacity(), capacity);
  }
}

TEST(SampleRing, RejectsUnsupportedCapacity) {
  EXPECT_THROW(tstream::make_sample_ring(8), std::invalid_argument);
  EXPECT_THROW(tstream::make_sample_ring(100), std::invalid_argument);
  EXPECT_THROW(tstream::make_sample_ring(131072), std::invalid_argument);
}

TEST(SampleRing, PushesAndPopsSamples) {
  const auto ring = tstream::make_sample_ring(16);
  Sample in{};
  in.source_id = 7;
  in.seq = 42;
  ASSERT_TRUE(ring->try_push(in));
  EXPECT_EQ(ring->size_approx(), 1U);
  Sample out{};
  ASSERT_EQ(ring->try_pop_bulk(&out, 1), 1U);
  EXPECT_EQ(out.source_id, 7);
  EXPECT_EQ(out.seq, 42U);
}

// --- Models -----------------------------------------------------------------

template <typename Model>
std::vector<Sample> generate(std::uint64_t seed, int count, double rate_hz) {
  Model model(seed);
  std::vector<Sample> out(static_cast<std::size_t>(count));
  for (int i = 0; i < count; ++i) {
    model.fill(out[static_cast<std::size_t>(i)], i / rate_hz);
  }
  return out;
}

bool same_payload(const Sample& a, const Sample& b) {
  // Payload bytes only; header fields are set by the source, not the model.
  return std::memcmp(&a.imu, &b.imu, sizeof(Sample) - offsetof(Sample, imu)) == 0;
}

TEST(SimModels, SameSeedGivesIdenticalSequence) {
  const auto a = generate<SimImuModel>(7, 500, 1000);
  const auto b = generate<SimImuModel>(7, 500, 1000);
  for (std::size_t i = 0; i < a.size(); ++i) {
    ASSERT_TRUE(same_payload(a[i], b[i])) << "sample " << i;
  }
}

TEST(SimModels, DifferentSeedsGiveDifferentNoise) {
  const auto a = generate<SimImuModel>(7, 50, 1000);
  const auto b = generate<SimImuModel>(8, 50, 1000);
  int identical = 0;
  for (std::size_t i = 0; i < a.size(); ++i) {
    identical += same_payload(a[i], b[i]) ? 1 : 0;
  }
  EXPECT_EQ(identical, 0);
}

TEST(SimModels, ModelSeedSeparatesSources) {
  EXPECT_NE(tstream::model_seed(42, 1), tstream::model_seed(42, 2));
  EXPECT_NE(tstream::model_seed(42, 1), tstream::model_seed(43, 1));
  EXPECT_EQ(tstream::model_seed(42, 1), tstream::model_seed(42, 1));
}

TEST(SimModels, ImuMeasuresGravityOnZ) {
  const auto samples = generate<SimImuModel>(1, 2000, 1000);
  double sum_az = 0;
  for (const auto& s : samples) {
    sum_az += static_cast<double>(s.imu.az);
  }
  EXPECT_NEAR(sum_az / static_cast<double>(samples.size()), 9.81, 0.01);
}

TEST(SimModels, GpsMovesNorthEastAtConfiguredSpeed) {
  const auto samples = generate<SimGpsModel>(1, 101, 10);  // 10 s at 10 Hz
  const auto& first = samples.front().gps;
  const auto& last = samples.back().gps;
  EXPECT_GT(last.lat, first.lat);
  EXPECT_GT(last.lon, first.lon);
  EXPECT_EQ(last.fix, 3);
  // 150 m in 10 s, north-east: ~106 m north = ~0.00095 degrees of latitude.
  EXPECT_NEAR(last.lat - SimGpsModel::kStartLat, 150.0 * std::cos(std::numbers::pi / 4) / 111'320.0, 0.0001);
  EXPECT_NEAR(static_cast<double>(last.speed_mps), SimGpsModel::kSpeedMps, 1.0);
}

TEST(SimModels, BatteryDischargesWithinPlausibleRanges) {
  const auto samples = generate<SimBatteryModel>(1, 600, 1);  // 10 minutes at 1 Hz
  for (const auto& s : samples) {
    EXPECT_GT(s.bat.voltage_v, 280.0F);
    EXPECT_LT(s.bat.voltage_v, 410.0F);
    EXPECT_LT(s.bat.current_a, 0.0F);  // discharging
    EXPECT_GE(s.bat.soc_pct, 0.0F);
    EXPECT_LE(s.bat.soc_pct, 100.0F);
  }
  EXPECT_LT(samples.back().bat.soc_pct, samples.front().bat.soc_pct);
  EXPECT_FLOAT_EQ(samples.front().bat.soc_pct, 80.0F);
}

// --- FixedRateSource --------------------------------------------------------

// A model that records nothing interesting: payload is the sim time.
class CountingModel final : public SampleModel {
 public:
  [[nodiscard]] SampleKind kind() const override { return SampleKind::Imu; }
  void fill(Sample& sample, double t_s) override { sample.imu.ax = static_cast<float>(t_s); }
};

std::unique_ptr<FixedRateSource> make_counting_source(tstream::SampleRing& ring, double rate_hz) {
  return std::make_unique<FixedRateSource>(9, "counting", rate_hz, ring,
                                           std::make_unique<CountingModel>());
}

std::vector<Sample> drain(tstream::SampleRing& ring) {
  std::vector<Sample> out(ring.capacity());
  out.resize(ring.try_pop_bulk(out.data(), out.size()));
  return out;
}

TEST(FixedRateSource, StampsHeaderFieldsInOrder) {
  const auto ring = tstream::make_sample_ring(1024);
  auto source = make_counting_source(*ring, 1000);
  source->start();
  std::this_thread::sleep_for(50ms);
  source->stop();

  const auto samples = drain(*ring);
  ASSERT_GT(samples.size(), 5U);
  for (std::size_t i = 0; i < samples.size(); ++i) {
    EXPECT_EQ(samples[i].source_id, 9);
    EXPECT_EQ(samples[i].kind, SampleKind::Imu);
    EXPECT_EQ(samples[i].seq, i);
    if (i > 0) {
      EXPECT_GE(samples[i].t_capture_ns, samples[i - 1].t_capture_ns);
    }
  }
}

// Generous bounds: sanitizer builds and loaded CI runners slow threads down.
TEST(FixedRateSource, ProducesRoughlyAtConfiguredRate) {
  const auto ring = tstream::make_sample_ring(1024);
  auto source = make_counting_source(*ring, 200);
  const auto start = tstream::Clock::now();
  source->start();
  std::this_thread::sleep_for(500ms);
  source->stop();
  const double elapsed_s = std::chrono::duration<double>(tstream::Clock::now() - start).count();

  const auto generated = static_cast<double>(source->counters().generated);
  EXPECT_GT(generated, 0.5 * 200 * elapsed_s);
  EXPECT_LT(generated, 1.2 * 200 * elapsed_s + 2);
}

// SPEC §4.1: a rejected sample still consumes its seq, so drops become gaps.
TEST(FixedRateSource, FullRingDropsNewestAndKeepsSeqConsumed) {
  const auto ring = tstream::make_sample_ring(16);  // nobody drains it
  auto source = make_counting_source(*ring, 2000);
  source->start();
  std::this_thread::sleep_for(100ms);
  source->stop();

  const auto counters = source->counters();
  ASSERT_GT(counters.generated, 16U);
  EXPECT_EQ(counters.dropped, counters.generated - 16);

  const auto samples = drain(*ring);
  ASSERT_EQ(samples.size(), 16U);
  for (std::size_t i = 0; i < samples.size(); ++i) {
    EXPECT_EQ(samples[i].seq, i);  // the first 16 got in; later seqs were dropped
  }
}

// Chunked sleep: a 1 Hz source must not take ~1 s to stop.
TEST(FixedRateSource, StopIsPromptForSlowSources) {
  const auto ring = tstream::make_sample_ring(16);
  auto source = make_counting_source(*ring, 1);
  source->start();
  std::this_thread::sleep_for(20ms);
  const auto before = tstream::Clock::now();
  source->stop();
  EXPECT_LT(tstream::Clock::now() - before, 200ms);
}

TEST(FixedRateSource, StopIsIdempotent) {
  const auto ring = tstream::make_sample_ring(16);
  auto source = make_counting_source(*ring, 100);
  source->stop();  // before start
  source->start();
  source->stop();
  source->stop();
  SUCCEED();
}

TEST(MakeSource, BuildsEachConfiguredType) {
  const auto ring = tstream::make_sample_ring(64);
  tstream::SourceConfig config;
  config.id = 3;
  config.rate_hz = 50;
  config.ring_capacity = 64;
  for (const auto type : {tstream::SourceType::SimImu, tstream::SourceType::SimGps,
                          tstream::SourceType::SimBattery}) {
    config.type = type;
    const auto source = tstream::make_source(config, *ring, 42);
    EXPECT_EQ(source->id(), 3);
    EXPECT_EQ(source->name(), tstream::source_type_name(type));
  }
}

}  // namespace
