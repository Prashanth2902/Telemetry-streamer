// Encoder output: frame header, Protobuf round trip, and the worst-case size bound.

#include "tstream/encoder.hpp"

#include <cstdint>
#include <cstring>
#include <limits>
#include <span>
#include <stdexcept>
#include <vector>

#include <gtest/gtest.h>

#include "tstream/framing.hpp"

#include "telemetry.pb.h"

namespace {

using tstream::BatchView;
using tstream::Encoder;
using tstream::Sample;
using tstream::SampleKind;
using tstream::framing::kHeaderBytes;

// One sample of each kind with distinctive field values.
std::vector<Sample> one_of_each_kind() {
  std::vector<Sample> samples(5);
  samples[0].kind = SampleKind::Imu;
  samples[0].imu = {.ax = 1, .ay = 2, .az = 9.81F, .gx = -1, .gy = -2, .gz = 0.5F};
  samples[1].kind = SampleKind::Gps;
  samples[1].gps = {.lat = 42.34, .lon = -71.09, .alt_m = 20, .speed_mps = 15, .fix = 3};
  samples[2].kind = SampleKind::Battery;
  samples[2].bat = {.voltage_v = 380, .current_a = -40, .soc_pct = 80, .temp_c = 25};
  samples[3].kind = SampleKind::Can;
  samples[3].can = {.can_id = 0x123, .dlc = 3, .data = {0xDE, 0xAD, 0xBE, 0, 0, 0, 0, 0}};
  samples[4].kind = SampleKind::Health;
  samples[4].health = {.source_id = 2, .monitor = 1, .severity = 3, .detail = 77};
  for (std::size_t i = 0; i < samples.size(); ++i) {
    samples[i].source_id = static_cast<std::uint16_t>(i + 1);
    samples[i].seq = static_cast<std::uint32_t>(100 + i);
    samples[i].t_capture_ns = 1'000'000'000 + static_cast<std::int64_t>(i);
  }
  return samples;
}

// Checks the frame header and parses the payload.
tstream::v1::Batch decode(std::span<const std::byte> frame) {
  EXPECT_GT(frame.size(), kHeaderBytes);
  const auto check = tstream::framing::parse_header(
      std::span<const std::byte, kHeaderBytes>(frame.data(), kHeaderBytes), 1 << 20);
  EXPECT_TRUE(check.ok);
  EXPECT_EQ(check.length, frame.size() - kHeaderBytes);
  tstream::v1::Batch batch;
  EXPECT_TRUE(batch.ParseFromArray(frame.data() + kHeaderBytes,
                                   static_cast<int>(frame.size() - kHeaderBytes)));
  return batch;
}

TEST(Encoder, RoundTripsEveryKind) {
  const auto samples = one_of_each_kind();
  Encoder encoder(16);
  const auto batch =
      decode(encoder.encode_frame(BatchView{.batch_seq = 7, .t_flush_ns = 0, .samples = samples},
                                  /*t_send_ns=*/5555));

  EXPECT_EQ(batch.batch_seq(), 7U);
  EXPECT_EQ(batch.t_send_ns(), 5555);
  ASSERT_EQ(batch.samples_size(), 5);
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(batch.samples(i).source_id(), static_cast<std::uint32_t>(i + 1));
    EXPECT_EQ(batch.samples(i).seq(), static_cast<std::uint32_t>(100 + i));
    EXPECT_EQ(batch.samples(i).t_capture_ns(), 1'000'000'000 + i);
  }

  const auto& imu = batch.samples(0);
  ASSERT_TRUE(imu.has_imu());
  EXPECT_FLOAT_EQ(imu.imu().az(), 9.81F);
  EXPECT_FLOAT_EQ(imu.imu().gy(), -2);

  const auto& gps = batch.samples(1);
  ASSERT_TRUE(gps.has_gps());
  EXPECT_DOUBLE_EQ(gps.gps().lat(), 42.34);
  EXPECT_DOUBLE_EQ(gps.gps().lon(), -71.09);
  EXPECT_EQ(gps.gps().fix(), 3U);

  const auto& bat = batch.samples(2);
  ASSERT_TRUE(bat.has_battery());
  EXPECT_FLOAT_EQ(bat.battery().voltage_v(), 380);
  EXPECT_FLOAT_EQ(bat.battery().current_a(), -40);

  const auto& can = batch.samples(3);
  ASSERT_TRUE(can.has_can());
  EXPECT_EQ(can.can().can_id(), 0x123U);
  EXPECT_EQ(can.can().data(), std::string("\xDE\xAD\xBE", 3));  // dlc bytes only

  const auto& health = batch.samples(4);
  ASSERT_TRUE(health.has_health());
  EXPECT_EQ(health.health().source_id(), 2U);
  EXPECT_EQ(health.health().detail(), 77U);
}

// The arena is reset between batches: a later batch must not carry leftovers.
TEST(Encoder, ReusesBuffersAcrossBatches) {
  auto samples = one_of_each_kind();
  Encoder encoder(16);
  const auto first =
      encoder.encode_frame(BatchView{.batch_seq = 1, .t_flush_ns = 0, .samples = samples}, 1);
  const std::byte* buffer = first.data();

  samples.resize(2);
  const auto second_frame =
      encoder.encode_frame(BatchView{.batch_seq = 2, .t_flush_ns = 0, .samples = samples}, 2);
  EXPECT_EQ(second_frame.data(), buffer);  // same output buffer
  const auto second = decode(second_frame);
  EXPECT_EQ(second.batch_seq(), 2U);
  EXPECT_EQ(second.samples_size(), 2);
}

// Maximal values for every field: each encoded frame must fit the SPEC §9 bound.
TEST(Encoder, WorstCaseSamplesFitTheSizeBound) {
  constexpr std::size_t kSamples = 64;
  for (const SampleKind kind : {SampleKind::Imu, SampleKind::Gps, SampleKind::Battery,
                                SampleKind::Can, SampleKind::Health}) {
    std::vector<Sample> samples(kSamples);
    for (Sample& s : samples) {
      s.kind = kind;
      s.source_id = std::numeric_limits<std::uint16_t>::max();
      s.seq = std::numeric_limits<std::uint32_t>::max();
      s.t_capture_ns = std::numeric_limits<std::int64_t>::min();  // negative: 10-byte varint
      s.imu = {.ax = -1e30F, .ay = 1e30F, .az = 1, .gx = 1, .gy = 1, .gz = 1};
      if (kind == SampleKind::Gps) {
        s.gps = {.lat = -90.1, .lon = 180.1, .alt_m = 1e9F, .speed_mps = 1e9F, .fix = 255};
      } else if (kind == SampleKind::Can) {
        s.can = {.can_id = 0xFFFFFFFF, .dlc = 8, .data = {1, 2, 3, 4, 5, 6, 7, 8}};
      } else if (kind == SampleKind::Health) {
        s.health = {.source_id = 65535, .monitor = 255, .severity = 255, .detail = 0xFFFFFFFF};
      } else if (kind == SampleKind::Battery) {
        s.bat = {.voltage_v = 1e9F, .current_a = -1e9F, .soc_pct = 100, .temp_c = -40};
      }
    }
    Encoder encoder(kSamples);
    const auto frame =
        encoder.encode_frame(BatchView{.batch_seq = std::numeric_limits<std::uint64_t>::max(),
                                       .t_flush_ns = 0,
                                       .samples = samples},
                             std::numeric_limits<std::int64_t>::min());
    EXPECT_LE(frame.size(), tstream::max_frame_bytes_for(kSamples))
        << "kind " << static_cast<int>(kind);
  }
}

TEST(Encoder, RejectsEmptyAndOversizedBatches) {
  Encoder encoder(2);
  const std::vector<Sample> none;
  EXPECT_THROW(static_cast<void>(encoder.encode_frame(
                   BatchView{.batch_seq = 0, .t_flush_ns = 0, .samples = none}, 1)),
               std::invalid_argument);
  const std::vector<Sample> three(3);
  EXPECT_THROW(static_cast<void>(encoder.encode_frame(
                   BatchView{.batch_seq = 0, .t_flush_ns = 0, .samples = three}, 1)),
               std::length_error);
}

TEST(Encoder, MaxFrameBytesBoundScalesWithBatchSize) {
  EXPECT_EQ(tstream::max_frame_bytes_for(0), kHeaderBytes + tstream::kMaxBatchOverheadBytes);
  EXPECT_EQ(tstream::max_frame_bytes_for(256) - tstream::max_frame_bytes_for(255),
            tstream::kMaxEncodedSampleBytes);
}

}  // namespace
