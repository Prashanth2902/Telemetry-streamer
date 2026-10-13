#include "tstream/encoder.hpp"

#include <algorithm>
#include <stdexcept>
#include <string>
#include <vector>

#include <google/protobuf/arena.h>

#include "telemetry.pb.h"

namespace tstream {

namespace {

namespace pb = tstream::v1;

// Arena bytes reserved per sample. A Sample message, its payload submessage,
// and its slot in the repeated field take well under this; the allocation
// test (tests/alloc) fails if a full batch ever outgrows the block.
constexpr std::size_t kArenaBytesPerSample = 256;
constexpr std::size_t kArenaBaseBytes = 4096;

// Copies one in-memory sample into its wire message.
void fill(pb::Sample& out, const Sample& in) {
  out.set_source_id(in.source_id);
  out.set_seq(in.seq);
  out.set_t_capture_ns(in.t_capture_ns);
  switch (in.kind) {
    case SampleKind::Imu: {
      pb::Imu& m = *out.mutable_imu();
      m.set_ax(in.imu.ax);
      m.set_ay(in.imu.ay);
      m.set_az(in.imu.az);
      m.set_gx(in.imu.gx);
      m.set_gy(in.imu.gy);
      m.set_gz(in.imu.gz);
      break;
    }
    case SampleKind::Gps: {
      pb::Gps& m = *out.mutable_gps();
      m.set_lat(in.gps.lat);
      m.set_lon(in.gps.lon);
      m.set_alt_m(in.gps.alt_m);
      m.set_speed_mps(in.gps.speed_mps);
      m.set_fix(in.gps.fix);
      break;
    }
    case SampleKind::Battery: {
      pb::Battery& m = *out.mutable_battery();
      m.set_voltage_v(in.bat.voltage_v);
      m.set_current_a(in.bat.current_a);
      m.set_soc_pct(in.bat.soc_pct);
      m.set_temp_c(in.bat.temp_c);
      break;
    }
    case SampleKind::Can: {
      pb::Can& m = *out.mutable_can();
      m.set_can_id(in.can.can_id);
      // At most 8 data bytes: fits std::string's small-buffer, so no heap.
      const std::size_t length = std::min<std::size_t>(in.can.dlc, sizeof(in.can.data));
      m.set_data(reinterpret_cast<const char*>(in.can.data), length);
      break;
    }
    case SampleKind::Health: {
      pb::Health& m = *out.mutable_health();
      m.set_source_id(in.health.source_id);
      m.set_monitor(in.health.monitor);
      m.set_severity(in.health.severity);
      m.set_detail(in.health.detail);
      break;
    }
  }
}

}  // namespace

struct Encoder::Impl {
  // Sizes both buffers once; the arena never needs another block after this.
  explicit Impl(std::size_t max)
      : max_samples(max),
        arena_block(kArenaBaseBytes + max * kArenaBytesPerSample),
        arena(arena_block.data(), arena_block.size()),
        frame(max_frame_bytes_for(max)) {}

  std::size_t max_samples;
  std::vector<char> arena_block;  // must outlive `arena`, so declared first
  google::protobuf::Arena arena;
  std::vector<std::byte> frame;
};

// All allocation for the encoder's lifetime happens here.
Encoder::Encoder(std::size_t max_samples) : impl_(std::make_unique<Impl>(max_samples)) {
  if (max_samples == 0) {
    throw std::invalid_argument("Encoder needs max_samples > 0");
  }
}

// Out of line, where Impl is complete.
Encoder::~Encoder() = default;

// Build on the arena, serialize after the header, then Reset() the arena so the
// next batch reuses the same block.
std::span<const std::byte> Encoder::encode_frame(const BatchView& batch, std::int64_t t_send_ns) {
  if (batch.samples.empty()) {
    throw std::invalid_argument("Encoder: empty batch");
  }
  if (batch.samples.size() > impl_->max_samples) {
    throw std::length_error("Encoder: batch of " + std::to_string(batch.samples.size()) +
                            " samples exceeds max_samples " + std::to_string(impl_->max_samples));
  }

  auto* message = google::protobuf::Arena::CreateMessage<pb::Batch>(&impl_->arena);
  message->set_batch_seq(batch.batch_seq);
  message->set_t_send_ns(t_send_ns);
  message->mutable_samples()->Reserve(static_cast<int>(batch.samples.size()));
  for (const Sample& sample : batch.samples) {
    fill(*message->add_samples(), sample);
  }

  const std::size_t payload_size = message->ByteSizeLong();
  if (framing::kHeaderBytes + payload_size > impl_->frame.size()) {
    // max_frame_bytes_for() is a proven upper bound, so this is a bug.
    throw std::logic_error("Encoder: batch exceeds the worst-case size bound");
  }
  std::byte* frame = impl_->frame.data();
  framing::write_header(std::span<std::byte, framing::kHeaderBytes>(frame, framing::kHeaderBytes),
                        static_cast<std::uint32_t>(payload_size));
  message->SerializeWithCachedSizesToArray(
      reinterpret_cast<std::uint8_t*>(frame + framing::kHeaderBytes));

  impl_->arena.Reset();  // destroys `message`; keeps the user-supplied block
  return {frame, framing::kHeaderBytes + payload_size};
}

// Fixed at construction.
std::size_t Encoder::max_samples() const {
  return impl_->max_samples;
}

}  // namespace tstream
