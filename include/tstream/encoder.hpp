// Batch -> framed Protobuf bytes, without allocating in steady state (SPEC §9).
//
// The generated Protobuf types stay out of this header (they are an
// implementation detail of the encoder and the receiver).

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>

#include "tstream/framing.hpp"
#include "tstream/sink.hpp"

namespace tstream {

// Worst-case encoded size of one Sample inside a Batch, in bytes.
//
//   Sample fields:   source_id   tag 1 + varint <= 5            =  6
//                    seq         tag 1 + varint <= 5            =  6
//                    t_capture   tag 1 + varint <= 10 (int64)   = 11
//                    payload     tag 1 + length 1 + Gps <= 34   = 36
//                                                         total = 59
//   Gps (the largest payload): lat, lon 2 x (1 + 8) + alt, speed 2 x (1 + 4)
//                              + fix (1 + 5)                    = 34
//   Inside the Batch, each Sample adds its own tag 1 + length 1:  61
inline constexpr std::size_t kMaxEncodedSampleBytes = 61;

// Batch fields outside the samples: batch_seq (1 + 10) + t_send_ns (1 + 10).
inline constexpr std::size_t kMaxBatchOverheadBytes = 22;

// Largest frame (header + Batch) a batch of max_samples can produce. Config
// validation requires this to fit in transport.max_frame_bytes.
constexpr std::size_t max_frame_bytes_for(std::size_t max_samples) {
  return framing::kHeaderBytes + kMaxBatchOverheadBytes + max_samples * kMaxEncodedSampleBytes;
}

// Encodes batches as complete frames into a buffer it owns.
//
// Each Batch message is built on a Protobuf arena whose first block is
// preallocated here and reused for every batch, then serialized into a
// preallocated output buffer (ADR-005). After construction, encoding a batch
// of up to max_samples allocates nothing; tests/alloc checks this.
class Encoder {
 public:
  // Allocates the arena block and output buffer for batches of up to max_samples.
  explicit Encoder(std::size_t max_samples);
  // Owns the arena and buffers: not copyable or movable.
  Encoder(const Encoder&) = delete;
  Encoder& operator=(const Encoder&) = delete;
  Encoder(Encoder&&) = delete;
  Encoder& operator=(Encoder&&) = delete;
  // Defined in the .cpp, where the Protobuf types are complete.
  ~Encoder();

  // Encodes `batch` with Batch.t_send_ns = t_send_ns, as header + payload.
  // The returned bytes are valid until the next call. Throws
  // std::invalid_argument for an empty batch (its payload could be 0 bytes,
  // which framing rejects) and std::length_error above max_samples.
  [[nodiscard]] std::span<const std::byte> encode_frame(const BatchView& batch,
                                                        std::int64_t t_send_ns);

  // The batch size this encoder was sized for.
  [[nodiscard]] std::size_t max_samples() const;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace tstream
