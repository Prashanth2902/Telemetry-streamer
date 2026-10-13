// Where finished batches go. M2 prints them (StdoutSink); M3 replaces this
// with Protobuf encoding over TCP behind the same interface.

#pragma once

#include <cstdint>
#include <cstdio>
#include <span>

#include "tstream/sample.hpp"

namespace tstream {

// A finished batch. Non-owning: `samples` is only valid during send().
struct BatchView {
  std::uint64_t batch_seq;
  std::int64_t t_flush_ns;
  std::span<const Sample> samples;
};

class BatchSink {
 public:
  // Used by reference through the interface: not copyable or movable.
  BatchSink() = default;
  BatchSink(const BatchSink&) = delete;
  BatchSink& operator=(const BatchSink&) = delete;
  BatchSink(BatchSink&&) = delete;
  BatchSink& operator=(BatchSink&&) = delete;
  // Virtual: sinks are used through the base class.
  virtual ~BatchSink() = default;

  // Called on the pipeline thread, once per batch, in batch_seq order.
  virtual void send(const BatchView& batch) = 0;
  // Called once after the final batch.
  virtual void close() {}
};

// One line per batch: sequence number, size, per-kind counts, and how long
// the oldest sample waited between capture and flush.
//
// Debug output only: stdio writes on the pipeline thread can block, which a
// real transport must not do (M3). Each line is formatted into a fixed stack
// buffer, so formatting itself doesn't allocate.
class StdoutSink final : public BatchSink {
 public:
  // Writes to `out`: stdout by default, any FILE* in tests.
  explicit StdoutSink(std::FILE* out = stdout) : out_(out) {}
  // Prints one summary line for the batch.
  void send(const BatchView& batch) override;
  // Flushes buffered output.
  void close() override;

 private:
  std::FILE* out_;
};

}  // namespace tstream
