// tstream-recv's core (SPEC §3): accept a sender, reassemble frames, decode
// and validate batches, and keep stats. Driven by poll(), so the executable
// and tests share it.
//
// - One sender at a time. A second connection is accepted and closed at once,
//   and counted.
// - A bad frame header closes the connection (the stream can't be
//   resynchronized) and is counted by reason; the sender reconnects.
// - Slow-consumer mode (SPEC §8 slow_consumer) forces backpressure: each
//   poll() reads at most kSlowReadBytes and then sleeps slow_read_delay, and the
//   listening socket gets a small receive buffer so the kernel can't hide the
//   backlog. slow_for ends the slow phase that long after the first connection.
// - Latency (SPEC §4.4) is only measured when the sender is on this host,
//   where both sides read the same CLOCK_MONOTONIC. For a remote sender it is
//   reported as unavailable instead of as misleading numbers.
// - Health samples (SPEC §7) are counted per monitor and queued for the caller
//   to print, in the order they arrived with the data.

#pragma once

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "tstream/framing.hpp"
#include "tstream/health.hpp"
#include "tstream/latency_histogram.hpp"
#include "tstream/sample.hpp"
#include "tstream/seq_tracker.hpp"

namespace tstream {

struct ReceiverStats {
  std::uint64_t connections = 0;           // accepted senders
  std::uint64_t rejected_connections = 0;  // extra senders while one was active
  std::uint64_t disconnects = 0;
  std::uint64_t bytes = 0;
  std::uint64_t batches = 0;
  std::uint64_t samples = 0;
  std::uint64_t batch_seq_gaps = 0;   // batches missing between received ones
  std::uint64_t decode_errors = 0;    // well-framed payloads that aren't a valid Batch
  std::uint64_t invalid_samples = 0;  // samples with no payload or an out-of-range id
  std::array<std::uint64_t, framing::kFrameErrorCount> frame_errors{};  // by FrameError
  // Health samples by HealthMonitorKind value; index 0 counts unknown monitors.
  std::array<std::uint64_t, kHealthMonitorKinds + 1> health_events{};

  // Total frame errors of all kinds.
  [[nodiscard]] std::uint64_t total_frame_errors() const;
  // Total health samples of all monitors.
  [[nodiscard]] std::uint64_t total_health_events() const;
};

// A health sample as received: when it was raised, and what it says.
struct ReceivedHealthEvent {
  std::int64_t t_capture_ns = 0;
  HealthData data{};
};

class Receiver {
 public:
  struct Options {
    std::string bind_host = "127.0.0.1";
    std::uint16_t port = 9000;  // 0 picks a free port; see port()
    std::size_t max_frame_bytes = 1 << 20;
    std::chrono::milliseconds slow_read_delay{0};  // > 0 turns on slow-consumer mode
    std::chrono::milliseconds slow_for{0};         // 0 = slow for the whole run
  };

  // Slow-consumer mode: bytes per read, and the listening socket's SO_RCVBUF.
  static constexpr std::size_t kSlowReadBytes = 1024;
  static constexpr int kSlowReceiveBuffer = 4096;

  // Binds and listens. Throws std::runtime_error if that fails.
  explicit Receiver(Options options);
  // Owns sockets: not copyable or movable.
  Receiver(const Receiver&) = delete;
  Receiver& operator=(const Receiver&) = delete;
  Receiver(Receiver&&) = delete;
  Receiver& operator=(Receiver&&) = delete;
  // Closes the listening and connected sockets.
  ~Receiver();

  // Waits up to timeout_ms for socket activity and handles all of it.
  void poll(int timeout_ms);

  // The port actually bound (useful with port 0).
  [[nodiscard]] std::uint16_t port() const { return port_; }
  // Counters since construction.
  [[nodiscard]] const ReceiverStats& stats() const { return stats_; }
  // Per-source seq tracking since construction.
  [[nodiscard]] const SeqTracker& seq() const { return seq_; }
  // True if the current (or last) sender is on this host, so latency is valid.
  [[nodiscard]] bool latency_available() const { return latency_available_; }

  // Capture-to-receive latency since construction.
  [[nodiscard]] const LatencyHistogram& e2e_latency() const { return e2e_; }
  // Capture-to-send latency since construction.
  [[nodiscard]] const LatencyHistogram& pipeline_latency() const { return pipeline_; }
  // Same two, since the last take_interval() call; then cleared.
  void take_interval(LatencyHistogram& e2e, LatencyHistogram& pipeline);
  // Health events received since the last call, oldest first; then cleared.
  [[nodiscard]] std::vector<ReceivedHealthEvent> take_health_events();

 private:
  // Accepts a pending connection (or rejects it if one is already active).
  void accept_connection();
  // Reads everything available and processes complete frames.
  void read_connection();
  // Closes the current connection and readies the decoder for the next one.
  void drop_connection();
  // Feeds `bytes` to the decoder and handles every complete frame; false if
  // a bad header closed the connection.
  bool consume(std::span<const std::byte> bytes);
  // True while slow-consumer mode is in effect.
  [[nodiscard]] bool slow() const;
  // Decodes one frame payload and updates stats; false if it isn't a valid Batch.
  bool handle_frame(std::span<const std::byte> payload);
  // Counts and queues one health sample.
  void record_health(std::int64_t t_capture_ns, std::uint32_t source_id, std::uint32_t monitor,
                     std::uint32_t severity, std::uint32_t detail);

  Options options_;
  int listen_fd_ = -1;
  int epoll_fd_ = -1;
  int conn_fd_ = -1;
  std::uint16_t port_ = 0;
  bool latency_available_ = false;
  bool have_batch_seq_ = false;
  std::optional<std::chrono::steady_clock::time_point> slow_until_;  // set on first connect
  std::uint64_t next_batch_seq_ = 0;

  framing::FrameDecoder decoder_;
  std::vector<std::byte> read_buffer_;
  ReceiverStats stats_;
  SeqTracker seq_;
  LatencyHistogram e2e_;
  LatencyHistogram pipeline_;
  LatencyHistogram e2e_interval_;
  LatencyHistogram pipeline_interval_;
  std::vector<ReceivedHealthEvent> health_pending_;
};

}  // namespace tstream
