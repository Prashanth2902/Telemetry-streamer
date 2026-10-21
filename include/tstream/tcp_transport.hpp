// Framed, non-blocking TCP sender (SPEC §3.1 TcpTransport, §6).
//
// A BatchSink, so the pipeline thread calls it directly; it has no thread of
// its own. All socket I/O is non-blocking: send() never waits for the network.
//
// - Connection: a non-blocking connect(), completed via epoll. TCP_NODELAY so
//   small frames aren't held back by Nagle. MSG_NOSIGNAL so a closed peer
//   produces EPIPE instead of killing the process with SIGPIPE.
// - Pending queue (SPEC §6): up to max_pending_batches (K) encoded frames wait
//   in K slots preallocated at construction, while the socket can't take more
//   (EAGAIN) or while disconnected. Frames leave the queue in order, each
//   counted as sent once its last byte is written.
// - Beyond K, the new batch is dropped whole, and its samples are counted per
//   source, so receiver-side gaps can be reconciled with sender drops (§13).
//   A frame cut off by a disconnect is dropped too; complete queued frames
//   survive and go out after the reconnect.
// - Reconnect: on any socket error or peer close, the socket is closed and a
//   new connect is tried after a backoff that doubles up to max_backoff, and
//   resets once connected.
// - Steady-state send allocates nothing: the host is resolved once at
//   construction, frames come from the Encoder's preallocated buffers, and the
//   queue slots and per-source tallies are preallocated too.

#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "tstream/clock.hpp"
#include "tstream/encoder.hpp"
#include "tstream/net.hpp"
#include "tstream/sink.hpp"

namespace tstream {

struct TransportCounters {
  std::uint64_t batches_sent = 0;  // frames fully written to the socket
  std::uint64_t samples_sent = 0;
  std::uint64_t bytes_sent = 0;
  std::uint64_t batches_dropped = 0;  // queue full, cut off mid-frame, or unsent at close
  std::uint64_t samples_dropped = 0;
  std::uint64_t queued = 0;            // frames waiting right now
  std::uint64_t queue_high_water = 0;  // most frames ever waiting at once
  std::uint64_t connects = 0;          // successful connections
  std::uint64_t disconnects = 0;       // errors or peer closes on a live connection
};

class TcpTransport final : public BatchSink {
 public:
  struct Options {
    std::string host = "127.0.0.1";
    std::uint16_t port = 9000;
    std::size_t max_samples = 256;         // sizes the encoder and each queue slot
    std::size_t max_pending_batches = 64;  // K
    // Ids of the configured sources, for per-source counts. Samples from any
    // other id (e.g. pipeline health samples) are counted under id 0.
    std::vector<std::uint16_t> source_ids;
    int socket_send_buffer_bytes = 0;  // SO_SNDBUF if > 0; else the kernel default
    Clock::duration initial_backoff = std::chrono::milliseconds(100);
    Clock::duration max_backoff = std::chrono::seconds(2);
    Clock::duration close_flush_timeout = std::chrono::seconds(2);
  };

  // Resolves the host (throws std::runtime_error if it can't), allocates the
  // queue, and starts connecting. Never blocks on the network.
  explicit TcpTransport(Options options);
  // Owns a socket and an epoll fd: not copyable or movable.
  TcpTransport(const TcpTransport&) = delete;
  TcpTransport& operator=(const TcpTransport&) = delete;
  TcpTransport(TcpTransport&&) = delete;
  TcpTransport& operator=(TcpTransport&&) = delete;
  // Closes the socket without waiting for unsent bytes.
  ~TcpTransport() override;

  // Writes the batch, queues it, or drops and counts it (see file comment).
  void send(const BatchView& batch) override;
  // Advances connect, reconnect, and queued writes without a new batch.
  void poll() override;
  // Waits up to close_flush_timeout for the queue to drain, counts whatever is
  // left as dropped, then closes.
  void close() override;

  // Relaxed snapshot, safe to read from another thread.
  [[nodiscard]] TransportCounters counters() const;
  // Samples from `source_id` written to the socket so far.
  [[nodiscard]] std::uint64_t samples_sent_for(std::uint16_t source_id) const;
  // Samples from `source_id` dropped by the transport so far.
  [[nodiscard]] std::uint64_t samples_dropped_for(std::uint16_t source_id) const;
  // True while a connection is established. Pipeline thread only (or after
  // the pipeline has stopped); counters() is the thread-safe view.
  [[nodiscard]] bool connected() const { return state_ == State::Connected; }

 private:
  enum class State : std::uint8_t { Disconnected, Connecting, Connected };

  // One queue slot: an encoded frame and what it carries, per source.
  struct Frame {
    std::vector<std::byte> bytes;  // capacity: the worst-case frame
    std::size_t size = 0;
    std::size_t offset = 0;  // bytes already written
    std::size_t samples = 0;
    std::vector<std::uint32_t> per_source;  // indexed like the counters
  };

  // Handles socket readiness: connect completion, errors, and peer close.
  void service_events(int timeout_ms);
  // Opens a socket and starts a non-blocking connect; schedules a retry on failure.
  void start_connect();
  // Moves from Connecting to Connected once SO_ERROR reports success.
  void finish_connect();
  // Closes the socket, drops a half-written frame, and schedules a reconnect.
  void disconnect(bool was_connected);
  // Writes queued frames until the queue is empty or the socket is full;
  // true if the queue is empty afterwards.
  bool flush_queue();
  // Writes bytes until done or EAGAIN; returns how many were written, or -1 on error.
  std::ptrdiff_t write_some(const std::byte* data, std::size_t size);
  // Points epoll at the events the current state needs.
  void update_interest();
  // Counts samples per source into tally_ (resized to the counter layout).
  void tally(const BatchView& batch);
  // Copies `bytes` and the current tally_ into the next free slot.
  void enqueue(std::span<const std::byte> bytes, std::size_t samples);
  // Counts a frame's samples as sent or dropped.
  void count_sent(std::size_t samples, std::span<const std::uint32_t> per_source);
  void count_dropped(std::size_t samples, std::span<const std::uint32_t> per_source);
  // Index into the per-source counters for an id.
  [[nodiscard]] std::size_t index_of(std::uint16_t source_id) const;

  Options options_;
  SocketAddress address_;
  Encoder encoder_;

  int epoll_fd_ = -1;
  int fd_ = -1;
  State state_ = State::Disconnected;
  Clock::time_point next_connect_{};
  Clock::duration backoff_;

  // Ring of K frame slots: queue_[head_] is the oldest of count_ frames.
  std::vector<Frame> queue_;
  std::size_t head_ = 0;
  std::size_t count_ = 0;

  // Source id -> counter index (0 = "other"), and a scratch tally per batch.
  std::vector<std::uint16_t> index_of_;
  std::vector<std::uint32_t> tally_;
  std::unique_ptr<std::atomic<std::uint64_t>[]> sent_by_source_;
  std::unique_ptr<std::atomic<std::uint64_t>[]> dropped_by_source_;

  std::atomic<std::uint64_t> batches_sent_{0};
  std::atomic<std::uint64_t> samples_sent_{0};
  std::atomic<std::uint64_t> bytes_sent_{0};
  std::atomic<std::uint64_t> batches_dropped_{0};
  std::atomic<std::uint64_t> samples_dropped_{0};
  std::atomic<std::uint64_t> queued_{0};
  std::atomic<std::uint64_t> queue_high_water_{0};
  std::atomic<std::uint64_t> connects_{0};
  std::atomic<std::uint64_t> disconnects_{0};
};

}  // namespace tstream
