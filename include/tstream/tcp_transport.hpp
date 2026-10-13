// Framed, non-blocking TCP sender (SPEC §3.1 TcpTransport, §6).
//
// A BatchSink, so the pipeline thread calls it directly; it has no thread of
// its own. All socket I/O is non-blocking: send() never waits for the network.
//
// - Connection: a non-blocking connect(), completed via epoll. TCP_NODELAY so
//   small frames aren't held back by Nagle. MSG_NOSIGNAL so a closed peer
//   produces EPIPE instead of killing the process with SIGPIPE.
// - Partial writes: whatever the kernel doesn't take is copied into one
//   preallocated pending frame and finished by later send()/poll() calls.
// - Backpressure (M3 scope): while a pending frame is unfinished, or while
//   disconnected, new batches are dropped and counted. M4 replaces this with
//   max_pending_batches buffers.
// - Reconnect: on any socket error or peer close, the socket is closed and a
//   new connect is tried after a backoff that doubles up to max_backoff, and
//   resets once connected.
// - Steady-state send allocates nothing: the host is resolved once at
//   construction, and frames come from the Encoder's preallocated buffers.

#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include <sys/socket.h>

#include "tstream/clock.hpp"
#include "tstream/encoder.hpp"
#include "tstream/sink.hpp"

namespace tstream {

struct TransportCounters {
  std::uint64_t batches_sent = 0;  // frames fully written to the socket
  std::uint64_t samples_sent = 0;
  std::uint64_t bytes_sent = 0;
  std::uint64_t batches_dropped = 0;  // disconnected, pending busy, or cut off mid-frame
  std::uint64_t samples_dropped = 0;
  std::uint64_t connects = 0;     // successful connections
  std::uint64_t disconnects = 0;  // errors or peer closes on a live connection
};

class TcpTransport final : public BatchSink {
 public:
  struct Options {
    std::string host = "127.0.0.1";
    std::uint16_t port = 9000;
    std::size_t max_samples = 256;  // sizes the encoder and pending buffer
    Clock::duration initial_backoff = std::chrono::milliseconds(100);
    Clock::duration max_backoff = std::chrono::seconds(2);
    Clock::duration close_flush_timeout = std::chrono::seconds(2);
  };

  // Resolves the host (throws std::runtime_error if it can't) and starts
  // connecting. Never blocks on the network.
  explicit TcpTransport(Options options);
  // Owns a socket and an epoll fd: not copyable or movable.
  TcpTransport(const TcpTransport&) = delete;
  TcpTransport& operator=(const TcpTransport&) = delete;
  TcpTransport(TcpTransport&&) = delete;
  TcpTransport& operator=(TcpTransport&&) = delete;
  // Closes the socket without waiting for unsent bytes.
  ~TcpTransport() override;

  // Encodes and writes the batch, or drops and counts it (see file comment).
  void send(const BatchView& batch) override;
  // Advances connect, reconnect, and pending writes without a new batch.
  void poll() override;
  // Waits up to close_flush_timeout for the pending frame, then closes.
  void close() override;

  // Relaxed snapshot, safe to read from another thread.
  [[nodiscard]] TransportCounters counters() const;
  // True while a connection is established. Pipeline thread only (or after
  // the pipeline has stopped); counters() is the thread-safe view.
  [[nodiscard]] bool connected() const { return state_ == State::Connected; }

 private:
  enum class State : std::uint8_t { Disconnected, Connecting, Connected };

  // Handles socket readiness: connect completion, errors, and peer close.
  void service_events(int timeout_ms);
  // Opens a socket and starts a non-blocking connect; schedules a retry on failure.
  void start_connect();
  // Moves from Connecting to Connected once SO_ERROR reports success.
  void finish_connect();
  // Closes the socket, drops any unfinished frame, and schedules a reconnect.
  void disconnect(bool was_connected);
  // Writes as much of the pending frame as the socket takes; true once it's empty.
  bool flush_pending();
  // Writes bytes until done or EAGAIN; returns how many were written, or -1 on error.
  std::ptrdiff_t write_some(const std::byte* data, std::size_t size);
  // Points epoll at the events the current state needs.
  void update_interest();
  // Counts a batch as dropped.
  void count_drop(std::size_t samples);

  Options options_;
  sockaddr_storage address_{};
  socklen_t address_len_ = 0;
  Encoder encoder_;

  int epoll_fd_ = -1;
  int fd_ = -1;
  State state_ = State::Disconnected;
  Clock::time_point next_connect_{};
  Clock::duration backoff_;

  // One partially written frame. Preallocated to the largest frame size.
  std::vector<std::byte> pending_;
  std::size_t pending_size_ = 0;
  std::size_t pending_offset_ = 0;
  std::size_t pending_samples_ = 0;

  std::atomic<std::uint64_t> batches_sent_{0};
  std::atomic<std::uint64_t> samples_sent_{0};
  std::atomic<std::uint64_t> bytes_sent_{0};
  std::atomic<std::uint64_t> batches_dropped_{0};
  std::atomic<std::uint64_t> samples_dropped_{0};
  std::atomic<std::uint64_t> connects_{0};
  std::atomic<std::uint64_t> disconnects_{0};
};

}  // namespace tstream
