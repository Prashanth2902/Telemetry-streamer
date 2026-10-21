#include "tstream/tcp_transport.hpp"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <system_error>
#include <thread>
#include <utility>

#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <unistd.h>

namespace tstream {

namespace {

// Closes fd if open and marks it closed.
void close_fd(int& fd) {
  if (fd >= 0) {
    ::close(fd);
    fd = -1;
  }
}

// Bumps `high_water` to at least `value` (single writer, so no CAS loop needed).
void raise_to(std::atomic<std::uint64_t>& high_water, std::uint64_t value) {
  if (value > high_water.load(std::memory_order_relaxed)) {
    high_water.store(value, std::memory_order_relaxed);
  }
}

}  // namespace

// Every buffer the transport will ever use is allocated here: K frame slots,
// the id lookup table, and per-source counters. Then a first connect.
TcpTransport::TcpTransport(Options options)
    : options_(std::move(options)),
      encoder_(options_.max_samples),
      backoff_(options_.initial_backoff),
      queue_(std::max<std::size_t>(options_.max_pending_batches, 1)),
      index_of_(options_.source_ids) {
  const std::size_t counters = index_of_.size();  // [0] = "other"
  tally_.resize(counters);
  sent_by_source_ = std::make_unique<std::atomic<std::uint64_t>[]>(counters);
  dropped_by_source_ = std::make_unique<std::atomic<std::uint64_t>[]>(counters);
  for (Frame& frame : queue_) {
    frame.bytes.resize(max_frame_bytes_for(options_.max_samples));
    frame.per_source.resize(counters);
  }

  address_ = resolve_tcp(options_.host, options_.port, /*passive=*/false);
  epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
  if (epoll_fd_ < 0) {
    throw std::runtime_error("epoll_create1: " + std::system_category().message(errno));
  }
  start_connect();
}

// No flush here: close() is the graceful path, the destructor just releases fds.
TcpTransport::~TcpTransport() {
  close_fd(fd_);
  close_fd(epoll_fd_);
}

// Order matters: catch up on socket events and drain the queue first, so
// frames leave in batch_seq order and this batch goes straight to the socket
// whenever nothing is waiting ahead of it.
void TcpTransport::send(const BatchView& batch) {
  service_events(0);
  if (state_ == State::Disconnected && Clock::now() >= next_connect_) {
    start_connect();
  }
  tally(batch);
  const bool queue_empty = state_ == State::Connected ? flush_queue() : count_ == 0;

  if (queue_empty && state_ == State::Connected) {
    const auto frame = encoder_.encode_frame(batch, now_ns());
    const std::ptrdiff_t written = write_some(frame.data(), frame.size());
    if (written < 0) {
      count_dropped(batch.samples.size(), tally_);
      disconnect(/*was_connected=*/true);
      return;
    }
    const auto done = static_cast<std::size_t>(written);
    bytes_sent_.fetch_add(done, std::memory_order_relaxed);
    if (done == frame.size()) {
      count_sent(batch.samples.size(), tally_);
      return;
    }
    // The kernel took part of the frame: queue the rest (the queue is empty).
    enqueue(frame.subspan(done), batch.samples.size());
    update_interest();
    return;
  }

  if (count_ == queue_.size()) {
    count_dropped(batch.samples.size(), tally_);  // all K slots busy: drop the new batch
    return;
  }
  enqueue(encoder_.encode_frame(batch, now_ns()), batch.samples.size());
  update_interest();
}

// Same as the start of send(), without a batch.
void TcpTransport::poll() {
  service_events(0);
  if (state_ == State::Disconnected && Clock::now() >= next_connect_) {
    start_connect();
  }
  if (state_ == State::Connected) {
    flush_queue();
  }
}

// Bounded wait so a dead or stalled receiver can't hang shutdown. Keeps
// reconnecting during the wait, so queued frames can still reach a receiver
// that comes back.
void TcpTransport::close() {
  const auto deadline = Clock::now() + options_.close_flush_timeout;
  while (count_ > 0) {
    const auto remaining =
        std::chrono::duration_cast<std::chrono::milliseconds>(deadline - Clock::now());
    if (remaining.count() <= 0) {
      break;
    }
    if (state_ == State::Disconnected) {
      if (Clock::now() >= next_connect_) {
        start_connect();
      } else {
        std::this_thread::sleep_for(
            std::min<Clock::duration>(next_connect_ - Clock::now(), std::chrono::milliseconds(50)));
      }
      continue;
    }
    service_events(static_cast<int>(std::min<std::int64_t>(remaining.count(), 50)));
    if (state_ == State::Connected) {
      flush_queue();
    }
  }
  while (count_ > 0) {
    const Frame& frame = queue_[head_];
    count_dropped(frame.samples, frame.per_source);
    head_ = (head_ + 1) % queue_.size();
    --count_;
  }
  queued_.store(0, std::memory_order_relaxed);
  if (fd_ >= 0) {
    ::shutdown(fd_, SHUT_WR);  // the receiver sees EOF after the last frame
  }
  close_fd(fd_);
  state_ = State::Disconnected;
}

// Relaxed loads: a monitoring snapshot.
TransportCounters TcpTransport::counters() const {
  return {.batches_sent = batches_sent_.load(std::memory_order_relaxed),
          .samples_sent = samples_sent_.load(std::memory_order_relaxed),
          .bytes_sent = bytes_sent_.load(std::memory_order_relaxed),
          .batches_dropped = batches_dropped_.load(std::memory_order_relaxed),
          .samples_dropped = samples_dropped_.load(std::memory_order_relaxed),
          .queued = queued_.load(std::memory_order_relaxed),
          .queue_high_water = queue_high_water_.load(std::memory_order_relaxed),
          .connects = connects_.load(std::memory_order_relaxed),
          .disconnects = disconnects_.load(std::memory_order_relaxed)};
}

// Unconfigured ids all share the "other" counter.
std::uint64_t TcpTransport::samples_sent_for(std::uint16_t source_id) const {
  return sent_by_source_[index_of_(source_id)].load(std::memory_order_relaxed);
}

// Unconfigured ids all share the "other" counter.
std::uint64_t TcpTransport::samples_dropped_for(std::uint16_t source_id) const {
  return dropped_by_source_[index_of_(source_id)].load(std::memory_order_relaxed);
}

// One fd, so one event at most; EPOLLRDHUP and EPOLLIN both mean the peer is
// gone, since the receiver never sends anything back.
void TcpTransport::service_events(int timeout_ms) {
  if (fd_ < 0) {
    return;
  }
  epoll_event event{};
  if (::epoll_wait(epoll_fd_, &event, 1, timeout_ms) <= 0) {
    return;
  }
  if (state_ == State::Connecting) {
    finish_connect();
    return;
  }
  if ((event.events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP | EPOLLIN)) != 0) {
    disconnect(/*was_connected=*/true);
  }
}

// connect() on a non-blocking socket usually returns EINPROGRESS; on loopback
// it can also succeed immediately.
void TcpTransport::start_connect() {
  fd_ = ::socket(address_.family(), SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  if (fd_ < 0) {
    disconnect(/*was_connected=*/false);
    return;
  }
  const int one = 1;
  ::setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
  if (options_.socket_send_buffer_bytes > 0) {
    ::setsockopt(fd_, SOL_SOCKET, SO_SNDBUF, &options_.socket_send_buffer_bytes,
                 sizeof(options_.socket_send_buffer_bytes));
  }

  const int rc = ::connect(fd_, address_.get(), address_.length);
  if (rc == 0) {
    state_ = State::Connecting;  // finish_connect() makes the bookkeeping uniform
    finish_connect();
    return;
  }
  if (errno != EINPROGRESS) {
    disconnect(/*was_connected=*/false);
    return;
  }
  state_ = State::Connecting;
  epoll_event event{};
  event.events = EPOLLOUT;
  ::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd_, &event);
}

// SO_ERROR holds the result of the asynchronous connect.
void TcpTransport::finish_connect() {
  int error = 0;
  socklen_t len = sizeof(error);
  if (::getsockopt(fd_, SOL_SOCKET, SO_ERROR, &error, &len) != 0 || error != 0) {
    disconnect(/*was_connected=*/false);
    return;
  }
  state_ = State::Connected;
  backoff_ = options_.initial_backoff;
  connects_.fetch_add(1, std::memory_order_relaxed);
  epoll_event event{};
  event.events = EPOLLIN | EPOLLRDHUP;
  if (::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd_, &event) != 0) {
    ::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd_, &event);  // connected without EINPROGRESS
  }
  update_interest();  // queued frames want EPOLLOUT
}

// The receiver can't use half a frame, so a partially written head frame is
// dropped; frames not started yet stay queued for the next connection.
void TcpTransport::disconnect(bool was_connected) {
  if (fd_ >= 0) {
    ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd_, nullptr);
  }
  close_fd(fd_);
  if (was_connected && state_ == State::Connected) {
    disconnects_.fetch_add(1, std::memory_order_relaxed);
  }
  if (count_ > 0 && queue_[head_].offset > 0) {
    const Frame& frame = queue_[head_];
    count_dropped(frame.samples, frame.per_source);
    head_ = (head_ + 1) % queue_.size();
    --count_;
    queued_.store(count_, std::memory_order_relaxed);
  }
  state_ = State::Disconnected;
  next_connect_ = Clock::now() + backoff_;
  backoff_ = std::min(backoff_ * 2, options_.max_backoff);
}

// A frame counts as sent only once its last byte is out.
bool TcpTransport::flush_queue() {
  while (count_ > 0) {
    Frame& frame = queue_[head_];
    const std::ptrdiff_t written =
        write_some(frame.bytes.data() + frame.offset, frame.size - frame.offset);
    if (written < 0) {
      disconnect(/*was_connected=*/true);
      return false;
    }
    frame.offset += static_cast<std::size_t>(written);
    bytes_sent_.fetch_add(static_cast<std::size_t>(written), std::memory_order_relaxed);
    if (frame.offset < frame.size) {
      return false;  // socket full; EPOLLOUT is already requested
    }
    count_sent(frame.samples, frame.per_source);
    head_ = (head_ + 1) % queue_.size();
    --count_;
    queued_.store(count_, std::memory_order_relaxed);
  }
  update_interest();
  return true;
}

// EINTR retries; EAGAIN stops with a partial count; anything else is an error.
std::ptrdiff_t TcpTransport::write_some(const std::byte* data, std::size_t size) {
  std::size_t total = 0;
  while (total < size) {
    const ssize_t n = ::send(fd_, data + total, size - total, MSG_NOSIGNAL);
    if (n > 0) {
      total += static_cast<std::size_t>(n);
    } else if (n < 0 && errno == EINTR) {
      continue;
    } else if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      break;
    } else {
      return -1;
    }
  }
  return static_cast<std::ptrdiff_t>(total);
}

// Ask for EPOLLOUT only while frames are waiting; otherwise it would fire on
// every poll of a writable socket.
void TcpTransport::update_interest() {
  if (fd_ < 0 || state_ != State::Connected) {
    return;
  }
  epoll_event event{};
  event.events = EPOLLIN | EPOLLRDHUP | (count_ > 0 ? EPOLLOUT : 0U);
  ::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd_, &event);
}

// O(samples + sources) and no allocation: tally_ is sized at construction.
void TcpTransport::tally(const BatchView& batch) {
  std::ranges::fill(tally_, 0U);
  for (const Sample& sample : batch.samples) {
    ++tally_[index_of_(sample.source_id)];
  }
}

// Caller guarantees a free slot; the slot's buffer is already big enough.
void TcpTransport::enqueue(std::span<const std::byte> bytes, std::size_t samples) {
  Frame& frame = queue_[(head_ + count_) % queue_.size()];
  std::memcpy(frame.bytes.data(), bytes.data(), bytes.size());
  frame.size = bytes.size();
  frame.offset = 0;
  frame.samples = samples;
  std::ranges::copy(tally_, frame.per_source.begin());
  ++count_;
  queued_.store(count_, std::memory_order_relaxed);
  raise_to(queue_high_water_, count_);
}

// Totals plus the per-source breakdown, in one place.
void TcpTransport::count_sent(std::size_t samples, std::span<const std::uint32_t> per_source) {
  batches_sent_.fetch_add(1, std::memory_order_relaxed);
  samples_sent_.fetch_add(samples, std::memory_order_relaxed);
  for (std::size_t i = 0; i < per_source.size(); ++i) {
    sent_by_source_[i].fetch_add(per_source[i], std::memory_order_relaxed);
  }
}

// Totals plus the per-source breakdown, in one place.
void TcpTransport::count_dropped(std::size_t samples, std::span<const std::uint32_t> per_source) {
  batches_dropped_.fetch_add(1, std::memory_order_relaxed);
  samples_dropped_.fetch_add(samples, std::memory_order_relaxed);
  for (std::size_t i = 0; i < per_source.size(); ++i) {
    dropped_by_source_[i].fetch_add(per_source[i], std::memory_order_relaxed);
  }
}

}  // namespace tstream
