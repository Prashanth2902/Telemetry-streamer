#include "tstream/tcp_transport.hpp"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>

#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <unistd.h>

namespace tstream {

namespace {

// Resolves host:port to the first TCP address getaddrinfo returns.
void resolve(const std::string& host, std::uint16_t port, sockaddr_storage& out,
             socklen_t& out_len) {
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  addrinfo* results = nullptr;
  const int rc = ::getaddrinfo(host.c_str(), std::to_string(port).c_str(), &hints, &results);
  if (rc != 0 || results == nullptr) {
    throw std::runtime_error("cannot resolve " + host + ": " + ::gai_strerror(rc));
  }
  std::memcpy(&out, results->ai_addr, results->ai_addrlen);
  out_len = results->ai_addrlen;
  ::freeaddrinfo(results);
}

// Closes fd if open and marks it closed.
void close_fd(int& fd) {
  if (fd >= 0) {
    ::close(fd);
    fd = -1;
  }
}

}  // namespace

// Resolution and buffer allocation happen here, once; then a first connect.
TcpTransport::TcpTransport(Options options)
    : options_(std::move(options)),
      encoder_(options_.max_samples),
      backoff_(options_.initial_backoff),
      pending_(max_frame_bytes_for(options_.max_samples)) {
  resolve(options_.host, options_.port, address_, address_len_);
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

// Order matters: catch up on socket events first, so a connect that finished
// or a peer that went away is reflected before deciding what to do.
void TcpTransport::send(const BatchView& batch) {
  service_events(0);
  if (state_ == State::Disconnected && Clock::now() >= next_connect_) {
    start_connect();
  }
  if (state_ != State::Connected || !flush_pending()) {
    count_drop(batch.samples.size());
    return;
  }

  const auto frame = encoder_.encode_frame(batch, now_ns());
  const std::ptrdiff_t written = write_some(frame.data(), frame.size());
  if (written < 0) {
    count_drop(batch.samples.size());
    disconnect(/*was_connected=*/true);
    return;
  }
  const auto done = static_cast<std::size_t>(written);
  bytes_sent_.fetch_add(done, std::memory_order_relaxed);
  if (done == frame.size()) {
    batches_sent_.fetch_add(1, std::memory_order_relaxed);
    samples_sent_.fetch_add(batch.samples.size(), std::memory_order_relaxed);
    return;
  }
  // The kernel took part of the frame: keep the rest for later.
  pending_size_ = frame.size() - done;
  std::memcpy(pending_.data(), frame.data() + done, pending_size_);
  pending_offset_ = 0;
  pending_samples_ = batch.samples.size();
  update_interest();
}

// Same as the start of send(), without a batch.
void TcpTransport::poll() {
  service_events(0);
  if (state_ == State::Disconnected && Clock::now() >= next_connect_) {
    start_connect();
  }
  if (state_ == State::Connected) {
    flush_pending();
  }
}

// Bounded wait so a dead receiver can't hang shutdown.
void TcpTransport::close() {
  const auto deadline = Clock::now() + options_.close_flush_timeout;
  while (pending_size_ > 0 && state_ != State::Disconnected) {
    const auto remaining =
        std::chrono::duration_cast<std::chrono::milliseconds>(deadline - Clock::now());
    if (remaining.count() <= 0) {
      break;
    }
    service_events(static_cast<int>(std::min<std::int64_t>(remaining.count(), 50)));
    if (state_ == State::Connected) {
      flush_pending();
    }
  }
  if (pending_size_ > 0) {
    count_drop(pending_samples_);
    pending_size_ = 0;
  }
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
          .connects = connects_.load(std::memory_order_relaxed),
          .disconnects = disconnects_.load(std::memory_order_relaxed)};
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
  fd_ = ::socket(address_.ss_family, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  if (fd_ < 0) {
    disconnect(/*was_connected=*/false);
    return;
  }
  const int one = 1;
  ::setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

  const int rc = ::connect(fd_, reinterpret_cast<const sockaddr*>(&address_), address_len_);
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
}

// A half-sent frame is useless to the receiver, so it is dropped too.
void TcpTransport::disconnect(bool was_connected) {
  if (fd_ >= 0) {
    ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd_, nullptr);
  }
  close_fd(fd_);
  if (was_connected && state_ == State::Connected) {
    disconnects_.fetch_add(1, std::memory_order_relaxed);
  }
  if (pending_size_ > 0) {
    count_drop(pending_samples_);
    pending_size_ = 0;
  }
  state_ = State::Disconnected;
  next_connect_ = Clock::now() + backoff_;
  backoff_ = std::min(backoff_ * 2, options_.max_backoff);
}

// Counts the batch as sent only once its last byte is out.
bool TcpTransport::flush_pending() {
  if (pending_size_ == 0) {
    return true;
  }
  const std::ptrdiff_t written =
      write_some(pending_.data() + pending_offset_, pending_size_ - pending_offset_);
  if (written < 0) {
    disconnect(/*was_connected=*/true);
    return false;
  }
  pending_offset_ += static_cast<std::size_t>(written);
  bytes_sent_.fetch_add(static_cast<std::size_t>(written), std::memory_order_relaxed);
  if (pending_offset_ < pending_size_) {
    return false;
  }
  batches_sent_.fetch_add(1, std::memory_order_relaxed);
  samples_sent_.fetch_add(pending_samples_, std::memory_order_relaxed);
  pending_size_ = 0;
  pending_offset_ = 0;
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

// Ask for EPOLLOUT only while bytes are waiting; otherwise it would fire on
// every poll of a writable socket.
void TcpTransport::update_interest() {
  if (fd_ < 0 || state_ != State::Connected) {
    return;
  }
  epoll_event event{};
  event.events = EPOLLIN | EPOLLRDHUP | (pending_size_ > 0 ? EPOLLOUT : 0U);
  ::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd_, &event);
}

// One counter pair per dropped batch.
void TcpTransport::count_drop(std::size_t samples) {
  batches_dropped_.fetch_add(1, std::memory_order_relaxed);
  samples_dropped_.fetch_add(samples, std::memory_order_relaxed);
}

}  // namespace tstream
