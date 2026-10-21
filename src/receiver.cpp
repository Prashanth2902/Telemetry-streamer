#include "tstream/receiver.hpp"

#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <system_error>
#include <thread>
#include <utility>

#include <ifaddrs.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include "tstream/clock.hpp"
#include "tstream/net.hpp"

#include "telemetry.pb.h"

namespace tstream {

namespace {

constexpr std::size_t kReadChunk = std::size_t{64} * 1024;

// True for 127.0.0.0/8, ::1, and IPv4-mapped loopback.
bool is_loopback(const sockaddr_storage& addr) {
  if (addr.ss_family == AF_INET) {
    const auto& v4 = reinterpret_cast<const sockaddr_in&>(addr);
    return (ntohl(v4.sin_addr.s_addr) >> 24) == 127;
  }
  if (addr.ss_family == AF_INET6) {
    const auto& v6 = reinterpret_cast<const sockaddr_in6&>(addr);
    if (IN6_IS_ADDR_LOOPBACK(&v6.sin6_addr)) {
      return true;
    }
    return IN6_IS_ADDR_V4MAPPED(&v6.sin6_addr) && v6.sin6_addr.s6_addr[12] == 127;
  }
  return false;
}

// True if `addr` is one of this host's interface addresses.
bool is_local_interface(const sockaddr_storage& addr) {
  ifaddrs* list = nullptr;
  if (::getifaddrs(&list) != 0) {
    return false;
  }
  bool found = false;
  for (const ifaddrs* it = list; it != nullptr && !found; it = it->ifa_next) {
    if (it->ifa_addr == nullptr || it->ifa_addr->sa_family != addr.ss_family) {
      continue;
    }
    if (addr.ss_family == AF_INET) {
      found = reinterpret_cast<const sockaddr_in*>(it->ifa_addr)->sin_addr.s_addr ==
              reinterpret_cast<const sockaddr_in&>(addr).sin_addr.s_addr;
    } else if (addr.ss_family == AF_INET6) {
      found = std::memcmp(&reinterpret_cast<const sockaddr_in6*>(it->ifa_addr)->sin6_addr,
                          &reinterpret_cast<const sockaddr_in6&>(addr).sin6_addr,
                          sizeof(in6_addr)) == 0;
    }
  }
  ::freeifaddrs(list);
  return found;
}

// Same host means the same CLOCK_MONOTONIC, so latency can be computed (SPEC §4.4).
bool peer_is_local(int fd) {
  sockaddr_storage peer{};
  socklen_t len = sizeof(peer);
  if (::getpeername(fd, reinterpret_cast<sockaddr*>(&peer), &len) != 0) {
    return false;
  }
  return is_loopback(peer) || is_local_interface(peer);
}

// Adds fd to the epoll set for input and hang-ups.
void watch(int epoll_fd, int fd) {
  epoll_event event{};
  event.events = EPOLLIN | EPOLLRDHUP;
  event.data.fd = fd;
  ::epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &event);
}

}  // namespace

// Sums the per-reason counters.
std::uint64_t ReceiverStats::total_frame_errors() const {
  std::uint64_t total = 0;
  for (const auto n : frame_errors) {
    total += n;
  }
  return total;
}

// Sums the per-monitor counters.
std::uint64_t ReceiverStats::total_health_events() const {
  std::uint64_t total = 0;
  for (const auto n : health_events) {
    total += n;
  }
  return total;
}

// Binds the first address bind_host:port resolves to.
Receiver::Receiver(Options options)
    : options_(std::move(options)), decoder_(options_.max_frame_bytes), read_buffer_(kReadChunk) {
  const SocketAddress address = resolve_tcp(options_.bind_host, options_.port, /*passive=*/true);
  listen_fd_ = ::socket(address.family(), SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  const int one = 1;
  if (listen_fd_ >= 0) {
    ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    if (options_.slow_read_delay.count() > 0) {
      // Inherited by accepted sockets; must be set before listen().
      ::setsockopt(listen_fd_, SOL_SOCKET, SO_RCVBUF, &kSlowReceiveBuffer,
                   sizeof(kSlowReceiveBuffer));
    }
  }
  const bool ok = listen_fd_ >= 0 && ::bind(listen_fd_, address.get(), address.length) == 0 &&
                  ::listen(listen_fd_, 4) == 0;
  if (!ok) {
    const std::string reason = std::system_category().message(errno);
    if (listen_fd_ >= 0) {
      ::close(listen_fd_);
    }
    throw std::runtime_error("cannot listen on " + options_.bind_host + ":" +
                             std::to_string(options_.port) + ": " + reason);
  }

  sockaddr_storage bound{};
  socklen_t len = sizeof(bound);
  ::getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&bound), &len);
  port_ = bound.ss_family == AF_INET6
              ? ntohs(reinterpret_cast<const sockaddr_in6&>(bound).sin6_port)
              : ntohs(reinterpret_cast<const sockaddr_in&>(bound).sin_port);

  epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
  watch(epoll_fd_, listen_fd_);
}

// Plain close; the sender sees a reset or EOF and reconnects.
Receiver::~Receiver() {
  if (conn_fd_ >= 0) {
    ::close(conn_fd_);
  }
  ::close(listen_fd_);
  ::close(epoll_fd_);
}

// Handles every ready fd; a timeout of 0 makes it a non-blocking check.
void Receiver::poll(int timeout_ms) {
  std::array<epoll_event, 4> events{};
  const int n = ::epoll_wait(epoll_fd_, events.data(), static_cast<int>(events.size()), timeout_ms);
  for (int i = 0; i < n; ++i) {
    const int fd = events[static_cast<std::size_t>(i)].data.fd;
    if (fd == listen_fd_) {
      accept_connection();
    } else if (fd == conn_fd_) {
      read_connection();
    }
  }
}

// Copies both interval histograms out, then clears them.
void Receiver::take_interval(LatencyHistogram& e2e, LatencyHistogram& pipeline) {
  e2e = e2e_interval_;
  pipeline = pipeline_interval_;
  e2e_interval_.reset();
  pipeline_interval_.reset();
}

// Swaps the queue out, so the caller owns it and the next batch starts a new one.
std::vector<ReceivedHealthEvent> Receiver::take_health_events() {
  std::vector<ReceivedHealthEvent> out;
  out.swap(health_pending_);
  return out;
}

// Only one sender at a time: extras are accepted just to be closed.
void Receiver::accept_connection() {
  const int fd = ::accept4(listen_fd_, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
  if (fd < 0) {
    return;
  }
  if (conn_fd_ >= 0) {
    ::close(fd);
    ++stats_.rejected_connections;
    return;
  }
  conn_fd_ = fd;
  ++stats_.connections;
  latency_available_ = peer_is_local(fd);
  if (!slow_until_ && options_.slow_for.count() > 0) {
    slow_until_ = std::chrono::steady_clock::now() + options_.slow_for;
  }
  have_batch_seq_ = false;  // a new connection restarts batch numbering checks
  decoder_.reset();
  watch(epoll_fd_, conn_fd_);
}

// Drains the socket, then hands complete frames to handle_frame().
void Receiver::read_connection() {
  const bool throttled = slow();
  const std::size_t chunk = throttled ? kSlowReadBytes : read_buffer_.size();
  while (true) {
    const ssize_t n = ::recv(conn_fd_, read_buffer_.data(), chunk, 0);
    if (n > 0) {
      if (!consume(std::span(read_buffer_.data(), static_cast<std::size_t>(n)))) {
        return;
      }
      if (throttled) {
        std::this_thread::sleep_for(options_.slow_read_delay);
        return;  // one small read per poll() while slow
      }
      continue;
    }
    if (n < 0 && errno == EINTR) {
      continue;
    }
    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      return;
    }
    drop_connection();  // EOF or a socket error
    return;
  }
}

// Stops at the first bad header: the rest of the stream can't be trusted.
bool Receiver::consume(std::span<const std::byte> bytes) {
  stats_.bytes += bytes.size();
  decoder_.feed(bytes);
  for (auto r = decoder_.next(); r.status != framing::FrameDecoder::Status::NeedMore;
       r = decoder_.next()) {
    if (r.status == framing::FrameDecoder::Status::Error) {
      ++stats_.frame_errors[static_cast<std::size_t>(r.error)];
      drop_connection();
      return false;
    }
    if (!handle_frame(r.payload)) {
      ++stats_.decode_errors;
    }
  }
  return true;
}

// Slow from the start; with slow_for, only until that long after the first connection.
bool Receiver::slow() const {
  if (options_.slow_read_delay.count() <= 0) {
    return false;
  }
  return !slow_until_ || std::chrono::steady_clock::now() < *slow_until_;
}

// Partial frames left in the decoder are discarded with the connection.
void Receiver::drop_connection() {
  if (conn_fd_ < 0) {
    return;
  }
  ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, conn_fd_, nullptr);
  ::close(conn_fd_);
  conn_fd_ = -1;
  ++stats_.disconnects;
  decoder_.reset();
}

// A parse failure keeps the connection: framing is intact, so the next frame
// is still readable.
bool Receiver::handle_frame(std::span<const std::byte> payload) {
  v1::Batch batch;
  if (!batch.ParseFromArray(payload.data(), static_cast<int>(payload.size()))) {
    return false;
  }
  const std::int64_t received_ns = now_ns();
  ++stats_.batches;

  if (have_batch_seq_ && batch.batch_seq() > next_batch_seq_) {
    stats_.batch_seq_gaps += batch.batch_seq() - next_batch_seq_;
  }
  next_batch_seq_ = batch.batch_seq() + 1;
  have_batch_seq_ = true;

  for (const v1::Sample& sample : batch.samples()) {
    if (sample.payload_case() == v1::Sample::PAYLOAD_NOT_SET || sample.source_id() > 0xFFFF) {
      ++stats_.invalid_samples;
      continue;
    }
    ++stats_.samples;
    seq_.on_sample(static_cast<std::uint16_t>(sample.source_id()), sample.seq());
    if (sample.has_health()) {
      const v1::Health& h = sample.health();
      record_health(sample.t_capture_ns(), h.source_id(), h.monitor(), h.severity(), h.detail());
    }
    if (latency_available_) {
      const std::int64_t e2e = received_ns - sample.t_capture_ns();
      const std::int64_t pipeline = batch.t_send_ns() - sample.t_capture_ns();
      e2e_.record(e2e);
      pipeline_.record(pipeline);
      e2e_interval_.record(e2e);
      pipeline_interval_.record(pipeline);
    }
  }
  return true;
}

// Wire values are uint32. A monitor or severity that doesn't fit a byte
// becomes 0xFF, which monitor_name() and severity_name() report as unknown
// (0 would read as a real severity, Cleared).
void Receiver::record_health(std::int64_t t_capture_ns, std::uint32_t source_id,
                             std::uint32_t monitor, std::uint32_t severity, std::uint32_t detail) {
  const auto narrow8 = [](std::uint32_t v) {
    return static_cast<std::uint8_t>(v <= 0xFF ? v : 0xFF);
  };
  ReceivedHealthEvent event;
  event.t_capture_ns = t_capture_ns;
  event.data = {.source_id = static_cast<std::uint16_t>(source_id <= 0xFFFF ? source_id : 0),
                .monitor = narrow8(monitor),
                .severity = narrow8(severity),
                .detail = detail};
  const std::size_t index = monitor <= kHealthMonitorKinds ? monitor : 0;
  ++stats_.health_events[index];
  health_pending_.push_back(event);
}

}  // namespace tstream
