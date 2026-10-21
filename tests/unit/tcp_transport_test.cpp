// TcpTransport against real loopback sockets: framing on the wire, drop
// accounting, reconnect, and partial writes under backpressure.

#include "tstream/tcp_transport.hpp"

#include <chrono>
#include <cstdint>
#include <span>
#include <stdexcept>
#include <thread>

#include <gtest/gtest.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include "tstream/framing.hpp"

#include "sockets.hpp"
#include "telemetry.pb.h"

namespace {

using namespace std::chrono_literals;
using tstream::BatchView;
using tstream::Sample;
using tstream::SampleKind;
using tstream::TcpTransport;
using tstream::testing::free_port;

// A listening socket on 127.0.0.1, standing in for tstream-recv.
class Listener {
 public:
  // Listens on `port`, or on an ephemeral port if 0.
  explicit Listener(std::uint16_t port = 0) {
    fd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    const int one = 1;
    ::setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(port);
    if (::bind(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0 ||
        ::listen(fd_, 4) != 0) {
      throw std::runtime_error("listener bind/listen failed");
    }
    socklen_t len = sizeof(addr);
    ::getsockname(fd_, reinterpret_cast<sockaddr*>(&addr), &len);
    port_ = ntohs(addr.sin_port);
  }
  // Owns a socket: not copyable or movable.
  Listener(const Listener&) = delete;
  Listener& operator=(const Listener&) = delete;
  Listener(Listener&&) = delete;
  Listener& operator=(Listener&&) = delete;
  // Closes the listening socket.
  ~Listener() { ::close(fd_); }

  // The port actually bound.
  [[nodiscard]] std::uint16_t port() const { return port_; }

  // Accepts one connection, or returns -1 after timeout.
  [[nodiscard]] int accept(std::chrono::milliseconds timeout = 2000ms) const {
    pollfd p{.fd = fd_, .events = POLLIN, .revents = 0};
    if (::poll(&p, 1, static_cast<int>(timeout.count())) <= 0) {
      return -1;
    }
    return ::accept4(fd_, nullptr, nullptr, SOCK_CLOEXEC);
  }

 private:
  int fd_ = -1;
  std::uint16_t port_ = 0;
};

// Reads from fd until EOF or timeout, then closes it.
std::vector<std::byte> read_all(int fd, std::chrono::milliseconds timeout = 5000ms) {
  std::vector<std::byte> out;
  std::vector<std::byte> chunk(std::size_t{64} * 1024);
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    pollfd p{.fd = fd, .events = POLLIN, .revents = 0};
    if (::poll(&p, 1, 50) <= 0) {
      continue;
    }
    const ssize_t n = ::recv(fd, chunk.data(), chunk.size(), 0);
    if (n <= 0) {
      break;
    }
    out.insert(out.end(), chunk.begin(), chunk.begin() + n);
  }
  ::close(fd);
  return out;
}

// Splits a received byte stream into frames and parses each Batch.
std::vector<tstream::v1::Batch> decode_stream(const std::vector<std::byte>& bytes) {
  tstream::framing::FrameDecoder decoder(16 << 20);
  decoder.feed(bytes);
  std::vector<tstream::v1::Batch> batches;
  for (auto r = decoder.next(); r.status == tstream::framing::FrameDecoder::Status::Frame;
       r = decoder.next()) {
    tstream::v1::Batch batch;
    EXPECT_TRUE(batch.ParseFromArray(r.payload.data(), static_cast<int>(r.payload.size())));
    batches.push_back(std::move(batch));
  }
  EXPECT_EQ(decoder.buffered(), 0U) << "trailing partial frame";
  EXPECT_FALSE(decoder.failed());
  return batches;
}

// `count` IMU samples with seq 0..count-1.
std::vector<Sample> imu_samples(std::size_t count) {
  std::vector<Sample> samples(count);
  for (std::size_t i = 0; i < count; ++i) {
    samples[i].source_id = 1;
    samples[i].kind = SampleKind::Imu;
    samples[i].seq = static_cast<std::uint32_t>(i);
    samples[i].imu = {.ax = 1, .ay = 2, .az = 9.81F, .gx = 0.1F, .gy = 0.2F, .gz = 0.3F};
  }
  return samples;
}

// Options pointing at `port`, with short backoffs so tests run fast.
TcpTransport::Options options_for(std::uint16_t port, std::size_t max_samples = 64) {
  TcpTransport::Options o;
  o.host = "127.0.0.1";
  o.port = port;
  o.max_samples = max_samples;
  o.initial_backoff = 10ms;
  o.max_backoff = 50ms;
  return o;
}

// Calls poll() until `done()` or timeout; false on timeout.
template <typename Predicate>
bool poll_until(TcpTransport& transport, Predicate done,
                std::chrono::milliseconds timeout = 2000ms) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (!done()) {
    if (std::chrono::steady_clock::now() > deadline) {
      return false;
    }
    transport.poll();
    std::this_thread::sleep_for(1ms);
  }
  return true;
}

TEST(TcpTransport, SendsFramesThatDecodeOnTheReceiver) {
  const Listener listener;
  TcpTransport transport(options_for(listener.port()));
  ASSERT_TRUE(poll_until(transport, [&] { return transport.connected(); }));
  const int peer = listener.accept();
  ASSERT_GE(peer, 0);

  const auto samples = imu_samples(10);
  for (std::uint64_t seq = 0; seq < 5; ++seq) {
    transport.send(BatchView{.batch_seq = seq, .t_flush_ns = 0, .samples = samples});
  }
  transport.close();

  const auto batches = decode_stream(read_all(peer));
  ASSERT_EQ(batches.size(), 5U);
  for (std::size_t i = 0; i < batches.size(); ++i) {
    EXPECT_EQ(batches[i].batch_seq(), i);
    EXPECT_GT(batches[i].t_send_ns(), 0);
    ASSERT_EQ(batches[i].samples_size(), 10);
    EXPECT_EQ(batches[i].samples(9).seq(), 9U);
    EXPECT_FLOAT_EQ(batches[i].samples(0).imu().az(), 9.81F);
  }
  const auto c = transport.counters();
  EXPECT_EQ(c.batches_sent, 5U);
  EXPECT_EQ(c.samples_sent, 50U);
  EXPECT_EQ(c.batches_dropped, 0U);
  EXPECT_EQ(c.connects, 1U);
}

// A batch mixing two sources: 4 samples from source 1, 2 from source 2.
std::vector<Sample> mixed_batch() {
  auto samples = imu_samples(6);
  for (std::size_t i = 4; i < 6; ++i) {
    samples[i].source_id = 2;
  }
  return samples;
}

// SPEC §6: with no receiver, the first K batches wait in the queue; beyond K
// the new batch is dropped whole and counted per source.
TEST(TcpTransport, QueuesUpToKThenDropsAndCountsPerSource) {
  auto options = options_for(free_port());
  options.max_pending_batches = 3;
  options.source_ids = {1, 2};
  TcpTransport transport(options);
  const auto samples = mixed_batch();
  for (std::uint64_t seq = 0; seq < 5; ++seq) {
    transport.send(BatchView{.batch_seq = seq, .t_flush_ns = 0, .samples = samples});
  }
  const auto c = transport.counters();
  EXPECT_FALSE(transport.connected());
  EXPECT_EQ(c.batches_sent, 0U);
  EXPECT_EQ(c.queued, 3U);
  EXPECT_EQ(c.queue_high_water, 3U);
  EXPECT_EQ(c.batches_dropped, 2U);
  EXPECT_EQ(c.samples_dropped, 12U);
  EXPECT_EQ(transport.samples_dropped_for(1), 8U);
  EXPECT_EQ(transport.samples_dropped_for(2), 4U);
  EXPECT_EQ(transport.samples_dropped_for(77), 0U);  // unconfigured id: the "other" bucket
}

// Batches queued while disconnected go out, in order, once a receiver appears.
TEST(TcpTransport, DeliversQueuedBatchesAfterConnecting) {
  const std::uint16_t port = free_port();
  auto options = options_for(port);
  options.source_ids = {1, 2};
  TcpTransport transport(options);
  const auto samples = mixed_batch();
  for (std::uint64_t seq = 0; seq < 4; ++seq) {
    transport.send(BatchView{.batch_seq = seq, .t_flush_ns = 0, .samples = samples});
  }
  EXPECT_EQ(transport.counters().queued, 4U);

  const Listener listener(port);
  ASSERT_TRUE(poll_until(transport, [&] { return transport.counters().queued == 0; }));
  const int peer = listener.accept();
  ASSERT_GE(peer, 0);
  transport.close();

  const auto batches = decode_stream(read_all(peer));
  ASSERT_EQ(batches.size(), 4U);
  for (std::size_t i = 0; i < batches.size(); ++i) {
    EXPECT_EQ(batches[i].batch_seq(), i);
  }
  EXPECT_EQ(transport.counters().batches_dropped, 0U);
  EXPECT_EQ(transport.samples_sent_for(1), 16U);
  EXPECT_EQ(transport.samples_sent_for(2), 8U);
}

// Nothing ever connects: close() gives up after its timeout and counts every
// queued batch as dropped, so nothing disappears unaccounted.
TEST(TcpTransport, CloseCountsUndeliveredQueueAsDropped) {
  auto options = options_for(free_port());
  options.close_flush_timeout = 100ms;
  TcpTransport transport(options);
  const auto samples = imu_samples(5);
  for (std::uint64_t seq = 0; seq < 3; ++seq) {
    transport.send(BatchView{.batch_seq = seq, .t_flush_ns = 0, .samples = samples});
  }
  const auto before = std::chrono::steady_clock::now();
  transport.close();
  EXPECT_LT(std::chrono::steady_clock::now() - before, 1s);
  const auto c = transport.counters();
  EXPECT_EQ(c.queued, 0U);
  EXPECT_EQ(c.batches_dropped, 3U);
  EXPECT_EQ(c.samples_dropped, 15U);
}

TEST(TcpTransport, ConnectsOnceReceiverAppears) {
  const std::uint16_t port = free_port();
  TcpTransport transport(options_for(port));
  std::this_thread::sleep_for(30ms);
  transport.poll();  // at least one refused attempt
  EXPECT_FALSE(transport.connected());

  const Listener listener(port);
  ASSERT_TRUE(poll_until(transport, [&] { return transport.connected(); }));
  const int peer = listener.accept();
  ASSERT_GE(peer, 0);
  const auto samples = imu_samples(3);
  transport.send(BatchView{.batch_seq = 0, .t_flush_ns = 0, .samples = samples});
  transport.close();
  EXPECT_EQ(decode_stream(read_all(peer)).size(), 1U);
}

TEST(TcpTransport, ReconnectsAfterPeerCloses) {
  const Listener listener;
  TcpTransport transport(options_for(listener.port()));
  ASSERT_TRUE(poll_until(transport, [&] { return transport.connected(); }));
  ::close(listener.accept());  // the receiver goes away

  ASSERT_TRUE(poll_until(transport, [&] {
    const auto c = transport.counters();
    return c.disconnects == 1 && c.connects == 2 && transport.connected();
  }));
  const int peer = listener.accept();
  ASSERT_GE(peer, 0);
  const auto samples = imu_samples(4);
  transport.send(BatchView{.batch_seq = 42, .t_flush_ns = 0, .samples = samples});
  transport.close();
  const auto batches = decode_stream(read_all(peer));
  ASSERT_EQ(batches.size(), 1U);
  EXPECT_EQ(batches[0].batch_seq(), 42U);
}

// The receiver stops reading, so the socket buffers fill and writes come back
// short. Frames must still arrive whole: every frame counted as sent decodes,
// and nothing is lost without being counted.
TEST(TcpTransport, PartialWritesCompleteAndCloseFlushesPending) {
  constexpr std::uint64_t kBatches = 100;
  constexpr std::size_t kSamples = 4096;  // ~200 KB per frame, ~20 MB in total
  const Listener listener;
  auto options = options_for(listener.port(), kSamples);
  options.max_pending_batches = 4;  // small, so ~20 MB overflows the queue for sure
  TcpTransport transport(options);
  ASSERT_TRUE(poll_until(transport, [&] { return transport.connected(); }));
  const int peer = listener.accept();
  ASSERT_GE(peer, 0);

  const auto samples = imu_samples(kSamples);
  for (std::uint64_t seq = 0; seq < kBatches; ++seq) {
    transport.send(BatchView{.batch_seq = seq, .t_flush_ns = 0, .samples = samples});
  }
  const auto before_close = transport.counters();
  EXPECT_GT(before_close.batches_dropped, 0U) << "buffers never filled; test proves nothing";

  // Start reading only after close() has begun waiting for the pending frame.
  std::vector<std::byte> received;
  std::thread reader([&] {
    std::this_thread::sleep_for(100ms);
    received = read_all(peer);
  });
  transport.close();
  reader.join();

  const auto c = transport.counters();
  EXPECT_EQ(c.batches_sent + c.batches_dropped, kBatches);
  const auto batches = decode_stream(received);
  EXPECT_EQ(batches.size(), c.batches_sent);
  for (std::size_t i = 1; i < batches.size(); ++i) {
    EXPECT_LT(batches[i - 1].batch_seq(), batches[i].batch_seq());
  }
  for (const auto& batch : batches) {
    EXPECT_EQ(batch.samples_size(), static_cast<int>(kSamples));
  }
}

}  // namespace
