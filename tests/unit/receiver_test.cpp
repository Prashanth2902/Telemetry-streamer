// Receiver building blocks (LatencyHistogram, SeqTracker) and the Receiver
// itself, driven in-process against the real TcpTransport.

#include "tstream/receiver.hpp"

#include <chrono>
#include <cstdint>
#include <limits>
#include <span>
#include <vector>

#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include "tstream/framing.hpp"
#include "tstream/latency_histogram.hpp"
#include "tstream/seq_tracker.hpp"
#include "tstream/tcp_transport.hpp"

namespace {

using namespace std::chrono_literals;
using tstream::BatchView;
using tstream::LatencyHistogram;
using tstream::Receiver;
using tstream::Sample;
using tstream::SampleKind;
using tstream::SeqTracker;
using tstream::TcpTransport;

// --- LatencyHistogram -------------------------------------------------------

TEST(LatencyHistogram, BucketsBoundRelativeErrorBySixteenth) {
  std::vector<std::uint64_t> values;
  values.reserve(5100);
  for (std::uint64_t v = 0; v < 5000; ++v) {
    values.push_back(v);
  }
  for (std::uint64_t v = 5000; v < (std::uint64_t{1} << 62); v = v * 3 + 7) {
    values.push_back(v);
  }
  values.push_back(static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()));
  for (const std::uint64_t v : values) {
    const std::size_t bucket = LatencyHistogram::bucket_for(v);
    ASSERT_LT(bucket, LatencyHistogram::kBuckets) << v;
    const std::uint64_t upper = LatencyHistogram::bucket_upper(bucket);
    ASSERT_GE(upper, v);
    if (v >= 16) {
      ASSERT_LE(static_cast<double>(upper - v), static_cast<double>(v) / 16.0) << v;
    } else {
      ASSERT_EQ(upper, v);  // exact below 16
    }
  }
}

TEST(LatencyHistogram, PercentilesOfUniformValues) {
  LatencyHistogram h;
  for (std::int64_t us = 1; us <= 1000; ++us) {
    h.record(us * 1000);
  }
  EXPECT_EQ(h.count(), 1000U);
  EXPECT_EQ(h.max(), 1'000'000);
  EXPECT_NEAR(static_cast<double>(h.percentile(0.50)), 500'000, 500'000 / 16.0);
  EXPECT_NEAR(static_cast<double>(h.percentile(0.99)), 990'000, 990'000 / 16.0);
  EXPECT_EQ(h.percentile(1.0), 1'000'000);  // capped at the exact max
}

TEST(LatencyHistogram, EmptyAndNegativeValues) {
  LatencyHistogram h;
  EXPECT_EQ(h.percentile(0.5), 0);
  EXPECT_EQ(h.max(), 0);
  h.record(-500);  // clock skew: counted as 0, never out of range
  EXPECT_EQ(h.count(), 1U);
  EXPECT_EQ(h.percentile(0.5), 0);
}

TEST(LatencyHistogram, ResetForgetsEverything) {
  LatencyHistogram a;
  a.record(100);
  a.record(1'000'000);
  EXPECT_EQ(a.count(), 2U);
  EXPECT_EQ(a.max(), 1'000'000);
  a.reset();
  EXPECT_EQ(a.count(), 0U);
  EXPECT_EQ(a.percentile(0.99), 0);
}

// --- SeqTracker -------------------------------------------------------------

TEST(SeqTracker, InOrderHasNoGaps) {
  SeqTracker t;
  for (std::uint32_t s = 0; s < 100; ++s) {
    t.on_sample(1, s);
  }
  EXPECT_EQ(t.sources().at(1).received, 100U);
  EXPECT_EQ(t.total_gaps(), 0U);
  EXPECT_EQ(t.total_out_of_order(), 0U);
}

TEST(SeqTracker, CountsMissingSamplesExactly) {
  SeqTracker t;
  for (const std::uint32_t s : {0U, 1U, 2U, 6U, 7U, 20U}) {
    t.on_sample(1, s);
  }
  EXPECT_EQ(t.sources().at(1).gaps, 3U + 12U);  // 3..5 and 8..19
}

TEST(SeqTracker, FirstSampleIsNotAGap) {
  SeqTracker t;
  t.on_sample(1, 5000);  // connected late
  t.on_sample(1, 5001);
  EXPECT_EQ(t.total_gaps(), 0U);
}

TEST(SeqTracker, CountsDuplicatesAndLateArrivals) {
  SeqTracker t;
  for (const std::uint32_t s : {0U, 1U, 2U, 2U, 1U, 3U}) {
    t.on_sample(1, s);
  }
  EXPECT_EQ(t.sources().at(1).out_of_order, 2U);
  EXPECT_EQ(t.sources().at(1).gaps, 0U);
}

TEST(SeqTracker, HandlesWraparound) {
  SeqTracker t;
  for (const std::uint32_t s : {0xFFFFFFFEU, 0xFFFFFFFFU, 0U, 1U}) {
    t.on_sample(1, s);
  }
  EXPECT_EQ(t.total_gaps(), 0U);
  EXPECT_EQ(t.total_out_of_order(), 0U);
}

TEST(SeqTracker, TracksSourcesIndependently) {
  SeqTracker t;
  t.on_sample(1, 0);
  t.on_sample(2, 0);
  t.on_sample(1, 2);  // gap of 1 on source 1 only
  t.on_sample(2, 1);
  EXPECT_EQ(t.sources().at(1).gaps, 1U);
  EXPECT_EQ(t.sources().at(2).gaps, 0U);
}

// --- Receiver ---------------------------------------------------------------

// Samples for source 1 with the given seqs, stamped now.
std::vector<Sample> samples_with_seqs(std::uint32_t first, std::uint32_t count) {
  std::vector<Sample> out(count);
  for (std::uint32_t i = 0; i < count; ++i) {
    out[i].source_id = 1;
    out[i].kind = SampleKind::Battery;
    out[i].seq = first + i;
    out[i].t_capture_ns = tstream::now_ns();
    out[i].bat = {.voltage_v = 380, .current_a = -40, .soc_pct = 80, .temp_c = 25};
  }
  return out;
}

// A Receiver on an ephemeral loopback port.
Receiver::Options receiver_options() {
  Receiver::Options o;
  o.bind_host = "127.0.0.1";
  o.port = 0;
  o.max_frame_bytes = 1 << 20;
  return o;
}

// A transport aimed at `port` with short backoffs.
TcpTransport::Options transport_options(std::uint16_t port) {
  TcpTransport::Options o;
  o.port = port;
  o.max_samples = 64;
  o.initial_backoff = 10ms;
  o.max_backoff = 50ms;
  return o;
}

// Polls both ends from this thread until `done()` or timeout.
template <typename Predicate>
bool pump(Receiver& receiver, TcpTransport* transport, Predicate done,
          std::chrono::milliseconds timeout = 3000ms) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (!done()) {
    if (std::chrono::steady_clock::now() > deadline) {
      return false;
    }
    if (transport != nullptr) {
      transport->poll();
    }
    receiver.poll(1);
  }
  return true;
}

// A plain blocking client socket connected to 127.0.0.1:port.
int raw_connect(std::uint16_t port) {
  const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = htons(port);
  EXPECT_EQ(::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);
  return fd;
}

TEST(Receiver, ReceivesBatchesWithLatencyAndNoGaps) {
  Receiver receiver(receiver_options());
  TcpTransport transport(transport_options(receiver.port()));
  ASSERT_TRUE(pump(receiver, &transport, [&] { return receiver.stats().connections == 1; }));

  for (std::uint32_t b = 0; b < 3; ++b) {
    const auto samples = samples_with_seqs(b * 10, 10);
    transport.send(BatchView{.batch_seq = b, .t_flush_ns = 0, .samples = samples});
  }
  transport.close();
  ASSERT_TRUE(pump(receiver, nullptr, [&] { return receiver.stats().disconnects == 1; }));

  const auto& s = receiver.stats();
  EXPECT_EQ(s.batches, 3U);
  EXPECT_EQ(s.samples, 30U);
  EXPECT_EQ(s.batch_seq_gaps, 0U);
  EXPECT_EQ(s.total_frame_errors(), 0U);
  EXPECT_EQ(receiver.seq().total_gaps(), 0U);
  EXPECT_TRUE(receiver.latency_available());  // loopback peer
  EXPECT_EQ(receiver.e2e_latency().count(), 30U);
  EXPECT_EQ(receiver.pipeline_latency().count(), 30U);
  EXPECT_GE(receiver.e2e_latency().percentile(0.5), receiver.pipeline_latency().percentile(0.5));
}

TEST(Receiver, ReportsSkippedSeqsAsGaps) {
  Receiver receiver(receiver_options());
  TcpTransport transport(transport_options(receiver.port()));
  ASSERT_TRUE(pump(receiver, &transport, [&] { return receiver.stats().connections == 1; }));
  const auto first = samples_with_seqs(0, 10);
  const auto second = samples_with_seqs(15, 5);  // 10..14 never sent
  transport.send(BatchView{.batch_seq = 0, .t_flush_ns = 0, .samples = first});
  transport.send(BatchView{.batch_seq = 1, .t_flush_ns = 0, .samples = second});
  transport.close();
  ASSERT_TRUE(pump(receiver, nullptr, [&] { return receiver.stats().disconnects == 1; }));
  EXPECT_EQ(receiver.seq().total_gaps(), 5U);
  EXPECT_EQ(receiver.stats().samples, 15U);
}

TEST(Receiver, RejectsBadHeaderByReasonAndAcceptsNextSender) {
  Receiver receiver(receiver_options());
  const int fd = raw_connect(receiver.port());
  ASSERT_TRUE(pump(receiver, nullptr, [&] { return receiver.stats().connections == 1; }));
  const char garbage[] = "GARBAGE!";  // 8 bytes: one bad header
  ASSERT_EQ(::send(fd, garbage, 8, MSG_NOSIGNAL), 8);
  ASSERT_TRUE(pump(receiver, nullptr, [&] { return receiver.stats().disconnects == 1; }));
  ::close(fd);
  EXPECT_EQ(receiver.stats()
                .frame_errors[static_cast<std::size_t>(tstream::framing::FrameError::BadMagic)],
            1U);

  TcpTransport transport(transport_options(receiver.port()));
  ASSERT_TRUE(pump(receiver, &transport, [&] { return receiver.stats().connections == 2; }));
  const auto samples = samples_with_seqs(0, 4);
  transport.send(BatchView{.batch_seq = 0, .t_flush_ns = 0, .samples = samples});
  transport.close();
  ASSERT_TRUE(pump(receiver, nullptr, [&] { return receiver.stats().samples == 4; }));
}

// Framing intact, payload not a Batch: counted, and the connection survives.
TEST(Receiver, CountsUndecodablePayloadAndKeepsConnection) {
  Receiver receiver(receiver_options());
  const int fd = raw_connect(receiver.port());
  ASSERT_TRUE(pump(receiver, nullptr, [&] { return receiver.stats().connections == 1; }));

  std::vector<std::byte> frame(tstream::framing::kHeaderBytes + 4, std::byte{0xFF});
  tstream::framing::write_header(std::span<std::byte, tstream::framing::kHeaderBytes>(
                                     frame.data(), tstream::framing::kHeaderBytes),
                                 4);
  ASSERT_EQ(::send(fd, frame.data(), frame.size(), MSG_NOSIGNAL),
            static_cast<ssize_t>(frame.size()));
  ASSERT_TRUE(pump(receiver, nullptr, [&] { return receiver.stats().decode_errors == 1; }));
  EXPECT_EQ(receiver.stats().disconnects, 0U);
  ::close(fd);
}

TEST(Receiver, RejectsSecondConcurrentSender) {
  Receiver receiver(receiver_options());
  const int first = raw_connect(receiver.port());
  ASSERT_TRUE(pump(receiver, nullptr, [&] { return receiver.stats().connections == 1; }));
  const int second = raw_connect(receiver.port());
  ASSERT_TRUE(pump(receiver, nullptr, [&] { return receiver.stats().rejected_connections == 1; }));
  ::close(first);
  ::close(second);
}

}  // namespace
