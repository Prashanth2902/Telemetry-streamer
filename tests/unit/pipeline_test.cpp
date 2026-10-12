// Pipeline with fake producers and a fake/mock sink (SPEC §13 "Mock-based").

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <thread>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "tstream/clock.hpp"
#include "tstream/pipeline.hpp"
#include "tstream/sample_ring.hpp"
#include "tstream/sink.hpp"

namespace {

using namespace std::chrono_literals;
using ::testing::Field;
using ::testing::SizeIs;
using tstream::BatchConfig;
using tstream::BatchView;
using tstream::Pipeline;
using tstream::Sample;
using tstream::SampleRing;

// Copies every batch it receives. send() runs on the pipeline thread; tests
// read `batches` only after stop() (join gives happens-before), or poll
// `count` while running.
class RecordingSink final : public tstream::BatchSink {
 public:
  struct Batch {
    std::uint64_t batch_seq;
    std::int64_t t_flush_ns;
    std::vector<Sample> samples;
  };

  void send(const BatchView& batch) override {
    batches.push_back({batch.batch_seq, batch.t_flush_ns, {batch.samples.begin(), batch.samples.end()}});
    count.fetch_add(1, std::memory_order_release);
  }

  std::vector<Batch> batches;
  std::atomic<std::size_t> count{0};
};

class MockSink final : public tstream::BatchSink {
 public:
  MOCK_METHOD(void, send, (const BatchView& batch), (override));
};

Sample make_sample(std::uint16_t source_id, std::uint32_t seq) {
  Sample s{};
  s.source_id = source_id;
  s.seq = seq;
  s.t_capture_ns = tstream::now_ns();
  return s;
}

void push_n(SampleRing& ring, std::uint16_t source_id, std::uint32_t count) {
  for (std::uint32_t seq = 0; seq < count; ++seq) {
    ASSERT_TRUE(ring.try_push(make_sample(source_id, seq)));
  }
}

BatchConfig batch_config(std::size_t max_samples, std::uint32_t max_delay_us) {
  BatchConfig c;
  c.max_samples = max_samples;
  c.max_delay_us = max_delay_us;
  return c;
}

template <typename Predicate>
bool wait_for(Predicate done, std::chrono::milliseconds timeout = 2000ms) {
  const auto deadline = tstream::Clock::now() + timeout;
  while (!done()) {
    if (tstream::Clock::now() > deadline) {
      return false;
    }
    std::this_thread::sleep_for(1ms);
  }
  return true;
}

TEST(Pipeline, FlushesWhenBatchIsFull) {
  const auto ring = tstream::make_sample_ring(64);
  push_n(*ring, 1, 8);

  MockSink sink;
  EXPECT_CALL(sink, send(Field(&BatchView::samples, SizeIs(4)))).Times(2);

  Pipeline pipeline({ring.get()}, batch_config(4, 10'000'000), sink);  // 10 s delay: size only
  pipeline.start();
  ASSERT_TRUE(wait_for([&] { return pipeline.batches() == 2; }));
  pipeline.stop();
  EXPECT_EQ(pipeline.delivered(0), 8U);
}

TEST(Pipeline, FlushesPartialBatchAfterMaxDelay) {
  const auto ring = tstream::make_sample_ring(64);
  RecordingSink sink;
  Pipeline pipeline({ring.get()}, batch_config(100, 5'000), sink);  // 5 ms
  pipeline.start();
  push_n(*ring, 1, 3);

  // Arrives while running, well before stop(): the timer flushed it.
  ASSERT_TRUE(wait_for([&] { return sink.count.load(std::memory_order_acquire) == 1; }));
  pipeline.stop();

  ASSERT_EQ(sink.batches.size(), 1U);
  EXPECT_EQ(sink.batches[0].samples.size(), 3U);
  const auto waited_ns = sink.batches[0].t_flush_ns - sink.batches[0].samples[0].t_capture_ns;
  EXPECT_GE(waited_ns, 5'000'000);
}

TEST(Pipeline, StopFlushesFinalPartialBatch) {
  const auto ring = tstream::make_sample_ring(64);
  push_n(*ring, 1, 5);
  RecordingSink sink;
  Pipeline pipeline({ring.get()}, batch_config(100, 10'000'000), sink);  // never due on its own
  pipeline.start();
  pipeline.stop();

  ASSERT_EQ(sink.batches.size(), 1U);
  EXPECT_EQ(sink.batches[0].samples.size(), 5U);
}

TEST(Pipeline, StopWithNothingQueuedSendsNoBatch) {
  const auto ring = tstream::make_sample_ring(16);
  RecordingSink sink;
  Pipeline pipeline({ring.get()}, batch_config(8, 1'000), sink);
  pipeline.start();
  pipeline.stop();
  EXPECT_TRUE(sink.batches.empty());
  EXPECT_EQ(pipeline.batches(), 0U);
}

TEST(Pipeline, BatchSeqStartsAtZeroAndIncrements) {
  const auto ring = tstream::make_sample_ring(64);
  push_n(*ring, 1, 10);
  RecordingSink sink;
  Pipeline pipeline({ring.get()}, batch_config(3, 10'000'000), sink);
  pipeline.start();
  pipeline.stop();

  ASSERT_EQ(sink.batches.size(), 4U);  // 3 + 3 + 3 + final 1
  for (std::size_t i = 0; i < sink.batches.size(); ++i) {
    EXPECT_EQ(sink.batches[i].batch_seq, i);
    EXPECT_LE(sink.batches[i].samples.size(), 3U);
  }
}

// Concurrent producers on real threads: every sample arrives exactly once and
// in per-source order. Under the tsan preset this also checks the pipeline
// and ring handoff for races.
TEST(Pipeline, DeliversEverySampleInPerSourceOrderUnderLoad) {
  constexpr std::uint32_t kPerSource = 50'000;
  const auto ring_a = tstream::make_sample_ring(256);
  const auto ring_b = tstream::make_sample_ring(16);  // tiny: constantly full
  RecordingSink sink;
  Pipeline pipeline({ring_a.get(), ring_b.get()}, batch_config(64, 500), sink);
  pipeline.start();

  auto produce = [](SampleRing& ring, std::uint16_t id) {
    for (std::uint32_t seq = 0; seq < kPerSource; ++seq) {
      while (!ring.try_push(make_sample(id, seq))) {
        std::this_thread::yield();
      }
    }
  };
  std::thread producer_a(produce, std::ref(*ring_a), std::uint16_t{1});
  std::thread producer_b(produce, std::ref(*ring_b), std::uint16_t{2});
  producer_a.join();
  producer_b.join();
  pipeline.stop();  // producers are done: the SPEC §3.2 shutdown order

  std::uint32_t next_a = 0;
  std::uint32_t next_b = 0;
  std::size_t total = 0;
  for (const auto& batch : sink.batches) {
    ASSERT_LE(batch.samples.size(), 64U);
    for (const Sample& s : batch.samples) {
      std::uint32_t& next = s.source_id == 1 ? next_a : next_b;
      ASSERT_EQ(s.seq, next) << "source " << s.source_id;
      ++next;
      ++total;
    }
  }
  EXPECT_EQ(next_a, kPerSource);
  EXPECT_EQ(next_b, kPerSource);
  EXPECT_EQ(total, 2U * kPerSource);
  EXPECT_EQ(pipeline.delivered(0), kPerSource);
  EXPECT_EQ(pipeline.delivered(1), kPerSource);
}

// Round-robin with a bounded drain: a busy ring can't starve a quiet one.
TEST(Pipeline, BoundedDrainInterleavesRings) {
  const auto busy = tstream::make_sample_ring(1024);
  const auto quiet = tstream::make_sample_ring(16);
  push_n(*busy, 1, 1000);
  push_n(*quiet, 2, 1);
  RecordingSink sink;
  Pipeline pipeline({busy.get(), quiet.get()}, batch_config(1000, 10'000'000), sink);
  pipeline.start();
  pipeline.stop();

  // One batch of all 1001 can't happen (max 1000), so look at where the quiet
  // sample landed: within the first two drains, not after all of `busy`.
  std::size_t position = 0;
  bool found = false;
  for (const auto& batch : sink.batches) {
    for (const Sample& s : batch.samples) {
      if (s.source_id == 2) {
        found = true;
        break;
      }
      ++position;
    }
    if (found) {
      break;
    }
  }
  ASSERT_TRUE(found);
  EXPECT_LE(position, 2 * Pipeline::kMaxDrainPerRing);
}

}  // namespace
