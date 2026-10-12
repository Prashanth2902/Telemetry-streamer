#include "tstream/pipeline.hpp"

#include <algorithm>
#include <stdexcept>
#include <utility>

namespace tstream {

Pipeline::Pipeline(std::vector<SampleRing*> rings, const BatchConfig& batch, BatchSink& sink)
    : rings_(std::move(rings)),
      sink_(sink),
      batcher_(batch.max_samples, std::int64_t{batch.max_delay_us} * 1000),
      in_open_batch_(rings_.size(), 0),
      delivered_(std::make_unique<std::atomic<std::uint64_t>[]>(rings_.size())) {
  if (rings_.empty()) {
    throw std::invalid_argument("Pipeline needs at least one ring");
  }
}

Pipeline::~Pipeline() { stop(); }

void Pipeline::start() {
  if (thread_.joinable()) {
    return;
  }
  thread_ = std::thread([this] { run(); });
}

void Pipeline::stop() {
  stop_requested_.store(true, std::memory_order_release);
  if (thread_.joinable()) {
    thread_.join();
  }
}

std::uint64_t Pipeline::delivered(std::size_t ring_index) const {
  return delivered_[ring_index].load(std::memory_order_relaxed);
}

std::uint64_t Pipeline::batches() const { return batches_.load(std::memory_order_relaxed); }

void Pipeline::run() {
  std::size_t first_ring = 0;
  while (true) {
    // Read the flag before draining: anything pushed before stop() was called
    // is then picked up by this pass or the final drain below.
    const bool stopping = stop_requested_.load(std::memory_order_acquire);

    const std::size_t drained = drain_pass(first_ring);
    first_ring = (first_ring + 1) % rings_.size();

    if (batcher_.due(now_ns())) {
      flush();
    }

    if (stopping) {
      // Sources are stopped, so the rings only get emptier: drain until a
      // whole pass finds nothing, then flush whatever is left.
      while (drain_pass(first_ring) > 0) {
      }
      if (!batcher_.empty()) {
        flush();
      }
      return;
    }

    if (drained == 0) {
      idle_wait();
    }
  }
}

std::size_t Pipeline::drain_pass(std::size_t first_ring) {
  std::size_t total = 0;
  for (std::size_t k = 0; k < rings_.size(); ++k) {
    const std::size_t index = (first_ring + k) % rings_.size();
    SampleRing& ring = *rings_[index];

    std::size_t taken = 0;
    while (taken < kMaxDrainPerRing) {
      const std::size_t want = std::min(batcher_.free_count(), kMaxDrainPerRing - taken);
      const std::size_t got = ring.try_pop_bulk(batcher_.free_slots(), want);
      if (got == 0) {
        break;
      }
      batcher_.commit(got, now_ns());
      in_open_batch_[index] += got;
      taken += got;
      if (batcher_.full()) {
        flush();
      }
    }
    total += taken;
  }
  return total;
}

void Pipeline::flush() {
  const BatchView view{next_batch_seq_++, now_ns(), batcher_.samples()};
  sink_.send(view);
  // Credit samples as delivered only once the sink has them, so a batch that
  // is drained but never sent shows up as lost instead of delivered.
  for (std::size_t i = 0; i < rings_.size(); ++i) {
    delivered_[i].fetch_add(in_open_batch_[i], std::memory_order_relaxed);
    in_open_batch_[i] = 0;
  }
  batches_.fetch_add(1, std::memory_order_relaxed);
  batcher_.clear();
}

void Pipeline::idle_wait() const {
  auto sleep = kIdleSleep;
  if (!batcher_.empty()) {
    const auto until_due = std::chrono::nanoseconds(batcher_.deadline_ns() - now_ns());
    if (until_due <= std::chrono::nanoseconds::zero()) {
      return;  // due now; the next loop iteration flushes it
    }
    sleep = std::min<Clock::duration>(sleep, until_due);
  }
  std::this_thread::sleep_for(sleep);
}

}  // namespace tstream
