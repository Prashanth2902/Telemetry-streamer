#include <algorithm>
#include <chrono>
#include <stdexcept>
#include <utility>

#include "tstream/cpu_relax.hpp"
#include "tstream/source.hpp"

namespace tstream {

namespace {

// 1 / rate_hz as a steady_clock duration; rejects non-positive rates.
Clock::duration period_for(double rate_hz) {
  if (!(rate_hz > 0)) {
    throw std::invalid_argument("rate_hz must be positive");
  }
  return std::chrono::duration_cast<Clock::duration>(std::chrono::duration<double>(1.0 / rate_hz));
}

}  // namespace

// Only stores configuration; the thread starts in start().
FixedRateSource::FixedRateSource(std::uint16_t id, std::string_view name, double rate_hz,
                                 SampleRing& ring, std::unique_ptr<SampleModel> model,
                                 PushPolicy push_policy)
    : id_(id),
      name_(name),
      rate_hz_(rate_hz),
      period_(period_for(rate_hz)),
      ring_(ring),
      model_(std::move(model)),
      push_policy_(push_policy) {}

// Joins the thread if the owner didn't call stop().
FixedRateSource::~FixedRateSource() {
  stop();
}

// No-op if the thread is already running.
void FixedRateSource::start() {
  if (thread_.joinable()) {
    return;
  }
  stop_requested_.store(false, std::memory_order_relaxed);
  thread_ = std::thread([this] { run(); });
}

// Relaxed is enough: the flag carries no data, and join() synchronizes.
void FixedRateSource::stop() {
  stop_requested_.store(true, std::memory_order_relaxed);
  if (thread_.joinable()) {
    thread_.join();
  }
}

// Relaxed loads of each counter.
SourceCounters FixedRateSource::counters() const {
  return {.generated = generated_.load(std::memory_order_relaxed),
          .dropped_newest = dropped_newest_.load(std::memory_order_relaxed),
          .dropped_timeout = dropped_timeout_.load(std::memory_order_relaxed),
          .blocked = blocked_.load(std::memory_order_relaxed),
          .overruns = overruns_.load(std::memory_order_relaxed)};
}

// seq advances whether or not try_push succeeds, so drops become gaps downstream.
void FixedRateSource::run() {
  std::uint32_t seq = 0;
  auto next = Clock::now();

  while (!stop_requested_.load(std::memory_order_relaxed)) {
    Sample sample{};
    sample.source_id = id_;
    sample.kind = model_->kind();
    sample.seq = seq;
    sample.t_capture_ns = now_ns();
    model_->fill(sample, static_cast<double>(seq) / rate_hz_);
    ++seq;  // consumed whether or not the push succeeds

    generated_.fetch_add(1, std::memory_order_relaxed);
    static_cast<void>(push(sample));  // drops are counted inside push()

    next += period_;
    const auto now = Clock::now();
    if (now - next > period_) {
      // More than a full period behind (e.g. descheduled): resynchronize
      // rather than emitting a burst of back-to-back samples.
      overruns_.fetch_add(1, std::memory_order_relaxed);
      next = now;
    }
    sleep_until_or_stop(next);
  }
}

// The fast path is one try_push. Blocking checks the clock on every retry,
// which is cheap next to the yield, and stops waiting once stop() is called.
bool FixedRateSource::push(const Sample& sample) {
  if (ring_.try_push(sample)) {
    return true;
  }
  if (push_policy_.policy == BackpressurePolicy::DropNewest) {
    dropped_newest_.fetch_add(1, std::memory_order_relaxed);
    return false;
  }
  const auto deadline = Clock::now() + push_policy_.timeout;
  for (int attempt = 0; !stop_requested_.load(std::memory_order_relaxed); ++attempt) {
    if (attempt < kSpinsBeforeYield) {
      cpu_relax();
    } else {
      std::this_thread::yield();
    }
    if (ring_.try_push(sample)) {
      blocked_.fetch_add(1, std::memory_order_relaxed);
      return true;
    }
    if (Clock::now() >= deadline) {
      break;
    }
  }
  dropped_timeout_.fetch_add(1, std::memory_order_relaxed);
  return false;
}

// Wakes at least every kStopPollInterval to check the stop flag.
void FixedRateSource::sleep_until_or_stop(Clock::time_point deadline) const {
  while (!stop_requested_.load(std::memory_order_relaxed)) {
    const auto now = Clock::now();
    if (now >= deadline) {
      return;
    }
    std::this_thread::sleep_until(std::min(deadline, now + kStopPollInterval));
  }
}

// Picks the model for the configured type and wires it to the ring.
std::unique_ptr<Source> make_source(const SourceConfig& config, SampleRing& ring,
                                    std::uint64_t seed) {
  const std::uint64_t source_seed = model_seed(seed, config.id);
  std::unique_ptr<SampleModel> model;
  switch (config.type) {
    case SourceType::SimImu:
      model = std::make_unique<SimImuModel>(source_seed);
      break;
    case SourceType::SimGps:
      model = std::make_unique<SimGpsModel>(source_seed);
      break;
    case SourceType::SimBattery:
      model = std::make_unique<SimBatteryModel>(source_seed);
      break;
  }
  const PushPolicy push_policy{
      .policy = config.policy,
      .timeout = std::chrono::microseconds(config.timeout_us.value_or(0)),
  };
  return std::make_unique<FixedRateSource>(config.id, source_type_name(config.type), config.rate_hz,
                                           ring, std::move(model), push_policy);
}

}  // namespace tstream
