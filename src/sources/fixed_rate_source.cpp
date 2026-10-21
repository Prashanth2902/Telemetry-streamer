#include <algorithm>
#include <chrono>
#include <optional>
#include <stdexcept>
#include <utility>
#include <vector>

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

// Only the faults that name `id`.
std::vector<Fault> faults_for(std::uint16_t id, const std::vector<Fault>& faults) {
  std::vector<Fault> mine;
  for (const Fault& f : faults) {
    if (f.source_id == id) {
      mine.push_back(f);
    }
  }
  return mine;
}

// The largest burst count, or 0 without bursts.
std::size_t largest_burst(const std::vector<Fault>& faults) {
  std::size_t largest = 0;
  for (const Fault& f : faults) {
    if (f.kind == FaultKind::Burst) {
      largest = std::max(largest, static_cast<std::size_t>(f.arg));
    }
  }
  return largest;
}

}  // namespace

// Only stores configuration; the thread starts in start().
FixedRateSource::FixedRateSource(std::uint16_t id, std::string_view name, double rate_hz,
                                 SampleRing& ring, std::unique_ptr<SampleModel> model,
                                 PushPolicy push_policy, const std::vector<Fault>& faults)
    : id_(id),
      name_(name),
      rate_hz_(rate_hz),
      period_(period_for(rate_hz)),
      ring_(ring),
      model_(std::move(model)),
      push_policy_(push_policy),
      faults_(faults_for(id, faults)),
      burst_buffer_(largest_burst(faults_)) {}

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

// seq advances whether or not try_push succeeds, so drops become gaps
// downstream. The burst flags are the only allocation, made before the loop.
void FixedRateSource::run() {
  std::uint32_t seq = 0;
  const auto started = Clock::now();
  auto next = started;
  std::vector<bool> burst_fired(faults_.size(), false);
  Sample last{};
  bool have_last = false;

  while (!stop_requested_.load(std::memory_order_relaxed)) {
    const std::int64_t elapsed =
        std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - started).count();
    if (const auto stall_end = stall_end_at(elapsed)) {
      sleep_until_or_stop(started + std::chrono::nanoseconds(*stall_end));
      next = Clock::now();  // resume now, not with a catch-up burst
      continue;
    }

    const bool frozen = have_last && frozen_at(elapsed);
    emit(next_sample(seq, frozen, last));
    have_last = true;
    for (std::size_t i = 0; i < faults_.size(); ++i) {
      if (faults_[i].kind == FaultKind::Burst && !burst_fired[i] &&
          elapsed >= faults_[i].start_ns) {
        burst_fired[i] = true;
        emit_burst(static_cast<std::size_t>(faults_[i].arg), seq, frozen, last);
      }
    }

    const Clock::duration period = period_at(elapsed);
    next += period;
    const auto now = Clock::now();
    if (now - next > period) {
      // More than a full period behind (e.g. descheduled): resynchronize
      // rather than emitting a burst of back-to-back samples.
      overruns_.fetch_add(1, std::memory_order_relaxed);
      next = now;
    }
    sleep_until_or_stop(next);
  }
}

// A frozen sample copies the whole previous sample, then gets a new header.
Sample FixedRateSource::next_sample(std::uint32_t& seq, bool frozen, Sample& last) {
  Sample sample{};
  if (frozen) {
    sample = last;
  } else {
    sample.kind = model_->kind();
    model_->fill(sample, static_cast<double>(seq) / rate_hz_);
  }
  sample.source_id = id_;
  sample.seq = seq;
  sample.t_capture_ns = now_ns();
  ++seq;  // consumed whether or not the push succeeds
  last = sample;
  return sample;
}

// Drops are counted inside push().
void FixedRateSource::emit(const Sample& sample) {
  generated_.fetch_add(1, std::memory_order_relaxed);
  static_cast<void>(push(sample));
}

// Generating payloads is the slow part, so it happens first; the pushes then
// run back to back, all stamped with the same instant.
void FixedRateSource::emit_burst(std::size_t count, std::uint32_t& seq, bool frozen, Sample& last) {
  for (std::size_t i = 0; i < count; ++i) {
    burst_buffer_[i] = next_sample(seq, frozen, last);
  }
  const std::int64_t t = now_ns();
  for (std::size_t i = 0; i < count; ++i) {
    burst_buffer_[i].t_capture_ns = t;
    emit(burst_buffer_[i]);
  }
}

// Overlapping stalls: the one that ends last wins.
std::optional<std::int64_t> FixedRateSource::stall_end_at(std::int64_t elapsed_ns) const {
  std::optional<std::int64_t> end;
  for (const Fault& f : faults_) {
    if (f.kind == FaultKind::Stall && f.active_at(elapsed_ns)) {
      end = std::max(end.value_or(0), *f.end_ns());
    }
  }
  return end;
}

// Any active freeze freezes.
bool FixedRateSource::frozen_at(std::int64_t elapsed_ns) const {
  return std::ranges::any_of(faults_, [&](const Fault& f) {
    return f.kind == FaultKind::Freeze && f.active_at(elapsed_ns);
  });
}

// Overlapping rate faults multiply.
Clock::duration FixedRateSource::period_at(std::int64_t elapsed_ns) const {
  double factor = 1.0;
  for (const Fault& f : faults_) {
    if (f.kind == FaultKind::Rate && f.active_at(elapsed_ns)) {
      factor *= f.arg;
    }
  }
  if (factor == 1.0) {
    return period_;
  }
  return std::chrono::duration_cast<Clock::duration>(period_ / factor);
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
                                    std::uint64_t seed, const std::vector<Fault>& faults) {
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
                                           ring, std::move(model), push_policy, faults);
}

}  // namespace tstream
