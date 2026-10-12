// Mutex-based queues used as baselines for SpscRing in bench_spsc (SPEC §5).
// Same try_push / try_pop API as SpscRing so benchmarks are generic over the
// queue type. Both are bounded to Capacity so every queue applies the same
// backpressure.

#pragma once

#include <array>
#include <cstddef>
#include <deque>
#include <mutex>

namespace tstream::bench {

// std::mutex + std::deque: the "obvious" thread-safe queue.
template <typename T, std::size_t Capacity>
class MutexDequeQueue {
 public:
  bool try_push(const T& value) {
    const std::scoped_lock lock(mutex_);
    if (items_.size() == Capacity) {
      return false;
    }
    items_.push_back(value);
    return true;
  }

  bool try_pop(T& out) {
    const std::scoped_lock lock(mutex_);
    if (items_.empty()) {
      return false;
    }
    out = items_.front();
    items_.pop_front();
    return true;
  }

 private:
  std::mutex mutex_;
  std::deque<T> items_;
};

// std::mutex + fixed array ring: isolates the cost of locking from the cost of
// deque allocation, so the SpscRing comparison measures lock-freedom alone.
template <typename T, std::size_t Capacity>
class MutexRingQueue {
 public:
  bool try_push(const T& value) {
    const std::scoped_lock lock(mutex_);
    if (size_ == Capacity) {
      return false;
    }
    slots_[(head_ + size_) % Capacity] = value;
    ++size_;
    return true;
  }

  bool try_pop(T& out) {
    const std::scoped_lock lock(mutex_);
    if (size_ == 0) {
      return false;
    }
    out = slots_[head_];
    head_ = (head_ + 1) % Capacity;
    --size_;
    return true;
  }

 private:
  std::mutex mutex_;
  std::array<T, Capacity> slots_{};
  std::size_t head_ = 0;
  std::size_t size_ = 0;
};

}  // namespace tstream::bench
