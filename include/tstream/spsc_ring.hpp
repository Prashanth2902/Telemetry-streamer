// Lock-free single-producer / single-consumer ring buffer (SPEC §5).
//
// Exactly one thread may push and exactly one thread may pop (SPEC §3.2).
// Debug builds assert this; it is what makes the lock-free design correct.
//
// How it works:
// - tail counts pushes and is written only by the producer. head counts pops
//   and is written only by the consumer. Both only ever increase; the slot for
//   index i is slots_[i & (Capacity - 1)]. tail - head is always the exact
//   fill level, so all Capacity slots are usable: full is
//   tail - head == Capacity, empty is tail == head. Unsigned wraparound is
//   harmless: Capacity is a power of two, so it divides 2^64 and both the
//   subtraction and the mask stay correct modulo 2^64.
// - Producer: write the slot, then tail.store(release). Consumer:
//   tail.load(acquire), then read the slot. The release/acquire pair makes
//   the slot write visible before the consumer can see the new tail.
// - The other direction mirrors it: the consumer reads the slot, then
//   head.store(release). The producer's head.load(acquire) guarantees that
//   read finished before the producer reuses the slot.
// - Each side keeps a plain (non-atomic) cached copy of the other side's
//   index and reloads the shared atomic only when the ring looks full (for
//   the producer) or empty (for the consumer). In steady state each side then
//   touches only its own cache line.

#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <thread>
#include <type_traits>

#include "tstream/cache_line.hpp"

namespace tstream {

// Layout and algorithm knobs. The defaults are the production configuration;
// the others exist so bench_spsc can measure what each choice is worth.
struct SpscRingOptions {
  // Keep a private copy of the other side's index (see file comment).
  bool cache_indices = true;
  // Alignment of each side's index group and of the slot storage. Two cache
  // lines, not one: measured ~45% higher throughput than kCacheLine on the dev
  // machine (docs/design-decisions.md ADR-010). kCacheLine still separates the
  // sides; alignof(std::atomic<std::size_t>) packs them together (false sharing).
  std::size_t alignment = 2 * kCacheLine;
};

template <typename T, std::size_t Capacity, SpscRingOptions Options = SpscRingOptions{}>
class BasicSpscRing {
  static_assert(std::is_trivially_copyable_v<T>, "SpscRing slots are copied with plain stores");
  static_assert(Capacity > 0 && (Capacity & (Capacity - 1)) == 0,
                "Capacity must be a power of two");
  static_assert((Options.alignment & (Options.alignment - 1)) == 0 &&
                    Options.alignment >= alignof(std::atomic<std::size_t>),
                "alignment must be a power of two of at least alignof(atomic<size_t>)");

 public:
  static constexpr std::size_t kCapacity = Capacity;

  BasicSpscRing() = default;
  BasicSpscRing(const BasicSpscRing&) = delete;
  BasicSpscRing& operator=(const BasicSpscRing&) = delete;
  BasicSpscRing(BasicSpscRing&&) = delete;
  BasicSpscRing& operator=(BasicSpscRing&&) = delete;
  ~BasicSpscRing() = default;

  // Producer only. Returns false, without blocking, if the ring is full.
  [[nodiscard]] bool try_push(const T& value) {
    check_owner(producer_.owner);
    // Only this thread writes tail, so a relaxed load sees our own last store.
    const std::size_t tail = producer_.tail.load(std::memory_order_relaxed);
    if (tail - head_for_producer(tail) == Capacity) {
      return false;
    }
    slots_[tail & kMask] = value;
    producer_.tail.store(tail + 1, std::memory_order_release);
    return true;
  }

  // Consumer only. Returns false, leaving `out` untouched, if the ring is empty.
  [[nodiscard]] bool try_pop(T& out) {
    check_owner(consumer_.owner);
    const std::size_t head = consumer_.head.load(std::memory_order_relaxed);
    if (tail_for_consumer(head, 1) == head) {
      return false;
    }
    out = slots_[head & kMask];
    consumer_.head.store(head + 1, std::memory_order_release);
    return true;
  }

  // Consumer only. Pops up to `max` items into out[0..n) and returns n.
  // One head update for the whole batch, so the producer sees the space
  // freed all at once.
  [[nodiscard]] std::size_t try_pop_bulk(T* out, std::size_t max) {
    check_owner(consumer_.owner);
    const std::size_t head = consumer_.head.load(std::memory_order_relaxed);
    const std::size_t available = tail_for_consumer(head, max) - head;
    const std::size_t n = std::min(available, max);
    if (n == 0) {
      return 0;
    }
    // The readable range may run past the end of slots_; copy it as up to two
    // contiguous pieces. (The explicit `first < n` check also lets GCC prove
    // n - first can't underflow; without it -Wstringop-overflow misfires.)
    const std::size_t start = head & kMask;
    const std::size_t first = std::min(n, Capacity - start);
    std::copy_n(slots_.begin() + static_cast<std::ptrdiff_t>(start), first, out);
    if (first < n) {
      std::copy_n(slots_.begin(), n - first, out + first);
    }
    consumer_.head.store(head + n, std::memory_order_release);
    return n;
  }

  // Any thread. Exact when called with no concurrent push/pop; otherwise a
  // value the ring held at some point during the call.
  [[nodiscard]] std::size_t size_approx() const {
    // Load head first: the consumer only advances head after seeing a tail at
    // least that large, so the tail loaded afterwards can't be smaller.
    const std::size_t head = consumer_.head.load(std::memory_order_acquire);
    const std::size_t tail = producer_.tail.load(std::memory_order_acquire);
    return std::min(tail - head, Capacity);
  }

 private:
  static constexpr std::size_t kMask = Capacity - 1;

  // Producer side: the head value to compare against. Reloads the shared
  // head only when the cached copy says the ring is full.
  std::size_t head_for_producer(std::size_t tail) {
    if constexpr (Options.cache_indices) {
      if (tail - producer_.cached_head == Capacity) {
        producer_.cached_head = consumer_.head.load(std::memory_order_acquire);
      }
      return producer_.cached_head;
    } else {
      return consumer_.head.load(std::memory_order_acquire);
    }
  }

  // Consumer side: the tail value to compare against. Reloads the shared
  // tail only when the cached copy shows fewer than `wanted` items.
  std::size_t tail_for_consumer(std::size_t head, std::size_t wanted) {
    if constexpr (Options.cache_indices) {
      if (consumer_.cached_tail - head < wanted) {
        consumer_.cached_tail = producer_.tail.load(std::memory_order_acquire);
      }
      return consumer_.cached_tail;
    } else {
      return producer_.tail.load(std::memory_order_acquire);
    }
  }

#ifndef NDEBUG
  // The first thread to use a side becomes its owner; any other thread fails.
  static void check_owner(std::atomic<std::thread::id>& owner) {
    const std::thread::id self = std::this_thread::get_id();
    std::thread::id expected{};
    if (!owner.compare_exchange_strong(expected, self, std::memory_order_relaxed)) {
      assert(expected == self && "SpscRing side used from more than one thread");
    }
  }
  using OwnerId = std::atomic<std::thread::id>;
#else
  // Release builds: no owner tracking, and [[no_unique_address]] makes the
  // empty member take no space.
  struct OwnerId {};
  static void check_owner(OwnerId& /*owner*/) {}
#endif

  // Each group holds one side's shared index plus that side's private data,
  // so in steady state each thread writes only its own cache line.
  struct alignas(Options.alignment) ProducerSide {
    std::atomic<std::size_t> tail{0};
    std::size_t cached_head = 0;
    [[no_unique_address]] OwnerId owner{};
  };
  struct alignas(Options.alignment) ConsumerSide {
    std::atomic<std::size_t> head{0};
    std::size_t cached_tail = 0;
    [[no_unique_address]] OwnerId owner{};
  };

  ProducerSide producer_;
  ConsumerSide consumer_;
  alignas(Options.alignment) std::array<T, Capacity> slots_{};
};

// The production configuration: cached indices, sides 128 bytes apart.
template <typename T, std::size_t Capacity>
using SpscRing = BasicSpscRing<T, Capacity>;

}  // namespace tstream
