// Single-threaded semantics of SpscRing (SPEC §5): empty, full, wraparound,
// bulk pop. Concurrency is covered by tests/stress/spsc_ring_stress_test.cpp.

#include "tstream/spsc_ring.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <thread>
#include <type_traits>
#include <vector>

#include <gtest/gtest.h>

namespace {

using tstream::SpscRing;

// Trivially copyable payload with a distinct value per item, so ordering,
// loss, and duplication are all detectable.
struct Item {
  std::uint64_t seq;
  std::array<std::uint8_t, 24> payload;
};
static_assert(std::is_trivially_copyable_v<Item>);

Item make_item(std::uint64_t seq) {
  Item item{};
  item.seq = seq;
  item.payload.fill(static_cast<std::uint8_t>(seq));
  return item;
}

void expect_item(const Item& item, std::uint64_t seq) {
  EXPECT_EQ(item.seq, seq);
  EXPECT_EQ(item.payload[0], static_cast<std::uint8_t>(seq));
  EXPECT_EQ(item.payload[23], static_cast<std::uint8_t>(seq));
}

template <std::size_t N>
struct Cap {
  static constexpr std::size_t value = N;
};

template <typename CapT>
class SpscRingTest : public ::testing::Test {
 protected:
  static constexpr std::size_t kCapacity = CapT::value;
  SpscRing<Item, kCapacity> ring_;

  // Push items [first, first + count); all must succeed.
  void push_range(std::uint64_t first, std::size_t count) {
    for (std::size_t i = 0; i < count; ++i) {
      ASSERT_TRUE(ring_.try_push(make_item(first + i))) << "push #" << i;
    }
  }

  // Pop count items, which must be [first, first + count) in order.
  void pop_range(std::uint64_t first, std::size_t count) {
    for (std::size_t i = 0; i < count; ++i) {
      Item out{};
      ASSERT_TRUE(ring_.try_pop(out)) << "pop #" << i;
      expect_item(out, first + i);
    }
  }
};

// Capacity 1 is the smallest power of two and the sharpest edge case.
using Capacities = ::testing::Types<Cap<1>, Cap<2>, Cap<64>, Cap<1024>>;
TYPED_TEST_SUITE(SpscRingTest, Capacities);

TYPED_TEST(SpscRingTest, NewRingIsEmpty) {
  Item out{};
  EXPECT_FALSE(this->ring_.try_pop(out));
  EXPECT_EQ(this->ring_.size_approx(), 0U);
}

TYPED_TEST(SpscRingTest, PopFromEmptyLeavesOutputUntouched) {
  Item out = make_item(0xABCD);
  EXPECT_FALSE(this->ring_.try_pop(out));
  expect_item(out, 0xABCD);
}

TYPED_TEST(SpscRingTest, PopReturnsPushedItemsInFifoOrder) {
  this->push_range(100, this->kCapacity);
  this->pop_range(100, this->kCapacity);
  Item out{};
  EXPECT_FALSE(this->ring_.try_pop(out));
}

// With monotonic counters and masking (SPEC §5), all Capacity slots are usable:
// no slot is sacrificed to tell full from empty.
TYPED_TEST(SpscRingTest, HoldsExactlyCapacityItems) {
  this->push_range(0, this->kCapacity);
  EXPECT_EQ(this->ring_.size_approx(), this->kCapacity);
  EXPECT_FALSE(this->ring_.try_push(make_item(999)));
}

TYPED_TEST(SpscRingTest, FailedPushOnFullRingDoesNotDisturbContents) {
  this->push_range(0, this->kCapacity);
  for (int i = 0; i < 3; ++i) {
    EXPECT_FALSE(this->ring_.try_push(make_item(999)));
  }
  this->pop_range(0, this->kCapacity);
}

TYPED_TEST(SpscRingTest, PopFromFullFreesExactlyOneSlot) {
  this->push_range(0, this->kCapacity);
  this->pop_range(0, 1);
  EXPECT_TRUE(this->ring_.try_push(make_item(this->kCapacity)));
  EXPECT_FALSE(this->ring_.try_push(make_item(999)));
  this->pop_range(1, this->kCapacity);
}

// Many fill/drain cycles push the indices far past Capacity, so every slot is
// reused and the masking arithmetic is exercised.
TYPED_TEST(SpscRingTest, WrapsAroundAcrossManyFullCycles) {
  std::uint64_t seq = 0;
  for (int cycle = 0; cycle < 10; ++cycle) {
    this->push_range(seq, this->kCapacity);
    this->pop_range(seq, this->kCapacity);
    seq += this->kCapacity;
  }
  EXPECT_EQ(this->ring_.size_approx(), 0U);
}

// Partial fills put the head and tail at every possible offset relative to the
// end of the storage array.
TYPED_TEST(SpscRingTest, WrapsAroundWithPartialFills) {
  const std::size_t chunk = this->kCapacity > 2 ? this->kCapacity / 2 + 1 : 1;
  std::uint64_t pushed = 0;
  std::uint64_t popped = 0;
  for (std::size_t round = 0; round < 4 * this->kCapacity + 3; ++round) {
    this->push_range(pushed, chunk);
    pushed += chunk;
    this->pop_range(popped, chunk);
    popped += chunk;
  }
  EXPECT_EQ(this->ring_.size_approx(), 0U);
}

TYPED_TEST(SpscRingTest, SizeApproxIsExactWhenSingleThreaded) {
  for (std::size_t i = 0; i < this->kCapacity; ++i) {
    EXPECT_EQ(this->ring_.size_approx(), i);
    ASSERT_TRUE(this->ring_.try_push(make_item(i)));
  }
  for (std::size_t i = this->kCapacity; i > 0; --i) {
    EXPECT_EQ(this->ring_.size_approx(), i);
    Item out{};
    ASSERT_TRUE(this->ring_.try_pop(out));
  }
  EXPECT_EQ(this->ring_.size_approx(), 0U);
}

TYPED_TEST(SpscRingTest, BulkPopFromEmptyReturnsZero) {
  std::vector<Item> out(this->kCapacity);
  EXPECT_EQ(this->ring_.try_pop_bulk(out.data(), out.size()), 0U);
}

TYPED_TEST(SpscRingTest, BulkPopWithZeroMaxPopsNothing) {
  this->push_range(0, this->kCapacity);
  Item out{};
  EXPECT_EQ(this->ring_.try_pop_bulk(&out, 0), 0U);
  EXPECT_EQ(this->ring_.size_approx(), this->kCapacity);
}

TYPED_TEST(SpscRingTest, BulkPopIsLimitedByMax) {
  this->push_range(0, this->kCapacity);
  const std::size_t max = this->kCapacity > 1 ? this->kCapacity / 2 : 1;
  std::vector<Item> out(max);
  ASSERT_EQ(this->ring_.try_pop_bulk(out.data(), max), max);
  for (std::size_t i = 0; i < max; ++i) {
    expect_item(out[i], i);
  }
  EXPECT_EQ(this->ring_.size_approx(), this->kCapacity - max);
  this->pop_range(max, this->kCapacity - max);
}

TYPED_TEST(SpscRingTest, BulkPopIsLimitedByAvailableItems) {
  const std::size_t available = this->kCapacity > 1 ? this->kCapacity / 2 : 1;
  this->push_range(0, available);
  std::vector<Item> out(this->kCapacity + 5);
  ASSERT_EQ(this->ring_.try_pop_bulk(out.data(), out.size()), available);
  for (std::size_t i = 0; i < available; ++i) {
    expect_item(out[i], i);
  }
  EXPECT_EQ(this->ring_.size_approx(), 0U);
}

// The readable region straddles the end of the storage array, so a bulk pop
// that copies in one contiguous block would return the wrong items.
TYPED_TEST(SpscRingTest, BulkPopAcrossWrapBoundaryPreservesOrder) {
  const std::size_t offset = this->kCapacity > 1 ? this->kCapacity - 1 : 0;
  this->push_range(0, offset);
  this->pop_range(0, offset);  // head and tail now sit just before the end
  this->push_range(offset, this->kCapacity);

  std::vector<Item> out(this->kCapacity);
  ASSERT_EQ(this->ring_.try_pop_bulk(out.data(), out.size()), this->kCapacity);
  for (std::size_t i = 0; i < this->kCapacity; ++i) {
    expect_item(out[i], offset + i);
  }
}

TYPED_TEST(SpscRingTest, BulkPopFreesSlotsForProducer) {
  this->push_range(0, this->kCapacity);
  std::vector<Item> out(this->kCapacity);
  ASSERT_EQ(this->ring_.try_pop_bulk(out.data(), out.size()), this->kCapacity);
  this->push_range(this->kCapacity, this->kCapacity);
  this->pop_range(this->kCapacity, this->kCapacity);
}

TEST(SpscRingTypes, WorksWithScalarTypes) {
  SpscRing<std::uint32_t, 4> ring;
  ASSERT_TRUE(ring.try_push(7U));
  std::uint32_t out = 0;
  ASSERT_TRUE(ring.try_pop(out));
  EXPECT_EQ(out, 7U);
}

// SPEC §3.2: debug builds record the thread that first pushes (or pops) and
// assert if a different thread does so later. Release and RelWithDebInfo
// builds (including the tsan presets) compile the check out.
#ifndef NDEBUG
void push_from_two_threads() {
  SpscRing<int, 8> ring;
  ring.try_push(1);
  std::thread other([&] { ring.try_push(2); });
  other.join();
}

void pop_from_two_threads() {
  SpscRing<int, 8> ring;
  ring.try_push(1);
  ring.try_push(2);
  int out = 0;
  ring.try_pop(out);
  std::thread other([&] { ring.try_pop(out); });
  other.join();
}

TEST(SpscRingDeathTest, SecondProducerThreadAssertsInDebug) {
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  EXPECT_DEATH(push_from_two_threads(), "");
}

TEST(SpscRingDeathTest, SecondConsumerThreadAssertsInDebug) {
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  EXPECT_DEATH(pop_from_two_threads(), "");
}
#endif

}  // namespace
