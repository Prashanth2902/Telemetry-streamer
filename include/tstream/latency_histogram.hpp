// Fixed-size log-linear histogram for latency percentiles.
//
// Values below 16 ns get one bucket each. Above that, every power of two is
// split into 16 equal buckets, so a reported percentile is at most 1/16
// (6.25%) above the true value. 976 buckets cover the whole int64 range; no
// allocation after construction, and memory doesn't grow with the sample count.

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

namespace tstream {

class LatencyHistogram {
 public:
  static constexpr int kSubBucketBits = 4;
  static constexpr std::size_t kSubBuckets = std::size_t{1} << kSubBucketBits;
  static constexpr std::size_t kBuckets = kSubBuckets + (63 - kSubBucketBits + 1) * kSubBuckets;

  // Adds one value in nanoseconds. Negative values (clock skew) count as 0.
  void record(std::int64_t value_ns);
  // Adds every value from `other`.
  void merge(const LatencyHistogram& other);
  // Forgets everything recorded.
  void reset();

  // Number of values recorded.
  [[nodiscard]] std::uint64_t count() const { return count_; }
  // Largest value recorded, exactly; 0 if empty.
  [[nodiscard]] std::int64_t max() const { return max_; }
  // Value at quantile q in [0, 1]: the upper edge of the bucket holding it,
  // capped at max(). 0 if empty.
  [[nodiscard]] std::int64_t percentile(double q) const;

  // Bucket index for a non-negative value (exposed for tests).
  static std::size_t bucket_for(std::uint64_t value);
  // Largest value that lands in `bucket` (exposed for tests).
  static std::uint64_t bucket_upper(std::size_t bucket);

 private:
  std::array<std::uint64_t, kBuckets> buckets_{};
  std::uint64_t count_ = 0;
  std::int64_t max_ = 0;
};

}  // namespace tstream
