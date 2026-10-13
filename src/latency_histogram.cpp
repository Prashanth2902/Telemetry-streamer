#include "tstream/latency_histogram.hpp"

#include <algorithm>
#include <bit>
#include <cmath>
#include <limits>

namespace tstream {

// Top kSubBucketBits bits below the leading one select the sub-bucket.
std::size_t LatencyHistogram::bucket_for(std::uint64_t value) {
  if (value < kSubBuckets) {
    return static_cast<std::size_t>(value);
  }
  const int exponent = std::bit_width(value) - 1;  // >= kSubBucketBits
  const int shift = exponent - kSubBucketBits;
  const auto sub = static_cast<std::size_t>((value >> shift) & (kSubBuckets - 1));
  return kSubBuckets + static_cast<std::size_t>(shift) * kSubBuckets + sub;
}

// Inverse of bucket_for: the bucket's lower edge plus its width, minus one.
std::uint64_t LatencyHistogram::bucket_upper(std::size_t bucket) {
  if (bucket < kSubBuckets) {
    return bucket;
  }
  const std::size_t shift = (bucket - kSubBuckets) / kSubBuckets;
  const std::size_t sub = (bucket - kSubBuckets) % kSubBuckets;
  const std::uint64_t lower = (std::uint64_t{kSubBuckets} + sub) << shift;
  const std::uint64_t width = std::uint64_t{1} << shift;
  return lower + (width - 1);
}

// Clamps negatives: cross-host or skewed clocks shouldn't index out of range.
void LatencyHistogram::record(std::int64_t value_ns) {
  const auto value = static_cast<std::uint64_t>(std::max<std::int64_t>(value_ns, 0));
  ++buckets_[bucket_for(value)];
  ++count_;
  max_ = std::max(max_, std::max<std::int64_t>(value_ns, 0));
}

// Bucket-wise sum; exact, since both use the same bucket layout.
void LatencyHistogram::merge(const LatencyHistogram& other) {
  for (std::size_t i = 0; i < kBuckets; ++i) {
    buckets_[i] += other.buckets_[i];
  }
  count_ += other.count_;
  max_ = std::max(max_, other.max_);
}

// Zeroes the counters; no memory is released or allocated.
void LatencyHistogram::reset() {
  buckets_.fill(0);
  count_ = 0;
  max_ = 0;
}

// Walks buckets until the cumulative count reaches ceil(q * count).
std::int64_t LatencyHistogram::percentile(double q) const {
  if (count_ == 0) {
    return 0;
  }
  q = std::clamp(q, 0.0, 1.0);
  const auto rank = std::max<std::uint64_t>(
      1, static_cast<std::uint64_t>(std::ceil(q * static_cast<double>(count_))));
  std::uint64_t seen = 0;
  for (std::size_t i = 0; i < kBuckets; ++i) {
    seen += buckets_[i];
    if (seen >= rank) {
      const std::uint64_t upper =
          std::min<std::uint64_t>(bucket_upper(i), std::numeric_limits<std::int64_t>::max());
      return std::min(static_cast<std::int64_t>(upper), max_);
    }
  }
  return max_;
}

}  // namespace tstream
