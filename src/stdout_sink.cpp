#include <algorithm>
#include <array>
#include <cstddef>
#include <limits>

#include <spdlog/fmt/fmt.h>

#include "tstream/sink.hpp"

namespace tstream {

void StdoutSink::send(const BatchView& batch) {
  std::array<std::size_t, 5> per_kind{};  // indexed by SampleKind
  std::int64_t oldest_capture_ns = std::numeric_limits<std::int64_t>::max();
  for (const Sample& s : batch.samples) {
    ++per_kind[static_cast<std::size_t>(s.kind)];
    oldest_capture_ns = std::min(oldest_capture_ns, s.t_capture_ns);
  }
  const std::int64_t max_age_us =
      batch.samples.empty() ? 0 : (batch.t_flush_ns - oldest_capture_ns) / 1000;

  std::array<char, 256> line{};
  const auto result = fmt::format_to_n(
      line.data(), line.size(),
      "batch {}: {} samples (imu {}, gps {}, battery {}, can {}, health {}) max_age_us={}\n",
      batch.batch_seq, batch.samples.size(), per_kind[0], per_kind[1], per_kind[2], per_kind[3],
      per_kind[4], max_age_us);
  std::fwrite(line.data(), 1, std::min(result.size, line.size()), out_);
}

void StdoutSink::close() { std::fflush(out_); }

}  // namespace tstream
