// Per-source sequence tracking on the receiver (SPEC §4.1, §13).
//
// Every sample a source creates consumes one seq, including samples dropped
// before they reach the wire, so a jump in seq is exactly the number of
// samples lost upstream. Comparisons use unsigned 32-bit wraparound, so a long
// run that wraps seq past 2^32 is still tracked correctly.

#pragma once

#include <cstdint>
#include <map>

namespace tstream {

class SeqTracker {
 public:
  struct Source {
    std::uint64_t received = 0;
    std::uint64_t gaps = 0;          // samples missing between received ones
    std::uint64_t out_of_order = 0;  // duplicates or late arrivals
    std::uint32_t expected = 0;      // next seq if nothing is lost
  };

  // Records one sample. The first sample from a source isn't a gap: the
  // receiver may have connected after the source started.
  void on_sample(std::uint16_t source_id, std::uint32_t seq) {
    auto [it, first] = sources_.try_emplace(source_id);
    Source& s = it->second;
    ++s.received;
    if (first) {
      s.expected = seq + 1;
      return;
    }
    const std::uint32_t ahead = seq - s.expected;  // wraps mod 2^32
    if (ahead == 0) {
      ++s.expected;
    } else if (ahead < 0x8000'0000U) {
      s.gaps += ahead;
      s.expected = seq + 1;
    } else {
      ++s.out_of_order;  // seq is behind what we expected
    }
  }

  // Per-source state, ordered by source id.
  [[nodiscard]] const std::map<std::uint16_t, Source>& sources() const { return sources_; }

  // Sum of gaps over all sources.
  [[nodiscard]] std::uint64_t total_gaps() const {
    std::uint64_t total = 0;
    for (const auto& [id, s] : sources_) {
      total += s.gaps;
    }
    return total;
  }

  // Sum of out-of-order samples over all sources.
  [[nodiscard]] std::uint64_t total_out_of_order() const {
    std::uint64_t total = 0;
    for (const auto& [id, s] : sources_) {
      total += s.out_of_order;
    }
    return total;
  }

 private:
  std::map<std::uint16_t, Source> sources_;
};

}  // namespace tstream
