// The sender's stats registry (SPEC §3.1 Stats): one snapshot over every
// source, the pipeline, and the transport, built from their relaxed-atomic
// counters, plus the formatting for the periodic stats line and the summary.
//
// Reading counters from the main thread while the pipeline runs is safe: each
// is a relaxed atomic. A snapshot isn't one consistent instant across all of
// them, which is fine for a stats line; the summary is taken after shutdown,
// when every counter is final and the accounting is exact.

#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "tstream/pipeline.hpp"
#include "tstream/source.hpp"
#include "tstream/tcp_transport.hpp"

namespace tstream {

struct SourceStats {
  std::uint16_t id = 0;
  std::string name;
  SourceCounters counters;       // generated, ring drops by policy, blocked, overruns
  std::uint64_t delivered = 0;   // handed to the sink by the pipeline
  std::uint64_t tx_sent = 0;     // written to the socket (TCP sink only)
  std::uint64_t tx_dropped = 0;  // dropped by the transport (TCP sink only)
};

struct SenderStats {
  double t_s = 0;  // seconds since the sender started
  std::vector<SourceStats> sources;
  std::uint64_t batches = 0;                   // handed to the sink
  std::optional<TransportCounters> transport;  // set when the sink is TCP

  // Per-source counters summed over all sources.
  [[nodiscard]] SourceStats totals() const;
};

// Snapshots every component. `transport` may be null (stdout sink).
SenderStats collect_stats(double t_s, const std::vector<std::unique_ptr<Source>>& sources,
                          const Pipeline& pipeline, const TcpTransport* transport);

// Samples neither delivered nor counted as dropped anywhere. With a TCP sink
// every sample must end up sent or dropped by the transport; without one,
// delivered to the sink. Must be 0 once the sender has shut down.
std::uint64_t lost(const SourceStats& s, bool has_transport);

// Sum of lost() per source. Computed per source rather than from totals, so a
// surplus on one source can't hide a shortfall on another.
std::uint64_t total_lost(const SenderStats& stats);

// One "stats ..." line: totals plus rates over the interval since `previous`.
std::string format_stats_line(const SenderStats& now, const SenderStats& previous);

// The "summary ..." lines printed at exit: one per source, a total, and (TCP
// only) the transport's own counters.
std::vector<std::string> format_summary(const SenderStats& final_stats);

}  // namespace tstream
