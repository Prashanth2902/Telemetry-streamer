#include "tstream/stats.hpp"

#include <algorithm>

#include <spdlog/fmt/fmt.h>

namespace tstream {

// Field-by-field sum; overruns and blocked are summed too, for the total line.
SourceStats SenderStats::totals() const {
  SourceStats total;
  for (const SourceStats& s : sources) {
    total.counters.generated += s.counters.generated;
    total.counters.dropped_newest += s.counters.dropped_newest;
    total.counters.dropped_timeout += s.counters.dropped_timeout;
    total.counters.blocked += s.counters.blocked;
    total.counters.overruns += s.counters.overruns;
    total.delivered += s.delivered;
    total.tx_sent += s.tx_sent;
    total.tx_dropped += s.tx_dropped;
  }
  return total;
}

// Sources and pipeline rings share an index: rings[i] belongs to sources[i].
SenderStats collect_stats(double t_s, const std::vector<std::unique_ptr<Source>>& sources,
                          const Pipeline& pipeline, const TcpTransport* transport) {
  SenderStats stats;
  stats.t_s = t_s;
  stats.batches = pipeline.batches();
  stats.sources.reserve(sources.size());
  for (std::size_t i = 0; i < sources.size(); ++i) {
    SourceStats s;
    s.id = sources[i]->id();
    s.name = std::string(sources[i]->name());
    s.counters = sources[i]->counters();
    s.delivered = pipeline.delivered(i);
    if (transport != nullptr) {
      s.tx_sent = transport->samples_sent_for(s.id);
      s.tx_dropped = transport->samples_dropped_for(s.id);
    }
    stats.sources.push_back(std::move(s));
  }
  if (transport != nullptr) {
    stats.transport = transport->counters();
  }
  return stats;
}

// Saturating, so a mid-run snapshot (counters read at slightly different
// moments) can't wrap around to a huge number.
std::uint64_t lost(const SourceStats& s, bool has_transport) {
  const std::uint64_t accounted =
      s.counters.dropped() + (has_transport ? s.tx_sent + s.tx_dropped : s.delivered);
  return s.counters.generated > accounted ? s.counters.generated - accounted : 0;
}

// Per source, then summed.
std::uint64_t total_lost(const SenderStats& stats) {
  std::uint64_t total = 0;
  for (const SourceStats& s : stats.sources) {
    total += lost(s, stats.transport.has_value());
  }
  return total;
}

// Deltas use the previous snapshot; the first line's "previous" is all zeros.
std::string format_stats_line(const SenderStats& now, const SenderStats& previous) {
  const SourceStats total = now.totals();
  const SourceStats before = previous.totals();
  const double interval_s = std::max(now.t_s - previous.t_s, 1e-9);
  std::string line = fmt::format(
      "stats t={:.1f}s generated={} rate={:.0f}/s ring_dropped={} delivered={} batches={}", now.t_s,
      total.counters.generated,
      static_cast<double>(total.counters.generated - before.counters.generated) / interval_s,
      total.counters.dropped(), total.delivered, now.batches);
  if (now.transport) {
    const TransportCounters& t = *now.transport;
    line +=
        fmt::format(" tx_sent={} tx_dropped={} queued={} queue_high_water={} connects={}",
                    t.samples_sent, t.samples_dropped, t.queued, t.queue_high_water, t.connects);
  }
  return line;
}

// Key=value pairs throughout, so scripts and tests can parse any line.
std::vector<std::string> format_summary(const SenderStats& final_stats) {
  const bool tcp = final_stats.transport.has_value();
  std::vector<std::string> lines;
  for (const SourceStats& s : final_stats.sources) {
    lines.push_back(fmt::format(
        "summary source={} type={} generated={} dropped_newest={} dropped_timeout={} blocked={} "
        "delivered={} tx_sent={} tx_dropped={} lost={} overruns={}",
        s.id, s.name, s.counters.generated, s.counters.dropped_newest, s.counters.dropped_timeout,
        s.counters.blocked, s.delivered, s.tx_sent, s.tx_dropped, lost(s, tcp),
        s.counters.overruns));
  }
  const SourceStats total = final_stats.totals();
  lines.push_back(fmt::format(
      "summary total generated={} dropped={} delivered={} tx_sent={} tx_dropped={} lost={} "
      "batches={}",
      total.counters.generated, total.counters.dropped(), total.delivered, total.tx_sent,
      total.tx_dropped, total_lost(final_stats), final_stats.batches));
  if (tcp) {
    const TransportCounters& t = *final_stats.transport;
    lines.push_back(fmt::format(
        "summary transport batches_sent={} samples_sent={} bytes_sent={} batches_dropped={} "
        "samples_dropped={} queue_high_water={} connects={} disconnects={}",
        t.batches_sent, t.samples_sent, t.bytes_sent, t.batches_dropped, t.samples_dropped,
        t.queue_high_water, t.connects, t.disconnects));
  }
  return lines;
}

}  // namespace tstream
