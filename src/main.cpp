// tstream: the sender (SPEC §3). Simulated sources -> rings -> pipeline ->
// TcpTransport (framed Protobuf to tstream-recv), or stdout with --sink stdout.
//
// Exit codes: 0 ok, 1 usage error, 2 config error, 3 samples unaccounted for.

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <spdlog/fmt/fmt.h>

#include "tstream/clock.hpp"
#include "tstream/config.hpp"
#include "tstream/fault.hpp"
#include "tstream/health.hpp"
#include "tstream/monitors.hpp"
#include "tstream/pipeline.hpp"
#include "tstream/sample_ring.hpp"
#include "tstream/sink.hpp"
#include "tstream/source.hpp"
#include "tstream/stats.hpp"
#include "tstream/stop_signal.hpp"
#include "tstream/tcp_transport.hpp"

namespace {

using namespace std::chrono_literals;

// Logs go to stderr so stdout carries only batches and the summary. Plain fmt
// rather than spdlog: Ubuntu 26.04's spdlog 1.15.3 + fmt 10.1.1 headers don't
// compile under clang 21 in C++20 mode (ADR-014).
template <typename... Args>
void log_line(std::string_view level, fmt::format_string<Args...> format, Args&&... args) {
  fmt::print(stderr, "[tstream] [{}] {}\n", level,
             fmt::format(format, std::forward<Args>(args)...));
}

enum class SinkKind : std::uint8_t { Tcp, Stdout };

struct Options {
  std::string config_path;
  std::optional<double> duration_s;
  SinkKind sink = SinkKind::Tcp;
  double stats_interval_s = 1.0;    // 0 turns the periodic stats line off
  std::vector<std::string> faults;  // --fault, added to the config's faults
};

constexpr std::string_view kUsage =
    "usage: tstream --config <path> [--duration <seconds>] [--sink tcp|stdout]\n"
    "               [--stats-interval <seconds>] [--fault <spec>]...\n"
    "  Runs until SIGINT/SIGTERM, or for --duration seconds.\n"
    "  --sink tcp (default) sends to transport.host:port; stdout prints one line per batch.\n"
    "  --stats-interval (default 1) prints a stats line that often; 0 turns it off.\n"
    "  --fault kind:source:arg[@start_ms[+dur_ms]] injects a fault (README):\n"
    "    stall:<id>:<ms>  burst:<id>:<n>  rate:<id>:<factor>  freeze:<id>\n";

// A non-negative number of seconds; nullopt for anything else, including "".
std::optional<double> parse_seconds(const char* text) {
  char* end = nullptr;
  const double seconds = std::strtod(text, &end);
  if (end == text || *end != '\0' || !(seconds >= 0)) {
    return std::nullopt;
  }
  return seconds;
}

// Parses the options above; nullopt on any malformed or unknown argument.
std::optional<Options> parse_args(int argc, char** argv) {
  Options options;
  for (int i = 1; i < argc; ++i) {
    const std::string_view arg = argv[i];
    const bool has_value = i + 1 < argc;
    if (arg == "--config" && has_value) {
      options.config_path = argv[++i];
    } else if (arg == "--sink" && has_value) {
      const std::string_view kind = argv[++i];
      if (kind == "tcp") {
        options.sink = SinkKind::Tcp;
      } else if (kind == "stdout") {
        options.sink = SinkKind::Stdout;
      } else {
        return std::nullopt;
      }
    } else if (arg == "--duration" && has_value) {
      const auto seconds = parse_seconds(argv[++i]);
      if (!seconds || *seconds == 0) {
        return std::nullopt;
      }
      options.duration_s = seconds;
    } else if (arg == "--fault" && has_value) {
      options.faults.emplace_back(argv[++i]);
    } else if (arg == "--stats-interval" && has_value) {
      const auto seconds = parse_seconds(argv[++i]);
      if (!seconds) {
        return std::nullopt;
      }
      options.stats_interval_s = *seconds;
    } else {
      return std::nullopt;
    }
  }
  if (options.config_path.empty()) {
    return std::nullopt;
  }
  return options;
}

// Blocks until a stop signal arrives or duration_s has elapsed, printing a
// stats line every stats_interval_s along the way.
void wait_for_stop(const Options& options,
                   const std::vector<std::unique_ptr<tstream::Source>>& sources,
                   const tstream::Pipeline& pipeline, const tstream::TcpTransport* transport) {
  const auto start = tstream::Clock::now();
  tstream::SenderStats previous;
  double next_report_s = options.stats_interval_s;
  while (!tstream::stop_requested()) {
    const double elapsed_s = std::chrono::duration<double>(tstream::Clock::now() - start).count();
    if (options.stats_interval_s > 0 && elapsed_s >= next_report_s) {
      tstream::SenderStats now = tstream::collect_stats(elapsed_s, sources, pipeline, transport);
      fmt::print("{}\n", tstream::format_stats_line(now, previous));
      std::fflush(stdout);
      previous = std::move(now);
      next_report_s += options.stats_interval_s;
    }
    if (options.duration_s && elapsed_s >= *options.duration_s) {
      return;
    }
    std::this_thread::sleep_for(10ms);
  }
}

// Builds everything from the config, runs it, shuts down in order; returns the exit code.
int run(const Options& options) {
  tstream::Config config = tstream::load_config(options.config_path);
  for (const std::string& fault : options.faults) {
    tstream::add_fault(config, fault);
  }

  std::vector<std::unique_ptr<tstream::SampleRing>> rings;
  std::vector<std::unique_ptr<tstream::Source>> sources;
  std::vector<tstream::SampleRing*> ring_ptrs;
  for (const auto& source_config : config.sources) {
    rings.push_back(tstream::make_sample_ring(source_config.ring_capacity));
    ring_ptrs.push_back(rings.back().get());
    sources.push_back(
        tstream::make_source(source_config, *rings.back(), config.seed, config.faults));
  }

  std::unique_ptr<tstream::BatchSink> sink;
  tstream::TcpTransport* transport = nullptr;  // owned by `sink` when set
  if (options.sink == SinkKind::Tcp) {
    tstream::TcpTransport::Options transport_options;
    transport_options.host = config.transport.host;
    transport_options.port = config.transport.port;
    transport_options.max_samples = config.batch.max_samples;
    transport_options.max_pending_batches = config.transport.max_pending_batches;
    transport_options.socket_send_buffer_bytes =
        static_cast<int>(config.transport.socket_send_buffer_bytes);
    for (const auto& source_config : config.sources) {
      transport_options.source_ids.push_back(source_config.id);
    }
    auto tcp = std::make_unique<tstream::TcpTransport>(transport_options);
    transport = tcp.get();
    sink = std::move(tcp);
    log_line("info", "sending to {}:{}", config.transport.host, config.transport.port);
  } else {
    sink = std::make_unique<tstream::StdoutSink>();
  }
  // Monitors start their clocks now, just before the sources do, so a source
  // that never produces still goes stale.
  const std::vector<tstream::SourceHealthThresholds> thresholds =
      tstream::health_thresholds(config);
  const std::int64_t start_ns = tstream::now_ns();
  tstream::StaleMonitor stale(thresholds, start_ns);
  tstream::RateMonitor rate(thresholds, start_ns);
  tstream::SeqGapMonitor seq_gap(thresholds);
  tstream::FrozenMonitor frozen(thresholds);
  tstream::Pipeline pipeline(ring_ptrs, config.batch, *sink, {&stale, &rate, &seq_gap, &frozen});

  log_line("info", "starting {} sources (batch max_samples={} max_delay_us={})", sources.size(),
           config.batch.max_samples, config.batch.max_delay_us);
  for (const tstream::Fault& fault : config.faults) {
    log_line("info", "fault {} on source {} at {} ms", tstream::fault_kind_name(fault.kind),
             fault.source_id, fault.start_ns / tstream::kNsPerMs);
  }
  pipeline.start();
  for (auto& source : sources) {
    source->start();
  }

  wait_for_stop(options, sources, pipeline, transport);

  // Shutdown order (SPEC §3.2): stop producing, then drain and flush, then
  // close the sink.
  log_line("info", "stopping");
  for (auto& source : sources) {
    source->stop();
  }
  pipeline.stop();
  sink->close();

  // Every counter is final now, so the accounting below is exact.
  const tstream::SenderStats final_stats = tstream::collect_stats(0, sources, pipeline, transport);
  for (const std::string& line : tstream::format_summary(final_stats)) {
    fmt::print("{}\n", line);
  }
  std::fflush(stdout);
  const std::uint64_t lost = tstream::total_lost(final_stats);
  if (lost != 0) {
    log_line("error", "{} samples unaccounted for", lost);
    return 3;
  }
  log_line("info", "stopped cleanly");
  return 0;
}

}  // namespace

// Parses arguments, installs signal handlers, and maps exceptions to exit codes.
int main(int argc, char** argv) {
  const auto options = parse_args(argc, argv);
  if (!options) {
    std::fwrite(kUsage.data(), 1, kUsage.size(), stderr);
    return 1;
  }

  tstream::install_stop_signal_handlers();

  try {
    return run(*options);
  } catch (const tstream::ConfigError& e) {
    log_line("error", "config error: {}", e.what());
    return 2;
  } catch (const std::exception& e) {
    log_line("error", "fatal: {}", e.what());
    return 1;
  }
}
