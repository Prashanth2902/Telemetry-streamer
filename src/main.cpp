// tstream: the sender (SPEC §3). Simulated sources -> rings -> pipeline ->
// TcpTransport (framed Protobuf to tstream-recv), or stdout with --sink stdout.
//
// Exit codes: 0 ok, 1 usage error, 2 config error, 3 samples unaccounted for.

#include <atomic>
#include <chrono>
#include <csignal>
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
#include "tstream/pipeline.hpp"
#include "tstream/sample_ring.hpp"
#include "tstream/sink.hpp"
#include "tstream/source.hpp"
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

// Set from the signal handler; std::atomic<bool> is lock-free, so storing to
// it is async-signal-safe.
std::atomic<bool> g_stop_requested{false};
static_assert(std::atomic<bool>::is_always_lock_free);

// SIGINT/SIGTERM handler: only sets the flag, which is async-signal-safe.
extern "C" void handle_stop_signal(int /*signal*/) {
  g_stop_requested.store(true, std::memory_order_relaxed);
}

enum class SinkKind : std::uint8_t { Tcp, Stdout };

struct Options {
  std::string config_path;
  std::optional<double> duration_s;
  SinkKind sink = SinkKind::Tcp;
};

constexpr std::string_view kUsage =
    "usage: tstream --config <path> [--duration <seconds>] [--sink tcp|stdout]\n"
    "  Runs until SIGINT/SIGTERM, or for --duration seconds.\n"
    "  --sink tcp (default) sends to transport.host:port; stdout prints one line per batch.\n";

// Parses --config, --duration, and --sink; nullopt on any malformed or unknown argument.
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
      char* end = nullptr;
      const double seconds = std::strtod(argv[++i], &end);
      if (end == nullptr || *end != '\0' || !(seconds > 0)) {
        return std::nullopt;
      }
      options.duration_s = seconds;
    } else {
      return std::nullopt;
    }
  }
  if (options.config_path.empty()) {
    return std::nullopt;
  }
  return options;
}

// Blocks until a stop signal arrives or duration_s has elapsed.
void wait_for_stop(std::optional<double> duration_s) {
  const auto start = tstream::Clock::now();
  while (!g_stop_requested.load(std::memory_order_relaxed)) {
    if (duration_s &&
        std::chrono::duration<double>(tstream::Clock::now() - start).count() >= *duration_s) {
      return;
    }
    std::this_thread::sleep_for(10ms);
  }
}

// Prints one line per source and a total, and returns the number of samples
// that are neither delivered nor counted as dropped. Must be 0.
std::uint64_t print_summary(const std::vector<std::unique_ptr<tstream::Source>>& sources,
                            const tstream::Pipeline& pipeline) {
  std::uint64_t total_generated = 0;
  std::uint64_t total_dropped = 0;
  std::uint64_t total_delivered = 0;
  for (std::size_t i = 0; i < sources.size(); ++i) {
    const auto counters = sources[i]->counters();
    const std::uint64_t delivered = pipeline.delivered(i);
    const std::uint64_t lost = counters.generated - counters.dropped - delivered;
    fmt::print(
        "summary source={} type={} generated={} dropped={} delivered={} lost={} overruns={}\n",
        sources[i]->id(), sources[i]->name(), counters.generated, counters.dropped, delivered, lost,
        counters.overruns);
    total_generated += counters.generated;
    total_dropped += counters.dropped;
    total_delivered += delivered;
  }
  const std::uint64_t total_lost = total_generated - total_dropped - total_delivered;
  fmt::print("summary total generated={} dropped={} delivered={} lost={} batches={}\n",
             total_generated, total_dropped, total_delivered, total_lost, pipeline.batches());
  std::fflush(stdout);
  return total_lost;
}

// One line of transport counters. Batches the transport dropped were already
// delivered to the sink, so they appear here rather than as `lost`.
void print_transport_summary(const tstream::TransportCounters& c) {
  fmt::print(
      "summary transport batches_sent={} samples_sent={} bytes_sent={} batches_dropped={} "
      "samples_dropped={} connects={} disconnects={}\n",
      c.batches_sent, c.samples_sent, c.bytes_sent, c.batches_dropped, c.samples_dropped,
      c.connects, c.disconnects);
  std::fflush(stdout);
}

// Builds everything from the config, runs it, shuts down in order; returns the exit code.
int run(const Options& options) {
  const tstream::Config config = tstream::load_config(options.config_path);

  std::vector<std::unique_ptr<tstream::SampleRing>> rings;
  std::vector<std::unique_ptr<tstream::Source>> sources;
  std::vector<tstream::SampleRing*> ring_ptrs;
  for (const auto& source_config : config.sources) {
    if (source_config.policy != tstream::BackpressurePolicy::DropNewest) {
      log_line("warn", "source {}: policy {} is enforced from M4; using drop_newest for now",
               source_config.id, tstream::policy_name(source_config.policy));
    }
    rings.push_back(tstream::make_sample_ring(source_config.ring_capacity));
    ring_ptrs.push_back(rings.back().get());
    sources.push_back(tstream::make_source(source_config, *rings.back(), config.seed));
  }

  std::unique_ptr<tstream::BatchSink> sink;
  tstream::TcpTransport* transport = nullptr;  // owned by `sink` when set
  if (options.sink == SinkKind::Tcp) {
    tstream::TcpTransport::Options transport_options;
    transport_options.host = config.transport.host;
    transport_options.port = config.transport.port;
    transport_options.max_samples = config.batch.max_samples;
    auto tcp = std::make_unique<tstream::TcpTransport>(transport_options);
    transport = tcp.get();
    sink = std::move(tcp);
    log_line("info", "sending to {}:{}", config.transport.host, config.transport.port);
  } else {
    sink = std::make_unique<tstream::StdoutSink>();
  }
  tstream::Pipeline pipeline(ring_ptrs, config.batch, *sink);

  log_line("info", "starting {} sources (batch max_samples={} max_delay_us={})", sources.size(),
           config.batch.max_samples, config.batch.max_delay_us);
  pipeline.start();
  for (auto& source : sources) {
    source->start();
  }

  wait_for_stop(options.duration_s);

  // Shutdown order (SPEC §3.2): stop producing, then drain and flush, then
  // close the sink.
  log_line("info", "stopping");
  for (auto& source : sources) {
    source->stop();
  }
  pipeline.stop();
  sink->close();

  const std::uint64_t lost = print_summary(sources, pipeline);
  if (transport != nullptr) {
    print_transport_summary(transport->counters());
  }
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

  std::signal(SIGINT, handle_stop_signal);
  std::signal(SIGTERM, handle_stop_signal);

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
