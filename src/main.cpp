// tstream: the sender (SPEC §3). M2 wiring: simulated sources -> rings ->
// pipeline -> stdout. M3 swaps the sink for Protobuf over TCP.
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
#include <vector>

#include <spdlog/fmt/fmt.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include "tstream/clock.hpp"
#include "tstream/config.hpp"
#include "tstream/pipeline.hpp"
#include "tstream/sample_ring.hpp"
#include "tstream/sink.hpp"
#include "tstream/source.hpp"

namespace {

using namespace std::chrono_literals;

// Set from the signal handler; std::atomic<bool> is lock-free, so storing to
// it is async-signal-safe.
std::atomic<bool> g_stop_requested{false};
static_assert(std::atomic<bool>::is_always_lock_free);

extern "C" void handle_stop_signal(int /*signal*/) {
  g_stop_requested.store(true, std::memory_order_relaxed);
}

struct Options {
  std::string config_path;
  std::optional<double> duration_s;
};

constexpr std::string_view kUsage =
    "usage: tstream --config <path> [--duration <seconds>]\n"
    "  Runs until SIGINT/SIGTERM, or for --duration seconds.\n";

std::optional<Options> parse_args(int argc, char** argv) {
  Options options;
  for (int i = 1; i < argc; ++i) {
    const std::string_view arg = argv[i];
    const bool has_value = i + 1 < argc;
    if (arg == "--config" && has_value) {
      options.config_path = argv[++i];
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

void wait_for_stop(std::optional<double> duration_s) {
  const auto start = tstream::Clock::now();
  while (!g_stop_requested.load(std::memory_order_relaxed)) {
    if (duration_s && std::chrono::duration<double>(tstream::Clock::now() - start).count() >=
                          *duration_s) {
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
        sources[i]->id(), sources[i]->name(), counters.generated, counters.dropped, delivered,
        lost, counters.overruns);
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

int run(const Options& options) {
  const tstream::Config config = tstream::load_config(options.config_path);

  std::vector<std::unique_ptr<tstream::SampleRing>> rings;
  std::vector<std::unique_ptr<tstream::Source>> sources;
  std::vector<tstream::SampleRing*> ring_ptrs;
  for (const auto& source_config : config.sources) {
    if (source_config.policy != tstream::BackpressurePolicy::DropNewest) {
      spdlog::warn("source {}: policy {} is enforced from M4; using drop_newest for now",
                   source_config.id, tstream::policy_name(source_config.policy));
    }
    rings.push_back(tstream::make_sample_ring(source_config.ring_capacity));
    ring_ptrs.push_back(rings.back().get());
    sources.push_back(tstream::make_source(source_config, *rings.back(), config.seed));
  }

  tstream::StdoutSink sink;
  tstream::Pipeline pipeline(ring_ptrs, config.batch, sink);

  spdlog::info("starting {} sources (batch max_samples={} max_delay_us={})", sources.size(),
               config.batch.max_samples, config.batch.max_delay_us);
  pipeline.start();
  for (auto& source : sources) {
    source->start();
  }

  wait_for_stop(options.duration_s);

  // Shutdown order (SPEC §3.2): stop producing, then drain and flush, then
  // close the sink.
  spdlog::info("stopping");
  for (auto& source : sources) {
    source->stop();
  }
  pipeline.stop();
  sink.close();

  const std::uint64_t lost = print_summary(sources, pipeline);
  if (lost != 0) {
    spdlog::error("{} samples unaccounted for", lost);
    return 3;
  }
  spdlog::info("stopped cleanly");
  return 0;
}

}  // namespace

int main(int argc, char** argv) {
  // Logs go to stderr so stdout carries only batches and the summary.
  spdlog::set_default_logger(spdlog::stderr_color_mt("tstream"));

  const auto options = parse_args(argc, argv);
  if (!options) {
    std::fputs(kUsage.data(), stderr);
    return 1;
  }

  std::signal(SIGINT, handle_stop_signal);
  std::signal(SIGTERM, handle_stop_signal);

  try {
    return run(*options);
  } catch (const tstream::ConfigError& e) {
    spdlog::error("config error: {}", e.what());
    return 2;
  } catch (const std::exception& e) {
    spdlog::error("fatal: {}", e.what());
    return 1;
  }
}
