// tstream-recv: receives framed batches from tstream, checks them, and reports
// throughput, sequence gaps, and latency (SPEC §3).
//
// stdout: "listening on HOST:PORT", one "stats ..." line per interval, then a
// "recv summary ..." line and one "recv source=..." line per source.
// Exit codes: 0 ok, 1 usage error, 2 cannot listen.

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include <spdlog/fmt/fmt.h>

#include "tstream/clock.hpp"
#include "tstream/framing.hpp"
#include "tstream/latency_histogram.hpp"
#include "tstream/receiver.hpp"
#include "tstream/stop_signal.hpp"

namespace {

struct Options {
  tstream::Receiver::Options receiver;
  double interval_s = 1.0;
  std::optional<double> duration_s;
};

constexpr std::string_view kUsage =
    "usage: tstream-recv [--port N] [--bind ADDR] [--max-frame-bytes N]\n"
    "                    [--interval SECONDS] [--duration SECONDS]\n"
    "  Defaults: --port 9000 --bind 127.0.0.1 --max-frame-bytes 1048576 --interval 1\n"
    "  --port 0 picks a free port (printed on the first line).\n";

// Parses a positive number; nullopt if `text` isn't one.
std::optional<double> parse_positive(const char* text) {
  char* end = nullptr;
  const double value = std::strtod(text, &end);
  if (end == text || *end != '\0' || !(value > 0)) {
    return std::nullopt;
  }
  return value;
}

// Parses an integer in [min, max]; nullopt otherwise.
std::optional<std::uint64_t> parse_uint(const char* text, std::uint64_t min, std::uint64_t max) {
  char* end = nullptr;
  const unsigned long long value = std::strtoull(text, &end, 10);
  if (end == text || *end != '\0' || value < min || value > max) {
    return std::nullopt;
  }
  return value;
}

// Parses the command line; nullopt on any malformed or unknown argument.
std::optional<Options> parse_args(int argc, char** argv) {
  Options options;
  for (int i = 1; i < argc; ++i) {
    const std::string_view arg = argv[i];
    if (i + 1 >= argc) {
      return std::nullopt;
    }
    const char* value = argv[++i];
    if (arg == "--port") {
      const auto port = parse_uint(value, 0, 65535);
      if (!port) {
        return std::nullopt;
      }
      options.receiver.port = static_cast<std::uint16_t>(*port);
    } else if (arg == "--bind") {
      options.receiver.bind_host = value;
    } else if (arg == "--max-frame-bytes") {
      const auto bytes = parse_uint(value, 1024, std::uint64_t{256} << 20);
      if (!bytes) {
        return std::nullopt;
      }
      options.receiver.max_frame_bytes = static_cast<std::size_t>(*bytes);
    } else if (arg == "--interval" || arg == "--duration") {
      const auto seconds = parse_positive(value);
      if (!seconds) {
        return std::nullopt;
      }
      (arg == "--interval" ? options.interval_s : options.duration_s.emplace()) = *seconds;
    } else {
      return std::nullopt;
    }
  }
  return options;
}

// "p50=.. p99=.. p999=.. max=.." in microseconds, or "unavailable".
std::string latency_text(const tstream::LatencyHistogram& h, bool available) {
  if (!available) {
    return "unavailable";
  }
  if (h.count() == 0) {
    return "none";
  }
  constexpr double kNsPerUs = 1000.0;
  return fmt::format("p50={:.1f} p99={:.1f} p999={:.1f} max={:.1f}",
                     static_cast<double>(h.percentile(0.50)) / kNsPerUs,
                     static_cast<double>(h.percentile(0.99)) / kNsPerUs,
                     static_cast<double>(h.percentile(0.999)) / kNsPerUs,
                     static_cast<double>(h.max()) / kNsPerUs);
}

// One line per interval: rates and this interval's latency.
void print_interval(const tstream::Receiver& receiver, double elapsed_s,
                    std::uint64_t samples_delta, double interval_s,
                    const tstream::LatencyHistogram& e2e,
                    const tstream::LatencyHistogram& pipeline) {
  const auto& s = receiver.stats();
  fmt::print(
      "stats t={:.1f}s samples={} rate={:.0f}/s batches={} gaps={} out_of_order={} "
      "frame_errors={} decode_errors={} e2e_us {} pipeline_us {}\n",
      elapsed_s, s.samples, static_cast<double>(samples_delta) / interval_s, s.batches,
      receiver.seq().total_gaps(), receiver.seq().total_out_of_order(), s.total_frame_errors(),
      s.decode_errors, latency_text(e2e, receiver.latency_available()),
      latency_text(pipeline, receiver.latency_available()));
  std::fflush(stdout);
}

// Final totals since start, then one line per source.
void print_summary(const tstream::Receiver& receiver) {
  const auto& s = receiver.stats();
  std::string frame_errors;
  for (std::size_t i = 0; i < s.frame_errors.size(); ++i) {
    frame_errors += fmt::format(
        " {}={}", tstream::framing::frame_error_name(static_cast<tstream::framing::FrameError>(i)),
        s.frame_errors[i]);
  }
  fmt::print(
      "recv summary samples={} batches={} bytes={} gaps={} out_of_order={} batch_seq_gaps={} "
      "frame_errors={} ({} ) decode_errors={} invalid_samples={} connections={} rejected={} "
      "e2e_us {} pipeline_us {}\n",
      s.samples, s.batches, s.bytes, receiver.seq().total_gaps(),
      receiver.seq().total_out_of_order(), s.batch_seq_gaps, s.total_frame_errors(), frame_errors,
      s.decode_errors, s.invalid_samples, s.connections, s.rejected_connections,
      latency_text(receiver.e2e_latency(), receiver.latency_available()),
      latency_text(receiver.pipeline_latency(), receiver.latency_available()));
  for (const auto& [id, src] : receiver.seq().sources()) {
    fmt::print("recv source={} samples={} gaps={} out_of_order={}\n", id, src.received, src.gaps,
               src.out_of_order);
  }
  std::fflush(stdout);
}

// Polls until a stop signal or the duration ends, printing a line per interval.
void run(tstream::Receiver& receiver, const Options& options) {
  const auto start = tstream::Clock::now();
  auto next_report = start + std::chrono::duration_cast<tstream::Clock::duration>(
                                 std::chrono::duration<double>(options.interval_s));
  std::uint64_t last_samples = 0;
  tstream::LatencyHistogram e2e;
  tstream::LatencyHistogram pipeline;

  while (!tstream::stop_requested()) {
    receiver.poll(20);
    const auto now = tstream::Clock::now();
    const double elapsed_s = std::chrono::duration<double>(now - start).count();
    if (now >= next_report) {
      receiver.take_interval(e2e, pipeline);
      print_interval(receiver, elapsed_s, receiver.stats().samples - last_samples,
                     options.interval_s, e2e, pipeline);
      last_samples = receiver.stats().samples;
      next_report += std::chrono::duration_cast<tstream::Clock::duration>(
          std::chrono::duration<double>(options.interval_s));
    }
    if (options.duration_s && elapsed_s >= *options.duration_s) {
      break;
    }
  }
}

}  // namespace

// Parses arguments, listens, runs until stopped, and prints the summary.
int main(int argc, char** argv) {
  const auto options = parse_args(argc, argv);
  if (!options) {
    std::fwrite(kUsage.data(), 1, kUsage.size(), stderr);
    return 1;
  }
  tstream::install_stop_signal_handlers();

  try {
    tstream::Receiver receiver(options->receiver);
    fmt::print("listening on {}:{}\n", options->receiver.bind_host, receiver.port());
    std::fflush(stdout);
    run(receiver, *options);
    print_summary(receiver);
    return 0;
  } catch (const std::exception& e) {
    fmt::print(stderr, "[tstream-recv] [error] {}\n", e.what());
    return 2;
  }
}
