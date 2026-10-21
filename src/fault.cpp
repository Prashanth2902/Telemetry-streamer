#include "tstream/fault.hpp"

#include <charconv>
#include <cmath>
#include <stdexcept>
#include <string>
#include <system_error>

namespace tstream {

namespace {

constexpr std::int64_t kNsPerMs = 1'000'000;
constexpr double kMaxMs = 3'600'000;  // an hour: longer is surely a typo

// Throws std::invalid_argument as "fault \"<text>\": <what>".
[[noreturn]] void fail(std::string_view text, const std::string& what) {
  throw std::invalid_argument("fault \"" + std::string(text) + "\": " + what);
}

// A finite number in [min, max] spanning all of `field`.
double parse_number(std::string_view text, std::string_view field, const char* name, double min,
                    double max) {
  double value = 0;
  const auto [end, ec] = std::from_chars(field.data(), field.data() + field.size(), value);
  if (ec != std::errc{} || end != field.data() + field.size() || !std::isfinite(value) ||
      value < min || value > max) {
    fail(text, std::string(name) + " must be a number in [" + std::to_string(min) + ", " +
                   std::to_string(max) + "], got \"" + std::string(field) + "\"");
  }
  return value;
}

// Milliseconds to whole nanoseconds.
std::int64_t ms_to_ns(double ms) {
  return std::llround(ms * static_cast<double>(kNsPerMs));
}

// Splits off everything before the first `sep`; `rest` keeps what follows.
// Without a `sep`, returns all of `rest` and leaves it empty.
std::string_view take_until(std::string_view& rest, char sep) {
  const std::size_t at = rest.find(sep);
  const std::string_view head = rest.substr(0, at);
  rest = at == std::string_view::npos ? std::string_view{} : rest.substr(at + 1);
  return head;
}

// Kind name to FaultKind.
FaultKind parse_kind(std::string_view text, std::string_view name) {
  for (const FaultKind kind : {FaultKind::Stall, FaultKind::Burst, FaultKind::Rate,
                               FaultKind::Freeze, FaultKind::SlowConsumer}) {
    if (name == fault_kind_name(kind)) {
      return kind;
    }
  }
  fail(text, "unknown kind \"" + std::string(name) +
                 "\" (expected stall, burst, rate, freeze, or slow_consumer)");
}

}  // namespace

// Stalls end after arg ms; rate and freeze after their optional duration.
std::optional<std::int64_t> Fault::end_ns() const {
  if (kind == FaultKind::Stall) {
    return start_ns + ms_to_ns(arg);
  }
  if (duration_ns) {
    return start_ns + *duration_ns;
  }
  return std::nullopt;
}

// Half-open interval [start, end).
bool Fault::active_at(std::int64_t elapsed_ns) const {
  if (kind == FaultKind::Burst || kind == FaultKind::SlowConsumer || elapsed_ns < start_ns) {
    return false;
  }
  const auto end = end_ns();
  return !end || elapsed_ns < *end;
}

// kind:source:arg, then the optional @start and +duration suffixes.
Fault parse_fault(std::string_view text) {
  std::string_view timing = text;
  std::string_view body = take_until(timing, '@');
  const bool has_timing = body.size() != text.size();

  std::string_view rest = body;
  Fault fault;
  fault.kind = parse_kind(text, take_until(rest, ':'));

  if (fault.kind == FaultKind::SlowConsumer) {
    if (rest.empty() || has_timing) {
      fail(text, "expected slow_consumer:<ms>");
    }
    fault.arg = parse_number(text, rest, "ms", 1, 60'000);
    return fault;
  }

  const double source = parse_number(text, take_until(rest, ':'), "source id", 1, 65535);
  if (std::floor(source) != source) {
    fail(text, "source id must be an integer");
  }
  fault.source_id = static_cast<std::uint16_t>(source);

  switch (fault.kind) {
    case FaultKind::Stall:
      fault.arg = parse_number(text, rest, "stall ms", 1, kMaxMs);
      break;
    case FaultKind::Burst:
      fault.arg = parse_number(text, rest, "burst count", 1, 100'000);
      if (std::floor(fault.arg) != fault.arg) {
        fail(text, "burst count must be an integer");
      }
      break;
    case FaultKind::Rate:
      fault.arg = parse_number(text, rest, "rate factor", 0.01, 100);
      break;
    case FaultKind::Freeze:
      if (!rest.empty()) {
        fail(text, "freeze takes no argument: freeze:<source>[@start_ms[+dur_ms]]");
      }
      break;
    case FaultKind::SlowConsumer:
      break;  // handled above
  }

  if (has_timing) {
    std::string_view duration = timing;
    const std::string_view start = take_until(duration, '+');
    fault.start_ns = ms_to_ns(parse_number(text, start, "start ms", 0, kMaxMs));
    if (start.size() != timing.size()) {  // there was a '+'
      if (fault.kind != FaultKind::Rate && fault.kind != FaultKind::Freeze) {
        fail(text, "+dur_ms only applies to rate and freeze");
      }
      fault.duration_ns = ms_to_ns(parse_number(text, duration, "duration ms", 1, kMaxMs));
    }
  }
  return fault;
}

// Inverse of parse_kind.
std::string_view fault_kind_name(FaultKind kind) {
  switch (kind) {
    case FaultKind::Stall:
      return "stall";
    case FaultKind::Burst:
      return "burst";
    case FaultKind::Rate:
      return "rate";
    case FaultKind::Freeze:
      return "freeze";
    case FaultKind::SlowConsumer:
      return "slow_consumer";
  }
  return "unknown";
}

}  // namespace tstream
