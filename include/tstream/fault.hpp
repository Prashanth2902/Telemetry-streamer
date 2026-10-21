// Fault injection (SPEC §8): faults that make health monitors and drop
// policies fire on demand.
//
// One text syntax everywhere, for `--fault` and the config's "faults" array:
//
//   stall:<source>:<ms>[@start_ms]              stop producing for ms
//   burst:<source>:<n>[@start_ms]               emit n (<= 100000) extra samples at once
//   rate:<source>:<factor>[@start_ms[+dur_ms]]  multiply the rate by factor
//   freeze:<source>[@start_ms[+dur_ms]]         repeat the last payload
//   slow_consumer:<ms>                          tstream-recv only: sleep between reads
//
// start_ms counts from the source's start (default 0). Without +dur_ms, rate
// and freeze last until shutdown. The sender runs each source's faults on its
// own producer thread from a plan fixed before start().

#pragma once

#include <cstdint>
#include <optional>
#include <string_view>

namespace tstream {

enum class FaultKind : std::uint8_t { Stall, Burst, Rate, Freeze, SlowConsumer };

struct Fault {
  FaultKind kind = FaultKind::Stall;
  std::uint16_t source_id = 0;              // 0 for slow_consumer
  double arg = 0;                           // stall/slow_consumer: ms; burst: count; rate: factor
  std::int64_t start_ns = 0;                // after the source starts
  std::optional<std::int64_t> duration_ns;  // rate, freeze: nullopt = until shutdown

  // True while the fault is in effect at `elapsed_ns` after the source started.
  // A stall lasts arg ms; a burst is a single instant and is never "active".
  [[nodiscard]] bool active_at(std::int64_t elapsed_ns) const;
  // When the fault stops being active, or nullopt if it never does.
  [[nodiscard]] std::optional<std::int64_t> end_ns() const;
};

// Parses the syntax above. Throws std::invalid_argument naming what's wrong.
Fault parse_fault(std::string_view text);

// The syntax's spelling of a kind, e.g. "slow_consumer".
std::string_view fault_kind_name(FaultKind kind);

}  // namespace tstream
