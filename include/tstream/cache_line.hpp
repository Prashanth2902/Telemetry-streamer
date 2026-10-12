#pragma once

#include <cstddef>

namespace tstream {

// Cache-line size used to keep data written by different threads on separate
// lines. Fixed at 64 instead of std::hardware_destructive_interference_size,
// which GCC warns about in headers (-Winterference-size) and whose value can
// change with compiler flags. See docs/design-decisions.md ADR-003.
inline constexpr std::size_t kCacheLine = 64;

}  // namespace tstream
