#pragma once

namespace tstream {

// Spin-wait hint (x86 PAUSE): eases pressure on the sibling hyperthread and the
// memory bus while busy-waiting. A no-op on other architectures.
inline void cpu_relax() {
#if defined(__x86_64__) || defined(__i386__)
  __builtin_ia32_pause();
#endif
}

}  // namespace tstream
