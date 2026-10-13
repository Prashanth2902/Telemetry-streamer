// Process-wide stop request from SIGINT/SIGTERM, shared by tstream and
// tstream-recv. The handler only stores to a lock-free atomic, which is
// async-signal-safe; everything else happens on normal threads.

#pragma once

namespace tstream {

// Installs SIGINT and SIGTERM handlers that set the stop flag. Call once, early.
void install_stop_signal_handlers();

// True once SIGINT or SIGTERM has arrived.
[[nodiscard]] bool stop_requested();

}  // namespace tstream
