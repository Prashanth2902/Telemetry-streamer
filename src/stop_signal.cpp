#include "tstream/stop_signal.hpp"

#include <atomic>
#include <csignal>

namespace tstream {

namespace {

std::atomic<bool> g_stop_requested{false};
static_assert(std::atomic<bool>::is_always_lock_free, "the handler must not take a lock");

// Signal handler: only sets the flag.
extern "C" void handle_stop_signal(int /*signal*/) {
  g_stop_requested.store(true, std::memory_order_relaxed);
}

}  // namespace

// std::signal is enough here: the handler is trivial and never re-armed.
void install_stop_signal_handlers() {
  std::signal(SIGINT, handle_stop_signal);
  std::signal(SIGTERM, handle_stop_signal);
}

// Relaxed: the flag carries no data, it only says "stop now".
bool stop_requested() {
  return g_stop_requested.load(std::memory_order_relaxed);
}

}  // namespace tstream
