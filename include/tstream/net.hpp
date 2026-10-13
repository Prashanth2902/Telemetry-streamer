// Small socket helpers shared by TcpTransport and Receiver.

#pragma once

#include <cstdint>
#include <string>

#include <sys/socket.h>

namespace tstream {

// A resolved socket address, stored by value so it can be reused (e.g. for
// every reconnect) without resolving or allocating again.
struct SocketAddress {
  sockaddr_storage storage{};
  socklen_t length = 0;

  // For connect() and bind().
  [[nodiscard]] const sockaddr* get() const { return reinterpret_cast<const sockaddr*>(&storage); }
  // AF_INET or AF_INET6, for socket().
  [[nodiscard]] int family() const { return storage.ss_family; }
};

// The first TCP address getaddrinfo returns for host:port. `passive` asks for
// an address to bind() rather than connect() to. Throws std::runtime_error.
SocketAddress resolve_tcp(const std::string& host, std::uint16_t port, bool passive);

}  // namespace tstream
