#include "tstream/net.hpp"

#include <cstring>
#include <stdexcept>

#include <netdb.h>

namespace tstream {

// getaddrinfo allocates, so callers resolve once (at startup) and keep the result.
SocketAddress resolve_tcp(const std::string& host, std::uint16_t port, bool passive) {
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = passive ? AI_PASSIVE : 0;
  addrinfo* results = nullptr;
  const int rc = ::getaddrinfo(host.c_str(), std::to_string(port).c_str(), &hints, &results);
  if (rc != 0 || results == nullptr) {
    throw std::runtime_error("cannot resolve " + host + ": " + ::gai_strerror(rc));
  }
  SocketAddress address;
  std::memcpy(&address.storage, results->ai_addr, results->ai_addrlen);
  address.length = results->ai_addrlen;
  ::freeaddrinfo(results);
  return address;
}

}  // namespace tstream
