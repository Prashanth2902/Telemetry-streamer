// Socket helpers shared by unit and integration tests.

#pragma once

#include <cstdint>

#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

namespace tstream::testing {

// An ephemeral TCP port on 127.0.0.1 that nothing is listening on: bound to
// let the kernel pick it, then released. Another process could grab it in
// between, which is acceptable for tests.
inline std::uint16_t free_port() {
  const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  EXPECT_EQ(::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);
  socklen_t len = sizeof(addr);
  ::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len);
  ::close(fd);
  return ntohs(addr.sin_port);
}

}  // namespace tstream::testing
