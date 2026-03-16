#include "socket.hpp"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>
#include <iostream>

void UDPSocketConnection::initializeUDPConnection() {
  struct addrinfo hints{}, *result, *curr = nullptr;

  buildAddrHints(hints, true);

  int rv = getaddrinfo(NULL, port.data(), &hints, &result);
  if (rv != 0) {
    std::cerr << "getaddrinfo: " << gai_strerror(rv) << std::endl;
    return;
  }

  for (curr = result; curr != NULL; curr = curr->ai_next) {
    fd = socket(curr->ai_family, curr->ai_socktype, curr->ai_protocol);
    if (fd == -1) continue;

    if (bind(fd, curr->ai_addr, curr->ai_addrlen) == 0) break;

    close(fd);
  }

  freeaddrinfo(result);

  if (curr == NULL) {
    perror("Couldn't initialize UDP server");
    exit(1);
  }

  // Make socket non-blocking
  int flags = fcntl(fd, F_GETFL, 0);
  if (flags == -1) {
    perror("fcntl F_GETFL failed");
    exit(1);
  }
  if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) {
    perror("fcntl F_SETFL failed");
    exit(1);
  }
}

void UDPSocketConnection::buildAddrHints(struct addrinfo &hints, const bool isServer) {
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_DGRAM;
  hints.ai_flags = isServer ? AI_PASSIVE : 0;
}

ssize_t UDPSocketConnection::read_from_socket(
    std::array<char, UDPSocketConnection::BUFFER_LEN> &buffer, const size_t bytes_to_read,
    struct sockaddr_in &clientAddr) const {
  socklen_t addr_len = sizeof(clientAddr);
  ssize_t bytes_read =
      recvfrom(fd, buffer.data(), bytes_to_read, 0, (struct sockaddr *)&clientAddr, &addr_len);

  if (bytes_read < 0) {
    if (errno == EAGAIN || errno == EWOULDBLOCK) return 0;

    perror("recvfrom failed");
  }
  return bytes_read;
}

void UDPSocketConnection::buildServerAddr(struct sockaddr_in &addr, const std::string_view &ip,
                                          const std::string &port) {
  memset(&addr, 0, sizeof(addr));
  struct addrinfo hints{}, *res;
  buildAddrHints(hints, false);

  int rv = getaddrinfo(std::string(ip).c_str(), port.c_str(), &hints, &res);
  if (rv != 0) {
    std::cerr << "getaddrinfo failed: " << gai_strerror(rv) << std::endl;
    return;
  }

  addr = *(struct sockaddr_in *)res->ai_addr;

  freeaddrinfo(res);
}

ssize_t UDPSocketConnection::write_to_socket(
    const std::array<char, UDPSocketConnection::BUFFER_LEN> &buffer, const size_t bytes_to_write,
    const struct sockaddr_in &clientAddr) const {
  ssize_t bytes_written = sendto(fd, buffer.data(), bytes_to_write, 0,
                                 (const struct sockaddr *)&clientAddr, sizeof(clientAddr));

  if (bytes_written < 0) {
    if (errno == EAGAIN || errno == EWOULDBLOCK) return 0;

    perror("sendto failed");
  }

  return bytes_written;
}

ssize_t UDPSocketConnection::write_to_socket(const std::string &buffer,
                                             const struct sockaddr_in &clientAddr) const {
  ssize_t bytes_written = sendto(fd, buffer.data(), buffer.size(), 0,
                                 (const struct sockaddr *)&clientAddr, sizeof(clientAddr));

  if (bytes_written < 0) {
    if (errno == EAGAIN || errno == EWOULDBLOCK) return 0;
    perror("sendto failed");
  }

  return bytes_written;
}

void UDPSocketConnection::closeConnection() const { close(fd); }
