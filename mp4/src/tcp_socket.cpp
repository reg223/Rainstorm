#include "tcp_socket.hpp"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>
#include <iostream>

void TCPSocketConnection::buildAddrHints(struct addrinfo &hints, bool isServer) {
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = isServer ? AI_PASSIVE : 0;
}

// ---------------- SERVER ----------------
void TCPSocketConnection::initializeTCPServer() {
  struct addrinfo hints{}, *result, *curr = nullptr;
  buildAddrHints(hints, true);

  int rv = getaddrinfo(nullptr, port.data(), &hints, &result);
  if (rv != 0) {
    std::cerr << "getaddrinfo: " << gai_strerror(rv) << std::endl;
    return;
  }

  for (curr = result; curr != nullptr; curr = curr->ai_next) {
    fd = socket(curr->ai_family, curr->ai_socktype, curr->ai_protocol);
    if (fd == -1) continue;

    int yes = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int));

    if (bind(fd, curr->ai_addr, curr->ai_addrlen) == 0) break;
    close(fd);
  }
  freeaddrinfo(result);

  if (curr == nullptr) {
    perror("Couldn't bind TCP server");
    exit(1);
  }

  if (listen(fd, 10) == -1) {
    perror("listen failed");
    exit(1);
  }

}

int TCPSocketConnection::acceptClient(struct sockaddr_in &clientAddr) const {
  socklen_t addr_len = sizeof(clientAddr);
  int client_fd = accept(fd, (struct sockaddr *)&clientAddr, &addr_len);
  if (client_fd < 0) perror("accept failed");
  return client_fd;
}

// ---------------- CLIENT ----------------
int TCPSocketConnection::connectToServer(const std::string &ip, const std::string &port) {
  struct addrinfo hints{}, *res;
  buildAddrHints(hints, false);

  int rv = getaddrinfo(ip.c_str(), port.c_str(), &hints, &res);
  if (rv != 0) {
    std::cerr << "getaddrinfo: " << gai_strerror(rv) << std::endl;
    return -1;
  }

  int sock_fd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
  if (sock_fd == -1) {
    perror("socket");
    freeaddrinfo(res);
    return -1;
  }

  if (connect(sock_fd, res->ai_addr, res->ai_addrlen) == -1) {
    perror("connect");
    close(sock_fd);
    freeaddrinfo(res);
    return -1;
  }

  freeaddrinfo(res);
  return sock_fd;
}

// ---------------- I/O ----------------
ssize_t TCPSocketConnection::read_from_socket(int sock_fd, std::vector<char> &buffer,
                                              size_t bytes_to_read) {
  buffer.resize(bytes_to_read);
  ssize_t bytes_read = recv(sock_fd, buffer.data(), bytes_to_read, 0);
  if (bytes_read < 0) perror("recv failed");
  return bytes_read;
}

ssize_t TCPSocketConnection::write_to_socket(int sock_fd, const std::vector<char> &buffer) {
  ssize_t bytes_written = send(sock_fd, buffer.data(), buffer.size(), 0);
  if (bytes_written < 0) perror("send failed");
  return bytes_written;
}

// ---------------- CLOSE ----------------
void TCPSocketConnection::closeConnection(int sock_fd) const {
  close(sock_fd);
}
