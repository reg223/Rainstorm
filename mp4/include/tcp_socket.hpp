#pragma once
#include <netinet/in.h>

#include <array>
#include <string>
#include <string_view>
#include <vector>

class TCPSocketConnection {
 public:
  static constexpr size_t BUFFER_LEN = 10000;

  TCPSocketConnection(const std::string_view &hostname, const std::string_view &port)
      : fd(-1), hostname(hostname), port(port) {}

  // --- Server-side initialization ---
  // Bind and listen on a TCP port (replaces initializeUDPConnection)
  void initializeTCPServer();

  // Accept an incoming TCP client connection
  // Returns the new client socket file descriptor
  int acceptClient(struct sockaddr_in &clientAddr) const;

  // --- Client-side connection ---
  // Connect to a remote TCP server
  // Returns a connected socket file descriptor
  int connectToServer(const std::string &ip, const std::string &port);

  // --- I/O operations ---
  // Read bytes from a connected TCP socket
  ssize_t read_from_socket(int sock_fd, std::vector<char> &buffer, size_t bytes_to_read);

  // Write bytes to a connected TCP socket
  ssize_t write_to_socket(int sock_fd, const std::vector<char> &buffer);

  // --- Connection teardown ---
  void closeConnection(int sock_fd) const;

 private:
  void buildAddrHints(struct addrinfo &hints, bool isServer = false);

  int fd;                              // listening socket for servers
  const std::string_view hostname;     // host/IP to bind or connect
  const std::string_view port;         // port as string
};
