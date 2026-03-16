#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <ostream>
#include <vector>

#include "shared.hpp"

struct NodeId {
  char host[33];  // +1 for null terminator
  char port[6];   // +1 for null terminator
  uint32_t time;  // unix epoch timestamp

  size_t serialize(char* buffer, const size_t bufferSize) const;
  static NodeId createNewNode(const std::string_view& host_str, const std::string_view& port_str);
  static NodeId deserialize(const char* buffer, size_t bufferSize);

  bool operator==(const NodeId& other) const {
    return std::strcmp(host, other.host) == 0 && std::strcmp(port, other.port) == 0 &&
           time == other.time;
  }
};

inline std::ostream& operator<<(std::ostream& os, const NodeId& node) {
  os << node.host << ":" << node.port << ":" << node.time;
  return os;
}

// Code from ChatGPT to enable hashing on NodeID to use it as a key type in unordered map
namespace std {
template <>
struct hash<NodeId> {
  std::size_t operator()(const NodeId& n) const {
    // Convert to string_view to avoid allocating
    std::size_t h1 = std::hash<std::string_view>()(n.host);
    std::size_t h2 = std::hash<std::string_view>()(n.port);
    std::size_t h3 = std::hash<uint32_t>()(n.time);

    // Combine hashes (like boost::hash_combine)
    std::size_t seed = h1;
    seed ^= h2 + 0x9e3779b9 + (seed << 6) + (seed >> 2);
    seed ^= h3 + 0x9e3779b9 + (seed << 6) + (seed >> 2);
    return seed;
  }
};
}  // namespace std

enum class NodeStatus : uint8_t { ALIVE, SUSPECT, DEAD, LEFT };

inline const char* to_string(NodeStatus status) {
  switch (status) {
    case NodeStatus::ALIVE:
      return "ALIVE";
    case NodeStatus::SUSPECT:
      return "SUSPECT";
    case NodeStatus::DEAD:
      return "DEAD";
    case NodeStatus::LEFT:
      return "LEFT";
  }
  return "UNKNOWN";
}

inline std::ostream& operator<<(std::ostream& os, NodeStatus status) {
  os << to_string(status);
  return os;
}

struct MembershipInfo {
  NodeId node_id;
  NodeStatus status;
  FailureDetectionMode mode;
  uint32_t local_time;
  uint32_t incarnation;
  uint32_t heartbeat_counter;

  size_t serialize(char* buffer, const size_t buffer_size, bool include_heartbeat = false) const;
  static MembershipInfo deserialize(const char* buffer, const size_t buffer_size,
                                    bool include_heartbeat = false);
};

inline std::ostream& operator<<(std::ostream& os, const MembershipInfo& m) {
  os << m.node_id << ", status=" << m.status << ", incarnation=" << m.incarnation
     << ", local_time=" << m.local_time << ", heartbeatCounter=" << m.heartbeat_counter
     << ", mode=" << SharedFunctions::modeToStr(m.mode);
  return os;
}

enum class MessageType : uint8_t { PING, ACK, GOSSIP, JOIN, LEAVE, SWITCH };

struct Message {
  MessageType type;
  uint32_t num_messages;
  std::vector<MembershipInfo> messages;

  size_t serialize(char* buffer, const size_t buffer_size) const;
  static Message deserialize(const char* buffer, const size_t buffer_size);
};

// utility
uint32_t currTime();