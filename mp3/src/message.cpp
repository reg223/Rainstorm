#include "message.hpp"

#include <arpa/inet.h>

#include <cstdint>
#include <cstring>
#include <ctime>
#include <stdexcept>
#include <string_view>

NodeId NodeId::createNewNode(const std::string_view& host_str, const std::string_view& port_str) {
  if (host_str.size() >= sizeof(NodeId::host)) throw std::runtime_error("Host too long");
  if (port_str.size() >= sizeof(NodeId::port)) throw std::runtime_error("Port too long");

  NodeId node{};
  std::memset(node.host, 0, sizeof(node.host));
  std::memset(node.port, 0, sizeof(node.port));
  std::memcpy(node.host, host_str.data(), host_str.size());
  std::memcpy(node.port, port_str.data(), port_str.size());

  // Fill time as unix epoch timestamp
  node.time = static_cast<uint32_t>(std::time(nullptr));

  return node;
}

NodeId NodeId::deserialize(const char* buffer, size_t bufferSize) {
  if (bufferSize < sizeof(NodeId::host) + sizeof(NodeId::port) + sizeof(NodeId::time))
    throw std::runtime_error("Buffer too small");

  NodeId node{};
  size_t offset = 0;

  std::memcpy(node.host, buffer + offset, sizeof(node.host));
  offset += sizeof(node.host);

  std::memcpy(node.port, buffer + offset, sizeof(node.port));
  offset += sizeof(node.port);

  std::memcpy(&node.time, buffer + offset, sizeof(node.time));
  node.time = ntohl(node.time);  // Convert from network byte order
  offset += sizeof(node.time);

  return node;
}

size_t NodeId::serialize(char* buffer, const size_t bufferSize) const {
  if (bufferSize < sizeof(host) + sizeof(port) + sizeof(time))
    throw std::runtime_error("Buffer too small");

  size_t offset = 0;
  std::memcpy(buffer + offset, host, sizeof(host));
  offset += sizeof(host);

  std::memcpy(buffer + offset, port, sizeof(port));
  offset += sizeof(port);

  uint32_t networkTime = htonl(time);  // Convert to network byte order
  std::memcpy(buffer + offset, &networkTime, sizeof(networkTime));
  offset += sizeof(networkTime);

  return offset;
}

size_t MembershipInfo::serialize(char* buffer, const size_t buffer_size,
                                 bool include_heartbeat) const {
  // Calculate total size needed

  // don't need local_time since we don't send that
  // node_id + status + mode + incarnation + heartbeat
  size_t needed_size = sizeof(NodeId::host) + sizeof(NodeId::port) + sizeof(NodeId::time) +
                       sizeof(uint8_t) + sizeof(uint8_t) + sizeof(uint32_t);
  if (include_heartbeat) needed_size += sizeof(uint32_t);  // heartbeat counter

  if (needed_size > buffer_size) throw std::runtime_error("Buffer too small for MembershipInfo");

  size_t offset = 0;

  // serialize NodeId
  offset += node_id.serialize(buffer + offset, buffer_size - offset);

  // serialize status
  uint8_t statusByte = static_cast<uint8_t>(status);
  std::memcpy(buffer + offset, &statusByte, sizeof(statusByte));
  offset += sizeof(statusByte);

  // serialize FailureDetectionMode
  uint8_t modeByte = static_cast<uint8_t>(mode);
  std::memcpy(buffer + offset, &modeByte, sizeof(modeByte));
  offset += sizeof(modeByte);

  // serialize incarnation (network byte order)
  uint32_t networkIncarnation = htonl(incarnation);
  std::memcpy(buffer + offset, &networkIncarnation, sizeof(networkIncarnation));
  offset += sizeof(networkIncarnation);

  if (include_heartbeat) {
    // Convert to network byte order
    uint32_t networkHeartbeat = htonl(heartbeat_counter);
    std::memcpy(buffer + offset, &networkHeartbeat, sizeof(networkHeartbeat));
    offset += sizeof(networkHeartbeat);
  }

  return offset;
}

MembershipInfo MembershipInfo::deserialize(const char* buffer, size_t buffer_size,
                                           bool include_heartbeat) {
  MembershipInfo info{};
  size_t offset = 0;

  // Deserialize NodeId
  info.node_id = NodeId::deserialize(buffer + offset, buffer_size - offset);
  offset += sizeof(NodeId::host) + sizeof(NodeId::port) + sizeof(NodeId::time);

  // Deserialize status
  if (offset + sizeof(uint8_t) > buffer_size)
    throw std::runtime_error("Buffer too small for status");

  uint8_t statusByte;
  std::memcpy(&statusByte, buffer + offset, sizeof(statusByte));
  info.status = static_cast<NodeStatus>(statusByte);
  offset += sizeof(statusByte);

  // Deserialize mode
  if (offset + sizeof(uint8_t) > buffer_size) {
    throw std::runtime_error("Buffer too small for mode");
  }

  uint8_t modeByte;
  std::memcpy(&modeByte, buffer + offset, sizeof(modeByte));
  info.mode = static_cast<FailureDetectionMode>(modeByte);
  offset += sizeof(modeByte);

  // Deserialize incarnation
  if (offset + sizeof(uint32_t) > buffer_size)
    throw std::runtime_error("Buffer too small for incarnation");

  uint32_t networkIncarnation;
  std::memcpy(&networkIncarnation, buffer + offset, sizeof(networkIncarnation));
  info.incarnation = ntohl(networkIncarnation);
  offset += sizeof(networkIncarnation);

  // Initialize local_time to current time
  info.local_time = static_cast<uint32_t>(std::time(nullptr));

  // Deserialize heartbeat counter (optional)
  if (include_heartbeat) {
    if (offset + sizeof(info.heartbeat_counter) > buffer_size) {
      throw std::runtime_error("Buffer too small for heartbeat");
    }
    uint32_t networkHeartbeat;
    std::memcpy(&networkHeartbeat, buffer + offset, sizeof(networkHeartbeat));
    info.heartbeat_counter = ntohl(networkHeartbeat);
    offset += sizeof(networkHeartbeat);
  } else {
    info.heartbeat_counter = 0;
  }

  return info;
}

size_t Message::serialize(char* buffer, const size_t buffer_size) const {
  std::memset(buffer, 0, buffer_size);
  const bool include_heartbeat = (type == MessageType::GOSSIP);
  const uint32_t count = static_cast<uint32_t>(messages.size());
  // nodeid + status + mode + incarnation + heartbeat
  size_t membership_size = sizeof(NodeId::host) + sizeof(NodeId::port) + sizeof(NodeId::time) +
                           sizeof(uint8_t) + sizeof(uint8_t) + sizeof(uint32_t);
  if (include_heartbeat) membership_size += sizeof(uint32_t);
  // message_type + num_messages + messages
  size_t needed_size = sizeof(uint8_t) + sizeof(uint32_t) + (count * membership_size);

  if (needed_size > buffer_size) throw std::runtime_error("Buffer too small for Message");

  size_t offset = 0;

  // Serialize type
  uint8_t type_byte = static_cast<uint8_t>(type);
  std::memcpy(buffer + offset, &type_byte, sizeof(type_byte));
  offset += sizeof(type_byte);

  // Serialize num_messages
  uint32_t networkNumMessages = htonl(count);
  std::memcpy(buffer + offset, &networkNumMessages, sizeof(networkNumMessages));
  offset += sizeof(networkNumMessages);

  // Serialize each membership info
  for (const auto& membership_info : messages) {
    size_t written =
        membership_info.serialize(buffer + offset, buffer_size - offset, include_heartbeat);
    offset += written;
  }

  return offset;
}

Message Message::deserialize(const char* buffer, const size_t buffer_size) {
  Message message;
  size_t offset = 0;

  // Deserialize type
  if (offset + sizeof(uint8_t) > buffer_size)
    throw std::runtime_error("Buffer too small for message type");

  uint8_t type_byte;
  std::memcpy(&type_byte, buffer + offset, sizeof(type_byte));
  message.type = static_cast<MessageType>(type_byte);
  offset += sizeof(type_byte);

  // Deserialize num_messages (convert from network byte order)
  if (offset + sizeof(uint32_t) > buffer_size)
    throw std::runtime_error("Buffer too small for num_messages");

  uint32_t networkNumMessages;
  std::memcpy(&networkNumMessages, buffer + offset, sizeof(networkNumMessages));
  message.num_messages = ntohl(networkNumMessages);
  offset += sizeof(networkNumMessages);

  // Deserialize each membership info
  const bool include_heartbeat = (message.type == MessageType::GOSSIP);
  for (uint32_t i = 0; i < message.num_messages; ++i) {
    message.messages.push_back(
        MembershipInfo::deserialize(buffer + offset, buffer_size - offset, include_heartbeat));
    // Calculate offset for next message
    offset += sizeof(NodeId::host) + sizeof(NodeId::port) + sizeof(NodeId::time) + sizeof(uint8_t) +
              sizeof(uint8_t) + sizeof(uint32_t);       // node_id + status + mode + incarnation
    if (include_heartbeat) offset += sizeof(uint32_t);  // heartbeat counter
  }

  return message;
}

uint32_t currTime() { return static_cast<uint32_t>(std::time(nullptr)); }