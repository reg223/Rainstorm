#include "node.hpp"

#include <array>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <thread>
#include <vector>

#include "message.hpp"
#include "shared.hpp"
#include "socket.hpp"

// // List all VM addresses where this file is currently being stored
void Node::ls(const std::string& HyDFSfilename){
  int file_key = file_system.hashToKey(HyDFSfilename);
  std::stringstream ss;
  ss << "File " << HyDFSfilename << " with key = " << file_key;
  logger.log(ss.str());

  FileChunkMessage message;
  message.type = FileMessageType::LS_REQ;
  std::strncpy(message.file_name, HyDFSfilename.c_str(), sizeof(message.file_name));
  message.file_name[sizeof(message.file_name) - 1] = '\0';
  std::strncpy(message.sender_host, self.host, sizeof(message.sender_host));
  message.sender_host[sizeof(message.sender_host) - 1] = '\0';

  auto all_members = mem_list.copy();
  if (all_members.empty()) return;

  for (const auto& member : all_members) {

    int sock = file_socket.connectToServer(member.node_id.host, FILE_PORT);

    std::array<char, TCPSocketConnection::BUFFER_LEN> buffer;
    size_t bytes_serialized = message.serialize(buffer.data(), buffer.size());
    uint32_t len = htonl(static_cast<uint32_t>(bytes_serialized));
    
    if (send(sock, &len, sizeof(len), 0) != sizeof(len)) {
        perror("[TCP-GET] send length failed");
    } else if (send(sock, buffer.data(), bytes_serialized, 0) < 0) {
        perror("[TCP-GET] send fetch request failed");
    }
    file_socket.closeConnection(sock);

  }
}

void Node::handle_LSReq(const FileChunkMessage& message){
  bool has_file = file_system.find_file(message.file_name);
  
  // send back
  FileChunkMessage reply;
  reply.type = FileMessageType::LS_RES;
  std::strncpy(reply.file_name, message.file_name, sizeof(reply.file_name));
  reply.file_name[sizeof(reply.file_name) - 1] = '\0';
  reply.chunk_id = has_file ? 1 : 0;   // reuse chunk_id field for boolean
  if (has_file){
    reply.timestamp = file_system.file_info[message.file_name].timestamp;
    reply.total_chunks = file_system.file_info[message.file_name].is_replica;  // reuse total_chunks feild for boolean
  }
    
  std::strncpy(reply.sender_host, self.host, sizeof(message.sender_host));
  reply.sender_host[sizeof(reply.sender_host) - 1] = '\0';
  
  int sock = file_socket.connectToServer(message.sender_host, FILE_PORT);

  std::array<char, TCPSocketConnection::BUFFER_LEN> buffer;
  size_t bytes_serialized = reply.serialize(buffer.data(), buffer.size());
  uint32_t len = htonl(static_cast<uint32_t>(bytes_serialized));
  
  if (send(sock, &len, sizeof(len), 0) != sizeof(len)) {
      perror("[TCP-GET] send length failed");
  } else if (send(sock, buffer.data(), bytes_serialized, 0) < 0) {
      perror("[TCP-GET] send fetch request failed");
  }
  file_socket.closeConnection(sock);
}

void Node::handle_LSRes(const FileChunkMessage& message) {
  std::stringstream ss;
  // reuse an int field for boolean, whether has file or not
  if (message.chunk_id == 1){
    if (message.total_chunks==0)
        ss << "[LS] " << message.sender_host << "(primary)" << ", with timestamp " << message.timestamp;
    else
        ss << "[LS] " << message.sender_host << "(replica)" << ", with timestamp " << message.timestamp;
    logger.log(ss.str());
  }
}