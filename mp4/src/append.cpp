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


void Node::append(const std::string& localfile, const std::string& HyDFSfilename) {

  // a process increments its counter when a send happen
  lamport_timestamp += 1;

  NodeId target_node = file_to_node(HyDFSfilename);

  // Ask FileSystem to create the chunks for local file
  vector<FileChunkMessage> chunks = file_system.make_file_chunks(self.host, FileMessageType::APPEND_REQ, localfile, HyDFSfilename, lamport_timestamp, true);

  // Send each chunk over the TCP
  send_file_chunk(target_node, chunks);

  std::stringstream ss;
  ss << "[APPEND] Completed sending file.";
  logger.log(ss.str());
}

void Node::handle_AppendReq(const FileChunkMessage& message) {

  std::string key = string(message.file_name) + "|" + string(message.sender_host);
  auto& file_buffer = pending_files[key];

  // Initialize metadata if first time seeing this file
  if (file_buffer.chunk_data.empty()) {
    // a process updates its lamport counter when a receive message
    lamport_timestamp = max(lamport_timestamp, message.timestamp) + 1;
    // update the file info

    file_system.file_info[message.file_name].timestamp = lamport_timestamp;

    file_buffer.total_chunks = message.total_chunks;
    file_buffer.received_chunks = 0;
    file_buffer.chunk_data.resize(message.total_chunks);
  }

  // Store chunk if not already received
  if (file_buffer.chunk_data[message.chunk_id].empty()) {
    file_buffer.chunk_data[message.chunk_id] = message.data;
    file_buffer.received_chunks++;
  }
  
  // Check if all chunks received
  if (file_buffer.received_chunks == file_buffer.total_chunks) {
    file_system.assemble_file(message.file_name, file_buffer, false, false);
    pending_files.erase(key); // cleanup
  }
}

void Node::multiappend(const std::string& HyDFSfilename, const std::vector<std::string> vm_id, const std::vector<std::string> local_files){
  
  for (size_t i = 0; i < vm_id.size(); i++) {
    FileChunkMessage message;
    message.type = FileMessageType::MULTIAPPEND_REQ;
    // local file
    std::strncpy(message.file_name, local_files[i].c_str(), sizeof(message.file_name));
    message.file_name[sizeof(message.file_name) - 1] = '\0';
    // hydfs file
    std::strncpy(message.file_name2, HyDFSfilename.c_str(), sizeof(message.file_name2));
    message.file_name2[sizeof(message.file_name2) - 1] = '\0';
    std::strncpy(message.sender_host, self.host, sizeof(message.sender_host));
    message.sender_host[sizeof(message.sender_host) - 1] = '\0';

    std::string target_host = "fa25-cs425-53" + vm_id[i] + ".cs.illinois.edu";

    // cout << target_host << endl;
    int sock = file_socket.connectToServer(target_host, FILE_PORT);
    if (sock < 0){
      cerr << "[ERROR] invalid socket fd\n";
    }
    
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

void Node::handle_MultiappendReq(const FileChunkMessage& message){
  // append(local_filename, HyDFS_filename)
  append(message.file_name, message.file_name2);
}

void Node::handle_AggreAppendReq(FileChunkMessage& message) {
  std::string key = string(message.file_name) + "|" + string(message.sender_host);
  auto& file_buffer = pending_files[key];

  // Initialize metadata if first time seeing this file
  if (file_buffer.chunk_data.empty()) {
    // a process updates its lamport counter when a receive message
    lamport_timestamp = max(lamport_timestamp, message.timestamp) + 1;
    // update the file info

    file_system.file_info[message.file_name].timestamp = lamport_timestamp;

    file_buffer.total_chunks = message.total_chunks;
    file_buffer.received_chunks = 0;
    file_buffer.chunk_data.resize(message.total_chunks);
  }

  // Store chunk if not already received
  if (file_buffer.chunk_data[message.chunk_id].empty()) {
    std::string input_line(message.data.begin(), message.data.end());
    aggregation_counts[input_line] += 1;
    std::string new_data = input_line + ":" + std::to_string(aggregation_counts[input_line]) + "\n";
    message.data.assign(new_data.begin(), new_data.end());
    file_buffer.chunk_data[message.chunk_id] = message.data;
    file_buffer.received_chunks++;
  }
  
  if (file_buffer.received_chunks == file_buffer.total_chunks) {
    file_system.assemble_file(message.file_name, file_buffer, false, false);
    pending_files.erase(key); // cleanup
  }
}