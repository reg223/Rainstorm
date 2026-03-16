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

void Node::create(const std::string& localfile, const std::string& HyDFSfilename){

  if(file_system.find_file(HyDFSfilename)){
    std::stringstream ss;
    ss << "[CREATE] File " << HyDFSfilename << " already exists on the system.";
    logger.log(ss.str());
    return;
  }
  //increment timestamp
  lamport_timestamp += 1;

  // generate chunks
  vector<FileChunkMessage> chunks = file_system.make_file_chunks(self.host, FileMessageType::CREATE_REQ, localfile, HyDFSfilename, lamport_timestamp, true);


  // find target
  NodeId target_node = file_to_node(HyDFSfilename);

  send_file_chunk(target_node, chunks);

  std::stringstream ss;
  ss << "[CREATE] Completed sending file.";
  logger.log(ss.str());

  return;
}


void Node::handle_createReq(const FileChunkMessage& message){

  std::string key = string(message.file_name) + "|" + string(message.sender_host);
  auto& file_buffer = pending_files[key];

  // Initialize metadata if first time seeing this file
  if (file_buffer.chunk_data.empty()) {
    // a process updates its lamport counter when a receive message
    lamport_timestamp = max(lamport_timestamp, message.timestamp) + 1;
    // update the file info
    file_system.file_info[message.file_name].is_replica = false; // own this file
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
    std::stringstream ss;
    ss << "[CREATE] Receive total chunks for file " 
       << message.file_name << ", with timestamp " 
       << file_system.file_info[message.file_name].timestamp;
    logger.log(ss.str());

    file_system.assemble_file(message.file_name, file_buffer, true, false);
    pending_files.erase(key);
    create_replica(message.file_name);
    return;
  }
}

void Node::create_replica(const std::string& HyDFSfilename){
  // generate chunks
  vector<FileChunkMessage> chunks = file_system.make_file_chunks(self.host, FileMessageType::CREATE_REPLICA_REQ, "", HyDFSfilename, lamport_timestamp, false);
  
  for(auto &node: successor_list.getSuccessors()){
    send_file_chunk(node, chunks);
  }
  std::stringstream ss;
  ss << "[CREATE] Completed sending file to replicas.";
  logger.log(ss.str());
}

void Node::handle_create_replica_Req(const FileChunkMessage& message){

  std::string key = string(message.file_name) + "|" + string(message.sender_host);
  auto& file_buffer = pending_files[key];

  // Initialize metadata if first time seeing this file
  if (file_buffer.chunk_data.empty()) {
    // a process updates its lamport counter when a receive message
    lamport_timestamp = max(lamport_timestamp, message.timestamp); // Should I plus 1 ?
    // update the file info
    file_system.file_info[message.file_name].is_replica = true; 
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
    std::stringstream ss;
    ss << "[CREATE] Receive total chunks for file " 
       << message.file_name << ", with timestamp " 
       << file_system.file_info[message.file_name].timestamp;
    logger.log(ss.str());

    file_system.assemble_file(message.file_name, file_buffer, true, false);
    pending_files.erase(key);
    return;
  }
}
