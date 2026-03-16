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

vector<NodeId> Node::get_merge_target(const std::string& HyDFSfilename){
  NodeId target_node = file_to_node(HyDFSfilename);

  auto all_members = mem_list.copy();

  // Sort by node_id
  std::sort(all_members.begin(), all_members.end(), [](const MembershipInfo& a, const MembershipInfo& b) {
    return std::string(a.node_id.host) < std::string(b.node_id.host);
  });
  // Find target_node
  auto it = std::find_if(all_members.begin(), all_members.end(),[&](const MembershipInfo& info) {
    return info.node_id == target_node;
  });

  // find target node and it's successor 
  vector<NodeId> target_nodes;
  target_nodes.push_back(target_node);
  // Compute N successors clockwise (wrap around ring)
  size_t start_idx = std::distance(all_members.begin(), it);
  for (int i = 1; i <= N_successors; ++i) {
    size_t idx = (start_idx + i) % all_members.size();
    // stop if wrapped back to self
    if (idx == start_idx)
      break;
    target_nodes.push_back(all_members[idx].node_id);
  }

  return target_nodes;
}

std::chrono::high_resolution_clock::time_point merge_start;
std::chrono::high_resolution_clock::time_point merge_end;

void Node::merge(const std::string& HyDFSfilename){
  // vector<NodeId> target_nodes = get_merge_target(HyDFSfilename);
  merge_target = get_merge_target(HyDFSfilename);

  

  
  FileChunkMessage message;
  message.type = FileMessageType::MERGE_REQ;
  std::strncpy(message.file_name, HyDFSfilename.c_str(), sizeof(message.file_name));
  message.file_name[sizeof(message.file_name) - 1] = '\0';
  std::strncpy(message.sender_host, self.host, sizeof(message.sender_host));
  message.sender_host[sizeof(message.sender_host) - 1] = '\0';

  // send message to every target node to get each timestamp for the file
  for (const auto& member : merge_target) {
    int sock = file_socket.connectToServer(member.host, FILE_PORT);

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

void Node::handle_MergeReq(const FileChunkMessage& message){
  merge_start = chrono::high_resolution_clock::now();
  int ts = 0;
  if (file_system.file_info.count(message.file_name))
      ts = file_system.file_info[message.file_name].timestamp;
  else{
    std::stringstream ss;
    ss << "[MERGE] No local copy of " << message.file_name;
    logger.log(ss.str());
  }


  FileChunkMessage reply;
  reply.type = FileMessageType::MERGE_RES;
  std::strncpy(reply.file_name, message.file_name, sizeof(reply.file_name));
  reply.file_name[sizeof(reply.file_name) - 1] = '\0';
  std::strncpy(reply.sender_host, self.host, sizeof(reply.sender_host));
  reply.sender_host[sizeof(reply.sender_host) - 1] = '\0';
  reply.timestamp = ts;

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

  std::stringstream ss;
  ss << "[MERGE] Sent MERGE_RES (" << ts << ") for " << message.file_name;
  logger.log(ss.str());
}

// send the fetch file request to the node with newest file
void Node::handle_MergeRes(const FileChunkMessage& message){
  merge_responses[message.file_name].push_back(message);

  // !! this might be dangerous, if some target doesn't respond, then this thread will keep waiting
  if (merge_responses[message.file_name].size()==merge_target.size()){
    int max_ts = -1;
    std::string newest_host;

    for (const auto& res : merge_responses[message.file_name]) {
        if (res.timestamp > max_ts) {
            max_ts = res.timestamp;
            newest_host = res.sender_host;
        }
    }
    std::stringstream ss;
    ss << "[MERGE] Newest copy of " << message.file_name << " is on " << newest_host << " (timestamp " << max_ts << ")";
    logger.log(ss.str());

    // request newest file on newest_host
    FileChunkMessage fetch_req;
    fetch_req.type = FileMessageType::MERGE_FETCH_REQ;
    std::strncpy(fetch_req.file_name, message.file_name, sizeof(fetch_req.file_name));
    fetch_req.file_name[sizeof(fetch_req.file_name) - 1] = '\0';
    std::strncpy(fetch_req.sender_host, self.host, sizeof(fetch_req.sender_host));
    fetch_req.sender_host[sizeof(fetch_req.sender_host) - 1] = '\0';

    int sock = file_socket.connectToServer(newest_host, FILE_PORT);
    
    std::array<char, TCPSocketConnection::BUFFER_LEN> buffer;
    size_t bytes_serialized = fetch_req.serialize(buffer.data(), buffer.size());
    uint32_t len = htonl(static_cast<uint32_t>(bytes_serialized));

    if (send(sock, &len, sizeof(len), 0) != sizeof(len)) {
        perror("[TCP-GET] send length failed");
    } else if (send(sock, buffer.data(), bytes_serialized, 0) < 0) {
        perror("[TCP-GET] send fetch request failed");
    }
    file_socket.closeConnection(sock);

    //clear the response after finish
    merge_responses.erase(message.file_name);
  }
  
}

// send the newest file to the initiator
void Node::handle_MergeFetchReq(const FileChunkMessage& message){

  // Ask FileSystem to create the chunks for fydfs file
  vector<FileChunkMessage> chunks = file_system.make_file_chunks(self.host, FileMessageType::MERGE_FETCH_RES, "", message.file_name, lamport_timestamp, false);

  NodeId target_node;
  std::strncpy(target_node.host, message.sender_host, sizeof(target_node.host));
  target_node.host[sizeof(target_node.host) - 1] = '\0'; 
  
  send_file_chunk(target_node, chunks);

  std::stringstream ss;
  ss << "[MERGE] Send newest file to " << message.sender_host << "(initiator)";
  logger.log(ss.str());
}

void Node::handle_MergeFetchRes(const FileChunkMessage& message){

  std::string key = string(message.file_name) + "|" + string(message.sender_host);
  auto& file_buffer = pending_files[key];

  // Initialize metadata if first time seeing this file
  if (file_buffer.chunk_data.empty()) {
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
    ss << "[MERGE] Receive total chunks for file " << message.file_name << " at initiator";
    logger.log(ss.str());

    // Write merged copy to HyDFS, will delete later.
    // We don't want the node to own the file that it should not have
    std::string temp_file = "merge_temp.log";
    file_system.assemble_file(temp_file, file_buffer, true, false);

    vector<FileChunkMessage> chunks = file_system.make_file_chunks(self.host, FileMessageType::MERGE_UPDATE, "", temp_file, message.timestamp, false);

    // Broadcast to other replicas
    for (const auto& target : merge_target) {
        // if (std::strcmp(target.host, message.sender_host) == 0) continue; // no need to send to newest node
        for (auto& chunk : chunks){
          // ensures the replicas save the correct HyDFS name, not "merge_temp.log"
          std::strncpy(chunk.file_name, string(message.file_name).c_str(), sizeof(message.file_name));
          chunk.file_name[sizeof(chunk.file_name) - 1] = '\0';
        }
        send_file_chunk(target, chunks);
            
    }
    
    //cleanup after finish the merge at the initiator side
    merge_target.clear();
    pending_files.erase(key);
    file_system.delete_file(temp_file);
    
  }
}

void Node::handle_MergeUpdate(const FileChunkMessage& message){

  std::string key = string(message.file_name) + "|" + string(message.sender_host);
  auto& file_buffer = pending_files[key];

  // Initialize metadata if first time seeing this file
  if (file_buffer.chunk_data.empty()) {
    // a process updates its lamport counter when a receive message
    lamport_timestamp = max(lamport_timestamp, message.timestamp) + 1;
    // update the file metadata
    NodeId owner_node = file_to_node(message.file_name);
    if (string(owner_node.host)==string(self.host))
        file_system.file_info[message.file_name].is_replica = false; // own this file
    else
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
    merge_end = chrono::high_resolution_clock::now();

    // auto latency = chrono::duration_cast<chrono::microseconds>(merge_end - merge_start).count();

    // cout << "[Merge] latency = " << latency << " seconds" << endl;
    
    file_system.assemble_file(message.file_name, file_buffer, true, false);
    std::stringstream ss;
    ss << "[MERGE] Updated file " << message.file_name << " to latest version";
    logger.log(ss.str());
    pending_files.erase(key); // cleanup
  }
}