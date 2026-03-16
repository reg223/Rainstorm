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

vector<NodeId> Node::get_Get_target(const std::string& HyDFSfilename){
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

void Node::get(const std::string& HyDFSfilename, const std::string& localfilename){
  get_target = get_Get_target(HyDFSfilename);
  get_localfilename[HyDFSfilename] = localfilename;
  
  FileChunkMessage message;
  message.type = FileMessageType::GET_REQ;
  std::strncpy(message.file_name, HyDFSfilename.c_str(), sizeof(message.file_name));
  message.file_name[sizeof(message.file_name) - 1] = '\0';
  std::strncpy(message.sender_host, self.host, sizeof(message.sender_host));
  message.sender_host[sizeof(message.sender_host) - 1] = '\0';

  // send message to every target node to get each timestamp for the file
  for (const auto& member : get_target) {
    // --- open TCP connection ---
    int sock = file_socket.connectToServer(member.host, FILE_PORT);
    if (sock < 0) {
        std::cerr << "[TCP-GET] Failed to connect to " << member.host << std::endl;
        continue;
    }

    // --- serialize and send message ---
    std::array<char, TCPSocketConnection::BUFFER_LEN> buffer;
    size_t bytes_serialized = message.serialize(buffer.data(), buffer.size());

    uint32_t len = htonl(static_cast<uint32_t>(bytes_serialized));
    if (send(sock, &len, sizeof(len), 0) != sizeof(len)) {
        perror("[TCP-GET] send length failed");
        file_socket.closeConnection(sock);
        continue;
    }
    if (send(sock, buffer.data(), bytes_serialized, 0) < 0) {
        perror("[TCP-GET] send message failed");
    }

    file_socket.closeConnection(sock);
  }
}

void Node::getfromreplica(const std::string& vm_id, const std::string& HyDFSfilename, const std::string& localfilename){
  std::string vm_addr = "fa25-cs425-53" + vm_id + ".cs.illinois.edu";
  get_localfilename[HyDFSfilename] = localfilename;

  FileChunkMessage message;
  message.type = FileMessageType::GET_FETCH_REQ;
  std::strncpy(message.file_name, HyDFSfilename.c_str(), sizeof(message.file_name));
  message.file_name[sizeof(message.file_name) - 1] = '\0';
  std::strncpy(message.sender_host, self.host, sizeof(message.sender_host));
  message.sender_host[sizeof(message.sender_host) - 1] = '\0';

  // --- connect to the replica over TCP ---
  int sock = file_socket.connectToServer(vm_addr, FILE_PORT);
  if (sock < 0) {
      std::cerr << "[TCP-GET] Failed to connect to " << vm_addr << std::endl;
      return;
  }

  // --- send request ---
  std::array<char, TCPSocketConnection::BUFFER_LEN> buffer;
  size_t bytes_serialized = message.serialize(buffer.data(), buffer.size());
  uint32_t len = htonl(static_cast<uint32_t>(bytes_serialized));

  if (send(sock, &len, sizeof(len), 0) != sizeof(len)) {
      perror("[TCP-GET] send length failed");
      file_socket.closeConnection(sock);
      return;
  }
  if (send(sock, buffer.data(), bytes_serialized, 0) < 0) {
      perror("[TCP-GET] send message failed");
      file_socket.closeConnection(sock);
      return;
  }
  file_socket.closeConnection(sock);
}

void Node::handle_GetReq(const FileChunkMessage& message){
  int ts = 0;
  if (file_system.file_info.count(message.file_name))
      ts = file_system.file_info[message.file_name].timestamp;
  else{
    std::stringstream ss;
    ss << "[GET] No local copy of " << message.file_name;
    logger.log(ss.str());
  }

  FileChunkMessage reply;
  reply.type = FileMessageType::GET_RES;
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
  ss << "[GET] Sent GET_RES (" << ts << ") for " << message.file_name;
  logger.log(ss.str());
}

// send the fetch file request to the node with newest file
void Node::handle_GetRes(const FileChunkMessage& message){
  get_responses[message.file_name].push_back(message);

  // !! this might be dangerous, if some target doesn't respond, then this thread will keep waiting
  if (get_responses[message.file_name].size()==get_target.size()){
    int max_ts = -1;
    std::string newest_host;

    for (const auto& res : get_responses[message.file_name]) {
        if (res.timestamp > max_ts) {
            max_ts = res.timestamp;
            newest_host = res.sender_host;
        }
    }
    std::stringstream ss;
    ss << "[GET] Newest copy of " << message.file_name << " is on " << newest_host << " (timestamp " << max_ts << ")";
    logger.log(ss.str());

    // request newest file on newest_host
    FileChunkMessage fetch_req;
    fetch_req.type = FileMessageType::GET_FETCH_REQ;
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
    get_responses.erase(message.file_name);
  }
  
}

// send the newest file to the initiator
void Node::handle_GetFetchReq(const FileChunkMessage& message){

  // Ask FileSystem to create the chunks for fydfs file
  vector<FileChunkMessage> chunks = file_system.make_file_chunks(self.host, FileMessageType::GET_FETCH_RES, "", message.file_name, lamport_timestamp, false);

  NodeId target_node;
  std::strncpy(target_node.host, message.sender_host, sizeof(target_node.host));
  target_node.host[sizeof(target_node.host) - 1] = '\0'; 
  
  send_file_chunk(target_node, chunks);

  std::stringstream ss;
  ss << "[GET] Send newest file to " << message.sender_host;
  logger.log(ss.str());
}

void Node::handle_GetFetchRes(const FileChunkMessage& message){

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
    ss << "[GET] Receive total chunks for file " << message.file_name << " at initiator";
    logger.log(ss.str());
  
    file_system.assemble_file(get_localfilename[message.file_name], file_buffer, true, true);

    //cleanup after finish the get at the initiator side
    get_target.clear();
    pending_files.erase(key);
    get_localfilename.erase(message.file_name);
  }
}