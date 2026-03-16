// Handle rebalance after new node join

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


void Node::startRebalance(){
    // ask the successor whether have to transfer ownership of the file
    FileChunkMessage message;
    message.type = FileMessageType::REBALANCE_REQ;
    std::strncpy(message.sender_host, self.host, sizeof(message.sender_host));
    message.sender_host[sizeof(message.sender_host) - 1] = '\0';

    // send to successor
    int sock = file_socket.connectToServer(successor_list.getSuccessors()[0].host, FILE_PORT);

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

void Node::handle_RebalanceReq(const FileChunkMessage& message){

    // Scan the file store on this vm, find the file that need to transfer the owner ship
    for (auto& it : file_system.file_info) {
        NodeId owner_node = file_to_node(it.first);
        // There are two kind of file need to be tansfered:
        // 1. The file_key belongs to to new join node
        // 2. The replica stored on this machine should also be send to new join node
        if (std::string(owner_node.host) == std::string(message.sender_host) || it.second.is_replica==true){
            
            // Mark the file at this vm as replica
            it.second.is_replica = true;

            vector<FileChunkMessage> chunks = file_system.make_file_chunks(self.host, FileMessageType::REBALANCE_RES, "", it.first, lamport_timestamp, false);

            NodeId target_node;
            std::strncpy(target_node.host, message.sender_host, sizeof(target_node.host));
            target_node.host[sizeof(target_node.host) - 1] = '\0'; 
            send_file_chunk(target_node, chunks);

            std::stringstream ss;
            ss << "[Rebalance] Send file " << it.first << " to " << message.sender_host;
            logger.log(ss.str());
        }
    }

    // after sending all the file, send REBALANCE_DEL_REQ message to the new join node
    FileChunkMessage new_message;
    new_message.type = FileMessageType::REBALANCE_END;
    std::strncpy(new_message.sender_host, self.host, sizeof(message.sender_host));
    new_message.sender_host[sizeof(new_message.sender_host) - 1] = '\0';

    // send to successor
    int sock = file_socket.connectToServer(message.sender_host, FILE_PORT);

    std::array<char, TCPSocketConnection::BUFFER_LEN> buffer;
    size_t bytes_serialized = new_message.serialize(buffer.data(), buffer.size());
    uint32_t len = htonl(static_cast<uint32_t>(bytes_serialized));
    
    if (send(sock, &len, sizeof(len), 0) != sizeof(len)) {
        perror("[TCP-GET] send length failed");
    } else if (send(sock, buffer.data(), bytes_serialized, 0) < 0) {
        perror("[TCP-GET] send fetch request failed");
    }
    file_socket.closeConnection(sock);
}

std::chrono::high_resolution_clock::time_point re_start;
std::chrono::high_resolution_clock::time_point re_end_time;


void Node::handle_RebalanceReS(const FileChunkMessage& message){
    std::string key = string(message.file_name) + "|" + string(message.sender_host);
    auto& file_buffer = pending_files[key];

    // Initialize metadata if first time seeing this file
    if (file_buffer.chunk_data.empty()) {
        if (re_start == std::chrono::high_resolution_clock::time_point{})re_start = chrono::high_resolution_clock::now();
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
    if (file_buffer.received_chunks == file_buffer.total_chunks){
        std::stringstream ss;
        ss << "[Rebalance] Receive total chunks for file " 
        << message.file_name << " from " << message.sender_host;
        logger.log(ss.str());


        re_end_time = chrono::high_resolution_clock::now();

        // auto latency = chrono::duration_cast<chrono::microseconds>(re_end_time - re_start).count();

        // cout << "[ReReplicate] latency = " << latency << " seconds" << endl;

        file_system.assemble_file(message.file_name, file_buffer, true, false);
        pending_files.erase(key);
    }
}

void Node::deleteRedundent(){
    // Notify all successors to delete files that no longer belong to them
    for (auto& it : successor_list.getSuccessors()){
        FileChunkMessage message;
        message.type = FileMessageType::REBALANCE_DEL;
        std::strncpy(message.sender_host, self.host, sizeof(message.sender_host));
        message.sender_host[sizeof(message.sender_host) - 1] = '\0';

        // send to successor
        int sock = file_socket.connectToServer(it.host, FILE_PORT);

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

void Node::handle_RebalanceDel() {
    // Build the list of nodes (self + predecessors) this node should store replicas for
    std::vector<std::string> target_vm;
    target_vm.push_back(std::string(self.host));
    for (auto& pre : successor_list.getPredecessors()) {
        target_vm.push_back(std::string(pre.host));
    }

    // Collect files that no longer belong on this node
    std::vector<std::string> to_delete;
    for (auto& it : file_system.file_info) {
        NodeId owner_node = file_to_node(it.first);
        if (std::find(target_vm.begin(), target_vm.end(), owner_node.host) == target_vm.end()) {
            to_delete.push_back(it.first);
        }
    }

    // Delete redundant files and log the cleanup
    std::stringstream ss;
    // ss << "[Rebalance] Delete files: ";
    for (auto& filename : to_delete) {
        file_system.delete_file(filename);
        // ss << filename << " ";
    }
    // logger.log(ss.str());
}


