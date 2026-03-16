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

void Node::forward_tuple_to_file(const Tuple& t){
    NodeId target_node = file_to_node(t.dst_file);

    std::vector<FileChunkMessage> chunks;
    FileChunkMessage message;
    message.type = FileMessageType::APPEND_REQ;
    std::strncpy(message.file_name, t.dst_file, sizeof(message.file_name));
    message.file_name[sizeof(message.file_name) - 1] = '\0';
    std::strncpy(message.sender_host, self.host, sizeof(message.sender_host));
    message.sender_host[sizeof(message.sender_host) - 1] = '\0';
    message.chunk_id = 0;
    message.total_chunks = 1;
    message.timestamp = lamport_timestamp;
    std::string payload = std::string(t.value) + "\n";
    message.data.assign(payload.begin(), payload.end());
    chunks.push_back(message);

    send_file_chunk(target_node, chunks);
}

void Node::forward_tuple_to_file_aggregate(const Tuple& t){
    NodeId target_node = file_to_node(t.dst_file);

    std::vector<FileChunkMessage> chunks;
    FileChunkMessage message;
    message.type = FileMessageType::AGGRE_APPEND_REQ;
    std::strncpy(message.file_name, t.dst_file, sizeof(message.file_name));
    message.file_name[sizeof(message.file_name) - 1] = '\0';
    std::strncpy(message.sender_host, self.host, sizeof(message.sender_host));
    message.sender_host[sizeof(message.sender_host) - 1] = '\0';
    message.chunk_id = 0;
    message.total_chunks = 1;
    message.timestamp = lamport_timestamp;
    std::string payload = std::string(t.value);
    message.data.assign(payload.begin(), payload.end());
    chunks.push_back(message);

    send_file_chunk(target_node, chunks);
}

void Node::handle_input_commit(const Tuple& t){
    NodeId target_node = file_to_node(t.dst_file);

    std::vector<FileChunkMessage> chunks;
    FileChunkMessage message;
    message.type = FileMessageType::APPEND_REQ;
    std::strncpy(message.file_name, t.dst_file, sizeof(message.file_name));
    message.file_name[sizeof(message.file_name) - 1] = '\0';
    std::strncpy(message.sender_host, self.host, sizeof(message.sender_host));
    message.sender_host[sizeof(message.sender_host) - 1] = '\0';
    message.chunk_id = 0;
    message.total_chunks = 1;
    message.timestamp = lamport_timestamp;
    
    std::string tuple_id_str = std::to_string(t.tuple_id);
    std::string payload = "INPUT_COMMITTED:" + tuple_id_str + "|" + t.value + "\n";
    message.data.assign(payload.begin(), payload.end());
    chunks.push_back(message);

    send_file_chunk(target_node, chunks); 
}

void Node::handle_ack_commit(const Tuple& t){
    NodeId target_node = file_to_node(t.dst_file);

    std::vector<FileChunkMessage> chunks;
    FileChunkMessage message;
    message.type = FileMessageType::APPEND_REQ;
    std::strncpy(message.file_name, t.dst_file, sizeof(message.file_name));
    message.file_name[sizeof(message.file_name) - 1] = '\0';
    std::strncpy(message.sender_host, self.host, sizeof(message.sender_host));
    message.sender_host[sizeof(message.sender_host) - 1] = '\0';
    message.chunk_id = 0;
    message.total_chunks = 1;
    message.timestamp = lamport_timestamp;
    
    std::string tuple_id_str = std::to_string(t.tuple_id);
    std::string payload = "OUTPUT_ACKED:" + tuple_id_str + "\n";
    message.data.assign(payload.begin(), payload.end());
    chunks.push_back(message);

    send_file_chunk(target_node, chunks); 
}