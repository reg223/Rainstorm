// Handle re-replicated after a node fail

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

// Provide a single definition for the start/end_time variables so the linker can resolve them.
std::chrono::high_resolution_clock::time_point start;
std::chrono::high_resolution_clock::time_point end_time;

NodeId Node::get_successor_fail(const NodeId& failed_node){
    int vm_id_fail = stoi(string(failed_node.host).substr(13, 2));
    // Sort the mem_list by node_id
    auto all_members = mem_list.copy();
    std::sort(all_members.begin(), all_members.end(),
    [](const MembershipInfo& a, const MembershipInfo& b) {
        return std::string(a.node_id.host) < std::string(b.node_id.host);
    });
    // Find first alive node whose VM number >= file_key
    NodeId target_node;
    bool found = false;
    for (auto it = all_members.begin(); it != all_members.end(); it++) {
    int vm_id = stoi(string(it->node_id.host).substr(13, 2));
        // find the first node whose vm_id > vm_id_fail
        if (vm_id > vm_id_fail) {
            target_node = it->node_id;
            found = true;
            break;
        }
    }
    // wrap-around case: failed_node larger than all existing node IDs
    if (!found) {
        target_node = all_members.front().node_id;
    }
    // std::stringstream ss;
    // ss << "[ReReplicate] get_successor_fail: " << target_node.host;
    // logger.log(ss.str());
    return target_node;
}

void Node::reassign_files_after_failure(){
    for (auto& it : file_system.file_info) {
        NodeId owner_node = file_to_node(it.first);
        if (string(owner_node.host)==string(self.host))
            file_system.file_info[it.first].is_replica = false; // own this file
        else
            file_system.file_info[it.first].is_replica = true;
    }
    return;
}

bool Node::was_my_successor(const NodeId& failed_node){
    bool found = false;

    for (const auto& succ : successor_list.getSuccessors()) {
        if (string(succ.host) == string(failed_node.host)) {
            found = true;
            break;
        }
    }
    return found;
}

void Node::startReReplicated(const NodeId& failed_node, bool should_re_replicate){
    start = chrono::high_resolution_clock::now();
    NodeId succ_of_failed = get_successor_fail(failed_node);
    if ((string(succ_of_failed.host) == string(self.host))){
        std::stringstream ss;
        ss << "[ReReplicate] Start re-replicate (successor of fail node)";
        logger.log(ss.str());
        reassign_files_after_failure();
        Run_ReReplicate();
    }
    if (should_re_replicate ){
        std::stringstream ss;
        ss << "[ReReplicate] Start re-replicate (successor fail)";
        logger.log(ss.str());
        Run_ReReplicate();
    } 
}

void Node::Run_ReReplicate(){
    // Scan the file store on this vm, find the primary
    for (auto& it : file_system.file_info) {
        if (it.second.is_replica==false){
            vector<FileChunkMessage> chunks = file_system.make_file_chunks(self.host, FileMessageType::REREPLICATE_RES, "", it.first, lamport_timestamp, false);
            
            //only send the primary to the newest successor node
            NodeId target_node = successor_list.getSuccessors().back();
            send_file_chunk(target_node, chunks);

            std::stringstream ss;
            ss << "[ReReplicate] Send file " << it.first << " to " << target_node.host;
            logger.log(ss.str());
        }
    }
}

void Node::handle_ReReplicateRes(const FileChunkMessage& message){
    std::string key = string(message.file_name) + "|" + string(message.sender_host);
    auto& file_buffer = pending_files[key];

    // Initialize metadata if first time seeing this file
    if (file_buffer.chunk_data.empty()) {
        // a process updates its lamport counter when a receive message
        lamport_timestamp = max(lamport_timestamp, message.timestamp); // Should I plus 1 ?
        // update the file metadata
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
        ss << "[ReReplicate] Receive total chunks for file " 
        << message.file_name << " from " << message.sender_host;
        logger.log(ss.str());

        end_time = chrono::high_resolution_clock::now();

        // auto latency = chrono::duration_cast<chrono::milliseconds>(end_time - start).count();

        // cout << "[ReReplicate] latency = " << latency << " seconds" << endl;

        file_system.assemble_file(message.file_name, file_buffer, true, false);
        pending_files.erase(key);
    }
}