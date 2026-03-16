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

#define MAXDATASIZE 512 
#define TOLERANCE 3
#define TIMEOUT 100000

void Node::grep(const std::string& pattern, const std::string& HyDFSfilename){
    file_system.reset_grep();
    FileChunkMessage message;
    message.type = FileMessageType::GREP_REQ;
    std::strncpy(message.file_name, HyDFSfilename.c_str(), sizeof(message.file_name));
    message.file_name[sizeof(message.file_name) - 1] = '\0';
    std::strncpy(message.file_name2, pattern.c_str(), sizeof(message.file_name2) - 1); // reuse file_name2 for pattern
    message.file_name2[sizeof(message.file_name2) - 1] = '\0';
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

    // std::stringstream ss;
    // ss << "[GREP] " << "start running grep";
    // logger.log(ss.str());
}

void Node::handle_GrepReq(const FileChunkMessage& message) {
    std::string filename = message.file_name;
    std::string pattern  = message.file_name2;

    // Run grep locally and get the result text
    std::string result = file_system.runGrep(pattern, filename);
    std::istringstream iss(result);
    std::string line;
    int count = 0;

    // --- Open TCP connection to requester ---
    int sock = file_socket.connectToServer(message.sender_host, FILE_PORT);
    if (sock < 0) {
        std::cerr << "[TCP-GREP] Failed to connect to " << message.sender_host << std::endl;
        return;
    }

    // --- Send each line as a GREP_RES message ---
    while (std::getline(iss, line)) {
        FileChunkMessage reply;
        reply.type = FileMessageType::GREP_RES;

        std::strncpy(reply.sender_host, self.host, sizeof(reply.sender_host));
        reply.sender_host[sizeof(reply.sender_host) - 1] = '\0';

        std::strncpy(reply.file_name, filename.c_str(), sizeof(reply.file_name));
        reply.file_name[sizeof(reply.file_name) - 1] = '\0';

        reply.data.assign(line.begin(), line.end());
        reply.data.push_back('\0');

        std::array<char, TCPSocketConnection::BUFFER_LEN> buffer;
        size_t bytes_serialized = reply.serialize(buffer.data(), buffer.size());

        // Prefix with length header
        uint32_t len = htonl(static_cast<uint32_t>(bytes_serialized));
        if (send(sock, &len, sizeof(len), 0) != sizeof(len)) {
            perror("[TCP-GREP] send length failed");
            break;
        }
        if (send(sock, buffer.data(), bytes_serialized, 0) < 0) {
            perror("[TCP-GREP] send data failed");
            break;
        }

        ++count;
    }

    // --- Send GREP_DONE marker ---
    FileChunkMessage done;
    done.type = FileMessageType::GREP_DONE;
    std::strncpy(done.sender_host, self.host, sizeof(done.sender_host));
    done.sender_host[sizeof(done.sender_host) - 1] = '\0';
    std::strncpy(done.file_name, filename.c_str(), sizeof(done.file_name));
    done.file_name[sizeof(done.file_name) - 1] = '\0';
    done.total_chunks = count;

    std::array<char, TCPSocketConnection::BUFFER_LEN> buffer;
    size_t bytes_done = done.serialize(buffer.data(), buffer.size());
    uint32_t len_done = htonl(static_cast<uint32_t>(bytes_done));

    send(sock, &len_done, sizeof(len_done), 0);
    send(sock, buffer.data(), bytes_done, 0);

    file_socket.closeConnection(sock);

    std::stringstream ss;
    ss << "[GREP] " << count << " lines sent for pattern " << pattern;
    logger.log(ss.str());
}


void Node::handle_GrepRes(const FileChunkMessage& message){
    // Each GREP_RES message contains one matching line in message.data
    file_system.update_grep(message.sender_host, std::string(message.data.begin(), message.data.end()));

}

void Node::handle_GrepDone(const FileChunkMessage& message) {
    std::stringstream ss;
    ss << "[GREP] Get " << message.total_chunks << " match from " << message.sender_host;
    logger.log(ss.str());
}