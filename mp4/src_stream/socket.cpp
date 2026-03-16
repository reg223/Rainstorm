#include "rainstorm.hpp"

// int RainStorm::get_persistent_socket(const std::string& host, const std::string& port) {
//     std::string key = host + ":" + port;

//     std::lock_guard<std::mutex> lock(sock_mtx);

//     // already exists
//     // if (persistent_sockets.count(key)) {
//     //     return persistent_sockets[key];
//     // }
//     if (persistent_sockets.count(key)) {
//         int sock = persistent_sockets[key];
//         int error = 0;
//         socklen_t len = sizeof(error);
//         if (getsockopt(sock, SOL_SOCKET, SO_ERROR, &error, &len) < 0 || error != 0) {
//             logger.log("[HEALTH_CHECK] Found dead socket (" + key + "). Error: " + std::to_string(error) + ". Closing and removing from cache.");
//             close(sock);
//             persistent_sockets.erase(key);
//             return -1;
//         } else {
//             return sock;
//         }
//     }

//     // create new connection
//     int sock = sending_socket.connectToServer(host, port);
//     if (sock < 0) {
//         logger.log("[ERROR] Failed to connect to " + key);
//         return -1;
//     }

//     persistent_sockets[key] = sock;
//     logger.log("[CONNECT] Established persistent socket to " + key);

//     return sock;
// }

int RainStorm::get_persistent_socket(const std::string& host, const std::string& port) {
    std::string key = host + ":" + port;
    int current_sock = -1;

    {
        std::lock_guard<std::mutex> lock(sock_mtx); 
        
        if (persistent_sockets.count(key)) {
            current_sock = persistent_sockets[key];
            int error = 0;
            socklen_t len = sizeof(error);

            // Health Check: Check for pending errors 
            if (getsockopt(current_sock, SOL_SOCKET, SO_ERROR, &error, &len) < 0 || error != 0) {
                logger.log("[HEALTH_CHECK] Found dead socket (" + key + "). Error: " + std::to_string(error) + ". Closing and removing from cache.");
                close(current_sock);
                persistent_sockets.erase(key);
            } else {
                return current_sock; 
            }
        }
    } 
    
    int new_sock = sending_socket.connectToServer(host, port);
    
    if (new_sock < 0) {
        logger.log("[ERROR] Failed to connect to " + key);
        return -1; // Connection attempt failed.
    }

    {
        std::lock_guard<std::mutex> lock(sock_mtx); 
        // Store the newly connected socket
        persistent_sockets[key] = new_sock;
        logger.log("[CONNECT] Established persistent socket to " + key);
    }
    
    return new_sock;
}

void RainStorm::send_tcp_message(const string target_host, const string target_port, const StreamTaskMessage& message){
    int sock = sending_socket.connectToServer(target_host, target_port);
    if (sock < 0) {
    std::cerr << "[RainStorm] Failed to connect to " << target_host << std::endl;
    return;
    }
    std::array<char, TCPSocketConnection::BUFFER_LEN> buffer;
    size_t bytes_serialized = message.serialize(buffer.data(), buffer.size());

    // Prefix each message with 4-byte length header
    uint32_t len = htonl(static_cast<uint32_t>(bytes_serialized));
    if (send(sock, &len, sizeof(len), 0) != sizeof(len)) {
        perror("[RainStorm] send length failed");
    }

    ssize_t written = send(sock, buffer.data(), bytes_serialized, 0);
    if (written < 0) {
        perror("[RainStorm] send message failed");
    }
    sending_socket.closeConnection(sock);
}

void RainStorm::send_persistent(const std::string& host, const std::string& port, const Tuple& msg) {
    // 1. Get socket ID (assume get_persistent_socket handles mutex for connection/lookup)
    int sock = get_persistent_socket(host, port);
    if (sock < 0) return;

    // --- Message Preparation ---
    std::array<char, TCPSocketConnection::BUFFER_LEN> buffer;
    size_t bytes_serialized = msg.serialize(buffer.data(), buffer.size());

    // 2. Prepare length header (network byte order)
    uint32_t len = htonl(static_cast<uint32_t>(bytes_serialized));
    const char* payload_ptr = buffer.data();

    // 3. Helper function for reliable sending
    auto reliable_send = [&](int s, const void* data, size_t size) -> bool {
        size_t total_sent = 0;
        const char* current_data = static_cast<const char*>(data);

        while (total_sent < size) {
            ssize_t sent = send(s, current_data + total_sent, size - total_sent, 0);
            if (sent < 0) {
                // Actual send error (EAGAIN, EBADF, etc.)
                return false; 
            }
            total_sent += sent;
        }
        return true; // All bytes sent successfully
    };

    // --- Critical Section Start (Error Handling) ---
    // Lambda to handle closing and removal of the socket from the persistent map
    auto handle_error_and_close = [this, &host, &port, sock]() {
        // LOCK THE MUTEX when modifying the shared map
        std::lock_guard<std::mutex> lock(sock_mtx); 
        close(sock);
        // The key is host:port
        persistent_sockets.erase(host + ":" + port); 
    };

    // Send length header (4 bytes)
    if (!reliable_send(sock, &len, sizeof(len))) {
        logger.log("[ERROR] Failed to send length header, closing socket");
        handle_error_and_close();
        return;
    }

    // Send payload (variable size)
    if (!reliable_send(sock, payload_ptr, bytes_serialized)) {
        logger.log("[ERROR] Failed to send entire payload, closing socket");
        handle_error_and_close();
        return;
    }

    // If both reliable_send calls succeeded, the socket remains open.
}

// bool RainStorm::send_and_wait_for_ack(const std::string& host, const std::string& port, const Tuple& msg) {
//     // 1. Get socket ID
//     int sock = get_persistent_socket(host, port);
//     if (sock < 0) return false; // Return false on connection failure

//     std::array<char, TCPSocketConnection::BUFFER_LEN> buffer;
//     size_t bytes_serialized = msg.serialize(buffer.data(), buffer.size());
//     uint32_t len = htonl(static_cast<uint32_t>(bytes_serialized));
//     const char* payload_ptr = buffer.data();
    
//     auto reliable_send = [&](int s, const void* data, size_t size) -> bool {
//         size_t total_sent = 0;
//         const char* current_data = static_cast<const char*>(data);
//         while (total_sent < size) {
//             ssize_t sent = send(s, current_data + total_sent, size - total_sent, 0);
//             if (sent < 0) {
//                 // Actual send error (EAGAIN, EBADF, etc.)
//                 return false; 
//             }
//             total_sent += sent;
//         }
//         return true; // All bytes sent successfully
//     };

//     auto handle_error_and_close = [this, &host, &port, sock]() {
//         // LOCK THE MUTEX when modifying the shared map
//         std::lock_guard<std::mutex> lock(sock_mtx); 
//         close(sock);
//         persistent_sockets.erase(host + ":" + port); 
//     };

//     if (!reliable_send(sock, &len, sizeof(len)) || 
//         !reliable_send(sock, payload_ptr, bytes_serialized)) {
//         logger.log("[ERROR] Failed to send tuple payload, closing socket.");
//         handle_error_and_close();
//         return false;
//     }
//     // --- Phase 2: Set Socket Timeout for ACK ---
    
//     // Define timeout structure (e.g., 500 milliseconds)
//     struct timeval tv;
//     tv.tv_sec = 0;
//     tv.tv_usec = 500000; // 500ms timeout
    
//     if (setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv, sizeof tv) < 0) {
//         logger.log("[FATAL] setsockopt failed on ACK wait.");
//     }
//     // --- Phase 3: Wait for ACK ---
    
//     // Define a minimal size for the ACK header
//     uint32_t ack_len_net = 0;

//     // --- Step 3a: Read the 4-byte length header of the ACK ---
    
//     // FIX 1: Use the correct socket handle 'sock'
//     ssize_t hdr_recv = recv(sock, &ack_len_net, sizeof(ack_len_net), MSG_WAITALL); 

//     // Reset timeout immediately after the first recv
//     struct timeval tv_reset; 
//     tv_reset.tv_sec = 0; tv_reset.tv_usec = 0; 
//     setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv_reset, sizeof tv_reset);

//     if (hdr_recv <= 0) {
//         if (errno == EWOULDBLOCK || errno == EAGAIN) {
//              logger.log("[PROCESSOR] ACK header timeout for tuple_id=" + std::to_string(msg.tuple_id));
//         } else {
//              logger.log("[ERROR] Socket error while waiting for ACK header. Closing.");
//              handle_error_and_close();
//         }
//         return false; 
//     }
    
//     // Crucial check: must receive the full 4 bytes
//     if (hdr_recv != sizeof(ack_len_net)) {
//         logger.log("[ERROR] Received incomplete ACK header. Closing.");
//         handle_error_and_close();
//         return false;
//     }

//     uint32_t ack_len = ntohl(ack_len_net);
//     if (ack_len == 0 || ack_len > 1024) { 
//         logger.log("[ERROR] Received invalid ACK length: " + std::to_string(ack_len));
//         handle_error_and_close();
//         return false;
//     }
    
    
//     std::vector<char> ack_buffer(ack_len);
    
//     ssize_t body_recv = recv(sock, ack_buffer.data(), ack_len, MSG_WAITALL);

//     if (body_recv <= 0) {
//         logger.log("[ERROR] Failed to receive ACK payload (socket error/close). Closing.");
//         handle_error_and_close();
//         return false; 
//     }
    
//     if (body_recv != static_cast<ssize_t>(ack_len)) {
//         logger.log("[ERROR] Failed to receive full ACK payload. Expected: " + std::to_string(ack_len) + 
//                    ", Got: " + std::to_string(body_recv) + ". Closing.");
//         handle_error_and_close();
//         return false;
//     }
    
    
//     Tuple received_ack_t = Tuple::deserialize(ack_buffer.data(), ack_len);
    
//     if (received_ack_t.type != TupleMessageType::ACK) { // Assuming ACK_COMMIT from prior context
//         logger.log("[PROCESSOR] Received non-ACK tuple type while waiting for ACK. Retrying.");
//         return false;
//     }
    
//     if (received_ack_t.tuple_id != msg.tuple_id) {
//         logger.log("[PROCESSOR] Received ACK for mismatching ID. Expected: " + std::to_string(msg.tuple_id) + 
//                    ", Got: " + std::to_string(received_ack_t.tuple_id) + ". Retrying.");
//         return false;
//     }
    
//     return true;
// }

bool RainStorm::send_and_wait_for_ack(const std::string& host, const std::string& port, const Tuple& msg) {
    // 1. Get socket ID
    int sock = get_persistent_socket(host, port);
    if (sock < 0) return false; // Return false on connection failure

    std::array<char, TCPSocketConnection::BUFFER_LEN> buffer;
    size_t bytes_serialized = msg.serialize(buffer.data(), buffer.size());
    uint32_t len = htonl(static_cast<uint32_t>(bytes_serialized));
    const char* payload_ptr = buffer.data();
    
    auto reliable_send = [&](int s, const void* data, size_t size) -> bool {
        size_t total_sent = 0;
        const char* current_data = static_cast<const char*>(data);
        while (total_sent < size) {
            ssize_t sent = send(s, current_data + total_sent, size - total_sent, 0);
            if (sent < 0) {
                // Actual send error (EAGAIN, EBADF, etc.)
                return false; 
            }
            total_sent += sent;
        }
        return true; // All bytes sent successfully
    };

    auto handle_error_and_close = [this, &host, &port, sock]() {
        // LOCK THE MUTEX when modifying the shared map
        std::lock_guard<std::mutex> lock(sock_mtx); 
        close(sock);
        persistent_sockets.erase(host + ":" + port); 
    };

    if (!reliable_send(sock, &len, sizeof(len)) || 
        !reliable_send(sock, payload_ptr, bytes_serialized)) {
        logger.log("[ERROR] Failed to send tuple payload, closing socket.");
        handle_error_and_close();
        return false;
    }
    // --- Phase 2: Set Socket Timeout for ACK ---
    
    // Define timeout structure (e.g., 500 milliseconds)
    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = 500000; // 500ms timeout
    
    if (setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv, sizeof tv) < 0) {
        logger.log("[FATAL] setsockopt failed on ACK wait.");
    }
    // --- Phase 3: Wait for ACK ---
    
    char ack_buffer[1024]; 
    
    ssize_t ack_recv_len = recv(sock, ack_buffer, sizeof(ack_buffer), 0);
    // Reset timeout immediately after recv to avoid affecting subsequent sends/receives
    tv.tv_sec = 0; tv.tv_usec = 0; // Set back to zero (blocking/default)
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv, sizeof tv);
    if (ack_recv_len <= 0) {
        if (errno == EWOULDBLOCK || errno == EAGAIN) {
             logger.log("[PROCESSOR] ACK timeout for tuple_id=" + std::to_string(msg.tuple_id));
        } else {
             logger.log("[ERROR] Socket error while waiting for ACK. Closing.");
             handle_error_and_close();
        }
        return false; 
    }

    return true; // ACK successfully received and validated
}

void RainStorm::commit_to_checkpoint(const Tuple& t, const TupleMessageType& type) {
    if (type==TupleMessageType::INPUT_COMMIT)
        logger.log("[RECEIVER] Committing input tuple_id=" + std::to_string(t.tuple_id) +
               " key=" + t.key + " to " + checkpoint_path);
    else if (type==TupleMessageType::ACK_COMMIT)
        logger.log("[RECEIVER] Committing ack tuple_id=" + std::to_string(t.tuple_id) +
               " key=" + t.key + " to " + checkpoint_path);

    Tuple commit_msg = t; 
    commit_msg.type = type;
    std::strncpy(commit_msg.dst_file, checkpoint_path.c_str(), sizeof(commit_msg.dst_file)); 
    commit_msg.dst_file[sizeof(commit_msg.dst_file) - 1] = '\0'; 
    
    send_persistent(self.host, TUPLE_PORT, commit_msg);
}


