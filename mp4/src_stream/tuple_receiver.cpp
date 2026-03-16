#include "rainstorm.hpp"

// Handles:
// 1. Accept TCP connections from upstream tasks
// 2. Reads tuple bytes from socket
// 3. Deserializes into StreamTuple
// 4. Pushes tuple onto a thread-safe queue

ssize_t reliable_recv(int s, void* data, size_t size) {
    size_t total_received = 0;
    char* current_data = static_cast<char*>(data);
    while (total_received < size) {
        // Use 0 flag here to allow partial reads, and rely on the loop for completion.
        ssize_t received = recv(s, current_data + total_received, size - total_received, 0); 
        if (received <= 0) {
            // Returns 0 on graceful close, -1 on error/reset
            return received; 
        }
        total_received += received;
    }
    return total_received;
}

void RainStorm::handleIncomingTuples() {

    logger.log("[RECEIVER] tuple_receiver_thread started.");

    while (true) {
        struct sockaddr_in client_addr;
        int client_fd = tuple_socket.acceptClient(client_addr);
        if (client_fd < 0) {
            std::cerr << "[TCP] accept() failed\n";
            continue;
        }

        std::thread([this, client_fd]() {
            while (true) {
                uint32_t len_net = 0;
                
                ssize_t hdr = reliable_recv(client_fd, &len_net, sizeof(len_net));

                if (hdr <= 0) break; 
                if (hdr != sizeof(len_net)) { // Crucial check for stream integrity
                    std::cerr << "[RainStorm_TCP] Incomplete header received." << std::endl;
                    break;
                }

                uint32_t msg_len = ntohl(len_net);
                if (msg_len == 0 || msg_len > TCPSocketConnection::BUFFER_LEN) {
                    std::cerr << "[RainStorm_TCP] Task_" << task_id << " invalid tuple length: " << msg_len << std::endl;
                    break;
                }

                std::vector<char> buffer(msg_len);
                
                ssize_t body = reliable_recv(client_fd, buffer.data(), msg_len);
                
                if (body <= 0) break;
                if (body != static_cast<ssize_t>(msg_len)) { // Crucial check for stream integrity
                    std::cerr << "[RainStorm_TCP] Incomplete payload received." << std::endl;
                    break;
                }

                Tuple t = Tuple::deserialize(buffer.data(), msg_len);
                // logger.log("[RECEIVER] New tuple received and enqueued: " + string(t.key));
                if (exactly_once==1) {
                    std::lock_guard<std::mutex> lock(processed_state_mtx); 
                    std::string tuple_id_str = std::to_string(t.tuple_id);
                    // Check for duplicate using the globally unique ID (t.tuple_id)
                    if (processed_state.count(tuple_id_str)) { //DUPLICATE
                        logger.log("[RECEIVER] Duplicate tuple received: " + tuple_id_str + ". Discarding and Acknowledging.");
                        send_tuple_ack(client_fd, t); 
                        continue; 
                    } else {

                        commit_to_checkpoint(t, TupleMessageType::INPUT_COMMIT);
                        processed_state.insert(tuple_id_str); 
                        tuple_queue.push(t);
                        send_tuple_ack(client_fd, t);
                        
                    }
                } else {
                    tuple_queue.push(t);
                    send_tuple_ack(client_fd, t);
                }
                {
                    std::lock_guard<std::mutex> lock(counter_mtx);
                    tuple_counter += 1;
                }
            }

            tuple_socket.closeConnection(client_fd);

        }).detach();
    }
}

void RainStorm::send_tuple_ack(int target_fd, const Tuple& t) {

    // Create the message payload: a 4-byte length header + the unique ID string.
    std::string ack_id = std::to_string(t.tuple_id);
    uint32_t payload_size = ack_id.size();
    uint32_t len_net = htonl(payload_size);

    // 2. Helper function for reliable sending (reused from send_persistent)
    auto reliable_send = [&](int s, const void* data, size_t size) -> bool {
        size_t total_sent = 0;
        const char* current_data = static_cast<const char*>(data);

        while (total_sent < size) {
            ssize_t sent = send(s, current_data + total_sent, size - total_sent, 0);
            if (sent < 0) {
                // Log the specific error (e.g., connection reset)
                logger.log("[ERROR] Failed to send ACK payload on fd " + std::to_string(target_fd) + ".");
                return false; 
            }
            total_sent += sent;
        }
        return true; 
    };

    // --- Send Logic ---
    
    // 3. Send length header (4 bytes)
    if (!reliable_send(target_fd, &len_net, sizeof(len_net))) {
        logger.log("[ERROR] Failed to send ACK length header for tuple " + ack_id);
        // Note: We don't close the socket here; we rely on the sender to detect the failure.
        return;
    }

    // 4. Send payload (the unique ID string)
    if (!reliable_send(target_fd, ack_id.c_str(), payload_size)) {
        logger.log("[ERROR] Failed to send ACK payload for tuple " + ack_id);
        return;
    }
    
    // Success
    logger.log("[RECEIVER] Sent ACK for tuple_id=" + ack_id);
}

// void RainStorm::send_tuple_ack(int target_fd, const Tuple& t) {

//     Tuple ack_msg;

//     ack_msg.tuple_id = t.tuple_id;
//     ack_msg.type = TupleMessageType::ACK;
    
//     std::array<char, TCPSocketConnection::BUFFER_LEN> buffer;
//     size_t bytes_serialized = ack_msg.serialize(buffer.data(), buffer.size());

//     if (bytes_serialized == 0) {
//         logger.log("[ACK_SENDER] Failed to serialize ACK for tuple_id=" + std::to_string(t.tuple_id));
//         return;
//     }

//     uint32_t len = htonl(static_cast<uint32_t>(bytes_serialized));
//     const char* payload_ptr = buffer.data();

//     ssize_t sent_hdr = send(target_fd, &len, sizeof(len), 0);
//     if (sent_hdr != sizeof(len)) {
//         logger.log("[ACK_SENDER] Failed to send ACK length header for tuple_id=" + std::to_string(t.tuple_id) + ". Sent: " + std::to_string(sent_hdr));
//         return;
//     }

//     // Send serialized Tuple payload
//     ssize_t sent_body = send(target_fd, payload_ptr, bytes_serialized, 0);
//     if (sent_body != static_cast<ssize_t>(bytes_serialized)) {
//         logger.log("[ACK_SENDER] Failed to send ACK payload for tuple_id=" + std::to_string(t.tuple_id) + ". Sent: " + std::to_string(sent_body));
//         return;
//     }
// }

void RainStorm::runSourceTask() {
    // {
    //     std::unique_lock<std::mutex> lock(routing_mtx);
    //     routing_cv.wait(lock, [this] { return routing_ready; });
    // }

    logger.log("Source task started. Reading input file: " + src_dir);

    // Extract filename (remove directory part)
    std::string filename = src_dir;
    size_t pos = filename.find_last_of('/');
    if (pos != std::string::npos) {
        filename = filename.substr(pos + 1); // e.g., dataset1.csv
    }

    std::ifstream infile(src_dir);
    if (!infile.is_open()) {
        logger.log("ERROR: Failed to open HyDFS file: " + src_dir);
        return;
    }

    int line_num = 0;
    std::string line;

    const int rate = 100;               // 100 tuples/sec (MP4 default)
    const int sleep_ms = 1000 / rate;

    while (true) {
        if (!std::getline(infile, line)) {
            logger.log("End of HyDFS file reached: " + src_dir);
            break;
        }

        Tuple t;

        // tuple_id = line number
        t.tuple_id = line_num;

        // Build key = "<filename:line_number>"
        std::string key_str = filename + ":" + std::to_string(line_num);
        std::strncpy(t.key, key_str.c_str(), sizeof(t.key) - 1);

        // Build value = full raw line from CSV
        std::strncpy(t.value, line.c_str(), sizeof(t.value) - 1);

        std::strncpy(t.dst_file, dst_file.c_str(), sizeof(t.dst_file) - 1);

        t.stage_id = stage_id;
        t.task_id  = task_id; // -1 for source (leader)

        // Push into tuple queue for processing
        tuple_queue.push(t);

        // logger.log("Source enqueued tuple_id=" + std::to_string(t.tuple_id) + " key = " + t.key + " value = " + t.value);

        line_num++;
        std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
    }

    logger.log("Source task completed.");
}

