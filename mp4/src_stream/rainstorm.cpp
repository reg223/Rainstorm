#include "rainstorm.hpp"

RainStorm::RainStorm(int task_id,
                     int stage_id,
                     const std::string& operator_exec,
                     const std::string& operator_args,
                     const std::string& src_dir,
                     const std::string& dst_file,
                     bool exactly_once,
                     const NodeId& self,
                     const NodeId& leader)
    : socket(self.host, self.port),
      tuple_socket(self.host, std::to_string(TUPLE_PORT_OFFSET + task_id)),
      sending_socket("", ""),      
      task_id(task_id),
      stage_id(stage_id),
      operator_exec(operator_exec),
      operator_args(operator_args),
      src_dir(src_dir),
      dst_file(dst_file),
      exactly_once(exactly_once),
      self(self),
      leader(leader){

    // Determine if we are the leader
    is_leader = (task_id == -1);

    socket.initializeTCPServer();
    tuple_socket.initializeTCPServer();

    std::string logname = "./stream_log/task_" + std::to_string(task_id) + ".log";
    std::remove(logname.c_str()); // for testing
    logger.open(logname);
    logger.log("RainStorm task constructed. task_id=" + std::to_string(task_id) + " stage_id=" + std::to_string(stage_id));

    checkpoint_path = "task_" + std::to_string(task_id) + "_checkpoint.log";

    tuple_counter = 0;

    std::cout << "[RainStormTask] Constructed task:\n";
    std::cout << "  task_id      = " << task_id << "\n";
    std::cout << "  stage_id     = " << stage_id << "\n";
    std::cout << "  operator     = " << operator_exec << "\n";
    std::cout << "  args         = " << operator_args << "\n";
    std::cout << "  src_dir      = " << src_dir << "\n";
    std::cout << "  dst_file     = " << dst_file << "\n";
    std::cout << "  exactly_once = " << exactly_once << "\n";
    std::cout << "  self_host    = " << self.host << "\n";
    std::cout << "  self_port    = " << self.port << "\n";
    std::cout << "  is_leader    = " << is_leader << "\n";
    std::cout << "  checkpoint   = " << checkpoint_path << "\n";
    // ask for routing table to distribute tuples to next stage
    stream_started = false;
    if (exactly_once) load_checkpoint_to_local();
    send_RountinTable_Req();

}

// Handles:
// 1. RoutingTable_Res
// 2. Autoscaling commands
// 3. Restart commands
// 4. LIST_REQ
// 5. Exactly-once control messages
void RainStorm::handleIncomingLeader() {

    while (true) {
        struct sockaddr_in client_addr;
        int client_fd = socket.acceptClient(client_addr);
        if (client_fd < 0) {
            std::cerr << "[TCP] accept() failed\n";
            continue;
        }

        std::thread([this, client_fd]() {
            while (true) {

                // ---- 1. Read message length (4 bytes)
                uint32_t len_net = 0;
                ssize_t hdr = recv(client_fd, &len_net, sizeof(len_net), MSG_WAITALL);
                if (hdr <= 0) break;

                uint32_t msg_len = ntohl(len_net);
                if (msg_len == 0 || msg_len > TCPSocketConnection::BUFFER_LEN) {
                    std::cerr << "[TCP] Invalid message length: " << msg_len << std::endl;
                    break;
                }

                // ---- 2. Read full payload
                std::vector<char> buffer(msg_len);
                size_t received = 0;

                while (received < msg_len) {
                    ssize_t n = recv(client_fd, buffer.data() + received, msg_len - received, 0);
                    if (n <= 0) break;
                    received += n;
                }

                if (received != msg_len) {
                    std::cerr << "[TCP] Received only " << received
                              << " of " << msg_len << std::endl;
                    break;
                }

                StreamTaskMessage message = StreamTaskMessage::deserialize(buffer.data(), msg_len);

                switch (message.type) {
                    case StreamMessageType::RoutingTable_Res:
                        handle_RoutingTable_Res(message);
                        break;
                    case StreamMessageType::STARTTHREAD_REQ:
                        handle_StartThread_Req();
                        break;
                    case StreamMessageType::ROUTE_UPDATE_REQ:
                        send_RountinTable_Req();
                        break;
                    default:
                        std::cerr << "[RainStorm] Unknown message type: "
                                  << static_cast<int>(message.type) << std::endl;
                        break;
                }
            }

            socket.closeConnection(client_fd);

        }).detach();
    }
}

// ask rountin table from leader
void RainStorm::send_RountinTable_Req(){
    StreamTaskMessage message = {};
    message.type = StreamMessageType::RoutingTable_Req;
    message.stage_id = stage_id;
    std::strncpy(message.sender_host, self.host, sizeof(message.sender_host));
    message.sender_host[sizeof(message.sender_host) - 1] = '\0';

    std::strncpy(message.task_port, self.port, sizeof(message.task_port));
    message.task_port[sizeof(message.task_port) - 1] = '\0';

    send_tcp_message(leader.host, leader.port, message);
    // cout << "[RainStorm] Ask for routing table" << endl;
    logger.log("[RainStorm] Ask for routing table.");
}

// after receive routing table, notify the leader that this process is ready to start
void RainStorm::handle_RoutingTable_Res(const StreamTaskMessage& message) {

    std::unique_lock<std::mutex> lock(routing_mtx);

    next_stage = message.next_stage;
    num_next_tasks = message.num_next_tasks;

    if (next_stage == -1) {
        is_last_stage = true;
        logger.log("[RainStorm] RoutingTable_Res received.");
        std::cout << "[RainStorm] RoutingTable_Res received:\n";
        notifyLeaderReady();
        return;
    }
    else
        is_last_stage = false;

    for (int t = 0; t < num_next_tasks; t++) {
        std::strncpy(next_task_hosts[t],message.next_task_hosts[t], sizeof(next_task_hosts[t]));
        next_task_hosts[t][sizeof(next_task_hosts[t]) - 1] = '\0';

        std::strncpy(next_task_ports[t], message.next_task_ports[t], sizeof(next_task_ports[t]));
        next_task_ports[t][sizeof(next_task_ports[t]) - 1] = '\0';
    }
    logger.log("[RainStorm] RoutingTable_Res received.");
    std::cout << "[RainStorm] RoutingTable_Res received:\n";
    std::cout << "  Next stage: " << next_stage << "\n";
    std::cout << "  Num downstream tasks: " << num_next_tasks << "\n";

    for (int t = 0; t < num_next_tasks; t++) {
        std::cout << " -> host=" << next_task_hosts[t]
                  << " port=" << next_task_ports[t] << "\n";
    }

    std::cout << "--------------------------------------------------------\n";

    // ready after get the routing table
    notifyLeaderReady();
}

void RainStorm::notifyLeaderReady() {
    StreamTaskMessage message = {};
    message.type = StreamMessageType::READY;
    message.task_id = task_id;
    message.stage_id = stage_id;

    send_tcp_message(leader.host, leader.port, message);

    logger.log("[RainStorm] Sent READY to leader");
}

// only receive START_STREAM will the process start sending tuples to next stage
// incase the next process hasn't ready for receiving tuples
void RainStorm::handle_StartThread_Req(){
    {
        std::lock_guard<std::mutex> lock(start_mtx);
        stream_started = true;
    }
    start_cv.notify_all();
    logger.log("[RainStorm] Received START_STREAM");
}

void RainStorm::load_checkpoint_to_local(){
  
    StreamTaskMessage message = {};
    message.type = StreamMessageType::GET_CHECKPOINT;
    message.stage_id = stage_id;
    std::strncpy(message.src_dir, checkpoint_path.c_str(), sizeof(message.src_dir)); // reuse the var
    message.src_dir[sizeof(message.src_dir) - 1] = '\0';

    std::strncpy(message.dst_file, checkpoint_path.c_str(), sizeof(message.dst_file)); // reuse the var
    message.dst_file[sizeof(message.dst_file) - 1] = '\0';

    //ask the file system to download the checkpoint to local, if exist
    send_tcp_message(self.host, leader.port, message); //resue the port

    std::this_thread::sleep_for(std::chrono::milliseconds(1000)); // wait for the download
    
    string path = "./storage/local_storage/" + checkpoint_path;
    if (std::filesystem::exists(path)) {
        logger.log("[RainStorm] Load checkpoint");
        read_check_point(path);
    }
    else{
        logger.log("[RainStorm] no checkpoint");
    }

}

// void RainStorm::read_check_point(const std::string& file_path) {
 
//     // CRITICAL FIX 1: Must open file in binary mode
//     std::ifstream infile(file_path, std::ios::binary); 
//     if (!infile.is_open()) {
//         logger.log("ERROR: Failed to open checkpoint file: " + file_path);
//         return;
//     }

//     std::lock_guard<std::mutex> lock(processed_state_mtx);
//     processed_state.clear();
    
//     // Map to track committed inputs missing an OUTPUT_ACKED record
//     std::map<std::string, std::string> unacknowledged_inputs;

//     uint32_t len_net = 0;
    
//     // --- 1. Read the Log Record by Record (Length-Prefix Framing) ---
//     // Reads 4 bytes for the length of the next record
//     while (infile.read(reinterpret_cast<char*>(&len_net), sizeof(len_net))) {
        
//         // Convert length from network byte order to host byte order
//         uint32_t record_len = ntohl(len_net);

//         // Basic validation (Assuming a max tuple size limit)
//         if (record_len == 0 || record_len > TCPSocketConnection::BUFFER_LEN) {
//             logger.log("[RELOAD] Invalid record length found: " + std::to_string(record_len) + ". Stopping recovery.");
//             break;
//         }

//         // Read the actual content payload into a string buffer
//         std::string record_content(record_len, '\0');
//         // CRITICAL FIX 2: Use binary read for the content, ensuring all bytes are read
//         if (!infile.read(&record_content[0], record_len)) {
//             logger.log("[RELOAD] Failed to read full record content. Stopping recovery.");
//             break;
//         }

//         // --- 2. Parse the State and Data (e.g., "INPUT_COMMITTED:5|RawValueData") ---
        
//         size_t colon_pos = record_content.find(':');
//         size_t pipe_pos = record_content.find('|');

//         if (colon_pos != std::string::npos && pipe_pos != std::string::npos && colon_pos < pipe_pos) {
            
//             std::string command = record_content.substr(0, colon_pos);
//             std::string id_str = record_content.substr(colon_pos + 1, pipe_pos - colon_pos - 1);
//             // The value content starts immediately after the pipe
//             std::string value_content = record_content.substr(pipe_pos + 1);

            
//             if (command == "INPUT_COMMITTED") {
//                 // Store the raw value content needed to rebuild and re-send the tuple
//                 unacknowledged_inputs[id_str] = value_content;
//             } 
//             else if (command == "OUTPUT_ACKED") {
//                 logger.log("[RELOAD] processed_state add " + id_str);
//                 processed_state.insert(id_str);
//                 unacknowledged_inputs.erase(id_str);
//             }
//         } else {
//             logger.log("[RELOAD] Skipping corrupted log record. Data size: " + std::to_string(record_len));
//         }
//     }
    
//     // --- 3. Final Recovery Step: Re-enqueue Unacknowledged Inputs ---
    
//     for (const auto& pair : unacknowledged_inputs) {
//         const std::string& tuple_id_str = pair.first;
//         const std::string& value_content = pair.second;
        
//         logger.log("[RECOVERY] Re-enqueuing tuple_id=" + tuple_id_str );

//         // --- Tuple Construction ---
//         Tuple t;
        
//         t.tuple_id = std::stoi(tuple_id_str); 

//         // Determine filename part for key
//         std::string filename = src_dir;
//         size_t pos = filename.find_last_of('/');
//         if (pos != std::string::npos) {
//             filename = filename.substr(pos + 1); 
//         }
        
//         // Reconstruct key: filename + ID
//         std::string key_str = filename + ":" + tuple_id_str;
//         std::strncpy(t.key, key_str.c_str(), sizeof(t.key) - 1);
//         t.key[sizeof(t.key) - 1] = '\0'; 

//         // Reconstruct value
//         std::strncpy(t.value, value_content.c_str(), sizeof(t.value) - 1);
//         t.value[sizeof(t.value) - 1] = '\0';

//         // Reconstruct destination file, stage, and task IDs (relying on member variables)
//         std::strncpy(t.dst_file, dst_file.c_str(), sizeof(t.dst_file) - 1);
//         t.dst_file[sizeof(t.dst_file) - 1] = '\0';

//         t.stage_id = stage_id;
//         t.task_id  = task_id;

//         tuple_queue.push(t);
//     }
    
//     infile.close();
// }

// void RainStorm::read_check_point(const std::string& file_path){
//     std::ifstream infile(file_path);
//     if (!infile.is_open()) {
//         logger.log("ERROR: Failed to open file: " + file_path);
//         return;
//     }
    
//     // Clear the current state before reloading
//     std::lock_guard<std::mutex> lock(processed_state_mtx);
//     processed_state.clear();

//     std::string line;

//     // Read log line by line
//     while (std::getline(infile, line)) {
//         if (line.empty()) continue;

//         std::string command;
//         std::string id_str;
        
//         size_t colon_pos = line.find(':');
//         if (colon_pos != std::string::npos) {
//             command = line.substr(0, colon_pos);
//             // Trim command 
//             command.erase(0, command.find_first_not_of(" \n\r\t"));
//             command.erase(command.find_last_not_of(" \n\r\t") + 1);

//             size_t id_start_pos = colon_pos + 1;
//             // Trim leading whitespace and get the rest of the string as the ID
//             while (id_start_pos < line.length() && std::isspace(line[id_start_pos])) {
//                 id_start_pos++;
//             }
            
//             id_str = line.substr(id_start_pos);
//             // Trim trailing whitespace/newline from the ID string
//             id_str.erase(id_str.find_last_not_of(" \n\r\t") + 1);

//         } else {
//             logger.log("WARNING: Skipping non-log line: " + line.substr(0, std::min((size_t)30, line.length())) + "...");
//             continue;
//         }

//         // Only store IDs that have been OUTPUT_ACKED (i.e., fully processed)
//         if (command == "OUTPUT_ACKED") {
//             logger.log("[RELOAD] processed_state add " + id_str);
//             processed_state.insert(id_str);
//         }
//         // NOTE: INPUT_COMMITTED entries are ignored, as we only track completion.
//     }
    
//     infile.close();
// }

void RainStorm::read_check_point(const std::string& file_path) {
 
    std::ifstream infile(file_path);
    if (!infile.is_open()) {
        logger.log("ERROR: Failed to open checkpoint file: " + file_path);
        return;
    }

    // Prepare state tracking for recovery
    std::lock_guard<std::mutex> lock(processed_state_mtx);
    processed_state.clear();
    
    // This maps committed inputs that need to be re-sent upon failure.
    std::map<std::string, std::string> unacknowledged_inputs;

    std::string record_content;
    
    while (std::getline(infile, record_content)) {
        

        // Parse the State and Data (e.g., "INPUT_COMMITTED:5|RawValueData") ---

        size_t colon_pos = record_content.find(':');
        
        if (colon_pos == std::string::npos) {
            logger.log("[RELOAD] Skipping record with no command/ID separator.");
            continue;
        }
        
        std::string command = record_content.substr(0, colon_pos);
        std::string id_content = record_content.substr(colon_pos + 1); // The rest of the string


        if (command == "INPUT_COMMITTED") {
            // EXPECTED FORMAT: ID|VALUE_CONTENT
            size_t pipe_pos = id_content.find('|');

            if (pipe_pos != std::string::npos) {
                // ID is before the pipe
                std::string id_str = id_content.substr(0, pipe_pos);
                // Value content is after the pipe
                std::string value_content = id_content.substr(pipe_pos + 1);
                unacknowledged_inputs[id_str] = value_content;
                
            } else {
                logger.log("[RELOAD] INPUT_COMMITTED record is missing pipe separator: " + id_content);
            }

        } else if (command == "OUTPUT_ACKED") {
            std::string id_str = id_content; 
            
            logger.log("[RELOAD] processed_state add " + id_str);
            processed_state.insert(id_str);
            unacknowledged_inputs.erase(id_str);
            
        } else {
            logger.log("[RELOAD] Unknown command found: " + command);
        }
    }
    
    // Any entry remaining in unacknowledged_inputs needs to be re-forwarded.
    for (const auto& pair : unacknowledged_inputs) {
        const std::string& tuple_id = pair.first;
        const std::string& value_content = pair.second;
        
        logger.log("[RECOVERY] Re-enqueuing tuple_id=" + tuple_id );

        std::string filename = src_dir;
        size_t pos = filename.find_last_of('/');
        if (pos != std::string::npos) {
            filename = filename.substr(pos + 1); // e.g., dataset1.csv
        }
        
        Tuple t;

        t.tuple_id = std::stoi(tuple_id);

        std::string key_str = filename + ":" + tuple_id;
        std::strncpy(t.key, key_str.c_str(), sizeof(t.key) - 1);

        // Build value = full raw line from CSV
        std::strncpy(t.value, value_content.c_str(), sizeof(t.value) - 1);

        std::strncpy(t.dst_file, dst_file.c_str(), sizeof(t.dst_file) - 1);

        t.stage_id = stage_id;
        t.task_id  = task_id;

        tuple_queue.push(t);

    }
    
    infile.close();
}