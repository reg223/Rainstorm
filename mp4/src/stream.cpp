#include "stream.hpp"

#include "message.hpp"
#include "shared.hpp"
#include "socket.hpp"

using namespace std;

StreamSystem::StreamSystem(NodeId& introducer, const std::string& host)
    : stream_socket(host, STREAM_PORT), leader(introducer){

    std::strncpy(self.host, host.c_str(), sizeof(self.host));
    self.host[sizeof(self.host) - 1] = '\0';
    std::strncpy(self.port, STREAM_PORT, sizeof(self.port));
    self.port[sizeof(self.port) - 1] = '\0';
    stream_socket.initializeTCPServer();
}

void StreamSystem::send_tcp_message(const string target_host, const string target_port, const StreamTaskMessage& message) {
    const int MAX_RETRIES = 3;
    const std::chrono::milliseconds RETRY_DELAY(100); // 100ms delay

    std::array<char, TCPSocketConnection::BUFFER_LEN> buffer;
    size_t bytes_serialized = message.serialize(buffer.data(), buffer.size());

    // 1. Pre-calculate the length prefix (always the same for this message)
    uint32_t len = htonl(static_cast<uint32_t>(bytes_serialized));

    for (int i = 0; i < MAX_RETRIES; ++i) {
        int sock = -1;
        
        // --- A. Attempt Connection ---
        sock = stream_socket.connectToServer(target_host, target_port);
        if (sock < 0) {
            std::cerr << "[StreamSystem] Failed to connect to " << target_host << ":" << target_port 
                      << " (Attempt " << i + 1 << "/" << MAX_RETRIES << ")" << std::endl;
            // Wait and try again in the next loop iteration
            std::this_thread::sleep_for(RETRY_DELAY);
            continue;
        }

        // --- B. Attempt Send ---
        bool success = true;

        // 2. Send length header
        if (send(sock, &len, sizeof(len), 0) != sizeof(len)) {
            perror("[StreamSystem] send length failed");
            success = false;
        }

        // 3. Send message body
        if (success) {
            ssize_t written = send(sock, buffer.data(), bytes_serialized, 0);
            // Check 1: Check for error (written < 0)
            // Check 2: Check if the amount written (cast to unsigned) matches the expected size.
            if (written < 0 || (size_t)written != bytes_serialized) {
                perror("[StreamSystem] send message failed or incomplete");
                success = false;
            }
        }
        
        // --- C. Handle Result ---
        stream_socket.closeConnection(sock); // Always close the socket before continuing

        if (success) {
            // Message successfully sent and connection closed. We are done.
            return; 
        }

        // If sending failed, wait and retry (or if connection failed above)
        std::this_thread::sleep_for(RETRY_DELAY);
    }

    // If the loop completes without success
    std::cerr << "[StreamSystem] FATAL: Failed to send message after " << MAX_RETRIES << " attempts." << std::endl;
}

void StreamSystem::startStream(const std::vector<MembershipInfo>& members, int NSTAGES, int NTASKS_per_stage, 
                            const std::vector<std::string>& op_exec, const std::vector<std::string>& op_args,
                            const std::string& src_dir, const std::string& dst_file,
                            bool exactly_once, bool autoscale_enabled_flag, int INPUT_RATE, int LW, int HW ){
    Nstages = NSTAGES;
    Ntasks_per_stage = NTASKS_per_stage;
    op_exec_ = op_exec;
    op_args_ = op_args;
    source_file = src_dir;
    exactly_once_enabled = exactly_once;
    autoscale_enabled = autoscale_enabled_flag;
    input_rate = INPUT_RATE;
    low_watermark = LW;
    high_watermark = HW;
    target_file = dst_file;
    delete_task_count = 0;

    // Filter out the leader from the worker list
    std::vector<string> workers_host;
    for (const auto& m : members) {
        if (std::strcmp(m.node_id.host, leader.host)!=0) {
            workers_host.push_back(m.node_id.host);
        }
    }
    if (workers_host.empty()) {
        std::cerr << "[StreamSystem] ERROR: No available worker VMs (leader is only node!)\n";
        return;
    }

    // distribute task across the vm
    for (int stage = 0; stage < Nstages; stage++) {
        stage_task_counts.push_back(Ntasks_per_stage);
        for (int t = 0; t < Ntasks_per_stage; t++) {
            int task_id = stage * Ntasks_per_stage + t;
            int port_num = PORT_OFFSET + task_id;
            string vm_host = workers_host[task_id % workers_host.size()];
            string vm_port = std::to_string(port_num);
            
            assignments.push_back({task_id, stage, vm_host, vm_port});
        }
    }


    // routing tuple port should be different from vm port
    routing_table.clear();
    routing_table.resize(Nstages);  // only worker stages
    for (int s = 0; s < Nstages; s++)
        routing_table[s].resize(Ntasks_per_stage);
    for (const auto& a : assignments) {
        int task_idx = a.task_id % Ntasks_per_stage;
        int tuple_port_num = TUPLE_PORT_OFFSET + a.task_id;
        routing_table[a.stage_id][task_idx] = NodeId::createNewNode(a.host, std::to_string(tuple_port_num));
    }


    // Launch StreamSystem task processes on each VM
    for (const auto& a : assignments) {
        
        StreamTaskMessage config = {};
        config.type = StreamMessageType::STARTTASK_REQ;
        config.task_id = a.task_id;
        config.stage_id = a.stage_id;
        config.exactly_once = exactly_once;

        std::strncpy(config.task_port, a.port.c_str(), sizeof(config.task_port));
        config.task_port[sizeof(config.task_port) - 1] = '\0';

        std::strncpy(config.operator_exec, op_exec[a.stage_id].c_str(), sizeof(config.operator_exec));
        config.operator_exec[sizeof(config.operator_exec) - 1] = '\0';

        std::strncpy(config.operator_args, op_args[a.stage_id].c_str(), sizeof(config.operator_args));
        config.operator_args[sizeof(config.operator_args) - 1] = '\0';

        std::strncpy(config.src_dir, src_dir.c_str(), sizeof(config.src_dir));
        config.src_dir[sizeof(config.src_dir) - 1] = '\0';

        std::strncpy(config.dst_file, dst_file.c_str(), sizeof(config.dst_file));
        config.dst_file[sizeof(config.dst_file) - 1] = '\0';

        send_tcp_message(a.host, STREAM_PORT, config);
    }

    StreamTaskMessage leader_config = {};
    leader_config.type = StreamMessageType::STARTTASK_REQ;
    leader_config.task_id = -1;       // special identifier
    leader_config.stage_id = -1;        // stage -1 = input source
    leader_config.exactly_once = exactly_once;

    string leader_port_num = std::to_string(PORT_OFFSET + leader_config.task_id);
    std::strncpy(leader_config.task_port, leader_port_num.c_str(), sizeof(leader_config.task_port));
    leader_config.task_port[sizeof(leader_config.task_port) - 1] = '\0';

    std::strncpy(leader_config.operator_exec, "SOURCE", sizeof(leader_config.operator_exec));
    leader_config.operator_exec[sizeof(leader_config.operator_exec) - 1] = '\0';

    std::strncpy(leader_config.operator_args, "NONE", sizeof(leader_config.operator_args));
    leader_config.operator_args[sizeof(leader_config.operator_args) - 1] = '\0';

    std::strncpy(leader_config.src_dir, src_dir.c_str(), sizeof(leader_config.src_dir));
    leader_config.src_dir[sizeof(leader_config.src_dir) - 1] = '\0';

    std::strncpy(leader_config.dst_file, dst_file.c_str(), sizeof(leader_config.dst_file));
    leader_config.dst_file[sizeof(leader_config.dst_file) - 1] = '\0';

    assignments.push_back({leader_config.task_id, leader_config.stage_id, leader.host, leader_port_num});
    send_tcp_message(leader.host, STREAM_PORT, leader_config);

}

void StreamSystem::handle_StartTask_Req(const StreamTaskMessage& config) {
    // std::cout << "[StreamSystem] Starting Stream Task " << config.task_id << " (stage " << config.stage_id << ")" << endl;

    // Convert config fields into a command-line string
    std::stringstream cmd;

    cmd << "./build/stream_main "
        << config.task_id << " "
        << config.stage_id << " "
        << config.task_port << " "
        << config.operator_exec << " "
        << config.operator_args << " "
        << config.src_dir << " "
        << config.dst_file << " "
        << int(config.exactly_once);

    std::string command = cmd.str();
    std::cout << "[StreamSystem] Executing: " << command << std::endl;

    pid_t pid = fork();
    if (pid < 0) {
        perror("[StreamSystem] fork failed");
        return;
    }
    if (pid == 0) {
        execl("/bin/sh", "sh", "-c", command.c_str(), (char*)nullptr);
        perror("[StreamSystem] exec failed");
        exit(1);
    }

    // record task info in StreamSystem
    TaskInfo info;
    info.task_id = config.task_id;
    info.stage_id = config.stage_id;
    info.vm = self.host;                    // leader’s VM = where task runs
    info.port = config.task_port;
    info.operator_exec = config.operator_exec;
    info.operator_args = config.operator_args;
    info.src_dir = config.src_dir;
    info.dst_file = config.dst_file;
    info.exactly_once = config.exactly_once;
    info.pid = pid;

    tasks[config.task_id] = info;

    // parent continues
    // std::cout << "[StreamSystem] Task process PID = " << std::to_string(pid) << std::endl;

}

void StreamSystem::handle_RoutingTable_Req(const StreamTaskMessage& message){
    StreamTaskMessage reply = {};
    reply.type = StreamMessageType::RoutingTable_Res;
    reply.next_stage = (message.stage_id + 1 < Nstages) ? message.stage_id + 1 : -1;
    reply.num_next_tasks = (message.stage_id + 1 < Nstages) ? stage_task_counts[message.stage_id + 1] : 0; //TODO: adjust the dynamic #tasks
    for (int t = 0; t < reply.num_next_tasks; t++) {
        NodeId dest = routing_table[message.stage_id + 1][t];
        std::strncpy(reply.next_task_hosts[t], dest.host, sizeof(reply.next_task_hosts[t]));
        reply.next_task_hosts[t][sizeof(reply.next_task_hosts[t]) - 1] = '\0';
        std::strncpy(reply.next_task_ports[t], dest.port, sizeof(reply.next_task_ports[t]));
        reply.next_task_ports[t][sizeof(reply.next_task_ports[t]) - 1] = '\0';
    }
    send_tcp_message(message.sender_host, message.task_port, reply);
    cout << "[StreamSystem] Send routing table to " << message.sender_host << ":" << message.task_port << endl;
}

// the leader have to makesure global synchronization
void StreamSystem::handle_Ready_Req(const StreamTaskMessage& message){
    ready_tasks.insert(message.task_id);
    // cout << "[StreamSystem] Ready progress " << ready_tasks.size() << "/" << assignments.size() << endl;
    if (ready_tasks.size() == (assignments.size())){
        for (const auto& a : assignments) {
        
            StreamTaskMessage reply = {};
            reply.type = StreamMessageType::STARTTHREAD_REQ;
            send_tcp_message(a.host, a.port, reply);
            
        }
        cout << "[StreamSystem] Broadcast STARTTHREAD_REQ" << endl;
        
    }
    
}

void StreamSystem::list_tasks(const std::vector<MembershipInfo>& members) {
    for (const auto& m : members) {
        StreamTaskMessage message = {};
        message.type = StreamMessageType::LIST_REQ;
        send_tcp_message(m.node_id.host, STREAM_PORT, message);
    }
}

void StreamSystem::handle_List_Req(){
    std::cout << "------ Task List ------\n";
    for (const auto& task : tasks) {
        const int id = task.first;
        const TaskInfo& t = task.second;

        std::cout << "Task " << id << ":\n"
                  << "  VM:       " << t.vm << "\n"
                  << "  stage_id: " << t.stage_id << "\n"
                  << "  task_id:  " << t.task_id << "\n"
                  << "  PID:      " << t.pid << "\n"
                  << "  Exec:     " << t.operator_exec << "\n"
                  << "  Port:     " << t.port << "\n";
    }

    std::cout << "-----------------------\n";
}

void StreamSystem::kill_restart_task(const std::string vm_id, const std::string task_id){
    // no need to update routing_table, cuz the it task will be restart at the same pot
    StreamTaskMessage message = {};
    message.type = StreamMessageType::KILL_RESTART_REQ;
    std::string host = "fa25-cs425-53" + vm_id + ".cs.illinois.edu";
    message.task_id = std::stoi(task_id);;
    ready_tasks.erase(message.task_id);
    send_tcp_message(host, STREAM_PORT, message);
}

void StreamSystem::handle_kill_restart_req(const StreamTaskMessage& message){

    std::stringstream kill_cmd;

    kill_cmd << "kill -9 " << std::to_string(tasks[message.task_id].pid);

    std::string kill_command = kill_cmd.str();
    std::cout << "[StreamSystem] Executing: " << kill_command << std::endl;
    pid_t _pid = fork();
    if (_pid < 0) {
        perror("[StreamSystem] fork failed");
        return;
    }
    if (_pid == 0) {
        execl("/bin/sh", "sh", "-c", kill_command.c_str(), (char*)nullptr);
        perror("[StreamSystem] exec failed");
        exit(1);
    }

    // restart the failed task, which should resume its work in that stage.
    cout << "[StreamSystem] Restart task " << tasks[message.task_id].task_id << endl;
    std::stringstream cmd;

    cmd << "./build/stream_main "
        << tasks[message.task_id].task_id << " "
        << tasks[message.task_id].stage_id << " "
        << tasks[message.task_id].port << " "
        << tasks[message.task_id].operator_exec << " "
        << tasks[message.task_id].operator_args << " "
        << tasks[message.task_id].src_dir << " "
        << tasks[message.task_id].dst_file << " "
        << int(tasks[message.task_id].exactly_once);

    std::string command = cmd.str();
    std::cout << "[StreamSystem] Executing: " << command << std::endl;

    pid_t pid = fork();
    if (pid < 0) {
        perror("[StreamSystem] fork failed");
        return;
    }
    if (pid == 0) {
        execl("/bin/sh", "sh", "-c", command.c_str(), (char*)nullptr);
        perror("[StreamSystem] exec failed");
        exit(1);
    }

    // update with new pid
    tasks[message.task_id].pid = pid;
}

void StreamSystem::handle_kill_req(const StreamTaskMessage& message){

    std::stringstream kill_cmd;

    kill_cmd << "kill -9 " << std::to_string(tasks[message.task_id].pid);

    std::string kill_command = kill_cmd.str();
    std::cout << "[StreamSystem] Executing: " << kill_command << std::endl;
    pid_t _pid = fork();
    if (_pid < 0) {
        perror("[StreamSystem] fork failed");
        return;
    }
    if (_pid == 0) {
        execl("/bin/sh", "sh", "-c", kill_command.c_str(), (char*)nullptr);
        perror("[StreamSystem] exec failed");
        exit(1);
    }

    // delete the pid
    auto it = tasks.find(message.task_id);
    tasks.erase(it);
}

void StreamSystem::reset(const std::vector<MembershipInfo>& members) {

    Nstages = 0;
    Ntasks_per_stage = 0;
    routing_table.clear();
    assignments.clear();  
    ready_tasks.clear();  


    stage_task_counts.clear();
    delete_task_count = 0;
    
    // Clear dynamic load reporting data
    {
        std::lock_guard<std::mutex> lock(task_load_mtx);
        stage_task_loads.clear();
    }

    // 3. Reset Configuration and Flags
    op_exec_.clear();
    op_args_.clear();

    exactly_once_enabled = false;
    autoscale_enabled = false;
    input_rate = 0;
    low_watermark = 0;
    high_watermark = 0;
    source_file.clear();
    target_file.clear();

    for (const auto& m : members) {
        StreamTaskMessage message = {};
        message.type = StreamMessageType::RESET_REQ;
        send_tcp_message(m.node_id.host, STREAM_PORT, message);
    }

    std::cout << "[StreamSystem] System state successfully reset. Ready for new stream configuration.\n";
}

void StreamSystem::handle_reset_req(){
    std::stringstream kill_cmd;

    kill_cmd << "pkill -f stream_main";

    std::string kill_command = kill_cmd.str();
    std::cout << "[StreamSystem] Executing: " << kill_command << std::endl;
    pid_t _pid = fork();
    if (_pid < 0) {
        perror("[StreamSystem] fork failed");
        return;
    }
    if (_pid == 0) {
        execl("/bin/sh", "sh", "-c", kill_command.c_str(), (char*)nullptr);
        perror("[StreamSystem] exec failed");
        exit(1);
    }

    std::stringstream kill_cmd_2;
    kill_cmd_2 << "rm -rf storage/HyDFS_storage/*checkpoint.log";

    std::string kill_command_2 = kill_cmd_2.str();
    std::cout << "[StreamSystem] Executing: " << kill_command_2 << std::endl;
    pid_t _pid2 = fork();
    if (_pid2 < 0) {
        perror("[StreamSystem] fork failed");
        return;
    }
    if (_pid2 == 0) {
        execl("/bin/sh", "sh", "-c", kill_command_2.c_str(), (char*)nullptr);
        perror("[StreamSystem] exec failed");
        exit(1);
    }

    Nstages = 0;
    Ntasks_per_stage = 0;
    routing_table.clear();
    assignments.clear();  
    ready_tasks.clear();  


    stage_task_counts.clear();
    delete_task_count = 0;
    
    op_exec_.clear();
    op_args_.clear();

    exactly_once_enabled = false;
    autoscale_enabled = false;
    input_rate = 0;
    low_watermark = 0;
    high_watermark = 0;
    source_file.clear();
    target_file.clear();
}

size_t StreamTaskMessage::serialize(char* buffer, const size_t buffer_size) const {
    // 1. Calculate the variable size of the downstream task arrays
    // This size depends on the actual number of tasks, NOT the maximum of 16.
    size_t hosts_data_size = num_next_tasks * sizeof(next_task_hosts[0]);
    size_t ports_data_size = num_next_tasks * sizeof(next_task_ports[0]);

    // 2. Calculate the total needed size (Fixed fields + Variable fields)
    size_t needed = sizeof(StreamMessageType) + 
                    sizeof(int) * 2 + // task_id, stage_id
                    sizeof(task_port) + 
                    sizeof(operator_exec) + 
                    sizeof(operator_args) + 
                    sizeof(src_dir) + 
                    sizeof(dst_file) + 
                    sizeof(bool) + 
                    sizeof(sender_host) + 
                    sizeof(input_load) + 
                    sizeof(int) * 2 + // next_stage, num_next_tasks
                    hosts_data_size + 
                    ports_data_size;


    if (needed > buffer_size)
        throw std::runtime_error("Buffer too small for StreamTaskMessage");
    
    // --- Check boundary condition: must not exceed array max size ---
    if (num_next_tasks > 16 || num_next_tasks < 0) {
        throw std::runtime_error("Invalid num_next_tasks value during serialization.");
    }

    size_t offset = 0;

    // --- Fixed Fields (1 byte Type, 2x4 bytes Ints, 5x Char Arrays, 1 byte Bool, 1x Char Array)
    uint8_t type_byte = static_cast<uint8_t>(type);
    std::memcpy(buffer + offset, &type_byte, sizeof(type_byte));
    offset += sizeof(type_byte);

    int net_task_id = htonl(task_id);
    std::memcpy(buffer + offset, &net_task_id, sizeof(net_task_id));
    offset += sizeof(net_task_id);

    int net_stage_id = htonl(stage_id);
    std::memcpy(buffer + offset, &net_stage_id, sizeof(net_stage_id));
    offset += sizeof(net_stage_id);

    std::memcpy(buffer + offset, task_port, sizeof(task_port));
    offset += sizeof(task_port);

    std::memcpy(buffer + offset, operator_exec, sizeof(operator_exec));
    offset += sizeof(operator_exec);

    std::memcpy(buffer + offset, operator_args, sizeof(operator_args));
    offset += sizeof(operator_args);

    std::memcpy(buffer + offset, src_dir, sizeof(src_dir));
    offset += sizeof(src_dir);

    std::memcpy(buffer + offset, dst_file, sizeof(dst_file));
    offset += sizeof(dst_file);

    std::memcpy(buffer + offset, &exactly_once, sizeof(exactly_once)); // Bool is 1 byte, usually fine without network conversion
    offset += sizeof(exactly_once);

    std::memcpy(buffer + offset, sender_host, sizeof(sender_host));
    offset += sizeof(sender_host);

    int net_input_load = htonl(input_load);
    std::memcpy(buffer + offset, &net_input_load, sizeof(net_input_load));
    offset += sizeof(net_input_load);

    int net_next_stage = htonl(next_stage);
    std::memcpy(buffer + offset, &net_next_stage, sizeof(net_next_stage));
    offset += sizeof(net_next_stage);

    int net_num_next_tasks = htonl(num_next_tasks);
    std::memcpy(buffer + offset, &net_num_next_tasks, sizeof(net_num_next_tasks));
    offset += sizeof(net_num_next_tasks);

    // --- Variable Fields (next_task_hosts)
    // ONLY copy the number of entries specified by num_next_tasks
    std::memcpy(buffer + offset, next_task_hosts, hosts_data_size);
    offset += hosts_data_size;

    // --- Variable Fields (next_task_ports)
    // ONLY copy the number of entries specified by num_next_tasks
    std::memcpy(buffer + offset, next_task_ports, ports_data_size);
    offset += ports_data_size;

    return offset;
}


StreamTaskMessage StreamTaskMessage::deserialize(const char* buffer, const size_t buffer_size) {
    StreamTaskMessage msg = {}; // IMPORTANT: Zero-initialize unused fields
    size_t offset = 0;

    // Helper macro for size checks (to reduce repetition)
    #define CHECK_SIZE(T) if (offset + sizeof(T) > buffer_size) throw std::runtime_error("Buffer too small for " #T)

    // --- Fixed Fields (Read in the exact same order as serialize)
    CHECK_SIZE(uint8_t);
    uint8_t type_byte;
    std::memcpy(&type_byte, buffer + offset, sizeof(type_byte));
    msg.type = static_cast<StreamMessageType>(type_byte);
    offset += sizeof(type_byte);

    CHECK_SIZE(int);
    int net_task_id;
    std::memcpy(&net_task_id, buffer + offset, sizeof(net_task_id));
    msg.task_id = ntohl(net_task_id);
    offset += sizeof(net_task_id);

    CHECK_SIZE(int);
    int net_stage_id;
    std::memcpy(&net_stage_id, buffer + offset, sizeof(net_stage_id));
    msg.stage_id = ntohl(net_stage_id);
    offset += sizeof(net_stage_id);

    CHECK_SIZE(msg.task_port);
    std::memcpy(msg.task_port, buffer + offset, sizeof(msg.task_port));
    offset += sizeof(msg.task_port);

    CHECK_SIZE(msg.operator_exec);
    std::memcpy(msg.operator_exec, buffer + offset, sizeof(msg.operator_exec));
    offset += sizeof(msg.operator_exec);

    CHECK_SIZE(msg.operator_args);
    std::memcpy(msg.operator_args, buffer + offset, sizeof(msg.operator_args));
    offset += sizeof(msg.operator_args);

    CHECK_SIZE(msg.src_dir);
    std::memcpy(msg.src_dir, buffer + offset, sizeof(msg.src_dir));
    offset += sizeof(msg.src_dir);

    CHECK_SIZE(msg.dst_file);
    std::memcpy(msg.dst_file, buffer + offset, sizeof(msg.dst_file));
    offset += sizeof(msg.dst_file);

    CHECK_SIZE(msg.exactly_once);
    std::memcpy(&msg.exactly_once, buffer + offset, sizeof(msg.exactly_once));
    offset += sizeof(msg.exactly_once);

    CHECK_SIZE(msg.sender_host);
    std::memcpy(msg.sender_host, buffer + offset, sizeof(msg.sender_host));
    offset += sizeof(msg.sender_host);

    CHECK_SIZE(int);
    int net_input_load;
    std::memcpy(&net_input_load, buffer + offset, sizeof(net_input_load));
    msg.input_load = ntohl(net_input_load);
    offset += sizeof(net_input_load);

    CHECK_SIZE(int);
    int net_next_stage;
    std::memcpy(&net_next_stage, buffer + offset, sizeof(net_next_stage));
    msg.next_stage = ntohl(net_next_stage);
    offset += sizeof(net_next_stage);

    CHECK_SIZE(int);
    int net_num_next_tasks;
    std::memcpy(&net_num_next_tasks, buffer + offset, sizeof(net_num_next_tasks));
    msg.num_next_tasks = ntohl(net_num_next_tasks);
    offset += sizeof(net_num_next_tasks);

    // --- Variable Fields (MUST use num_next_tasks to determine size)

    // Check boundary condition: must not exceed array max size
    if (msg.num_next_tasks > 16 || msg.num_next_tasks < 0) {
        throw std::runtime_error("Invalid num_next_tasks value during deserialization.");
    }
    
    // Calculate the size we actually need to read for hosts
    size_t hosts_data_size = msg.num_next_tasks * sizeof(msg.next_task_hosts[0]);
    if (offset + hosts_data_size > buffer_size)
        // THIS IS THE CRITICAL FIX: The size check now relies on the communicated num_next_tasks
        throw std::runtime_error("Buffer too small for next_task_hosts");

    std::memcpy(msg.next_task_hosts, buffer + offset, hosts_data_size);
    offset += hosts_data_size;


    // Calculate the size we actually need to read for ports
    size_t ports_data_size = msg.num_next_tasks * sizeof(msg.next_task_ports[0]);
    if (offset + ports_data_size > buffer_size)
        throw std::runtime_error("Buffer too small for next_task_ports");
        
    std::memcpy(msg.next_task_ports, buffer + offset, ports_data_size);
    offset += ports_data_size;

    return msg;

    #undef CHECK_SIZE
}


