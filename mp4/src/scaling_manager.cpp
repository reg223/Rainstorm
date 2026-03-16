#include "stream.hpp"

#include "message.hpp"
#include "shared.hpp"
#include "socket.hpp"


void StreamSystem::handle_load_report(const StreamTaskMessage& message){
    // std::cout << "[LOAD_REPORT] Stage " << message.stage_id 
    //           << ", Task " << message.task_id 
    //           << " reported load: " << message.input_load << " tuples/sec." << std::endl;
    std::lock_guard<std::mutex> lock(task_load_mtx);
    stage_task_loads[message.stage_id][message.task_id] = message.input_load;
}

void StreamSystem::increase_task(const std::vector<MembershipInfo>& members, const int stage_id){

    std::vector<std::string> workers_host;
    for (const auto& m : members) {
        if (std::strcmp(m.node_id.host, leader.host)!=0) {
            workers_host.push_back(m.node_id.host);
        }
    }
    if (workers_host.empty()) {
        std::cerr << "[StreamSystem] WARNING: Cannot increase task - No available worker VMs.\n";
        return;
    }

    int task_id = assignments.size();
    int port_num = PORT_OFFSET + task_id + delete_task_count; // avoid repeat id after increase and delete
    
    // VM Assignment (Round-Robin, as per the original startStream logic using task_id)
    std::string vm_host = workers_host[task_id % workers_host.size()]; 
    std::string vm_port = std::to_string(port_num);
    
    // Update the StreamSystem state
    TaskAssignment new_assignment = {task_id, stage_id, vm_host, vm_port};
    assignments.push_back(new_assignment);

    std::cout << "[StreamSystem] INFO: New task assigned (Task ID: " << task_id 
          << ", Stage ID: " << stage_id << ") to VM: " << vm_host << ":" << vm_port << " (Control Port).\n";

    // NOTE: The tuple routing port must be different from the task's control port.
    int tuple_port_num = TUPLE_PORT_OFFSET + task_id; 
    
    // Add the new task to the stage's routing table vector
    routing_table[stage_id].push_back(NodeId::createNewNode(vm_host, std::to_string(tuple_port_num)));
    stage_task_counts[stage_id] += 1;

    // Send STARTTASK_REQ to the new worker
    StreamTaskMessage config = {};
    config.type = StreamMessageType::STARTTASK_REQ;
    config.task_id = task_id;
    config.stage_id = stage_id;
    config.exactly_once = exactly_once_enabled;

    std::strncpy(config.task_port, vm_port.c_str(), sizeof(config.task_port));
    config.task_port[sizeof(config.task_port) - 1] = '\0';

    std::strncpy(config.operator_exec, op_exec_[stage_id].c_str(), sizeof(config.operator_exec));
    config.operator_exec[sizeof(config.operator_exec) - 1] = '\0';

    std::strncpy(config.operator_args, op_args_[stage_id].c_str(), sizeof(config.operator_args));
    config.operator_args[sizeof(config.operator_args) - 1] = '\0';

    std::strncpy(config.src_dir, source_file.c_str(), sizeof(config.src_dir));
    config.src_dir[sizeof(config.src_dir) - 1] = '\0';

    std::strncpy(config.dst_file, target_file.c_str(), sizeof(config.dst_file));
    config.dst_file[sizeof(config.dst_file) - 1] = '\0';

    send_tcp_message(vm_host, STREAM_PORT, config);

    // 5. Inform the previous stage tasks to update their routing table
    if (stage_id >= 0) {
        update_preStage_RoutingTable(stage_id - 1);
    }
}

void StreamSystem::update_preStage_RoutingTable(const int prev_stage_id){
    StreamTaskMessage update_msg = {};
    update_msg.type = StreamMessageType::ROUTE_UPDATE_REQ;
    update_msg.stage_id = prev_stage_id; 

    // Find all assignments for the previous stage and send the request
    if (prev_stage_id==-1){
        send_tcp_message(leader.host, "8999", update_msg); 
        // std::cout << "[StreamSystem] INFO: Sent ROUTE_UPDATE_REQ to stage -1" << std::endl;
    }
    else{
        for (const auto& a : assignments) {
            if (a.stage_id == prev_stage_id) {
                send_tcp_message(a.host, a.port, update_msg); 
                // std::cout << "[StreamSystem] INFO: Sent ROUTE_UPDATE_REQ to stage " << prev_stage_id 
                //         << " task at " << a.host << ":" << a.port << std::endl;
            }
        }
    }
    
}

void StreamSystem::decrease_task(const int stage_id){

    // 1. Validate the stage and task count
    if (stage_task_counts[stage_id] <= 1) {
        std::cerr << "[StreamSystem] WARNING: Cannot decrease task - Minimum tasks (1) reached for stage " << stage_id << ".\n";
        return;
    }

    TaskAssignment del_task;
    int index_to_remove = -1;
    bool found = false;

    NodeId task_to_remove = routing_table[stage_id].back();
    int task_id_to_remove = std::stoi(task_to_remove.port) - TUPLE_PORT_OFFSET;

    for (size_t i = 0; i < assignments.size(); ++i) {
        if (assignments[i].task_id == task_id_to_remove) {
            del_task = assignments[i];
            index_to_remove = i;
            found = true;
            break;
        }
    }
    
    if (!found) {
        std::cerr << "[StreamSystem] ERROR: Cannot find task to delete for stage " << stage_id << ".\n";
        return;
    }

    routing_table[stage_id].pop_back(); 
    stage_task_counts[stage_id]--;
    delete_task_count += 1;

    std::cout << "[StreamSystem] INFO: Kill task (Task ID: " << del_task.task_id 
              << ", Stage ID: " << stage_id << ") on VM: " << del_task.host << ":" << del_task.port << ".\n";

    if (stage_id >= 0) {
        update_preStage_RoutingTable(stage_id - 1); 
    }
    
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));

    StreamTaskMessage message = {};
    message.type = StreamMessageType::KILL_REQ;
    message.task_id = del_task.task_id;
    send_tcp_message(del_task.host, STREAM_PORT, message);

    assignments.erase(assignments.begin() + index_to_remove);
}
