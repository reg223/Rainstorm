#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <unordered_set>
#include <unordered_map>
#include <array>
#include <chrono>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <thread>
#include <unistd.h>   
#include <sstream>
#include <sys/types.h>

#include "logger.hpp"
#include "message.hpp"
#include "shared.hpp"
#include "tcp_socket.hpp"
#include "tuple.hpp"

#define STREAM_PORT "8082"
#define TUPLE_PORT "8083"
#define PORT_OFFSET 9000
#define TUPLE_PORT_OFFSET 9500

enum class StreamMessageType : uint8_t {
    STARTTASK_REQ, STARTTASK_RES, RoutingTable_Req, RoutingTable_Res,
    LIST_REQ, READY, STARTTHREAD_REQ, TUPLE_APPEND, GET_CHECKPOINT,
    KILL_RESTART_REQ, LOAD_REPORT, ROUTE_UPDATE_REQ, KILL_REQ, RESET_REQ
};
struct TaskAssignment {
    int task_id;
    int stage_id;
    std::string host;
    std::string port;
};

struct TaskInfo {
    int task_id;
    int stage_id;
    std::string vm;
    std::string port;
    std::string operator_exec;
    std::string operator_args;
    std::string src_dir;
    std::string dst_file;
    int exactly_once;
    pid_t pid;
    std::string log_file;
};

struct StreamTaskMessage {
    StreamMessageType type;
    int task_id;
    int stage_id;
    char task_port[16];  
    char operator_exec[32];
    char operator_args[64];
    char src_dir[128];
    char dst_file[128];
    bool exactly_once;
    char sender_host[64];

    int input_load;

    int next_stage;         // -1 if last stage
    int num_next_tasks;

    char next_task_hosts[8][64]; // allow up to 16 downstream tasks
    char next_task_ports[8][16];

    size_t serialize(char* buffer, const size_t buffer_size) const;
    static StreamTaskMessage deserialize(const char* buffer, const size_t buffer_size);
};

class StreamSystem {
public:

    StreamSystem(NodeId& introducer, const std::string& host);

    void startStream(const std::vector<MembershipInfo>& members, int NSTAGES, int NTASKS_per_stage, 
                    const std::vector<std::string>& op_exec, const std::vector<std::string>& op_args,
                    const std::string& src_dir, const std::string& dst_file,
                    bool exactly_once, bool autoscale_enabled_flag, int INPUT_RATE, int LW, int HW );
    void handle_StartTask_Req(const StreamTaskMessage& config);
    void handle_RoutingTable_Req(const StreamTaskMessage& message);
    void list_tasks(const std::vector<MembershipInfo>& members);
    void handle_List_Req();
    void handle_Ready_Req(const StreamTaskMessage& message);

    void kill_restart_task(const std::string vm_id, const std::string task_id);
    void handle_kill_restart_req(const StreamTaskMessage& message);
    void handle_load_report(const StreamTaskMessage& message);
    void handle_kill_req(const StreamTaskMessage& message);

    void increase_task(const std::vector<MembershipInfo>& members, const int stage_id); 
    void decrease_task(const int stage_id);
    void update_preStage_RoutingTable(const int prev_stage_id);
    void reset(const std::vector<MembershipInfo>& members);
    void handle_reset_req();

    TCPSocketConnection stream_socket;
    std::string target_file;
    std::string source_file;

    std::unordered_map<int, std::unordered_map<int, double>> stage_task_loads; // only for the leader
    std::mutex task_load_mtx;

    int Nstages;
    int Ntasks_per_stage;

    std::vector<std::string> op_exec_;
    std::vector<std::string> op_args_;

    bool exactly_once_enabled;
    bool autoscale_enabled;
    int input_rate;
    int low_watermark;
    int high_watermark;

private:
    void send_tcp_message(const std::string target_host, const std::string target_port, const StreamTaskMessage& message);
    NodeId self, leader;
    

    std::vector<std::vector<NodeId>> routing_table; // only for the leader
    std::vector<TaskAssignment> assignments;
    std::unordered_set<int> ready_tasks;
    std::unordered_map<int, TaskInfo> tasks; // each vm record the tasks running on it

    std::vector<int> stage_task_counts; // leader record the number of task at each stage
    int delete_task_count;
    
    
};

