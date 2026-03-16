#include <cstdint>
#include <iostream>
#include <sstream>
#include <string_view>
#include <random>
#include <algorithm>
#include <unordered_set>
#include <thread>
#include <vector>
#include <queue>
#include <map>
#include <filesystem>
#include "message.hpp"
#include "tcp_socket.hpp"
#include "stream.hpp"
#include "tuple.hpp"
#include "stream_logger.hpp"

#define LEADER_HOST "fa25-cs425-5301.cs.illinois.edu"
#define LEADER_PORT "8082"

#define MAX_RETRIES 10
#define TIMER_PERIOD 2000

using namespace std;

class RainStorm {
    public:
        RainStorm(int task_id,
                int stage_id,
                const std::string& operator_exec,
                const std::string& operator_args,
                const std::string& src_dir,
                const std::string& dst_file,
                bool exactly_once,
                const NodeId& self,
                const NodeId& leader);

        void handleIncomingLeader();
        void runSourceTask();
        void handleOutgoingTuples();
        void handleIncomingTuples();
        void handle_timer();
                
    private:

        
        void send_tcp_message(const string target_host, const string target_port, const StreamTaskMessage& message);
        
        
        void send_RountinTable_Req();
        void handle_RoutingTable_Res(const StreamTaskMessage& message);
        void notifyLeaderReady();
        void handle_StartThread_Req();
        void load_checkpoint_to_local();
        void read_check_point(const std::string& file_path);

        void forwardTuple(const Tuple& t);
        void appendToOutput(Tuple& t);
        void commit_to_checkpoint(const Tuple& t, const TupleMessageType& type);

        int get_persistent_socket(const std::string& host, const std::string& port);

        TCPSocketConnection socket;
        TCPSocketConnection tuple_socket;
        TCPSocketConnection sending_socket;
        
        void send_persistent(const std::string& host, const std::string& port, const Tuple& msg);
        bool send_and_wait_for_ack(const std::string& host, const std::string& port, const Tuple& msg);
        void send_tuple_ack(int target_fd, const Tuple& t);

        int calculate_load();
        void ReportLeader(const int& input_load);

        //application
        bool filter(const Tuple& t, const std::string& pattern);
        void transform(Tuple& t);
        void aggregate_by_key(Tuple& t, int N);
        std::string extract_nth_field(const std::string& line, int N);
        

        bool is_leader;
        int task_id;
        int stage_id;
        string operator_exec;
        string operator_args;
        string src_dir;
        string dst_file;
        bool exactly_once;

        NodeId self, leader;

        Stream_Logger logger;

        int next_stage;         // -1 if last stage
        int num_next_tasks;
        bool is_last_stage;

        char next_task_hosts[8][64];
        char next_task_ports[8][16];
        std::mutex routing_mtx;

        std::mutex start_mtx;
        std::condition_variable start_cv;
        bool stream_started;

        std::map<std::string, int> persistent_sockets; 
        std::mutex sock_mtx;

        
        BlockingQueue<Tuple> tuple_queue;

        std::unordered_set<std::string> processed_state; // tuple_id as key
        std::mutex processed_state_mtx;

        std::string checkpoint_path;

        int tuple_counter;
        std::mutex counter_mtx;

        // std::unordered_map<std::string, int> aggregation_counts; // for aggregate_by_key

};