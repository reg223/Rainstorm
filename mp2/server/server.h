#ifndef SERVER_H
#define SERVER_H

#include <string>
#include <map>
#include <fstream>
#include <mutex>
#include "json.hpp"

#define PORT "8081"
#define CLIENTPORT "8083"
#define PATH "."
#define LOGFILE "/result.log"
#define UNIT_TEST_LOGFILE "/machine.01.log"

using json = nlohmann::json;
using namespace std;


//switch gossip suspect
//switch ping suspect
//display_protocol

const auto T_clock = chrono::milliseconds(100); // check time every 0.1s
const auto T_send = chrono::milliseconds(150);
const auto T_suspect = chrono::milliseconds(1500); // Suspect after 1s silence
const auto T_fail = chrono::milliseconds(1500); // confirm dead after 1s silence
const auto T_cleanup = chrono::milliseconds(3000); // confirm dead after 1s silence

enum class NodeStatus {
    Alive,
    Suspect,
    Fail,
    Leave
};

enum class FDType {
    Gossip,
    PingAck
};

struct HeartBeatAttribute{
    int heartbeat_counter;
    chrono::high_resolution_clock::time_point timestamp; //when the status being updated
    NodeStatus status;
    int incarnations_number;
};

class LogServer {
public:
    LogServer(string introducer);
    ~LogServer();
    int run();  
    

private:

    int tracker_time;
    int tracker_bytes;
    int count;
    

    string result;

    
    string vm_id;
    string node_id;

    FDType FD_type; //gossip, swim
    bool activate_suspect; //using suspect or not

    map<string, HeartBeatAttribute> membership_lists;
    map<string, chrono::high_resolution_clock::time_point> timer;
    

    int selfstatus;// whether joined group (true) or not
    int drop;// percentage of dropped message (between it and server), 0 = 100%
    string introducer;
    bool is_introducer;
    char hostname[40];

    // file stream as member
    ofstream fout;


    int sockfd;
    int sockfd_udp;
    int activate_unit_test;

    mutex membership_lock;
    mutex timer_lock;

    void init_session();
    bool losePacket();
    void leave();
    void join();
    // void settings(const json& incoming);
    

    string runGrep(string& pattern);
    int host_VM();
    int setup_udp_host();
    // void handleClient(int client_fd);

    void timing();
    void listening();
    void sending();
    
    void send_grep(string& result, string& hostname);
    void send_heartbeat();
    void send_ping();
    void send_ack(string);
    void send_leave();
    void send_join();
    void reply_join(string source);

    string pick_random_alive_member();
    json map_to_json();
    void merge_membership_lists(const json& incoming);

    void display_key(string key);

    void list_mem();
    void list_self();
    void call_join();
    void call_leave();
    void list_sus();
    void switch_mode(bool gossip, bool sus);
    void display();

    int connect_client(string& hostname);
    string nowString();
    
    
};

#endif
