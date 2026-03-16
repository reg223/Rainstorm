#include "server.h"
#include <iostream>
#include <stdexcept>
#include <cstring>
#include <unistd.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <cstdio>
#include <thread>
#include <chrono>
#include "json.hpp"
#include <fstream>
#include <mutex>
#include <iomanip>
#include <sstream>


using namespace std;
using json = nlohmann::json;



LogServer::LogServer(string intro) {

    count = 0;

    this->introducer = "fa25-cs425-53" + string(intro) + ".cs.illinois.edu";
    cout << "introducer: " << this->introducer << endl;
    this->FD_type = FDType::Gossip;
    this->activate_suspect = true;
    
    gethostname(hostname, sizeof(hostname));
    init_session();
    if (strcmp(introducer.c_str(),hostname)){
        is_introducer = false;
        this->selfstatus = 0;
    } else {
        is_introducer = true;
        this->selfstatus = 1; // introducer will always in the group
        this->membership_lists[this->node_id].status = NodeStatus::Alive;
    }
    this->activate_unit_test = 0;
    this->drop = 100;

    this->tracker_bytes = 0;
    this->tracker_time = 0;

    fout.open("result.log", std::ios::trunc);
}

void LogServer::init_session(){
    auto timestamp = chrono::high_resolution_clock::now();
    auto str_time = timestamp.time_since_epoch().count();
    this->vm_id = string(hostname).substr(13, 2);
    this->node_id = vm_id + "_" + to_string(str_time);
    
    this->selfstatus = 1;

    HeartBeatAttribute new_entry;
    new_entry.timestamp = timestamp;
    new_entry.heartbeat_counter = 0;
    new_entry.status = NodeStatus::Leave;
    new_entry.incarnations_number = 0;
    this->membership_lists[this->node_id] = new_entry;
}

LogServer::~LogServer() {
    if (sockfd_udp >= 0) {
        close(sockfd_udp);
    }
}

// Convert enum -> string
string statusToString(NodeStatus status) {
    switch (status) {
        case NodeStatus::Alive: return "Alive";
        case NodeStatus::Suspect: return "Suspect";
        case NodeStatus::Fail: return "Fail";
        case NodeStatus::Leave: return "Leave";
    }
    return "Unknown";
}

// Convert string -> enum
NodeStatus stringToStatus(const string& s) {
    if (s == "Alive") return NodeStatus::Alive;
    if (s == "Suspect") return NodeStatus::Suspect;
    if (s == "Fail") return NodeStatus::Fail;
    if (s == "Leave") return NodeStatus::Leave;
}

string LogServer::nowString() {
    auto now = chrono::system_clock::now();
    auto time_t_now = chrono::system_clock::to_time_t(now);
    auto ms = chrono::duration_cast<chrono::milliseconds>(
                  now.time_since_epoch()) % 1000;

    std::ostringstream oss;
    oss << std::put_time(std::localtime(&time_t_now), "%F %T")
        << "." << std::setfill('0') << std::setw(3) << ms.count();
    return "[" + oss.str() + "] ";
}

bool LogServer::losePacket(){
    int roll = rand()%100;
    if (roll < drop) return false;
    return true;
}

// Performs grep locally and return search result as string
string LogServer::runGrep(string& pattern) {
    string cmd;
    if (activate_unit_test)
        cmd = "grep " + pattern + " " + PATH + UNIT_TEST_LOGFILE;
    else
        cmd = "grep " + pattern + " " + PATH + LOGFILE;
    
    FILE* pipe = popen(cmd.c_str(), "r");
    char buffer[256];
    string result;
    while (fgets(buffer, sizeof(buffer), pipe) != nullptr) {
        result += buffer;
    }
    pclose(pipe);
    return result;
}

// Sets up the the server's UDP port
int LogServer::setup_udp_host() {
    struct addrinfo hints, *res;

    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_DGRAM; 
    hints.ai_flags = AI_PASSIVE;  

    getaddrinfo(NULL, PORT, &hints, &res);

    sockfd_udp = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (sockfd_udp < 0) {
        perror("UDP socket");
        freeaddrinfo(res);
        return -1;
    }

    if (::bind(sockfd_udp, res->ai_addr, res->ai_addrlen) < 0) {
        perror("UDP bind");
        close(sockfd_udp);
        freeaddrinfo(res);
        return -1;
    }

    freeaddrinfo(res);

    cout << "listening on port " << PORT << "..." << std::endl;
    return sockfd_udp;
}

string LogServer::pick_random_alive_member() {
    vector<string> candidates;
    for (const auto& [key, val] : this->membership_lists) {
        if (key != this->node_id && val.status == NodeStatus::Alive) {
            candidates.push_back(key);
        }
    }
    if (candidates.empty()) return "";  // no candidate
    int idx = rand() % candidates.size();
    return candidates[idx];
    // return candidates[idx].substr(0, candidates[idx].find('_'));
}

json LogServer::map_to_json() {
    json message;
    for (const auto& [key, val] : this->membership_lists) {
        message[key] = {
            {"hb", val.heartbeat_counter},
            {"ts", chrono::duration_cast<chrono::nanoseconds>(val.timestamp.time_since_epoch()).count()},
            {"st", statusToString(val.status)},
            {"ic", val.incarnations_number}
        };
    }
    return message;
}

//establish TCP connection to client
int LogServer::connect_client(string& hostname) {
    int sockfd;
    struct addrinfo hints{}, *res;
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;     
    hints.ai_socktype = SOCK_STREAM; 

    getaddrinfo(hostname.c_str(), CLIENTPORT, &hints, &res);

    sockfd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (sockfd < 0) {
        // perror("socket");
        return -1;
    }

    if (connect(sockfd, res->ai_addr, res->ai_addrlen) < 0) {
        // perror("connect");
        close(sockfd);
        return -1;
    }

    freeaddrinfo(res);
    return sockfd;

}

void LogServer::send_grep(string& result, string& hostname){
    if (this->selfstatus==0) return;
    if (this->membership_lists[this->node_id].status != NodeStatus::Alive) return;

    int client_fd = connect_client(hostname);


    int size = result.size();
    send(client_fd, &size, sizeof(size), 0);
    send(client_fd, result.c_str(), size, 0);

}

void LogServer::send_heartbeat(){
    if (this->selfstatus==0) return;
    if (this->membership_lists[this->node_id].status != NodeStatus::Alive) return;

    string target_node_id;
    json mem_json;
    {
        lock_guard<std::mutex> lock(membership_lock);
        target_node_id = pick_random_alive_member();
        mem_json= map_to_json();
        this->membership_lists[this->node_id].heartbeat_counter += 1; //increase own heartbeat
        // fout << "current hb_counter: " << membership_lists[node_id].heartbeat_counter << endl;
    }
    if (target_node_id=="") return;
    // fout << nowString() << "Sending heartbeat to " << target_node_id << endl;
    // setup connection to target vm
    struct addrinfo hints{}, *servinfo;
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_INET;    
    hints.ai_socktype = SOCK_DGRAM;
    string target = target_node_id.substr(0, target_node_id.find('_'));
    target = "fa25-cs425-53" +target + ".cs.illinois.edu";
    int info = getaddrinfo(target.c_str(), PORT, &hints, &servinfo);

    json message;
    message["com_type"] = "server";
    message["type"] = "hb";
    message["source"] = this->node_id;
    message["ml"] = mem_json;

    string msg = message.dump();
    // cout << "sent" << msg.size() << "bytes in gsp\n";
    this->tracker_bytes += msg.size();
    int bytes = sendto(sockfd_udp, msg.c_str(), msg.size(), 0, servinfo->ai_addr, servinfo->ai_addrlen);
    if (bytes < 0) perror("sendto");

    freeaddrinfo(servinfo);
        
}

void LogServer::send_ping(){
    if (this->selfstatus==0) return;
    if (this->membership_lists[this->node_id].status != NodeStatus::Alive) return;
    string target_node_id;
    json mem_json;
    {
        lock_guard<std::mutex> lock(this->membership_lock);
        target_node_id = pick_random_alive_member();
        mem_json= map_to_json();
    }

    if (target_node_id=="") return;

    // fout << nowString() << "Send Ping to " << target_node_id << endl;
    // setup connection to target vm
    struct addrinfo hints{}, *servinfo;
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_INET;    
    hints.ai_socktype = SOCK_DGRAM; 
    string target = target_node_id.substr(0, target_node_id.find('_'));
    target = "fa25-cs425-53" +target + ".cs.illinois.edu";
    int info = getaddrinfo(target.c_str(), PORT, &hints, &servinfo);

    json message;
    message["com_type"] = "server";
    message["type"] = "ping";
    message["source"] = this->node_id;
    message["ml"] = mem_json; //piggyback

    {
        lock_guard<std::mutex> lock(this->timer_lock);
        // Only start the timer if not already waiting for an ACK
        if (this->timer.find(target_node_id) == this->timer.end()) {
            this->timer[target_node_id] = chrono::high_resolution_clock::now();
        }
    }

    string msg = message.dump();
    // cout << "sent" << msg.size() << "bytes in ping\n";
    this->tracker_bytes += msg.size();
    int bytes = sendto(sockfd_udp, msg.c_str(), msg.size(), 0, servinfo->ai_addr, servinfo->ai_addrlen);
    if (bytes < 0) perror("sendto");
    

    freeaddrinfo(servinfo);
    
}


void LogServer::send_ack(string target_node_id){
    
    json mem_json;
    {
        lock_guard<std::mutex> lock(this->membership_lock);
        mem_json= map_to_json();
    }
    

    json message;
    message["com_type"] = "server";
    message["type"] = "ack";
    message["source"] = this->node_id;
    message["ml"] = mem_json; //piggyback
    // setup connection to target vm
    struct addrinfo hints{}, *servinfo;
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_INET;    
    hints.ai_socktype = SOCK_DGRAM; 
    string target = target_node_id.substr(0, target_node_id.find('_'));
    target = "fa25-cs425-53" + target + ".cs.illinois.edu";
    int info = getaddrinfo(target.c_str(), PORT, &hints, &servinfo);

    string msg = message.dump();
    // cout << "sent" << msg.size() << "bytes in ack\n";
    this->tracker_bytes += msg.size();
    int bytes = sendto(sockfd_udp, msg.c_str(), msg.size(), 0, servinfo->ai_addr, servinfo->ai_addrlen);
    if (bytes < 0) perror("sendto");
    
    freeaddrinfo(servinfo);

}

void LogServer::sending(){
    while(true){
        std::this_thread::sleep_for(T_send);
        if (this->FD_type == FDType::Gossip){
            send_heartbeat();
        }
        else if (this->FD_type == FDType::PingAck){
            send_ping();
        }
        this->tracker_time++;
        if (this->tracker_time >= 5){
            cout << this->count++ << ": Bytes Per Sec-> "<< this->tracker_bytes << endl;
            this->tracker_bytes = this->tracker_time = 0;
        
        }
        
    }
    return;
}


void LogServer::listening() {
    if (setup_udp_host() < 0) throw runtime_error("Failed to setup udp host");
    while (true) {
        
        char buf[16384];
        struct sockaddr_storage peer_addr;
        socklen_t peer_size = sizeof(peer_addr);

        int bytes = recvfrom(sockfd_udp, buf, sizeof(buf), 0, (struct sockaddr*)&peer_addr, &peer_size);
        if (bytes < 0) {
            perror("recvfrom");
            continue;
        }

        string message_str(buf, bytes);
        json message = json::parse(message_str);

        //INTERSERVER communications
        if (message["com_type"] == "server" and this->selfstatus==1) {
            if (losePacket()) {
                // cout << "packet loss!\n" ;
                continue;
            }
            if (message["type"] == "hb"){
                // cout << "Got HB!\n" ;
                fout << nowString() << "receive heartbeat" << endl;
                merge_membership_lists(message["ml"]);
            } 
            else if (message["type"] == "ping"){
                // cout << "Got PING!\n" ;
                // fout << nowString() << "receive ping from " << message["source"] << endl;
                merge_membership_lists(message["ml"]);
                send_ack(message["source"]);
            } 
            else if (message["type"] == "ack"){
                // cout << "Got ACK!\n" ;
                // fout << nowString() << "receive ack from " << message["source"] << endl;
                merge_membership_lists(message["ml"]);
                {
                    lock_guard<std::mutex> lock(timer_lock);
                    timer.erase(message["source"]); // ACK received -> stop silence timer

                }
                {
                    lock_guard<std::mutex> lock(membership_lock);
                    // once receive ack, update the time 
                    this->membership_lists[message["source"]].timestamp = chrono::high_resolution_clock::now();
                }             
            } 
            // only introducer can reveive this message
            else if (message["type"] == "join" and this->is_introducer) {
                try {
                    fout << nowString() << "receive join request from " << message["source"] << endl;
                } catch (const std::exception& e) {
                    fout << nowString() << "introducer already in the group" << endl;
                }
                reply_join(message["shostname"]);
                merge_membership_lists(message["ml"]);
            } 

            else if (message["type"] == "leave" ) {
                fout << nowString() << "Node " << message["source"] << " leaving" << endl;
                cout << "Mark " << message["source"] << " -> Leave" << endl;
                merge_membership_lists(message["ml"]);
                // TODO: does merge handle leave now?
                {
                    lock_guard<std::mutex> lock(this->timer_lock);
                    this->timer[message["source"]] = chrono::high_resolution_clock::now();
                }
            }
        }    
        else if (message["com_type"] == "client") {
            //CLIENT-SERVER communication
            if (message["type"] == "list_mem"){
                fout << nowString() << "Command from client:" << message["type"] << endl;
                list_mem();
    
            } else if (message["type"] == "list_self"){
                fout << nowString() << "Command from client:" << message["type"] << endl;
                list_self();
                
            } else if (message["type"] == "join"){
                fout << nowString() << "Command from client:" << message["type"] << endl;
                call_join();
                
            } else if (message["type"] == "leave"){
                fout << nowString() << "Command from client:" << message["type"] << endl;
                call_leave();
                this->membership_lists.clear();
                
            } else if (message["type"] == "list_sus"){
                fout << nowString() << "Command from client:" << message["type"] << endl;
                list_sus();
                
            } else if (message["type"] == "switch"){
                fout << nowString() << "Command from client:" << message["type"] << endl;
                switch_mode(message["gossip"],message["suspect"]);
                
            } else if (message["type"] == "display"){
                fout << nowString() << "Command from client:" << message["type"] << endl;
                display();
                
            } else if (message["type"] == "drop"){
                fout << nowString() << "Command from client:" << message["type"] << endl;
                drop = message["drop"];
                
            } else if (message["type"] == "kill"){
                fout << nowString() << "Command from client:" << message["type"] << endl;
                cout << "killed by client\n";
                exit(0);
                
            } else if (message["type"] == "query"){
                string query = message["query"];
                string client = message["source"];
                fout << nowString() << "query from client: "<< query << endl;
                result = runGrep(query);
                send_grep(result,client);
            }
        }
        
    }
    close(sockfd_udp);
}

void LogServer::merge_membership_lists(const json& incoming) {
    // if (this->selfstatus==0) return;
    lock_guard<mutex> lock(this->membership_lock);
    auto cur_time = chrono::high_resolution_clock::now();
    for (auto& [node_id_in, entry] : incoming.items()) {

        // // skip merging my own entry if I've already left
        // if (!selfstatus && node_id_in == this->node_id) {
        //     continue;
        // }

        HeartBeatAttribute incoming_attr;
        incoming_attr.heartbeat_counter = entry["hb"];
        incoming_attr.incarnations_number = entry["ic"];
        incoming_attr.status = stringToStatus(entry["st"]);
        incoming_attr.timestamp = cur_time;

        auto it = this->membership_lists.find(node_id_in);

        if (it == this->membership_lists.end()) {
            // New node
            this->membership_lists[node_id_in] = incoming_attr;
            fout << nowString() << "New node added: " << node_id_in << endl;
            
            continue;
        }

        HeartBeatAttribute& local_attr = it->second;

        // Case 1: Incoming has higher incarnation
        if (incoming_attr.incarnations_number > local_attr.incarnations_number) {
            local_attr = incoming_attr; // trust newer incarnation
            local_attr.timestamp = cur_time;
            continue;
        }

        // Case 2: Incoming has lower incarnation
        if (incoming_attr.incarnations_number < local_attr.incarnations_number) {
            continue; // stale
        }

        // Case 3: Same incarnation
        if (incoming_attr.status == NodeStatus::Fail) {
            local_attr = incoming_attr; // Fail overrides everything
            local_attr.timestamp = cur_time;
            continue;
        }

        if (incoming_attr.status == NodeStatus::Leave) {
            local_attr = incoming_attr;
            {
                lock_guard<std::mutex> lock(this->timer_lock);
                if (this->timer.find(node_id_in) == this->timer.end()) {
                    this->timer[node_id_in] = chrono::high_resolution_clock::now();
                }
            }
            continue;
        }

        if (this->activate_suspect) {
            // Suspicion enabled
            if (node_id_in == this->node_id) {
                // My own entry
                if (incoming_attr.status == NodeStatus::Suspect) {
                    // Someone suspects me -> increase incarnation
                    local_attr.incarnations_number += 1;
                    local_attr.status = NodeStatus::Alive;
                    local_attr.timestamp = cur_time;
                }
            } else {
                // Other nodes
                if (incoming_attr.status == NodeStatus::Suspect && local_attr.status == NodeStatus::Alive) {
                    local_attr.status = NodeStatus::Suspect;
                    fout << nowString() << "Mark " << node_id_in << " -> Suspect" << endl;
                    cout << "Mark " << node_id_in << " -> Suspect" << endl;
                    local_attr.timestamp = cur_time;
                } 
                else if (incoming_attr.status == NodeStatus::Alive && local_attr.status == NodeStatus::Suspect) {
                    local_attr.status = NodeStatus::Alive;
                    local_attr.timestamp = cur_time;
                }
            }
        }

        // Heartbeat check for Alive nodes 
        if (incoming_attr.status == NodeStatus::Alive && local_attr.status == NodeStatus::Alive) {
            // fout << nowString() << it->first << " hb: " << local_attr.heartbeat_counter << endl;
            // fout << nowString() << node_id_in << " hb: " << incoming_attr.heartbeat_counter << endl;
            if (this->FD_type == FDType::Gossip && incoming_attr.heartbeat_counter >= local_attr.heartbeat_counter) {
                // fout << "update local" << endl;
                local_attr.heartbeat_counter = incoming_attr.heartbeat_counter;
                local_attr.timestamp = cur_time;
            }
        }
    }
}
std::string timePointToString(const std::chrono::high_resolution_clock::time_point& tp) {
    using namespace std::chrono;
    auto t = system_clock::time_point{duration_cast<system_clock::duration>(tp.time_since_epoch())};
    auto time_t_now = system_clock::to_time_t(t);
    auto ms = duration_cast<milliseconds>(tp.time_since_epoch()) % 1000;

    std::ostringstream oss;
    oss << std::put_time(std::localtime(&time_t_now), "%F %T")
        << "." << std::setw(3) << std::setfill('0') << ms.count();
    return oss.str();
}

void LogServer::timing() {
    // only timing when in the group
    while (true) {
        std::this_thread::sleep_for(T_clock);
        if (this->selfstatus==0) continue;
        if (this->membership_lists[this->node_id].status != NodeStatus::Alive) continue; 
        auto cur_time = std::chrono::high_resolution_clock::now();

        if (this->FD_type == FDType::Gossip) {
            lock_guard<std::mutex> lock(this->membership_lock);

            for (auto it = this->membership_lists.begin(); it != this->membership_lists.end(); ) {

                if (it->first == this->node_id) { ++it; continue; }

                auto elapsed = cur_time - it->second.timestamp;

                if (this->activate_suspect) {
                    // With suspicion: Alive -> Suspect -> Fail
                    if (it->second.status == NodeStatus::Alive && elapsed > T_suspect) {
                        fout << nowString() << "Mark " << it->first << " -> Suspect" << endl;
                        cout << "Mark " << it->first << " -> Suspect" << endl;
                        fout << "now: " << timePointToString(cur_time)
                            << " | last: " << timePointToString(it->second.timestamp)
                            << " | elapsed: "
                            << std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count()
                            << " ms" << std::endl;
                        it->second.status = NodeStatus::Suspect;
                        it->second.timestamp = cur_time;
                    }
                    else if (it->second.status == NodeStatus::Suspect && elapsed > T_fail) {
                        fout << nowString() << "Mark " << it->first << " -> Fail" << endl;
                        cout << "Mark " << it->first << " -> Fail" << endl;
                        it->second.status = NodeStatus::Fail;
                        it->second.timestamp = cur_time;
                    }
                } else {
                    // Without suspicion: Alive -> Fail directly
                    if (it->second.status == NodeStatus::Alive && elapsed > T_fail) {
                        fout << nowString() << "Mark " << it->first << " -> Fail (no suspicion)" << endl;
                        cout << "Mark " << it->first << " -> Fail (no suspicion)" << endl;
                        it->second.status = NodeStatus::Fail;
                        it->second.timestamp = cur_time;
                    }
                    else if (it->second.status == NodeStatus::Suspect) {
                        fout << nowString() << "Mark suspects" << it->first << " -> Fail (not in suspicion)" << endl;
                        cout << "Mark suspect" << it->first << " -> Fail" << endl;
                        it->second.status = NodeStatus::Fail;
                        it->second.timestamp = cur_time;
                    }
                }

                // Cleanup applies in both modes
                if (it->second.status == NodeStatus::Fail && elapsed > T_cleanup) {
                    fout << nowString() << "Delete " << it->first << " from the membership_lists" << endl;
                    it = this->membership_lists.erase(it); // remove from membership
                    this->timer.erase(it->first);
                } 
                else if (it->second.status == NodeStatus::Leave && elapsed > T_cleanup) {
                    fout << nowString() << "Delete " << it->first << " from the membership_lists" << endl;
                    it = this->membership_lists.erase(it); // remove from membership
                    this->timer.erase(it->first);
                } 
                else {
                    ++it;
                }
            }
        }
        else if (this->FD_type == FDType::PingAck) {
            // lock_guard<std::mutex> lock(this->membership_lock);
            std::unique_lock<std::mutex> tlock(this->timer_lock);
            std::unique_lock<std::mutex> mlock(this->membership_lock);
            for (auto it = this->timer.begin(); it != this->timer.end(); ) {
                auto elapsed = cur_time - it->second;
                // fout << "now: " << timePointToString(cur_time)
                //     << " | last: " << timePointToString(it->second)
                //     << " | elapsed: "
                //     << std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count()
                //     << " ms" << std::endl;

                if (this->activate_suspect) {
                    // With suspicion
                    if (this->membership_lists[it->first].status == NodeStatus::Alive && elapsed > T_suspect) {
                        fout << nowString() << "Mark " << it->first << " -> Suspect" << endl;
                        cout << "Mark " << it->first << " -> Suspect" << endl;
                        this->membership_lists[it->first].status = NodeStatus::Suspect;
                        this->membership_lists[it->first].timestamp = cur_time;
                    }
                    else if (this->membership_lists[it->first].status == NodeStatus::Suspect && elapsed > T_fail) {
                        fout << nowString() << "Mark " << it->first << " -> Fail" << endl;
                        cout << "Mark " << it->first << " -> Fail" << endl;
                        this->membership_lists[it->first].status = NodeStatus::Fail;
                        this->membership_lists[it->first].timestamp = cur_time;
                    }
                } else {
                    // Without suspicion
                    if (this->membership_lists[it->first].status == NodeStatus::Alive && elapsed > T_fail) {
                        fout << nowString() << "Mark " << it->first << " -> Fail (no suspicion)" << endl;
                        cout << "Mark " << it->first << " -> Fail (no suspicion)" << endl;
                        this->membership_lists[it->first].status = NodeStatus::Fail;
                        this->membership_lists[it->first].timestamp = cur_time;
                    }
                    else if (this->membership_lists[it->first].status == NodeStatus::Suspect) {
                        fout << nowString() << "Mark suspects" << it->first << " -> Fail (not in suspicion)" << endl;
                        cout << "Mark suspect" << it->first << " -> Fail (not in suspicion)" << endl;
                        this->membership_lists[it->first].status = NodeStatus::Fail;
                        this->membership_lists[it->first].timestamp = cur_time;
                    }
                }

                // Cleanup applies in both modes
                if (this->membership_lists[it->first].status == NodeStatus::Fail && elapsed > T_cleanup) {
                    fout << nowString() << "Delete(Fail) " << it->first << " from the table" << endl;
                    this->membership_lists.erase(it->first);   // remove from membership
                    it = this->timer.erase(it);  // also remove timer
                } 
                else if (this->membership_lists[it->first].status == NodeStatus::Leave && elapsed > T_cleanup) {
                    fout << nowString() << "Delete(Leave) " << it->first << " from the table" << endl;
                    this->membership_lists.erase(it->first);   // remove from membership
                    it = this->timer.erase(it);  // also remove timer
                } 
                else {
                    ++it;
                }
            }
        }
    }
}

void LogServer::display_key(string key){
    HeartBeatAttribute member = this->membership_lists[key];
    auto str_time = member.timestamp.time_since_epoch().count();
    cout << "node_id: " << key 
            << " | status: " << statusToString(member.status)
            << " | heartbeat: " << member.heartbeat_counter
            << " | incarnations: " << member.incarnations_number
            << " | timestamp: " << str_time  << endl;
}

void LogServer::list_mem(){
    cout << "=== Membership List ===" << endl;
    for (const auto& [key, val] : this->membership_lists) {
        display_key(key);
    }
    cout << "=====================\n";
}


void LogServer::list_self(){
    cout << "======== Self ========\n";
    cout << "in group: " <<  this->selfstatus << endl;
    if (this->node_id=="") return;
    display_key(this->node_id);
    cout << "in group: " <<  this->selfstatus << endl;
    cout << "======================\n";
}

void LogServer::list_sus(){
    cout << "==== Suspect List ====" << endl;
    for (const auto& [key, val] : this->membership_lists) {
        if (activate_suspect && val.status == NodeStatus::Suspect){
            display_key(key);
        }
    }
    cout << "======================\n";
}
void LogServer::switch_mode(bool gossip, bool sus){
    this->FD_type = gossip ?  FDType::Gossip : FDType::PingAck;
    this->activate_suspect = sus;
}
void LogServer::display(){
    cout << "Current protocol:"
         << "< " << ((this->FD_type == FDType::Gossip) ? "gossip, " : "ping, " )
         << ((this->activate_suspect )? "suspect>\n" : "nosuspect>\n");
}
int LogServer::run() {

    try{

        thread Timer(&LogServer::timing, this);
        thread Listener(&LogServer::listening, this);
        thread Sender(&LogServer::sending, this);
        

        Timer.join();
        Listener.join(); 
        Sender.join();

    }
    catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        return EXIT_FAILURE;
    } 
    return 0;
    
}

