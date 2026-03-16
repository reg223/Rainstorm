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

void LogServer::call_leave(){
    leave();
}

// TODO: resolve introducer rejoin
void LogServer::leave(){
    send_leave();
    this->selfstatus = 0;
    // lock_guard<std::mutex> lock(membership_lock);
    unique_lock<mutex> tlock(this->timer_lock);
    unique_lock<mutex> mlock(this->membership_lock);
    this->membership_lists.clear();
    this->timer.clear();
    this->node_id = "";
}

void LogServer::send_leave(){
    std::this_thread::sleep_for(T_send);
    if (this->membership_lists[this->node_id].status != NodeStatus::Alive) return;

    this->membership_lists[this->node_id].status = NodeStatus::Leave;
    string target_node_id;
    json mem_json;
    {
        lock_guard<std::mutex> lock(membership_lock);
        target_node_id = pick_random_alive_member();
        mem_json= map_to_json();
    }
    if (target_node_id=="") return;
    // fout << nowString() << "Leaving; notifying " << target_node_id << endl;
    // setup connection to target vm
    struct addrinfo hints{}, *servinfo;
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_INET;    
    hints.ai_socktype = SOCK_DGRAM; 
    // string target = target_node_id.substr(0, target_node_id.find('_'));
    string target = this->introducer;
    fout << nowString() << "Leaving; notifying " << target << endl;
    int info = getaddrinfo(target.c_str(), PORT, &hints, &servinfo);

    json message;
    message["com_type"] = "server";
    message["type"] = "leave";
    message["source"] = this->node_id;
    message["ml"] = mem_json;

    string msg = message.dump();
    int bytes = sendto(sockfd_udp, msg.c_str(), msg.size(), 0, servinfo->ai_addr, servinfo->ai_addrlen);
    if (bytes < 0) perror("sendto");

    freeaddrinfo(servinfo);

}