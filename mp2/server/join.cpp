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


void LogServer::call_join(){
    join();
}

void LogServer::send_join(){
    // std::this_thread::sleep_for(T_send);
    // if (this->membership_lists[this->node_id].status != NodeStatus::Leave) return;

    // this->membership_lists[this->node_id].status = NodeStatus::Leave;
    json mem_json;
    {
        lock_guard<std::mutex> lock(membership_lock);
        mem_json= map_to_json();
    }

    fout << nowString() << "Attempting to join via introducer: " << this->introducer << endl;
    cout << nowString() << "Attempting to join via introducer: " << this->introducer << endl;
    // setup connection to introducer vm
    struct addrinfo hints{}, *servinfo;
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_INET;    
    hints.ai_socktype = SOCK_DGRAM; 
    int info = getaddrinfo(this->introducer.c_str(), PORT, &hints, &servinfo);

    json message;

    message["com_type"] = "server";
    message["type"] = "join";
    message["source"] = this->node_id;
    message["shostname"] = hostname;
    message["ml"] = mem_json;

    string msg = message.dump();
    int bytes = sendto(sockfd_udp, msg.c_str(), msg.size(), 0, servinfo->ai_addr, servinfo->ai_addrlen);
    if (bytes < 0) perror("sendto");

    freeaddrinfo(servinfo);

}

void LogServer::reply_join(string source){
        json mem_json;
    {
        lock_guard<std::mutex> lock(this->membership_lock);
        mem_json= map_to_json();
    }
    

    json message;
    message["com_type"] = "server";
    message["type"] = "reply";
    message["source"] = this->node_id;
    message["ml"] = mem_json; //piggyback
    // setup connection to target vm
    struct addrinfo hints{}, *servinfo;
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_INET;    
    hints.ai_socktype = SOCK_DGRAM; 
    int info = getaddrinfo(source.c_str(), PORT, &hints, &servinfo);

    string msg = message.dump();
    int bytes = sendto(sockfd_udp, msg.c_str(), msg.size(), 0, servinfo->ai_addr, servinfo->ai_addrlen);
    if (bytes < 0) perror("sendto");
    fout << nowString() << "Replying newly joined " << source << endl;
    cout << nowString() << "Replying newly joined " << source << endl;

    freeaddrinfo(servinfo);
}

void LogServer::join(){
    selfstatus = true;
    bool replied = false;
    if (this->membership_lists.empty()) {
        // only when rejoin need to init new node id
        init_session();
    }
    this->membership_lists[this->node_id].status = NodeStatus::Alive;

    if(!is_introducer){
        bool replied = false;
        while (!replied){
            char buf[4096];
            struct sockaddr_storage peer_addr;
            socklen_t peer_size = sizeof(peer_addr);

            send_join(); //send join request to introducer

            // wait for the reply
            int bytes = recvfrom(sockfd_udp, buf, sizeof(buf), 0, (struct sockaddr*)&peer_addr, &peer_size);
            if (bytes < 0) {
                // sleep(1);
                std::this_thread::sleep_for(T_clock);
                continue;
            }
            string message_str(buf, bytes);
            json message = json::parse(message_str);
            if (message["type"] == "reply"){
                // if (losePacket()) continue;
                
                merge_membership_lists(message["ml"]);
                this->selfstatus = 1;
                fout << nowString() << "introducer replied, resuming, in group: " << this->selfstatus << endl;

                replied = true;
            }
            
        }
    }
    this->membership_lists[this->node_id].status = NodeStatus::Alive;
    return;
}