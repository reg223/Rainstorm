// CLIENT.CPP
// Author: Kuangyi Hu (samhu2) and Yun-Yang Huang (yh76)
// The client program of the log-query system.
// The client keeps track of active VMs, collects query from user through stdin,
// sends query patterns to all active VMs in parallel, collects and displays 
// results into standard output line by line.

// Notes: 
// 1. Currently, the client treats all failures as permanent and will not 
// attempt to reconnect.
// 2. The client will not print the results from different machines 
// sequentially. The current design approach may have different lines
// from different machines interleaved in the output.


#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <stdexcept>
#include <cstring>
#include <thread>
#include <mutex>
#include <unistd.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <algorithm>
#include <fstream>
#include <map>
#include <chrono>
#include "../server/json.hpp" 

using namespace std;
using json = nlohmann::json;

#define PORT "8081" 
#define CLIENTPORT "8083" 
#define MAXDATASIZE 512 
#define TOLERANCE 3
#define TIMEOUT 100000

mutex mtx, vm_mutex;
map<string, bool> vm_status;
int sockfd_tcp;

    // Iniltialize list of active VMs, will update if machines fail
vector<string> VMS = {
    "fa25-cs425-5301.cs.illinois.edu",
    "fa25-cs425-5302.cs.illinois.edu",
    "fa25-cs425-5303.cs.illinois.edu",
    "fa25-cs425-5304.cs.illinois.edu",
    "fa25-cs425-5305.cs.illinois.edu",
    "fa25-cs425-5306.cs.illinois.edu",
    "fa25-cs425-5307.cs.illinois.edu",
    "fa25-cs425-5308.cs.illinois.edu",
    "fa25-cs425-5309.cs.illinois.edu",
    "fa25-cs425-5310.cs.illinois.edu"
};

map<string, map<string,int>> unit_test_table = {
    {"fix", {
        {"01",4},{"02",4},{"03",4},{"04",4},{"05",4},
        {"06",4},{"07",4},{"08",4},{"09",4},{"10",4}
    }},
    {"rare", {
        {"01",0},{"02",0},{"03",1},{"04",0},{"05",0},
        {"06",0},{"07",0},{"08",0},{"09",0},{"10",0}
    }},
    {"frequent", {
        {"01",50},{"02",0},{"03",50},{"04",0},{"05",50},
        {"06",0},{"07",50},{"08",0},{"09",0},{"10",0}
    }},
    {"random", {
        {"01",500},{"02",500},{"03",500},{"04",500},{"05",500},
        {"06",500},{"07",500},{"08",500},{"09",500},{"10",500}
    }},
};

map<string,int> vm_match_counts;
mutex count_mutex;


// Connect to a VM, return socket fd if success, -1 if fail




int disconnect_VM(int sockfd) {
    return close(sockfd);
}

//tcp
int host_client() {
    struct addrinfo hints, *res;

    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;

    getaddrinfo(NULL, CLIENTPORT, &hints, &res);

    int sockfd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (sockfd < 0) {
        perror("socket");
        return -1;
    }

    if (::bind(sockfd, res->ai_addr, res->ai_addrlen) < 0) {
        perror("bind");
        return -1;
    }

    listen(sockfd, 256);

    return sockfd;
}

int setup_udp_host() {
    struct addrinfo hints, *res;

    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_DGRAM; 
    hints.ai_flags = AI_PASSIVE;  

    getaddrinfo(NULL, PORT, &hints, &res);

    int sockfd_udp = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
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

    return sockfd_udp;
}


// int setup_udp() {
//     struct addrinfo hints, *res;

//     memset(&hints, 0, sizeof hints);
//     hints.ai_family = AF_UNSPEC;
//     hints.ai_socktype = SOCK_DGRAM; 
//     hints.ai_flags = AI_PASSIVE;  

//     getaddrinfo(NULL, CLIENTPORT, &hints, &res);

//     int sockfd_udp = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
//     if (sockfd_udp < 0) {
//         perror("UDP socket");
//         freeaddrinfo(res);
//         return -1;
//     }

//     if (::bind(sockfd_udp, res->ai_addr, res->ai_addrlen) < 0) {
//         perror("UDP bind");
//         close(sockfd_udp);
//         freeaddrinfo(res);
//         return -1;
//     }

//     freeaddrinfo(res);

//     cout << "listening on port " << PORT << "..." << std::endl;
//     return sockfd_udp;
// }


void send_server(const string& hostname, const json& message) {
    string msg = message.dump();

    struct addrinfo hints{}, *servinfo;
    memset(&hints, 0, sizeof hints);
    hints.ai_family   = AF_INET;
    hints.ai_socktype = SOCK_DGRAM;

    if (getaddrinfo(hostname.c_str(), PORT, &hints, &servinfo) != 0) {
        perror("getaddrinfo");
        return;
    }

    int sock = socket(servinfo->ai_family, servinfo->ai_socktype, servinfo->ai_protocol);
    if (sock < 0) { perror("socket"); freeaddrinfo(servinfo); return; }

    int bytes = sendto(sock, msg.c_str(), msg.size(), 0, servinfo->ai_addr, servinfo->ai_addrlen);
    if (bytes < 0) perror("sendto");

    close(sock);
    freeaddrinfo(servinfo);
}


// Query a VM with given pattern, print results to stdout
// also handle failures and update active VM list



void send_cmd_to_VM(string& hostname, json& message) {
        // cout << "send " << message << " to " << hostname << endl;
        send_server(hostname, message);
}



int query_VM(string& hostname, string& pattern, int activate_unit_test) {
    char inbuf[MAXDATASIZE]; // buffer for incoming data
    string result; 
    string line; 
    int bufsize = 0;// size of received data
    int remain = 0; // size of remaining data to be received
    try {
        //compile message to server
        json message;
        message["com_type"] = "client";
        message["type"] = "query";
        char source[40];
        gethostname(source, sizeof(source));
        message["source"] = string(source);
        message["query"] = pattern;
        // string msg = message.dump();
        send_cmd_to_VM(hostname, message);
        //to fetch results
        struct sockaddr_storage client_addr;
        socklen_t addr_size = sizeof client_addr;
        int new_fd  = accept(sockfd_tcp, (struct sockaddr*)&client_addr, &addr_size);
        // Receive result size and data from server
        bool received = false;
        for (int i = 0; i < TOLERANCE; i++){
            bufsize = recv(new_fd, &remain, sizeof(remain), 0);
            if(!bufsize){
                usleep(TIMEOUT);
            } else {
                received = true;
                while (remain) {
                    bufsize = recv(new_fd, inbuf, min(remain, MAXDATASIZE), 0);
                    remain -= bufsize;
                    result.append(inbuf, bufsize);
                }
                if (pattern=="test_mode" || pattern=="normal_mode"){
                    disconnect_VM(new_fd);
                    return 0;
                }
                int count = 0;
                // Write to result.log
                {
                    lock_guard<mutex> guard(mtx);
                    ofstream fout("qresult.log", ios::app);
                    fout << "\n=== Results from " << hostname << " ===" << endl;
                    istringstream stream(result);
                    while (getline(stream, line)) {
                        count += 1;
                        fout << line << endl;
                    }
                    string vm_id = hostname.substr(13, 2);
                    cout << "Total " << count << " match from VM" << vm_id << endl;
                    fout.close();
                    {
                        lock_guard<mutex> guard(count_mutex);
                        vm_match_counts[vm_id] = count;
                    }
                }
                if (activate_unit_test){
                    string vm_id = hostname.substr(13, 2);
                    if (count == unit_test_table[pattern][vm_id]){
                        cout << "✅ result correct for VM" << vm_id << endl;
                    }
                    else{
                        cout << "❌ Result incorrect for VM" << vm_id  << endl;
                    }
                }
                break;
            }
        }
        if (!received) {
            cerr << "Timeout waiting for response from " << hostname << endl;
            close(sockfd_tcp);
            throw runtime_error("connect() failed");
        }
        disconnect_VM(sockfd_tcp);
        return 0;

    } catch (const exception& e) {
        // cerr << "Error with " << hostname << ": " << e.what() << ";\n";
        // cerr << "Removing " << hostname << " from active VM list." << endl;
        // vm_mutex.lock();
        // VMS.erase(remove(VMS.begin(), VMS.end(), hostname), VMS.end());
        // vm_mutex.unlock();
        {
            //TODO: figure out what to do with this
            lock_guard<mutex> guard(vm_mutex);
            vm_status[hostname] = false;  // mark VM as failed
        }
        return 1;
    }
}

// Run all unit test patterns on all active VMs
void run_unit_tests() {
    int activate_unit_test = 0;
    vector<thread> threads;
    string mode = "test_mode";
    for (auto& vm : VMS) {
            if (!vm_status[vm]) continue;
            threads.emplace_back(thread(query_VM, ref(vm), ref(mode), ref(activate_unit_test)));
        }
    for (auto& t : threads) {
        t.join();
    }
        
    activate_unit_test = 1;
    for (auto &pattern_entry : unit_test_table) {
        string pattern = pattern_entry.first;

        cerr << "\n=== Running unit test for pattern: " << pattern << " ===" << endl;

        threads.clear();
        for (auto& vm : VMS) {
            if (!vm_status[vm]) continue;
            threads.emplace_back(thread(query_VM, ref(vm), ref(pattern), ref(activate_unit_test)));
        }
        for (auto& t : threads) {
            t.join();
        }
    }

    activate_unit_test = 0;
    mode = "normal_mode";
    threads.clear();
    for (auto& vm : VMS) {
            if (!vm_status[vm]) continue;
            threads.emplace_back(thread(query_VM, ref(vm), ref(mode), ref(activate_unit_test)));
        }
    for (auto& t : threads) {
        t.join();
    }


    return;
}

void thread_send_all(json& message, bool& test) {
    vector<thread> threads;
    for (auto& vm : VMS) {
       if (!vm_status[vm]) continue;
        threads.emplace_back(thread(send_cmd_to_VM, ref(vm), ref(message)));
    }
    for (auto& t : threads) {
        t.join();
    }
}

int main(int argc, char* argv[]) {
    sockfd_tcp = host_client();
    string input;
    string line;
    bool activate_unit_test = 0;
    ofstream fout("result.log", ios::trunc);
    for (auto& vm : VMS) {
        vm_status[vm] = true;
    }
    json message;
    string msg;
    string i2,i3;
    while (true) {
        message.clear();
        msg.clear();
        input.clear();
        line.clear();
        i3.clear();
        cerr << "Enter command: ";
        getline(cin,line);
        istringstream iss(line);
        vector<string> input{
            istream_iterator<string>{iss},
            istream_iterator<string>{}
        };
        if (input.empty()) continue;
        message["com_type"] = "client";
        if (input[0] == "quit") {
            cerr << "Exiting..." << endl;
            break;
        } 
        else if (input[0] == "unit_test") {
            activate_unit_test = 1;
            run_unit_tests();
            continue; 
        } 
        else if (input[0] == "list_mem") {
            message["type"] = input[0];
            thread_send_all(message, activate_unit_test);
        } 
        else if (input[0] == "list_self") {
            message["type"] = input[0];
            thread_send_all(message,activate_unit_test);
        } 
        else if (input[0] == "switch") {
            
            if ((input[1] != "gossip") and (input[1] != "ping")){
                cout << "invalid protocol " << input[1] << endl;
                continue;
            }
            if ((input[2] != "suspect") and (input[2] != "nosuspect")){
                cout << "invalid protocol " << input[1] << endl;
                continue;
            }
            message["type"] = input[0];
            message["gossip"] = (input[1] == "gossip")? true : false;
            message["suspect"] = (input[2] == "suspect")? true : false;
            thread_send_all(message,activate_unit_test);
        } 
        else if (input[0] == "display_suspects") {
            message["type"] = "list_sus";
            thread_send_all(message, activate_unit_test);
        } 
        else if (input[0] == "display_protocol") {
            message["type"] = "display";
            thread_send_all(message, activate_unit_test);
        } 
        else if (input[0] == "drop") {
            message["type"] = input[0];
            int num = stoi(input[1]);
            if (0 > num || num > 100){
                cerr << "Drop Usage: must be number from 0-100\n";
                continue;
            }
            message["drop"]  = num;
            thread_send_all(message, activate_unit_test);
        } 
        else if (input[0] == "join" || input[0] == "leave" || input[0] == "kill") {
            for (int i = 1; i < input.size(); i++) {
                if (strlen(input[i].c_str()) != 2){
                    cerr << "Join/Leave Usage: {join,leave} <xx> for vmxx\n";
                    continue;
                }
                i3 = "fa25-cs425-53" + input[i] + ".cs.illinois.edu";
                message["type"] = input[0];
                send_server(i3, message);
            }
            
            
        }  
        else if (input[0] == "query"){
            activate_unit_test = 0;
            vector<thread> threads;
            auto start = chrono::high_resolution_clock::now().time_since_epoch().count();
            for (auto& vm : VMS) {
                if (!vm_status[vm]) continue;
                threads.emplace_back(thread(query_VM, ref(vm), ref(input[1]), ref(activate_unit_test)));
            }
            for (auto& t : threads) {
                t.join();
            }

            int total_matches = 0;
            for (auto &p : vm_match_counts) {
                total_matches += p.second;
            }
            vm_match_counts.clear(); // reset for next query
            cout << "### Total matches across all VMs = " << total_matches << " ###" << endl;

            auto end = chrono::high_resolution_clock::now().time_since_epoch().count();
            double latency = chrono::duration<double>(end - start).count();
            cout << "Query latency = " << latency << " seconds" << endl;
        }
        else {
            cout << "unrecognized command: " << input[0] << ", ignored.\n";
            
        }
        

        
    }

    return 0;
}

