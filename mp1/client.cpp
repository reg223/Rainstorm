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

using namespace std;

#define PORT "8083"  
#define MAXDATASIZE 512 
#define TOLERANCE 3
#define TIMEOUT 100000

mutex mtx, vm_mutex;
map<string, bool> vm_status;

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
int connect_VM(string& hostname) {
    int sockfd;
    struct addrinfo hints{}, *res;
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;     
    hints.ai_socktype = SOCK_STREAM; 

    getaddrinfo(hostname.c_str(), PORT, &hints, &res);

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

int disconnect_VM(int sockfd) {
    return close(sockfd);
}

// Query a VM with given pattern, print results to stdout
// also handle failures and update active VM list
int query_VM(string& hostname, string& pattern, int activate_unit_test) {
    char inbuf[MAXDATASIZE]; // buffer for incoming data
    string result; 
    string line; 
    int bufsize = 0;// size of received data
    int remain = 0; // size of remaining data to be received
    try {
        //Establish connection
        int sockfd = connect_VM(hostname);
        if (sockfd < 0) {
            cerr << "Failed to connect to " << hostname << endl;
            close(sockfd);
            throw runtime_error("connect() failed");
        }

        //Send query pattern to server
        // cerr << "Send to: " << hostname << endl;
        if (send(sockfd, pattern.c_str(), pattern.size(), 0) < 0) {
            cerr << "send() failed for " << hostname << endl;
            close(sockfd);
            throw runtime_error("send() failed");
        }

        // Receive result size and data from server
        bool received = false;
        for (int i = 0; i < TOLERANCE; i++){
            bufsize = recv(sockfd, &remain, sizeof(remain), 0);
            if(!bufsize){
                usleep(TIMEOUT);
            } else {
                received = true;
                while (remain) {
                    bufsize = recv(sockfd, inbuf, min(remain, MAXDATASIZE), 0);
                    if (bufsize < 0) {
                        cerr << "recv() failed for " << hostname << endl;
                        close(sockfd);
                        throw runtime_error("recv() failed");
                    }
                    remain -= bufsize;
                    result.append(inbuf, bufsize);
                }

                if (pattern=="test_mode" || pattern=="normal_mode"){
                    disconnect_VM(sockfd);
                    return 0;
                }

                int count = 0;
                // Write to result.log
                {
                    lock_guard<mutex> guard(mtx);
                    ofstream fout("result.log", ios::app);
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
            close(sockfd);
            throw runtime_error("connect() failed");
        }
        

        disconnect_VM(sockfd);
        return 0;


    } catch (const exception& e) {
        // cerr << "Error with " << hostname << ": " << e.what() << ";\n";
        // cerr << "Removing " << hostname << " from active VM list." << endl;
        // vm_mutex.lock();
        // VMS.erase(remove(VMS.begin(), VMS.end(), hostname), VMS.end());
        // vm_mutex.unlock();
        {
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


int main(int argc, char* argv[]) {

    string input;
    int activate_unit_test = 0;
    ofstream fout("result.log", ios::trunc);
    for (auto& vm : VMS) {
        vm_status[vm] = true;
    }
    while (true) {
        input.clear();
        cerr << "Enter search pattern (or 'quit' to exit): ";
        getline(cin, input);

        if (input == "quit") {
            cerr << "Exiting..." << endl;
            break;
        }
        else if (input == "unit_test") {
            activate_unit_test = 1;
            run_unit_tests();
            continue; 
        }

        activate_unit_test = 0;
        vector<thread> threads;
        auto start = chrono::high_resolution_clock::now().time_since_epoch().count();
        for (auto& vm : VMS) {
            if (!vm_status[vm]) continue;
            threads.emplace_back(thread(query_VM, ref(vm), ref(input), ref(activate_unit_test)));
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



        auto end = chrono::high_resolution_clock::now();
        double latency = chrono::duration<double>(end - start).count();
        cout << "Query latency = " << latency << " seconds" << endl;
    }

    return 0;
}
