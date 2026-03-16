// SERVER.CPP
// Author: Kuangyi Hu (samhu2) and Yun-Yang Huang (yh76)
// The server program of the log-query system.
// The server listens on a specified port, accepts connections from clients,
// receives query patterns, searches the local log file for matching entries,
// and sends the results back to the client.

// Note: The server does not check whether log file exists or is empty

#include <iostream>
#include <string>
#include <stdexcept>
#include <cstring>
#include <unistd.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <cstdio>



using namespace std;

#define PORT "8083"  
#define PATH "." 
#define LOGFILE "/vm8.log"
#define UNIT_TEST_LOGFILE "/machine.08.log"

// Performs grep locally and return search result as string
string runGrep(string& pattern, int activate_unit_test) {
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

// Sets up the server and have its listens to the port
int host_VM() {
    struct addrinfo hints, *res;
    int sockfd;

    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;     
    hints.ai_socktype = SOCK_STREAM; 
    hints.ai_flags = AI_PASSIVE;
    getaddrinfo(NULL, PORT, &hints, &res);

    sockfd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (sockfd < 0){
        perror("socket");
        return -1;
    }

    if (::bind(sockfd, res->ai_addr, res->ai_addrlen) < 0) {
        perror("bind");
        return -1;
    }

        
    listen(sockfd, 5);

    cout << "Server listening on port " << PORT << "..." << endl;

    return sockfd;

}

int main() {
    try {
        struct addrinfo hints, *res;
        int sockfd, new_fd;
        int activate_unit_test = 0;
        sockfd = host_VM();
        if (sockfd < 0) throw runtime_error("Failed to host VM");
        while (true) {
            struct sockaddr_storage client_addr;
            socklen_t addr_size = sizeof client_addr;
            new_fd  = accept(sockfd, (struct sockaddr*)&client_addr, &addr_size);
            if (new_fd < 0) throw runtime_error("accept() failed");

            char buf[512];
            memset(buf, 0, sizeof(buf)); //clears buffer so not to misinterpret queries sent
            int bytes = recv(new_fd, buf, sizeof(buf), 0);
            
            
            if (bytes > 0) {
                string query(buf);
                if (query=="test_mode") {
                    cerr << "Change to testing mode" << endl;
                    activate_unit_test = 1;
                    int size = 0;
                    send(new_fd, &size, sizeof(size), 0);   
                }
                else if (query=="normal_mode") {
                    cerr << "Change to normal mode" << endl;
                    activate_unit_test = 0;
                    int size = 0;
                    send(new_fd, &size, sizeof(size), 0);  
                }
                else{//regular query
                    cerr << "Query: " << query << endl;
                    string result = runGrep(query, activate_unit_test);
                    int size = result.size();
                    send(new_fd, &size, sizeof(size), 0);
                    send(new_fd, result.c_str(), size, 0);
                }
                
            }

            close(new_fd);
        }

        freeaddrinfo(res);

    } catch (const exception& e) {
        cerr << "Fatal error: " << e.what() << endl;
        return EXIT_FAILURE;
    }

    return 0;
}
