#include <iostream>
#include <string>
#include <cstring>
#include <unistd.h>
#include <netdb.h>
#include <sys/socket.h>
#include "json.hpp"   // your local json.hpp

using json = nlohmann::json;

#define SERVER_ADDR "fa25-cs425-5301.cs.illinois.edu"
#define SERVER_PORT "8080"

int main() {
    // 1. Create UDP socket
    int sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sockfd < 0) {
        perror("socket");
        return 1;
    }

    // 2. Resolve introducer address
    struct addrinfo hints{}, *servinfo;
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_INET;      // IPv4
    hints.ai_socktype = SOCK_DGRAM; // UDP

    int rv = getaddrinfo(SERVER_ADDR, SERVER_PORT, &hints, &servinfo);
    if (rv != 0) {
        std::cerr << "getaddrinfo: " << gai_strerror(rv) << std::endl;
        return 1;
    }

    json membership = {
        {"fa25-cs425-5302.cs.illinois.edu:8080", {
            {"heartbeat", 5},
            {"timestamp", 123456789},
            {"status", "alive"}
        }},
        {"fa25-cs425-5303.cs.illinois.edu:8080", {
            {"heartbeat", 3},
            {"timestamp", 123456700},
            {"status", "suspect"}
        }}
    };

    json heartbeat_msg = {
        {"type", "heartbeat"},
        {"membership_lists", membership}
    };


    std::string msg = heartbeat_msg.dump();

    // 4. Send the JSON message to introducer
    int bytes = sendto(sockfd, msg.c_str(), msg.size(), 0,
                       servinfo->ai_addr, servinfo->ai_addrlen);
    if (bytes < 0) {
        perror("sendto");
        return 1;
    }
    std::cout << "Sent message to " << SERVER_ADDR << ":" << SERVER_PORT
              << " → " << msg << std::endl;

    // 5. Try to receive a response (e.g., full membership list)
    char buf[2048];
    struct sockaddr_storage their_addr;
    socklen_t addr_len = sizeof(their_addr);

    int n = recvfrom(sockfd, buf, sizeof(buf), 0,
                     (struct sockaddr*)&their_addr, &addr_len);
    if (n > 0) {
        std::string response(buf, n);
        std::cout << "Got reply: " << response << std::endl;
    } else {
        std::cout << "No reply (server may not send one)" << std::endl;
    }

    // 6. Cleanup
    freeaddrinfo(servinfo);
    close(sockfd);

    return 0;
}
