#include "server.h"
#include <iostream>

int main(int argc, char* argv[]) {
    std::string intro_id = "01";   // default introducer

    if (argc == 2) {
        intro_id = argv[1];       // override if provided
    }

    LogServer server(intro_id);
    server.run();
    return 0;
}

