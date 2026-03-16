CPP = g++
CFLAGS = -g -Wall -Wextra -std=c++17

all: run_server run_client

run_server: server/main.cpp server/server.cpp server/join.cpp server/leave.cpp server/server.h
	$(CPP) $(CFLAGS) -o $@ server/main.cpp server/server.cpp server/join.cpp server/leave.cpp server/server.h

run_client: client/client.cpp
	$(CPP) $(CFLAGS) -o $@ client/client.cpp 


clean:
	rm -f run_server run_client *.o