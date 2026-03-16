CPP = g++


CFLAGS = -g -Wall -Wextra -std=c++11

all: client server gen_logs

client%: client.cpp
	$(CPP) $(CFLAGS) -o $@ client.cpp
server%: server.cpp
	$(CPP) $(CFLAGS) -o $@ server.cpp
gen_logs: gen_logs.cpp
	$(CPP) $(CFLAGS) -o $@ gen_logs.cpp
clean:
	rm -f client*[!.cpp] server*[!.cpp] unit_test gen_logs *.o