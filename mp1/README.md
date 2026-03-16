# G53 - MP1



MP 1 for the course CS 425: Distributed Systems.

## Description
A simple log-query program that runs grep on up to 10 designated machines and their local logs. User query through only one of the machine (the client) and the program will search through all of the them. Results are sent back via TCP stream.

```client.cpp```: client code of the program

```server.cpp```: server code of the program

```gen_logs.cpp```: for generating custom test logs; not an actual part of the program.

## Sources referred to

- [Beej Network Guide](https://beej.us/guide/bgnet/html/)
- man pages for network related functions, for example [recv()](https://linux.die.net/man/2/recv)





## Compile

run ```make``` or ```make client && make server``` under the directory mp1

## Usage
```
./client
./server
./gen_log <vmnumber>
```

In client:
- type or pipe in files of query
- type quit to exit
- type unit_test to perform unit tests on custom logs
- everything else is treated as a query
- actual results goes to `results.log` as large outputs may crash console

Server assigns its own machine logs based on query and is fully automatic after executed.


## Contributions

- Kuangyi: Code design, ~40% of Log-Query, unit test design, report write up

- Yun-Yang: Code design, ~60% of Log-Query, unit test design/write up


