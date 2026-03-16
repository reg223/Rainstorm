# G53 - MP2

MP2 project for **CS 425: Distributed Systems**.

## Description

- `client/client.cpp`: Client-side code.  
- `server/main.cpp`: Entry point to run the server.  
- `server/server.h`: Server header file.  
- `server/server.cpp`: Core server-related functions.  
- `server/join.cpp`: Handles join operations.  
- `server/leave.cpp`: Handles leave operations.  
- `server/json.hpp`: JSON handling library.  
  *(Since the VM environment did not allow importing this external library, we included the source code directly for handling JSON formatting.)*

## Sources Referred To

- [Beej’s Guide to Network Programming](https://beej.us/guide/bgnet/html/)  
- Linux man pages for networking functions (e.g., [recv()](https://linux.die.net/man/2/recv))  

## Compile

run ```make``` under the directory mp2

## Usage
```
./run_client
./run_server
```

In client: type the query

- `list_mem`: list the membership list  
- `list_self`: list self’s id  
- `join <vm_id>`: join the group with specific VM  
- `leave <vm_id>`: voluntarily leave the group  
- `display_suspects`: list suspected nodes  
- `switch {gossip|ping} {suspect|nosuspect}`: change the protocol  
- `display_protocol`: output a pair `<{gossip|ping}, {suspect|nosuspect}>`

In server: execute the input command


## Contributions

- Kuangyi: Code design, ~40% of code, run expereimtn, report write up

- Yun-Yang: Code design, ~60% of code, run expereimtn


