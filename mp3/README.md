# G53 - MP3

MP3 project for **CS 425: Distributed Systems**.

## Description

- `src/main.cpp`: Entry point of HyDFS. Initializes each node, starts heartbeat and file-handling threads, and provides the interactive command-line interface for user input.
- `src/node.cpp`: Handle failure detection (code from MP2).
- `src/file_system.cpp`: Implements the local file management layer. Handles reading, writing, chunking, reassembling files, and maintaining file metadata.
- `src/create.cpp`: Implements the `CREATE` operation. Splits a local file into chunks and distributes them to responsible nodes with replication.
- `src/append.cpp`: Handles the `APPEND` operation, allowing incremental additions to existing distributed files while preserving version consistency.
- `src/get.cpp`: Implements the `GET` operation to retrieve files. Queries replicas for the newest version and fetches data.
- `src/merge.cpp`: Supports the `MERGE` operation, combining updated file chunks or replicas into a unified consistent version.
- `src/rebalance.cpp`: Handles ownership transfers and replica redistribution when new nodes join or existing nodes leave.
- `src/re-replicated.cpp`: Performs re-replication to recover lost replicas after failures, ensuring redundancy and durability.
- `src/ls.cpp`: Implements the `LS` command to list all files currently in the distributed system and their corresponding replica locations.
- `src/successor_list.cpp`: Manages the consistent hashing ring’s successor and predecessor relationships, ensuring proper file ownership and replication mapping.
- `src/grep.cpp`: Implements the distributed `GREP` command. Executes pattern matching locally on each node and streams results to the requesting node.

## Sources Referred To

- [Beej’s Guide to Network Programming](https://beej.us/guide/bgnet/html/)  
- Linux man pages for networking functions (e.g., [recv()](https://linux.die.net/man/2/recv))  

## Compile

run ```make``` under the directory mp3

## Run the system
```
./build/main
```

## Commands

- `create localfilename HyDFSfilename`
  Uploads a local file (`localfilename`) from the VM’s local directory to HyDFS and stores it as `HyDFSfilename`.  

- `get HyDFSfilename localfilename`
  Downloads a file named `HyDFSfilename` from HyDFS and saves it locally as `localfilename`.  

- `append localfilename HyDFSfilename`
  Appends the contents of `localfilename` to the existing distributed file `HyDFSfilename` in HyDFS.  

- `merge HyDFSfilename`  
  Merges multiple updated replicas of `HyDFSfilename` into a single consistent version after concurrent updates.

- `ls HyDFSfilename`
  Lists all VMs currently storing replicas of the specified `HyDFSfilename`.

- `liststore`
  Lists all files currently stored on the local VM (including replicas and owned files).

- `getfromreplica VMaddress HyDFSfilename localfilename` 
  Fetches the file `HyDFSfilename` directly from a specified replica (`VMaddress`) and saves it locally as `localfilename`.

- `list_mem_ids` 
  Displays all active members (VM IDs) currently in the HyDFS membership list.

- `multiappend HyDFSfilename #num_of_local_file VMi ... VMj localfilenamei ... localfilenamej`
  Performs a multi-node append operation, appending multiple local files (`localfilenamei`, …, `localfilenamej`) to a shared distributed file (`HyDFSfilename`) across the specified nodes (`VMi`, …, `VMj`).


## Contributions

- Kuangyi: Code design, ~30% of code, run expereimtn, report write up

- Yun-Yang: Code design, ~70% of code


