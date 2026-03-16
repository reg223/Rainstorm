#include <cstring>
#include <future>
#include <iostream>
#include <unistd.h>

#include "logger.hpp"
#include "message.hpp"
#include "node.hpp"

#define INTRODUCER_HOST "fa25-cs425-5301.cs.illinois.edu"
#define INTRODUCER_PORT "8080"

#define PORT "8080"


using namespace std;

// //for testing
uint64_t fnv1a_test(const std::string& str) {
    uint64_t hash = 1469598103934665603ULL;
    for (char c : str) {
        hash ^= static_cast<unsigned char>(c);
        hash *= 1099511628211ULL;
    }
    return hash;
}

int hashToKey_test(const std::string& filename) {
    uint64_t hash = fnv1a_test(filename);
    return static_cast<int>(hash % 10) + 1;
}

int main(int argc, char* argv[]) {
  // if (argc != 3 && argc != 5) {
  //   std::cerr << "usage: ./build/main host port [introducer_host introducer_port]" << std::endl;
  //   exit(1);
  // }

  // Use command line arguments for introducer if provided, otherwise use defaults
  const char* introducer_host = (argc == 5) ? argv[3] : INTRODUCER_HOST;
  const char* introducer_port = (argc == 5) ? argv[4] : INTRODUCER_PORT;
  
  NodeId introducer_id = NodeId::createNewNode(introducer_host, introducer_port);

  Logger logger{std::cout};
  char hostname[40];
  gethostname(hostname, sizeof(hostname));
  Node node(hostname, PORT, introducer_id, logger);

  auto incoming_future = std::async(std::launch::async, [&node]() { node.handleIncoming(); });
  auto incoming_file_future = std::async(std::launch::async, [&node]() { node.handleIncomingFile(); });
  auto outgoing_future = std::async(std::launch::async, [&node]() { node.handleOutgoing(); });
  if (std::strcmp(hostname, introducer_host) != 0 || std::strcmp(PORT, introducer_port) != 0) {
    node.joinNetwork(); // just for test
  }
  std::string input;
  while (true) {

    std::cin >> input;
    if (input == "list_mem") {
      node.logMemList();
    } 
    else if (input == "list_mem_ids") {
      node.logMemList();
    }
    else if (input == "list_self") {
      node.logSelf();
    }
    else if (input == "grep"){
      std::string pattern, HyDFSfilename;
      std::cin >> pattern >> HyDFSfilename;
      node.grep(pattern, HyDFSfilename);
    }
    else if (input == "list_succ"){
      node.logSuccList();
    } 
    else if (input == "join") {
      // introducer cannot join to itself
      if (std::strcmp(hostname, introducer_host) != 0 || std::strcmp(PORT, introducer_port) != 0) {
        node.joinNetwork();
      }
      else {
        std::cout << "This node is the introducer and cannot join itself \n\n";
      }
    } else if (input == "leave") {
      node.leaveNetwork();
      break;
    } else if (input == "display_suspects") {
      node.logSuspects();
    } else if (input == "switch") {
      // input: switch gossip|ping suspect|nosuspect
      std::string failure_mode, suspicion_mode;
      std::cin >> failure_mode >> suspicion_mode;
      FailureDetectionMode new_fd_mode;
      bool enable_suspicion = (suspicion_mode == "suspect");
      if (failure_mode == "gossip") {
        new_fd_mode = enable_suspicion ? GOSSIP_WITH_SUSPICION : GOSSIP;
      } else if (failure_mode == "ping") {
        new_fd_mode = enable_suspicion ? PINGACK_WITH_SUSPICION : PINGACK;
      } else {
        std::cerr << "Invalid switch command" << std::endl;
        continue;
      }
      node.switchModes(new_fd_mode);
    } else if (input == "display_protocol") {
      node.logProtocol();
    } else if (input == "append") {
      std::string localfilename, HyDFSfilename;
      std::cin >> localfilename >> HyDFSfilename;
      node.append(localfilename, HyDFSfilename);
    } else if (input == "create") {
      std::string localfilename, HyDFSfilename;
      std::cin >> localfilename >> HyDFSfilename;
      node.create(localfilename, HyDFSfilename);
    } else if (input == "get") {
      std::string localfilename, HyDFSfilename;
      std::cin >> HyDFSfilename >> localfilename;
      node.get(HyDFSfilename, localfilename);
    } else if (input == "getfromreplica") {
      std::string vm_addr, localfilename, HyDFSfilename;
      std::cin >> vm_addr >> HyDFSfilename >> localfilename;
      node.getfromreplica(vm_addr, HyDFSfilename, localfilename);
    }
    else if (input == "merge") {
      std::string HyDFSfilename;
      std::cin >> HyDFSfilename;
      node.merge(HyDFSfilename);
    } else if (input == "ls") {
      std::string HyDFSfilename;
      std::cin >> HyDFSfilename;
      node.ls(HyDFSfilename);
    } else if (input == "liststore"){
      node.liststore();
    } else if (input == "multiappend") {
      std::string HyDFSfilename;
      int num_vms;
      std::cin >> HyDFSfilename >> num_vms;   // e.g., multiappend test.log 3
      std::vector<std::string> vm_id(num_vms);
      std::vector<std::string> local_files(num_vms);
      for (int i = 0; i < num_vms; ++i)
          std::cin >> vm_id[i];
      for (int i = 0; i < num_vms; ++i)
          std::cin >> local_files[i];
      if (vm_id.size() != local_files.size())
        std::cerr << "[Error] Mismatched number of VM IDs and local files\n";
      else
        node.multiappend(HyDFSfilename, vm_id, local_files);
    }
    else if (input == "hash"){
      std::string filename;
      std::cin >> filename;
      // Hash and map result to [1, 10]
      cout << hashToKey_test(filename) << endl;
      
    }else if (input == "read"){
      std::string filename;
      std::cin >> filename;
      // Hash and map result to [1, 10]
      
      
    } else {
      std::cerr << "INVALID COMMAND" << std::endl;
    }
  }

  incoming_future.get();
  incoming_file_future.get();
  outgoing_future.get();
  return 0;
}



