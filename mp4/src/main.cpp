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

int main(int argc, char* argv[]) {

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
  auto incoming_stream_future = std::async(std::launch::async, [&node]() { node.handleIncomingStreamMsg(); });
  auto incoming_tuple_future = std::async(std::launch::async, [&node]() { node.handleIncomingTuples(); });
  auto outgoing_future = std::async(std::launch::async, [&node]() { node.handleOutgoing(); });
  std::future<void> scaling_manager_thread; 

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
    else if (input == "RainStorm"){

      int Nstages, Ntasks;
      std::cin >> Nstages >> Ntasks;

      if (Nstages < 1 || Nstages > 3) {
        std::cerr << "[RainStorm] Error: Nstages must be between 1 and 3.\n";
        continue;
      }

      // Read operator exec + args for each stage
      std::vector<std::string> op_exec(Nstages);
      std::vector<std::string> op_args(Nstages);

      for (int i = 0; i < Nstages; i++) {
        std::cin >> op_exec[i] >> op_args[i];
      }

      // Input/output in HyDFS
      std::string src_dir, dst_file;
      std::cin >> src_dir >> dst_file;

      // Control flags
      int exactly_once_flag, autoscale_flag;
      std::cin >> exactly_once_flag >> autoscale_flag;

      bool exactly_once     = (exactly_once_flag != 0);
      bool autoscale_enabled = (autoscale_flag != 0);

      // Autoscale parameters (only used when autoscale_enabled == true)
      int INPUT_RATE = 100, LW = 0, HW = 0;
      if (autoscale_enabled) {
        std::cin >> INPUT_RATE >> LW >> HW;
        scaling_manager_thread = std::async(std::launch::async, [&node]() { node.runScalingManager(); });
      }
      node.startRainStorm(Nstages, Ntasks, op_exec, op_args, src_dir, dst_file, 
                          exactly_once, autoscale_enabled, INPUT_RATE, LW, HW);
      
    } 
    else if (input == "list_tasks"){
      node.list_tasks();
    }
    else if (input == "kill_task"){
      std::string vm_id, task_id;
      std::cin >> vm_id >> task_id;
      node.kill_restart_task(vm_id, task_id);
    }
    else if (input == "reset"){
      node.reset_stream();
    }
    else {
      std::cerr << "INVALID COMMAND" << std::endl;
    }
  }

  incoming_future.get();
  incoming_file_future.get();
  incoming_stream_future.get();
  incoming_tuple_future.get();
  outgoing_future.get();
  return 0;
}



