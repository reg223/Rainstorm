#include <cstring>
#include <future>
#include <iostream>
#include <unistd.h>

#include "rainstorm.hpp"

using namespace std;


int main(int argc, char* argv[]) {
  if (argc != 9) {
      std::cerr << "[StreamTask] Error: expect 9 argc\n";
      return 1;
  }

  

  char hostname[40];
  gethostname(hostname, sizeof(hostname));
  std::string self_host = hostname;

  int task_id = std::stoi(argv[1]);
  int stage_id = std::stoi(argv[2]);
  string self_port = argv[3];
  std::string operator_exec = argv[4];
  std::string operator_args = argv[5];
  std::string src_dir = argv[6];
  std::string dst_file = argv[7];
  int exactly_once = std::stoi(argv[8]);

  NodeId leader = NodeId::createNewNode(LEADER_HOST, LEADER_PORT);
  NodeId self = NodeId::createNewNode(self_host, self_port);

  RainStorm task(
    task_id,
    stage_id,
    operator_exec,
    operator_args,
    "./storage/local_storage/" + src_dir, // read the src from local storage
    dst_file,
    exactly_once,
    self,
    leader
  );

  auto incoming_leader = std::async(std::launch::async, [&task]() { task.handleIncomingLeader(); });

  // Either tuple receiver OR source task
  std::future<void> tuple_receiver_thread;

  if (task_id == -1) {
      // SOURCE TASK
      tuple_receiver_thread = std::async(std::launch::async, [&task]() { task.runSourceTask(); });
  } else {
      // WORKER TASK
      tuple_receiver_thread = std::async(std::launch::async, [&task]() { task.handleIncomingTuples(); });
  }

  // // Tuple processor thread (same for all tasks)
  auto tuple_processor_thread = std::async(std::launch::async, [&task]() { task.handleOutgoingTuples(); });

  auto timer_thread = std::async(std::launch::async, [&task]() { task.handle_timer(); });

  // incoming_leader.get();
  // data_input_thread.get();
  // tuple_processor_thread.get();


    return 0;
}



