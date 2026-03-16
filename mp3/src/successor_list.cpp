#include "node.hpp"

#include <array>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <thread>
#include <vector>


SuccessorList::SuccessorList(int n_successors, NodeId& self_node, Logger& logger)
    : N(n_successors), self(self_node), logger(logger) {
    }

// Update the successor list
void SuccessorList::update_successor_list(const MembershipList& mem_list) {
  std::lock_guard<std::mutex> lock(mtx);
  auto all_members = mem_list.copy();
  if (all_members.empty()) return;

  // Sort by node_id
  std::sort(all_members.begin(), all_members.end(),
          [](const MembershipInfo& a, const MembershipInfo& b) {
            return std::string(a.node_id.host) < std::string(b.node_id.host);
          });


  // Find current node
  auto it = std::find_if(all_members.begin(), all_members.end(),
                         [&](const MembershipInfo& info) {
                           return info.node_id == self;
                         });

  if (it == all_members.end()) {
    std::cerr << "[SuccessorList] Error: self node not found in membership list.\n";
    return;
  }

  size_t total = all_members.size();

  successor_list.clear();
  // Compute N successors clockwise (wrap around ring)
  size_t start_idx = std::distance(all_members.begin(), it);
  // If there's only one node, no successors
  // if (total <= 1) return;     !!!! not sure if this OK
  for (int i = 1; i <= N; ++i) {
    size_t idx = (start_idx + i) % total;
    // stop if wrapped back to self
    if (idx == start_idx)
      break;
    successor_list.push_back(all_members[idx].node_id);
  }

  // // Build predecessor list
  predecessor_list.clear();
  for (int i = 1; i <= N; ++i) {
    // move counter-clockwise
    size_t idx = (start_idx + total - i) % total;
    if (idx == start_idx) break;
    predecessor_list.push_back(all_members[idx].node_id);
  }
  

  // std::stringstream ss;
  // ss << "Updated successor_list:\n";
  // for (long unsigned int i=0; i<successor_list.size(); i++){
  //   ss << "successor " << i << ": " << successor_list[i] << "\n";
  // }
  // for (long unsigned int i=0; i<predecessor_list.size(); i++){
  //   ss << "predecessor " << i << ": " << predecessor_list[i] << "\n";
  // }
  // logger.log(ss.str());
}

// Get successors safely
vector<NodeId> SuccessorList::getSuccessors() const {
  std::lock_guard<std::mutex> lock(mtx);
  return successor_list;
}

vector<NodeId> SuccessorList::getPredecessors() const {
  std::lock_guard<std::mutex> lock(mtx);
  return predecessor_list;
}

void SuccessorList::printSuccList() const {
  std::lock_guard<std::mutex> lock(mtx);
  // cout << successor_list.size() << endl;
  for (long unsigned int i=0; i<successor_list.size(); i++){
    std::stringstream ss;
    ss << "successor " << i << ": " << successor_list[i];
    logger.log(ss.str());
  }

  for (long unsigned int i=0; i<predecessor_list.size(); i++){
    std::stringstream ss;
    ss << "predecessor " << i << ": " << predecessor_list[i];
    logger.log(ss.str());
  }

  cout << std::endl;
  
}
