#pragma once

#include <cstdint>
#include <random>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

#include "logger.hpp"
#include "message.hpp"
#include "shared.hpp"
class MembershipList {
 public:
  MembershipList(Logger& logger) : logger(logger), rng(rd()) {}

  void addNode(MembershipInfo info);
  void updateNodeStatus(const NodeId& node_id, NodeStatus new_status);
  void removeNode(const NodeId& node_id, bool called_by_handle_leave = false);
  void updateHeartBeatCounter(const NodeId& node_id, uint32_t new_heartbeat);
  void incrementHeartBeatCounter(const NodeId& node_id);
  void updateLocalTime(const NodeId& node_id);
  void updateIncarnation(const NodeId& node_id, uint32_t new_incarnation);
  void incrementIncarnation(const NodeId& node_id);
  void updateMode(const NodeId& node_id, FailureDetectionMode new_mode);
  MembershipInfo getNodeInfo(const NodeId& node_id);

  std::vector<MembershipInfo> selectKRandom(unsigned int k, const NodeId& self_node_id) const;
  std::vector<MembershipInfo> copy() const {
    std::shared_lock<std::shared_mutex> lock(mtx);
    std::vector<MembershipInfo> values;
    values.reserve(mem_list.size());
    for (auto& [key, value] : mem_list) values.push_back(value);
    return values;
  }

  // helper
  void printMemList() const;

 private:
  mutable Logger logger;
  std::unordered_map<NodeId, MembershipInfo> mem_list;

  mutable std::shared_mutex mtx;  // allows multiple readers, single writer
  
  // Random number generation for selectKRandom optimization
  std::random_device rd;
  mutable std::mt19937 rng;
};