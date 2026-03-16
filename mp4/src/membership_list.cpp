#include "membership_list.hpp"

#include <cstdint>
#include <ctime>
#include <mutex>
#include <random>
#include <shared_mutex>
#include <sstream>
#include <stdexcept>
#include <vector>
#include <algorithm>

#include "message.hpp"
#include <iostream>

void MembershipList::addNode(MembershipInfo info) {
  std::unique_lock<std::shared_mutex> lock(mtx);
  const auto id = info.node_id;
  if (auto it = mem_list.find(id); it != mem_list.end()) return;  // node alr exists
  if (info.status == NodeStatus::DEAD || info.status == NodeStatus::LEFT)
    return;  // if we don't have node and someone tells us its dead don't need to add
  const auto mode = SharedFunctions::modeToStr(info.mode);
  mem_list.emplace(id, std::move(info));
  std::stringstream ss;
  ss << "Added node: " << id << " in mode: " << mode << "\n";
  logger.log(ss.str());
}

void MembershipList::updateNodeStatus(const NodeId& node_id, NodeStatus new_status) {
  std::unique_lock<std::shared_mutex> lock(mtx);
  if (auto it = mem_list.find(node_id); it != mem_list.end()) {
    it->second.status = new_status;
    it->second.local_time = currTime();
    std::stringstream ss;
    ss << "Updated node " << node_id << " status to " << new_status;
    logger.log(ss.str());
  }
}

void MembershipList::removeNode(const NodeId& node_id, bool called_by_handle_leave) {
  std::unique_lock<std::shared_mutex> lock(mtx);
  mem_list.erase(node_id);
  std::stringstream ss;
  if (called_by_handle_leave) {
    ss << "Node left: " << node_id << "\n";
  } else {
    ss << "Removed node: " << node_id << "\n";
  }
  logger.log(ss.str());
}

void MembershipList::updateHeartBeatCounter(const NodeId& node_id, uint32_t new_heartbeat) {
  std::unique_lock<std::shared_mutex> lock(mtx);
  if (auto it = mem_list.find(node_id); it != mem_list.end()) {
    it->second.heartbeat_counter = new_heartbeat;
    it->second.local_time = currTime();
    // std::stringstream ss;
    // ss << "Updated node " << node_id << " heartbeat to " << new_heartbeat;
    // logger.log(ss.str());
  }
}

void MembershipList::incrementHeartBeatCounter(const NodeId& node_id) {
  std::unique_lock<std::shared_mutex> lock(mtx);
  if (auto it = mem_list.find(node_id); it != mem_list.end()) {
    ++it->second.heartbeat_counter;
    it->second.local_time = currTime();
    // std::stringstream ss;
    // ss << "Incremented node " << node_id << " heartbeat to " << it->second.heartbeat_counter;
    // logger.log(ss.str());
  }
}

MembershipInfo MembershipList::getNodeInfo(const NodeId& node_id) {
  std::shared_lock<std::shared_mutex> lock(mtx);
  if (auto it = mem_list.find(node_id); it != mem_list.end()) {
    return it->second;
  }
  throw std::runtime_error("Trying to get info about node that doesn't exist");
}

void MembershipList::updateMode(const NodeId& node_id, FailureDetectionMode new_mode) {
  std::unique_lock<std::shared_mutex> lock(mtx);
  if (auto it = mem_list.find(node_id); it != mem_list.end()) {
    it->second.mode = new_mode;
    it->second.local_time = currTime();
    std::stringstream ss;
    ss << "Updated node " << node_id << " mode to " << SharedFunctions::modeToStr(new_mode);
    logger.log(ss.str());
  }
}

// code inspired by stackoverflow and ChatGPT
// this algo is reservoir sampling
std::vector<MembershipInfo> MembershipList::selectKRandom(unsigned int k,
                                                          const NodeId& self_node_id) const {
  std::shared_lock<std::shared_mutex> lock(mtx);

  // Count eligible nodes (excluding self)
  size_t eligible_count = 0;
  for (const auto& kv : mem_list) {
    if (!(kv.first == self_node_id)) {
      eligible_count++;
    }
  }

  const auto n = eligible_count;
  std::vector<MembershipInfo> result;
  result.reserve(k < n ? k : n);

  size_t i = 0;
  for (const auto& kv : mem_list) {
    // Skip self node
    if (kv.first == self_node_id) {
      continue;
    }

    if (result.size() < k) {
      result.push_back(kv.second);
    } else {
      std::uniform_int_distribution<size_t> dist(0, i);
      size_t j = dist(rng);
      if (j < k) {
        result[j] = kv.second;
      }
    }
    ++i;
  }

  return result;
}

void MembershipList::updateIncarnation(const NodeId& node_id, uint32_t new_incarnation) {
  std::unique_lock<std::shared_mutex> lock(mtx);
  if (auto it = mem_list.find(node_id); it != mem_list.end()) {
    it->second.local_time = currTime();
    it->second.incarnation = new_incarnation;
  }
}

void MembershipList::incrementIncarnation(const NodeId& node_id) {
  std::unique_lock<std::shared_mutex> lock(mtx);
  if (auto it = mem_list.find(node_id); it != mem_list.end()) {
    it->second.local_time = currTime();
    it->second.incarnation++;
  }
}

void MembershipList::updateLocalTime(const NodeId& node_id) {
  std::unique_lock<std::shared_mutex> lock(mtx);
  if (auto it = mem_list.find(node_id); it != mem_list.end()) {
    it->second.local_time = currTime();
    // std::stringstream ss;
    // ss << "Updated local_time for " << it->second.node_id << " to " << it->second.local_time;
    // logger.log(ss.str());
  }
}

void MembershipList::printMemList() const {
  auto all_members = this->copy();
  // Sort by node_id
  std::sort(all_members.begin(), all_members.end(),
          [](const MembershipInfo& a, const MembershipInfo& b) {
            return std::string(a.node_id.host) < std::string(b.node_id.host);
          });
  for (const auto& mem : all_members) {
    int vm_id = std::stoi(std::string(mem.node_id.host).substr(13, 2));
    std::stringstream ss;
    ss << mem;
    ss << ", ring_id=" << vm_id;
    logger.log(ss.str());
  }
  std::cout << "\n";
}