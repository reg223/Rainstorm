#pragma once


#include <cstdint>
#include <random>
#include <shared_mutex>
#include <vector>
#include <algorithm>

#include "logger.hpp"
#include "message.hpp"
#include "shared.hpp"

using namespace std;

class SuccessorList {
 public:
    SuccessorList(int n_successors, NodeId& self_node, Logger& logger);

    void update_successor_list(const MembershipList& mem_list);
    vector<NodeId> getSuccessors() const;
    vector<NodeId> getPredecessors() const;

    // helper
    void printSuccList() const;

 private:
    int N;
    NodeId self;
    vector<NodeId> successor_list;
    vector<NodeId> predecessor_list;
    mutable Logger logger;
    mutable std::mutex mtx;
};