#pragma once

#include <netinet/in.h>
#include <arpa/inet.h>

#include <cstdint>
#include <iostream>
#include <sstream>
#include <string_view>
#include <random>
#include <algorithm>
#include <unordered_set>

#include "logger.hpp"
#include "membership_list.hpp"
#include "message.hpp"
#include "shared.hpp"
#include "socket.hpp"
#include "tcp_socket.hpp"
#include "successor_list.hpp"
#include "file_system.hpp"

#define HEARTBEAT_FREQ 1  // seconds
#define PING_FREQ 1       // seconds
#define T_TIMEOUT 2       // seconds
#define T_FAIL 2          // seconds
#define T_CLEANUP 2       // seconds
#define K_RANDOM 3
#define N_successors 2

#define FILE_PORT "8081"

class Node {
 public:
  Node(const std::string_view& host, const std::string_view& port, NodeId& introducer,
       Logger& logger);

  void handleIncoming();
  void handleIncomingFile();
  void handleOutgoing();

  void append(const std::string& localfile, const std::string& HyDFSfilename);
  void multiappend(
    const std::string& HyDFSfilename, 
    const std::vector<std::string> vm_id, 
    const std::vector<std::string> local_files);
  void merge(const std::string& HyDFSfilename);
  void ls(const std::string& HyDFSfilename);
  void liststore();
  void create(const std::string& localfile, const std::string& HyDFSfilename);
  void getfromreplica(const std::string& vm_id, const std::string& HyDFSfilename, const std::string& localfilename);
  void get(const std::string& HyDFSfilename, const std::string& localfilename);
  void grep(const std::string& pattern, const std::string& HyDFSfilename);


  void joinNetwork();
  void leaveNetwork();
  void switchModes(FailureDetectionMode mode);

  inline void logMemList() { mem_list.printMemList(); };
  inline void logSuccList() { successor_list.printSuccList(); };
  inline void logSelf() {
    std::stringstream ss;
    ss << mem_list.getNodeInfo(self);
    logger.log(ss.str());
    std::cout << "\n";
  };
  inline void logSuspects() {
    std::cout << "Suspected nodes:\n";
    bool found = false;

    for (const auto& member : mem_list.copy()) {
      if (member.status == NodeStatus::SUSPECT) {
        std::cout << " --> " << member.node_id << " (incarnation=" << member.incarnation << ")\n";
        found = true;
      }
    }

    if (!found) {
      std::cout << " --> None\n";
    }
    std::cout << "\n";
  };
  inline void logProtocol() {
    std::string protocol;
    switch (fd_mode) {
      case FailureDetectionMode::GOSSIP_WITH_SUSPICION:
        protocol = "<gossip, suspect>\n";
        break;
      case FailureDetectionMode::PINGACK_WITH_SUSPICION:
        protocol = "<ping, suspect>\n";
        break;
      case FailureDetectionMode::GOSSIP:
        protocol = "<gossip, nosuspect>\n";
        break;
      case FailureDetectionMode::PINGACK:
        protocol = "<ping, nosuspect>\n";
        break;
    }
    logger.log(protocol);
  }

 private:
  void handleJoin(std::array<char, UDPSocketConnection::BUFFER_LEN>& buffer,
                  struct sockaddr_in& client_addr, MembershipInfo& new_node);

  void handleLeave(const MembershipInfo& leaving_node);

  void handlePing(std::array<char, UDPSocketConnection::BUFFER_LEN>& buffer,
                  struct sockaddr_in& dest_addr, const MembershipInfo& node);
  Message sendPing();

  void handleAck(const MembershipInfo& node);
  Message sendAck();

  std::vector<MembershipInfo> handleGossip(const Message& messages);
  void sendGossip(std::array<char, UDPSocketConnection::BUFFER_LEN>& buffer,
                  std::vector<MembershipInfo>& updates,
                  MessageType message_type = MessageType::GOSSIP);

  void runPingAck(bool enable_suspicion);
  void runGossip(bool enable_suspicion);
  bool updateStatus(const MembershipInfo& node, const uint32_t time_delta, bool enable_suspicion);

  void handleSwitch(const Message& message);

  std::string modePrefix(FailureDetectionMode mode);


  // ====function for mp3

  //locate a file on the ring
  NodeId file_to_node(const std::string& HyDFSfilename);
  void send_file_chunk(const NodeId& target_node, const vector<FileChunkMessage>& chunks);

  void handle_AppendReq(const FileChunkMessage& message);

  void handle_createReq(const FileChunkMessage& message);
  void create_replica(const std::string& HyDFSfilename);
  void handle_create_replica_Req(const FileChunkMessage& message);

  void handle_GetReq(const FileChunkMessage& message);
  void handle_GetRes(const FileChunkMessage& message);
  void handle_GetFetchReq(const FileChunkMessage& message);
  void handle_GetFetchRes(const FileChunkMessage& message);
  vector<NodeId> get_Get_target(const std::string& HyDFSfilename);
 
  
  void handle_MergeReq(const FileChunkMessage& message);
  void handle_MergeRes(const FileChunkMessage& message);
  void handle_MergeFetchReq(const FileChunkMessage& message);
  void handle_MergeFetchRes(const FileChunkMessage& message);
  void handle_MergeUpdate(const FileChunkMessage& message);
  vector<NodeId> get_merge_target(const std::string& HyDFSfilename);

  void handle_MultiappendReq(const FileChunkMessage& message);

  void handle_LSReq(const FileChunkMessage& message);
  void handle_LSRes(const FileChunkMessage& message);

  void startRebalance();
  void handle_RebalanceReq(const FileChunkMessage& message);
  void handle_RebalanceReS(const FileChunkMessage& message);
  void deleteRedundent();
  void handle_RebalanceDel();

  NodeId get_successor_fail(const NodeId& failed_node);
  void reassign_files_after_failure();
  bool was_my_successor(const NodeId& failed_node);
  void startReReplicated(const NodeId& failed_node, bool should_re_replicate);
  void Run_ReReplicate();
  void handle_ReReplicateRes(const FileChunkMessage& message);


  void handle_GrepReq(const FileChunkMessage& message);
  void handle_GrepRes(const FileChunkMessage& message);
  void handle_GrepDone(const FileChunkMessage& message);
  
  
  UDPSocketConnection socket;
  // use different port for file transfer
  // UDPSocketConnection file_socket;
  TCPSocketConnection file_socket;
  NodeId self, introducer;
  MembershipList mem_list;
  // add successor list
  SuccessorList successor_list;
  // for local storage and HyDFS storage
  FileSystem file_system;

  Logger& logger;
  FailureDetectionMode fd_mode;
  bool left = false, introducer_alive = false;
  float drop_rate = 0.0f;

  int lamport_timestamp;

  

  //keep the incoming file chunk in the buffer
  std::unordered_map<std::string, FileBuffer> pending_files;

  std::vector<NodeId> get_target, merge_target;
  std::unordered_map<std::string, std::vector<FileChunkMessage>> get_responses, merge_responses;
  std::unordered_map<std::string, string> get_localfilename;

  // if is new node, after receive membership, start rebalance
  bool is_new_node;
};