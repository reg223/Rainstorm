#pragma once

#include <netinet/in.h>

#include <string>

enum FailureDetectionMode : uint8_t {
  GOSSIP_WITH_SUSPICION,
  PINGACK_WITH_SUSPICION,
  GOSSIP,
  PINGACK,
};

class SharedFunctions {
 public:
  static std::string modeToStr(FailureDetectionMode mode);
};