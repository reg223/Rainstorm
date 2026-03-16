
#include "shared.hpp"

std::string SharedFunctions::modeToStr(FailureDetectionMode mode) {
  switch(mode) {
    case FailureDetectionMode::GOSSIP_WITH_SUSPICION:
      return "GOSSIP_WITH_SUSPICION";
    case FailureDetectionMode::GOSSIP:
      return "GOSSIP";
    case FailureDetectionMode::PINGACK_WITH_SUSPICION:
      return "PINGACK_WITH_SUSPICION";
    case FailureDetectionMode::PINGACK:
      return "PINGACK";
    default:
      return "INVALID";
  }
}