
#include "logger.hpp"

#include <cstdlib>
#include <ctime>
#include <iostream>

void Logger::log(const std::string &stmt) {
  std::time_t now = std::time(nullptr);
  out << "[" << static_cast<uint32_t>(now) << "]: " << stmt << std::endl;
}