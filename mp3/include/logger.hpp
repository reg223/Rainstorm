#pragma once
#include <ostream>
#include <string>

class Logger {
 public:
  Logger(std::ostream& out) : out(out) {}
  void log(const std::string& stmt);

 private:
  std::ostream& out;
};