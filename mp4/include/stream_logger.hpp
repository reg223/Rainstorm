#pragma once
#include <fstream>
#include <mutex>
#include <string>
#include <chrono>
#include <iomanip>
#include <sstream>

class Stream_Logger {
public:
    Stream_Logger() = default;   // Required for Option A

    // Open log file after construction
    void open(const std::string& file_path) {
        file.open(file_path, std::ios::app);  // append mode
        if (!file.is_open()) {
            throw std::runtime_error("Failed to open log file: " + file_path);
        }
    }

    void log(const std::string& msg) {
        std::lock_guard<std::mutex> lock(mu);
        file << timestamp() << " " << msg << std::endl;
        file.flush();
    }

private:
    std::ofstream file;
    std::mutex mu;
    
    std::string timestamp() {
        using namespace std::chrono;

        auto now = system_clock::now();
        auto itt = system_clock::to_time_t(now);
        auto ms = duration_cast<milliseconds>(now.time_since_epoch()) % 1000;

        std::ostringstream ss;
        
        // Define a thread-local tm structure
        struct tm tm_buf; 
        
        // Use the thread-safe version: localtime_r
        if (localtime_r(&itt, &tm_buf)) {
            ss << std::put_time(&tm_buf, "%Y-%m-%d %H:%M:%S"); 
        } else {
            ss << "YYYY-MM-DD HH:MM:SS"; 
        }

        ss << "." << std::setw(3) << std::setfill('0') << ms.count();
        return ss.str();
    }
};
