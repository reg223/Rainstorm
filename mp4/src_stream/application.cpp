#include <stdio.h>
#include <string>
#include <vector>
#include "rainstorm.hpp"


using namespace std;


bool RainStorm::filter(const Tuple& t, const std::string& pattern) {
    const std::string& content = t.value; 
    if (content.find(pattern) != std::string::npos) {
        return true;
    }
    return false;
}

void RainStorm::transform(Tuple& t) {
    std::string input_line(t.value);
    char delimiter = ',';
    int delimiter_count = 0;
    bool in_quotes = false;
    size_t end_position = input_line.length();

    // 1. Iterate through the line to find the position of the 3rd FIELD SEPARATOR
    for (size_t i = 0; i < input_line.length(); ++i) {
        char c = input_line[i];

        if (c == '"') 
            in_quotes = !in_quotes;

        if (c == delimiter && !in_quotes) {
            delimiter_count++;
            if (delimiter_count == 3) {
                end_position = i; 
                break;
            }
        }
    }
    
    std::string projected_content = input_line.substr(0, end_position);

    std::strncpy(t.value, projected_content.c_str(), sizeof(t.value) - 1);
    t.value[sizeof(t.value) - 1] = '\0'; 

    return;
}

void RainStorm::aggregate_by_key(Tuple& t, int N) {

  std::string col_val = extract_nth_field(t.value, N);

  // string new_key = std::string(t.key) + ":" + col_val;
  string new_value = col_val;

  std::strncpy(t.value, new_value.c_str(), sizeof(t.value) - 1);
  t.value[sizeof(t.value) - 1] = '\0';

  return;
}

std::string RainStorm::extract_nth_field(const std::string& line, int N) {
    if (N <= 0) return ""; 

    std::string current_field_value;
    int current_field_index = 0;
    bool in_quotes = false;
    
    for (size_t i = 0; i < line.length(); ++i) {
        char c = line[i];

        if (c == '"') {
            in_quotes = !in_quotes;
            continue;
        }

        if (c == ',' && !in_quotes) {
            current_field_index++;
            
            if (current_field_index == N)
                return current_field_value;
            
            current_field_value = "";
            continue;
        }
        current_field_value += c;
    }

    current_field_index++;
    if (current_field_index == N) {
        return current_field_value;
    }

    return ""; // Requirement: treat missing data as an empty string key.
}