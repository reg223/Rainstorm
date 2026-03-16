// #include <stdio.h>
// #include <string>
// #include <vector>
// #include "tuple.hpp"


// using namespace std;

// int main(int argc, char* argv[]) {
//   string pattern;
//   pattern = argv[1];
//   char key[128],value[512];
//   std::cin >> key >> value;
//   Tuple result = filter()
//   // cout << 
//   return 0;
// }

// Tuple filter(Tuple input, const string& pattern) {
//     string result;
//     size_t pos = 0, found;
//     string f(input.value);
//     if (f.find(pattern, pos) != string::npos) {
//         return input;
//     }
//     return;
// }

// vector<Tuple> aggregate(vector<Tuple> history, Tuple newT, char *key){
//   if (!strcmp(newT.key, key) ) {
//     history.push_back(newT);
//   }
//   return history;
// }


// Tuple trans_firstt(Tuple input, const string& pattern){
  
//   char* token = input.value;
//   int count = 0;
//   int idx = 0;
//   while (count < 3 && idx++ < 512){
//     if (token[idx] == ',') count++;
//   }
//   Tuple out;
//   out.tuple_id = input.tuple_id;

//   std::strncpy(out.key, input.key, sizeof(out.key) - 1);
//   std::strncpy(out.value, string(input.value).substr(0,idx).c_str(), sizeof(out.value) - 1);
//   std::strncpy(out.dst_file, input.dst_file, sizeof(out.dst_file) - 1);
//   out.stage_id = input.stage_id;
//   out.task_id = input.task_id;
//   return out;

// }
//TODO tuple checkpoint
// TODO