#include <iostream>
#include <fstream>
#include <cstdlib>
using namespace std;

// #define STRESSTEST

const int SEED = 42;

int main(int argc, char* argv[]) {
    string vm_id = argv[1];  
    string filename = "machine." + vm_id + ".log";

    ofstream fout(filename);
    if (!fout) {
        cerr << "Error creating " << filename << endl;
        return 1;
    }

    srand(SEED + stoi(vm_id)); 


    // patterns that occur in all logs
    fout << "fix pattern 1234\n";
    fout << "fix pattern 2345\n";
    fout << "fix pattern 3456\n";
    fout << "fix pattern 4567\n";

    // patterns that occur in one logs
    if (vm_id == "03") {
        fout << "rare pattern in vm03 9999\n";
    }

    // patterns that occur in some
    if (vm_id == "01" || vm_id == "03" || vm_id == "05" || vm_id == "07"){
        for (int i = 0; i < 50; i++) {
            fout << "frequent pattern " <<  + i << "\n";
        }
    }
    
    // Random pattern
    for (int i = 0; i < 500; i++) {
        fout << "random pattern" << rand() % 10000 << "\n";
    }

#ifdef STRESSTEST

    // Add a lot of lines to test performance
    for (int i = 0; i < 50000; i++) {
        fout << "extensive pattern " << i << ": some log entry here\n";
        fout << "A Super Looooooooooooooooooooooooong line that repeats itself A Super Looooooooooooooooooooooooong line that repeats itself A Super Looooooooooooooooooooooooong line that repeats itself\n";
    }

#endif

    fout.close();
    cout << "Generated log file " << filename << endl;
    return 0;
}
