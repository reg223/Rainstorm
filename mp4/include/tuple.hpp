#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <ostream>
#include <vector>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>

enum class TupleMessageType : uint8_t {
    FINAL_OPUTPUT, INPUT_COMMIT, ACK_COMMIT, AGGREGATE_OUTPUT
};

struct Tuple {
    TupleMessageType type;
    int tuple_id;
    char key[128];
    char value[512];
    char dst_file[128];

    int stage_id;
    int task_id;

    size_t serialize(char* buffer, size_t buffer_size) const;
    static Tuple deserialize(const char* buffer, size_t buffer_size);
};

template <typename T>
class BlockingQueue {
    private:
        std::queue<T> q;
        std::mutex m;
        std::condition_variable cv;

    public:
        void push(const T& item) {
            {
                std::lock_guard<std::mutex> lock(m);
                q.push(item);
            }
            cv.notify_one();
        }

        T pop() {
            std::unique_lock<std::mutex> lock(m);
            cv.wait(lock, [this] { return !q.empty(); });

            T item = q.front();
            q.pop();
            return item;
        }

        bool empty() {
            std::lock_guard<std::mutex> lock(m);
            return q.empty();
        }

        size_t size() {
            std::lock_guard<std::mutex> lock(m);
            return q.size();
        }
};

