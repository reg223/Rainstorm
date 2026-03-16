#include "rainstorm.hpp"

// Handles:
// 1. pop tuple from queue
// 2. apply operator
// 3. forward or write output

void RainStorm::handleOutgoingTuples() {

    {
        std::unique_lock<std::mutex> lock(start_mtx);
        start_cv.wait(lock, [&]{ return stream_started; });
    }

    logger.log("[PROCESSOR] tuple_processor_thread started.");

    // const int hw = 25; // 25 tuples/sec (default)
    // const int sleep_ms = 1000 / hw;

    while (true) {

        // Block until a tuple is available
        Tuple t = tuple_queue.pop();

        logger.log("[PROCESSOR] Processing tuple_id=" + std::to_string(t.tuple_id) + " key=" + std::string(t.key));

        if (operator_exec == "SOURCE"){ // SOURCE task
            // just forward
        }
        else if (operator_exec == "dummy"){ // do nothing
            t.stage_id = stage_id;
            t.task_id  = task_id;
        }
        else if (operator_exec == "Filter"){
            if (filter(t, operator_args)){
                t.stage_id = stage_id;
                t.task_id  = task_id;
            }
            else
                continue;
        }
        else if (operator_exec == "Transform"){
            transform(t);
            t.stage_id = stage_id;
            t.task_id  = task_id;
        }
        else if (operator_exec == "AggregateByKey"){
            aggregate_by_key(t, std::stoi(operator_args));
            t.stage_id = stage_id;
            t.task_id  = task_id;
        }

        if (is_last_stage) {
            appendToOutput(t);
        }
        else{
            // Not last stage , forward to next stage workers
            forwardTuple(t);
        }
        

        // std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
    }
}

// send the tuples to self node, then append to the file
void RainStorm::appendToOutput(Tuple& t){
    logger.log("[PROCESSOR] Writing tuple_id=" + std::to_string(t.tuple_id) +
               " key=" + t.key + " to " + self.host + ":" + TUPLE_PORT);
    if (operator_exec == "AggregateByKey")
        t.type = TupleMessageType::AGGREGATE_OUTPUT;
    else
        t.type = TupleMessageType::FINAL_OPUTPUT;
    send_persistent(self.host, TUPLE_PORT, t);
}

void RainStorm::forwardTuple(const Tuple& t) {
    // Hash partitioning on key 
    std::string key_str(t.key);
    size_t h = std::hash<std::string>{}(key_str);
    int idx = static_cast<int>(h % num_next_tasks);

    // Declare variables OUTSIDE the lock scope
    std::string target_host;
    std::string target_port;

    // Get target host and port safely within the critical section
    {
        // routing_mtx protects num_next_tasks, next_task_hosts, and next_task_ports
        std::unique_lock<std::mutex> lock(routing_mtx);
        
        // Use bounds check just in case the partitioning logic or update failed
        if (idx >= num_next_tasks) {
            // FATAL ERROR: Partition index out of bounds, routing state is inconsistent
            logger.log("[FATAL] Routing index " + std::to_string(idx) + 
                       " out of bounds for " + std::to_string(num_next_tasks) + " tasks.");
            return; 
        }

        // Safely copy the values
        target_host = next_task_hosts[idx];
        target_port = next_task_ports[idx];
    } // Mutex
    

    logger.log("[PROCESSOR] Attempting to forward tuple_id=" + std::to_string(t.tuple_id) + " to " + target_host + ":" + target_port);
    
    // ACK and Retry Logic
    bool ack_received = false;

    for (int attempt = 0; attempt < MAX_RETRIES; ++attempt) {
        // This helper function now sends, waits for ACK, and returns its status.
        if (send_and_wait_for_ack(target_host, target_port, t)) {
            ack_received = true;
            if (exactly_once==1 && task_id!=-1)
                commit_to_checkpoint(t, TupleMessageType::ACK_COMMIT);
            logger.log("[PROCESSOR] ACK received for tuple_id=" + std::to_string(t.tuple_id) + " on attempt " + std::to_string(attempt + 1));
            break; // Success, exit retry loop
        }
        
        logger.log("[PROCESSOR] Timeout or failure for tuple_id=" + std::to_string(t.tuple_id) + ". Retrying...");
        // Implement a backoff or sleep before retrying to avoid overwhelming the network
        std::this_thread::sleep_for(std::chrono::milliseconds(1000)); 
    }

    if (!ack_received) {
        logger.log("[FATAL] Failed to deliver tuple_id=" + std::to_string(t.tuple_id) + " after " + std::to_string(MAX_RETRIES) + " attempts.");
    }
}