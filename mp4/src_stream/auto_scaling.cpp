#include "rainstorm.hpp"

void RainStorm::handle_timer() {

    while (true){
        int input_load = calculate_load();
        if (input_load > 0) {
            ReportLeader(input_load);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(TIMER_PERIOD)); 
    }
}

int RainStorm::calculate_load(){
    std::lock_guard<std::mutex> lock(counter_mtx);
    int rate_per_sec = tuple_counter/(TIMER_PERIOD/1000);
    tuple_counter = 0;
    return rate_per_sec;
}

void RainStorm::ReportLeader(const int& input_load) {
    StreamTaskMessage message = {};
    message.type = StreamMessageType::LOAD_REPORT;
    message.task_id = task_id;
    message.stage_id = stage_id;
    message.input_load = input_load;

    send_tcp_message(leader.host, leader.port, message);
}