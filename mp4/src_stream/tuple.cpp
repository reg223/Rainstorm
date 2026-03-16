#include "tuple.hpp"


size_t Tuple::serialize(char* buffer, size_t buffer_size) const {

    const size_t required =
        sizeof(uint8_t) + 
        sizeof(tuple_id) +       // Now 4 bytes (assuming int=32bit)
        sizeof(key) +
        sizeof(value) +
        sizeof(dst_file) +
        sizeof(stage_id) +
        sizeof(task_id);

    if (buffer_size < required) return 0;

    size_t offset = 0;

    uint8_t type_byte = static_cast<uint8_t>(type);
    std::memcpy(buffer + offset, &type_byte, sizeof(type_byte));
    offset += sizeof(type_byte);

    // 2. tuple_id (int) - Host to Network
    int tuple_id_net = htonl(tuple_id); 
    std::memcpy(buffer + offset, &tuple_id_net, sizeof(tuple_id_net));
    offset += sizeof(tuple_id_net); // 4 bytes

    // 3. key (char array)
    std::memcpy(buffer + offset, key, sizeof(key));
    offset += sizeof(key);

    // 4. value (char array)
    std::memcpy(buffer + offset, value, sizeof(value));
    offset += sizeof(value);

    // 5. dst_file (char array)
    std::memcpy(buffer + offset, dst_file, sizeof(dst_file));
    offset += sizeof(dst_file);

    // 6. stage_id (int) - Host to Network
    int stage_id_net = htonl(stage_id);
    std::memcpy(buffer + offset, &stage_id_net, sizeof(stage_id_net));
    offset += sizeof(stage_id_net); // 4 bytes

    // 7. task_id (int) - Host to Network
    int task_id_net = htonl(task_id);
    std::memcpy(buffer + offset, &task_id_net, sizeof(task_id_net));
    offset += sizeof(task_id_net); // 4 bytes

    return offset;
}

Tuple Tuple::deserialize(const char* buffer, size_t buffer_size) {
    Tuple t;

    size_t required =
        sizeof(uint8_t) + 
        sizeof(t.tuple_id) +
        sizeof(t.key) +
        sizeof(t.value) +
        sizeof(t.dst_file) +
        sizeof(t.stage_id) +
        sizeof(t.task_id);

    if (buffer_size < required) {
        std::memset(&t, 0, sizeof(Tuple)); 
        return t;
    }

    size_t offset = 0;

    uint8_t type_byte;
    std::memcpy(&type_byte, buffer + offset, sizeof(type_byte)); 
    t.type = static_cast<TupleMessageType>(type_byte);
    offset += sizeof(type_byte);

    // 2. tuple_id (int) - Network to Host
    int tuple_id_net;
    std::memcpy(&tuple_id_net, buffer + offset, sizeof(tuple_id_net));
    t.tuple_id = ntohl(tuple_id_net);
    offset += sizeof(tuple_id_net);

    // 3-5. key, value, dst_file (char arrays, no conversion)
    std::memcpy(t.key, buffer + offset, sizeof(t.key));
    offset += sizeof(t.key);
    t.key[sizeof(t.key) - 1] = '\0';     

    std::memcpy(t.value, buffer + offset, sizeof(t.value));
    offset += sizeof(t.value);
    t.value[sizeof(t.value) - 1] = '\0'; 

    std::memcpy(t.dst_file, buffer + offset, sizeof(t.dst_file));
    offset += sizeof(t.dst_file);
    t.dst_file[sizeof(t.dst_file) - 1] = '\0'; 

    // 6. stage_id (int) - Network to Host
    int stage_id_net;
    std::memcpy(&stage_id_net, buffer + offset, sizeof(stage_id_net));
    t.stage_id = ntohl(stage_id_net);
    offset += sizeof(stage_id_net);

    // 7. task_id (int) - Network to Host
    int task_id_net;
    std::memcpy(&task_id_net, buffer + offset, sizeof(task_id_net));
    t.task_id = ntohl(task_id_net);
    offset += sizeof(task_id_net);

    return t;
}
