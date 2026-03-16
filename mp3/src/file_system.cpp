#include "file_system.hpp"

FileSystem::FileSystem(const std::string& local_dir , const std::string& hydfs_dir, Logger& logger)
    : local_path(local_dir), hydfs_path(hydfs_dir), logger(logger) {
  try {
    // Create or ensure local directory exists
    if (!fs::exists(local_path)) {
      fs::create_directories(local_path);
    }

    // If HyDFS directory already exists, remove it completely
    if (fs::exists(hydfs_path)) {
    //   std::cout << "[FileSystem] Removing old HyDFS storage: " 
    //             << fs::absolute(hydfs_path) << std::endl;
      fs::remove_all(hydfs_path);
    }

    // Recreate fresh HyDFS directory
    fs::create_directories(hydfs_path);

    // std::cout << "[FileSystem] Using:\n"
    //           << "  Local: " << fs::absolute(local_path) << "\n"
    //           << "  HyDFS: " << fs::absolute(hydfs_path) << "\n";
  } catch (const fs::filesystem_error& e) {
    std::cerr << "[Error] Directory setup failed: " << e.what() << std::endl;
  }
}

uint64_t fnv1a(const std::string& str) {
    uint64_t hash = 1469598103934665603ULL;
    for (char c : str) {
        hash ^= static_cast<unsigned char>(c);
        hash *= 1099511628211ULL;
    }
    return hash;
}

int FileSystem::hashToKey(const std::string& filename) {
    uint64_t hash = fnv1a(filename);
    return static_cast<int>(hash % 10) + 1;
}

std::vector<FileChunkMessage> FileSystem::make_file_chunks(std::string sender_host, FileMessageType type, const std::string& localfilename, const std::string& HyDFSfilename, int lamport_timestamp, bool from_local) {
    std::vector<FileChunkMessage> chunks;
    
    fs::path full_path;
    if (from_local){
        //create and append may send local file
        full_path = local_path / localfilename;
    }
    else
        full_path = hydfs_path / HyDFSfilename;
        
    std::ifstream infile(full_path, std::ios::binary);
    if (!infile.is_open()) {
        std::cerr << "[Error] Cannot open file: " << full_path << "\n";
        return chunks;
    }

    std::vector<char> buffer(CHUNK_SIZE);
    infile.seekg(0, std::ios::end);
    size_t file_size = infile.tellg();
    infile.seekg(0, std::ios::beg);
    int total_chunks = (file_size + CHUNK_SIZE - 1) / CHUNK_SIZE;

    // std::cout << "[make_file_chunks] Reading " << full_path << " (" << file_size << " bytes, " << total_chunks << " chunks)\n";

    int chunk_id = 0;
    while (infile.read(buffer.data(), CHUNK_SIZE) || infile.gcount() > 0) {
        size_t bytes_read = infile.gcount();
        FileChunkMessage message;
        message.type = type;
        std::strncpy(message.file_name, HyDFSfilename.c_str(), sizeof(message.file_name));
        message.file_name[sizeof(message.file_name) - 1] = '\0';
        std::strncpy(message.sender_host, sender_host.c_str(), sizeof(message.sender_host));
        message.sender_host[sizeof(message.sender_host) - 1] = '\0';
        message.chunk_id = chunk_id++;
        message.total_chunks = total_chunks;
        message.timestamp = lamport_timestamp;
        message.data.assign(buffer.begin(), buffer.begin() + bytes_read);
        chunks.push_back(message);
    }

    infile.close();
    return chunks;
}

void FileSystem::assemble_file(const std::string& fName, const FileBuffer& file_buffer, bool replace, bool write_local) {
    fs::path target_path;

    if (!write_local) {
        // Acquire per-file lock before any operation
        auto &file_mutex = file_locks[fName];
        std::lock_guard<std::mutex> file_lock(file_mutex);
        target_path = hydfs_path / fName;

        std::ofstream outfile(
            target_path,
            std::ios::binary | (replace ? std::ios::trunc : std::ios::app)
        );

        for (const auto& chunk : file_buffer.chunk_data) {
            outfile.write(chunk.data(), chunk.size());
        }
        outfile.close();
    } 
    else {
        // No locking needed for local writes
        target_path = local_path / fName;
        std::ofstream outfile(
            target_path,
            std::ios::binary | (replace ? std::ios::trunc : std::ios::app)
        );

        for (const auto& chunk : file_buffer.chunk_data) {
            outfile.write(chunk.data(), chunk.size());
        }
        outfile.close();
    }
}

bool FileSystem::find_file(const std::string& HyDFSfilename) const {
    fs::path target = hydfs_path / HyDFSfilename;
    return fs::exists(target);
}

void FileSystem::delete_file(const std::string& HyDFSfilename){
    fs::path full_path = hydfs_path / HyDFSfilename;
    if (fs::exists(full_path)) {
        fs::remove(full_path);
        file_info.erase(HyDFSfilename);
        // std::stringstream ss;
        // // ss << "Deleted file: " << full_path;
        // logger.log(ss.str());
    }
}

// void FileSystem::create_file(const FileBuffer& file_buffer, const std::string& HyDFSfilename, int lamort_timestamp, bool is_replica){

//         file_info[HyDFSfilename] = FileMetadata{lamort_timestamp, is_replica};
//         file_locks[HyDFSfilename];
//         assemble_file(HyDFSfilename, file_buffer, true, false);
// }

void FileSystem::get_file_local(const std::string& localfilename, const std::string& HyDFSfilename){
    fs::path full_path = hydfs_path / HyDFSfilename;
    std::stringstream ss;
    if (!fs::exists(full_path)) {
        
        ss << "File does not exist: " << full_path;
        
        
    } else {
        fs::path local_path_full = local_path / localfilename;
        
        fs::copy_file(full_path,local_path_full);
        ss << "File saved to: " << local_path_full;

    }
    logger.log(ss.str());
    return;
}

void FileSystem::liststore() {
    std::stringstream ss;
    for (auto it = file_info.begin(); it != file_info.end(); ++it) {
        if (it->second.is_replica)
            ss << it->first << " ";  
        else
            ss << it->first << "(primary) ";  
    }
    logger.log(ss.str());
}

// Performs grep locally and return search result as string
std::string FileSystem::runGrep(const std::string& pattern, const std::string& HyDFSfilename) {
    std::string cmd;
    std::string full_path = hydfs_path / HyDFSfilename;
    cmd = "grep " + pattern + " " + full_path;
    
    FILE* pipe = popen(cmd.c_str(), "r");
    char buffer[256];
    std::string result;
    while (fgets(buffer, sizeof(buffer), pipe) != nullptr) {
        result += buffer;
    }
    pclose(pipe);
    return result;
}

void FileSystem::update_grep(const std::string& sender_host, const std::string& result){
    std::lock_guard<std::mutex> lock(grep_mutex);
    fs::path full_path = local_path / "grep_results.log";
    std::ofstream fout(full_path, std::ios::app);
    fout << "[" << sender_host << "] " << result << std::endl;
    fout.close();
}

void FileSystem::reset_grep(){
    std::lock_guard<std::mutex> lock(grep_mutex);
    fs::path full_path = local_path / "grep_results.log";
    if (fs::exists(full_path)) {
        fs::remove(full_path);
    }
}



size_t FileChunkMessage::serialize(char* buffer, const size_t buffer_size) const {
    // 1 byte (type) + 33 bytes (sender_host) + 30x2 bytes (file_name) + 3×4 bytes (ints) + 4 bytes (data size) + N bytes (data)
    size_t needed = sizeof(uint8_t) + sizeof(sender_host) + sizeof(file_name) + sizeof(file_name2) + sizeof(int) * 3 + data.size();



    if (needed > buffer_size)
        throw std::runtime_error("Buffer too small for FileChunkMessage");

    size_t offset = 0;

    uint8_t type_byte = static_cast<uint8_t>(type);
    std::memcpy(buffer + offset, &type_byte, sizeof(type_byte));
    offset += sizeof(type_byte);

    std::memcpy(buffer + offset, sender_host, sizeof(sender_host));
    offset += sizeof(sender_host);

    std::memcpy(buffer + offset, file_name, sizeof(file_name));
    offset += sizeof(file_name);

    std::memcpy(buffer + offset, file_name2, sizeof(file_name2));
    offset += sizeof(file_name2);

    int net_chunk_id = htonl(chunk_id);
    int net_total_chunks = htonl(total_chunks);
    int net_timestamp = htonl(timestamp);

    std::memcpy(buffer + offset, &net_chunk_id, sizeof(net_chunk_id));
    offset += sizeof(net_chunk_id);
    std::memcpy(buffer + offset, &net_total_chunks, sizeof(net_total_chunks));
    offset += sizeof(net_total_chunks);
    std::memcpy(buffer + offset, &net_timestamp, sizeof(net_timestamp));
    offset += sizeof(net_timestamp);

    uint32_t data_size = htonl(static_cast<uint32_t>(data.size()));
    std::memcpy(buffer + offset, &data_size, sizeof(data_size));
    offset += sizeof(data_size);

    std::memcpy(buffer + offset, data.data(), data.size());
    offset += data.size();

    return offset;
}

FileChunkMessage FileChunkMessage::deserialize(const char* buffer, const size_t buffer_size) {
    FileChunkMessage msg;
    size_t offset = 0;

    // ---- type (1 byte)
    if (offset + sizeof(uint8_t) > buffer_size)
        throw std::runtime_error("Buffer too small for type");
    uint8_t type_byte;
    std::memcpy(&type_byte, buffer + offset, sizeof(type_byte));
    msg.type = static_cast<FileMessageType>(type_byte);
    offset += sizeof(type_byte);

    // ---- sender_host (33 bytes)
    if (offset + sizeof(msg.sender_host) > buffer_size)
        throw std::runtime_error("Buffer too small for sender_host");
    std::memcpy(msg.sender_host, buffer + offset, sizeof(msg.sender_host));
    offset += sizeof(msg.sender_host);

    // ---- file_name (30 bytes)
    if (offset + sizeof(msg.file_name) > buffer_size)
        throw std::runtime_error("Buffer too small for file_name");
    std::memcpy(msg.file_name, buffer + offset, sizeof(msg.file_name));
    offset += sizeof(msg.file_name);

    // ---- file_name2 (30 bytes)
    if (offset + sizeof(msg.file_name2) > buffer_size)
        throw std::runtime_error("Buffer too small for file_name");
    std::memcpy(msg.file_name2, buffer + offset, sizeof(msg.file_name2));
    offset += sizeof(msg.file_name2);

    // ---- chunk_id (4 bytes)
    if (offset + sizeof(int) > buffer_size)
        throw std::runtime_error("Buffer too small for chunk_id");
    int net_chunk_id;
    std::memcpy(&net_chunk_id, buffer + offset, sizeof(net_chunk_id));
    msg.chunk_id = ntohl(net_chunk_id);
    offset += sizeof(net_chunk_id);

    // ---- total_chunks (4 bytes)
    if (offset + sizeof(int) > buffer_size)
        throw std::runtime_error("Buffer too small for total_chunks");
    int net_total_chunks;
    std::memcpy(&net_total_chunks, buffer + offset, sizeof(net_total_chunks));
    msg.total_chunks = ntohl(net_total_chunks);
    offset += sizeof(net_total_chunks);

    // ---- timestamp (4 bytes)
    if (offset + sizeof(int) > buffer_size)
        throw std::runtime_error("Buffer too small for timestamp");
    int net_timestamp;
    std::memcpy(&net_timestamp, buffer + offset, sizeof(net_timestamp));
    msg.timestamp = ntohl(net_timestamp);
    offset += sizeof(net_timestamp);

    // ---- data_size (4 bytes)
    if (offset + sizeof(uint32_t) > buffer_size)
        throw std::runtime_error("Buffer too small for data_size");
    uint32_t net_data_size;
    std::memcpy(&net_data_size, buffer + offset, sizeof(net_data_size));
    uint32_t data_size = ntohl(net_data_size);
    offset += sizeof(net_data_size);

    // ---- data (variable)
    if (offset + data_size > buffer_size)
        throw std::runtime_error("Buffer too small for data payload");
    msg.data.resize(data_size);
    std::memcpy(msg.data.data(), buffer + offset, data_size);
    offset += data_size;

    return msg;
}