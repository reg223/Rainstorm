#pragma once

#include <string>
#include <fstream>
#include <filesystem>
#include <iostream>
#include <vector>
#include <sstream>
#include <unordered_map>
#include <mutex>

#include "logger.hpp"
#include "message.hpp"
#include "shared.hpp"
#include "socket.hpp"

#define CHUNK_SIZE 3800 // lave some space for meta data

namespace fs = std::filesystem;

enum class FileMessageType : uint8_t { 
    APPEND_REQ, MERGE_REQ, MERGE_RES, 
    MERGE_FETCH_REQ, MERGE_FETCH_RES, 
    MERGE_UPDATE, LS_REQ, LS_RES, MULTIAPPEND_REQ,
    REBALANCE_REQ, REBALANCE_RES, REBALANCE_DEL, REBALANCE_END, 
    CREATE_REQ, CREATE_REPLICA_REQ, 
    GET_REQ, GET_RES, GET_FETCH_REQ, GET_FETCH_RES,
    REREPLICATE_RES, GREP_REQ, GREP_RES, GREP_DONE,
    AGGRE_APPEND_REQ};

// Seperate file into chunks for transfering
struct FileChunkMessage{
    FileMessageType type;
    char sender_host[33];
    char file_name[30];  
    char file_name2[30]; // for multiappend
    int chunk_id;
    int total_chunks;
    int timestamp;
    std::vector<char> data;

    size_t serialize(char* buffer, const size_t buffer_size) const;
    static FileChunkMessage deserialize(const char* buffer, const size_t buffer_size);
};

// Once the node receive the chunk, wait for all chucnk to arrive, then assemble
struct FileBuffer {
    int total_chunks;
    int received_chunks;
    std::vector<std::vector<char>> chunk_data;  // chunk_id -> data
};

struct FileMetadata {
    int timestamp;
    // Indicate whether the file is the copy from other node
    bool is_replica;
};


class FileSystem {
    public:
        FileSystem(const std::string& ,const std::string&, Logger& logger);
        // Map a filename to a VM number on the 1–10 ring
        static int hashToKey(const std::string& filename);
        std::vector<FileChunkMessage> make_file_chunks(
            std::string sender_host, FileMessageType type, const std::string& localfilename, const std::string& HyDFSfilename, 
            int lamport_timestamp, bool from_local);
        void assemble_file(const std::string& fName, const FileBuffer& file_buffer, bool replace, bool write_local);
        bool find_file(const std::string& HyDFSfilename) const;
        void delete_file(const std::string& HyDFSfilename);
        void create_file(const FileBuffer& file_buffer , const std::string& HyDFSfilename, int lamort_timestamp, bool is_replica);
        void get_file_local(const std::string& localfilename, const std::string& HyDFSfilename);
        void liststore();
        std::string runGrep(const std::string& pattern, const std::string& HyDFSfilename);
        void update_grep(const std::string& sender_host, const std::string& result);
        void reset_grep();

        std::unordered_map<std::string, FileMetadata> file_info;

    private:
        fs::path local_path;
        fs::path hydfs_path;
        fs::path temp_path;
        Logger& logger;

        std::unordered_map<std::string, std::mutex> file_locks;
        std::mutex grep_mutex;

        


};
