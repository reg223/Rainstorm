# === Configuration ===
CXX := g++
CXXFLAGS := -std=c++17 -O2 -Wall -Wextra -Wpedantic -Werror -Werror=missing-include-dirs
INCLUDES := -Iinclude

SRC_DIR := src
SRC_DIR_STREAM := src_stream
BUILD_DIR := build
LIB_NAME := libcore.a

# === Autodetect source files ===
CORE_SRCS := $(filter-out $(SRC_DIR)/main.cpp, $(wildcard $(SRC_DIR)/*.cpp))
CORE_SRCS += $(SRC_DIR_STREAM)/tuple.cpp
CORE_OBJS := $(CORE_SRCS:%.cpp=$(BUILD_DIR)/%.o)

MAIN_OBJ := $(BUILD_DIR)/$(SRC_DIR)/main.o

STREAM_SRCS := $(wildcard $(SRC_DIR_STREAM)/*.cpp)
STREAM_OBJS := $(STREAM_SRCS:%.cpp=$(BUILD_DIR)/%.o)

# === Targets ===
.PHONY: all clean dirs

all: dirs $(BUILD_DIR)/main $(BUILD_DIR)/stream_main

dirs:
	mkdir -p $(BUILD_DIR)/$(SRC_DIR)
	mkdir -p $(BUILD_DIR)/$(SRC_DIR_STREAM)

# --- Core static library ---
$(BUILD_DIR)/$(LIB_NAME): $(CORE_OBJS)
	ar rcs $@ $^

# --- Build main executable ---
$(BUILD_DIR)/main: $(MAIN_OBJ) $(BUILD_DIR)/$(LIB_NAME)
	$(CXX) $(CXXFLAGS) $(INCLUDES) $^ -o $@

# --- Build stream worker executable ---
$(BUILD_DIR)/stream_main: $(STREAM_OBJS) $(BUILD_DIR)/$(LIB_NAME)
	$(CXX) $(CXXFLAGS) $(INCLUDES) $^ -o $@

# --- Compile rule for any .cpp file ---
$(BUILD_DIR)/%.o: %.cpp
	mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c $< -o $@

clean:
	rm -rf $(BUILD_DIR)

delete:
	rm -rf stream_log/*
	rm -rf storage/local_storage/*checkpoint.log
