# === Configuration ===
CXX := g++
CXXFLAGS := -std=c++17 -O2 -Wall -Wextra -Wpedantic -Werror -Werror=missing-include-dirs
INCLUDES := -Iinclude

SRC_DIR := src
BUILD_DIR := build
LIB_NAME := libcore.a

# === Source files ===
CORE_SRCS := $(SRC_DIR)/socket.cpp \
			 $(SRC_DIR)/tcp_socket.cpp \
             $(SRC_DIR)/logger.cpp \
             $(SRC_DIR)/message.cpp \
             $(SRC_DIR)/membership_list.cpp \
			 $(SRC_DIR)/successor_list.cpp \
             $(SRC_DIR)/node.cpp \
             $(SRC_DIR)/shared.cpp \
			 $(SRC_DIR)/file_system.cpp \
			 $(SRC_DIR)/create.cpp \
			 $(SRC_DIR)/get.cpp \
			 $(SRC_DIR)/append.cpp \
			 $(SRC_DIR)/merge.cpp \
			 $(SRC_DIR)/rebalance.cpp \
			 $(SRC_DIR)/re-replicated.cpp \
			 $(SRC_DIR)/ls.cpp \
			 $(SRC_DIR)/grep.cpp


CORE_OBJS := $(CORE_SRCS:%.cpp=$(BUILD_DIR)/%.o)
MAIN_OBJ  := $(BUILD_DIR)/$(SRC_DIR)/main.o

# === Targets ===
.PHONY: all clean dirs

all: dirs $(BUILD_DIR)/main

dirs:
	mkdir -p $(BUILD_DIR)/src

# --- Core library ---
$(BUILD_DIR)/$(LIB_NAME): $(CORE_OBJS)
	ar rcs $@ $^

# --- Main executable ---
$(BUILD_DIR)/main: $(MAIN_OBJ) $(BUILD_DIR)/$(LIB_NAME)
	$(CXX) $(CXXFLAGS) $(INCLUDES) $^ -o $@

# --- Compile any .cpp into .o ---
$(BUILD_DIR)/%.o: %.cpp
	mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c $< -o $@

# --- Cleanup ---
clean:
	rm -rf $(BUILD_DIR)
