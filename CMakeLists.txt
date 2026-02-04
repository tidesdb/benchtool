cmake_minimum_required(VERSION 3.10)
project(benchtool C)

set(CMAKE_C_STANDARD 11)

# Sanitizer options
option(ENABLE_ASAN "Enable AddressSanitizer" OFF)
option(ENABLE_UBSAN "Enable UndefinedBehaviorSanitizer" OFF)
option(ENABLE_TSAN "Enable ThreadSanitizer" OFF)


if(ENABLE_ASAN)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=address -fno-omit-frame-pointer -g")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address")
    message(STATUS "AddressSanitizer enabled")
endif()

if(ENABLE_UBSAN)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=undefined -fno-omit-frame-pointer -g")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=undefined")
    message(STATUS "UndefinedBehaviorSanitizer enabled")
endif()

if(ENABLE_TSAN)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=thread -fno-omit-frame-pointer -g")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=thread")
    message(STATUS "ThreadSanitizer enabled")
endif()

# If building against TidesDB build dir, create symlink for tidesdb_version.h
if(TIDESDB_BUILD_DIR AND EXISTS "${TIDESDB_BUILD_DIR}/tidesdb_version.h")
    set(TIDESDB_SYMLINK_DIR "${CMAKE_BINARY_DIR}/tidesdb_include/tidesdb")
    file(MAKE_DIRECTORY "${CMAKE_BINARY_DIR}/tidesdb_include")
    if(NOT EXISTS "${TIDESDB_SYMLINK_DIR}")
        file(CREATE_LINK "${TIDESDB_BUILD_DIR}" "${TIDESDB_SYMLINK_DIR}" SYMBOLIC)
        message(STATUS "Created symlink: ${TIDESDB_SYMLINK_DIR} -> ${TIDESDB_BUILD_DIR}")
    endif()
    include_directories("${CMAKE_BINARY_DIR}/tidesdb_include")
endif()

if(TIDESDB_INCLUDE_DIR)
    include_directories(${TIDESDB_INCLUDE_DIR})
endif()

if(ROCKSDB_DIR)
    include_directories(${ROCKSDB_DIR}/include)
endif()

find_library(TIDESDB_LIB tidesdb PATHS ${TIDESDB_BUILD_DIR} REQUIRED)

find_library(ROCKSDB_LIB rocksdb PATHS ${ROCKSDB_DIR} ${ROCKSDB_DIR}/lib)
if(ROCKSDB_LIB)
    add_definitions(-DHAVE_ROCKSDB)
    set(ROCKSDB_LIBS ${ROCKSDB_LIB})
endif()

# LMDB support (optional)
if(LMDB_DIR)
    include_directories(${LMDB_DIR}/include)
endif()

find_library(LMDB_LIB lmdb PATHS ${LMDB_DIR} ${LMDB_DIR}/lib)
if(LMDB_LIB)
    add_definitions(-DHAVE_LMDB)
    set(LMDB_LIBS ${LMDB_LIB})
    message(STATUS "LMDB found: ${LMDB_LIB}")
endif()

# Auto-detect RocksDB version from version.h if not specified
if(ROCKSDB_LIB)
    if(NOT ROCKSDB_MAJOR)
        find_path(ROCKSDB_INCLUDE_DIR rocksdb/version.h
            PATHS ${ROCKSDB_DIR}/include /usr/include /usr/local/include
            NO_DEFAULT_PATH)
        if(NOT ROCKSDB_INCLUDE_DIR)
            find_path(ROCKSDB_INCLUDE_DIR rocksdb/version.h)
        endif()
        if(ROCKSDB_INCLUDE_DIR)
            file(READ "${ROCKSDB_INCLUDE_DIR}/rocksdb/version.h" ROCKSDB_VERSION_H)
            string(REGEX MATCH "#define ROCKSDB_MAJOR ([0-9]+)" _ ${ROCKSDB_VERSION_H})
            set(ROCKSDB_MAJOR ${CMAKE_MATCH_1})
            string(REGEX MATCH "#define ROCKSDB_MINOR ([0-9]+)" _ ${ROCKSDB_VERSION_H})
            set(ROCKSDB_MINOR ${CMAKE_MATCH_1})
            string(REGEX MATCH "#define ROCKSDB_PATCH ([0-9]+)" _ ${ROCKSDB_VERSION_H})
            set(ROCKSDB_PATCH ${CMAKE_MATCH_1})
            message(STATUS "Detected RocksDB version: ${ROCKSDB_MAJOR}.${ROCKSDB_MINOR}.${ROCKSDB_PATCH}")
        endif()
    endif()
    # Pass RocksDB version to compiler
    if(ROCKSDB_MAJOR)
        add_definitions(-DROCKSDB_MAJOR=${ROCKSDB_MAJOR})
    endif()
    if(ROCKSDB_MINOR)
        add_definitions(-DROCKSDB_MINOR=${ROCKSDB_MINOR})
    endif()
    if(ROCKSDB_PATCH)
        add_definitions(-DROCKSDB_PATCH=${ROCKSDB_PATCH})
    endif()
endif()

add_executable(benchtool
        main.c
        benchmark.c
        engine_tidesdb.c
        engine_rocksdb.c
        engine_lmdb.c
        engine_registry.c
)

target_link_libraries(benchtool ${TIDESDB_LIB} ${ROCKSDB_LIBS} ${LMDB_LIBS} pthread m)