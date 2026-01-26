cmake_minimum_required(VERSION 3.10)
project(benchtool C)

set(CMAKE_C_STANDARD 11)

# Sanitizer options
option(ENABLE_ASAN "Enable AddressSanitizer" OFF)
option(ENABLE_UBSAN "Enable UndefinedBehaviorSanitizer" OFF)
option(ENABLE_TSAN "Enable ThreadSanitizer" OFF)

# Memory allocator options (engine-specific)
# TidesDB only supports mimalloc; RocksDB commonly uses jemalloc or tcmalloc
# These allocators override malloc/free globally when linked
option(TIDESDB_WITH_MIMALLOC "Use mimalloc for TidesDB (must match TidesDB library build)" OFF)
option(ROCKSDB_WITH_JEMALLOC "Use jemalloc for RocksDB benchmarks" OFF)
option(ROCKSDB_WITH_TCMALLOC "Use tcmalloc for RocksDB benchmarks (Google's allocator)" OFF)

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

# Memory allocator configuration (engine-specific)
# Note: Only one allocator can be active at runtime since they override malloc globally
set(ALLOCATOR_LIBS "")

# Check for conflicting allocator options
if(TIDESDB_WITH_MIMALLOC AND (ROCKSDB_WITH_JEMALLOC OR ROCKSDB_WITH_TCMALLOC))
    message(FATAL_ERROR "Cannot use mimalloc (TidesDB) with jemalloc/tcmalloc (RocksDB) simultaneously. "
            "Choose one allocator for consistent benchmarking.")
endif()

if(ROCKSDB_WITH_JEMALLOC AND ROCKSDB_WITH_TCMALLOC)
    message(FATAL_ERROR "Cannot enable both jemalloc and tcmalloc. Choose one.")
endif()

# TidesDB allocator: mimalloc
if(TIDESDB_WITH_MIMALLOC)
    find_package(mimalloc QUIET)
    if(mimalloc_FOUND)
        set(ALLOCATOR_LIBS mimalloc)
        message(STATUS "TidesDB: Using mimalloc memory allocator")
    else()
        find_library(MIMALLOC_LIB mimalloc)
        if(MIMALLOC_LIB)
            set(ALLOCATOR_LIBS ${MIMALLOC_LIB})
            message(STATUS "TidesDB: Using mimalloc memory allocator")
        else()
            message(FATAL_ERROR "mimalloc requested but not found. Install mimalloc or disable TIDESDB_WITH_MIMALLOC")
        endif()
    endif()
endif()

# RocksDB allocator: jemalloc
if(ROCKSDB_WITH_JEMALLOC)
    find_library(JEMALLOC_LIB jemalloc)
    if(JEMALLOC_LIB)
        set(ALLOCATOR_LIBS ${JEMALLOC_LIB})
        message(STATUS "RocksDB: Using jemalloc memory allocator")
    else()
        message(FATAL_ERROR "jemalloc requested but not found. Install jemalloc or disable ROCKSDB_WITH_JEMALLOC")
    endif()
endif()

# RocksDB allocator: tcmalloc
if(ROCKSDB_WITH_TCMALLOC)
    # Try tcmalloc_minimal first (smaller, no heap profiling), then full tcmalloc
    find_library(TCMALLOC_LIB NAMES tcmalloc_minimal tcmalloc)
    if(TCMALLOC_LIB)
        set(ALLOCATOR_LIBS ${TCMALLOC_LIB})
        message(STATUS "RocksDB: Using tcmalloc memory allocator")
    else()
        message(FATAL_ERROR "tcmalloc requested but not found. Install gperftools or disable ROCKSDB_WITH_TCMALLOC")
    endif()
endif()

find_library(TIDESDB_LIB tidesdb PATHS ${TIDESDB_BUILD_DIR} REQUIRED)

find_library(ROCKSDB_LIB rocksdb)
if(ROCKSDB_LIB)
    add_definitions(-DHAVE_ROCKSDB)
    set(ROCKSDB_LIBS ${ROCKSDB_LIB})
endif()

add_executable(benchtool
        main.c
        benchmark.c
        engine_tidesdb.c
        engine_rocksdb.c
        engine_registry.c
)

# Link allocator first for proper malloc override, then other libraries
target_link_libraries(benchtool ${ALLOCATOR_LIBS} ${TIDESDB_LIB} ${ROCKSDB_LIBS} pthread m)