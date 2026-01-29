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

add_executable(benchtool
        main.c
        benchmark.c
        engine_tidesdb.c
        engine_rocksdb.c
        engine_registry.c
)

target_link_libraries(benchtool ${TIDESDB_LIB} ${ROCKSDB_LIBS} pthread m)