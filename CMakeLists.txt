cmake_minimum_required(VERSION 3.10)
project(benchtool C)

set(CMAKE_C_STANDARD 11)

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

target_link_libraries(benchtool ${TIDESDB_LIB} ${ROCKSDB_LIBS} pthread m)