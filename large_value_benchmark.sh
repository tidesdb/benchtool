#!/bin/bash

set -e  # Exit on error

BENCH="./build/benchtool"
DB_PATH="db-bench"
RESULTS="large_value_benchmark_results.txt"

# Set to "true" to enable fsync-fdatasync (durability), "false" for maximum performance
SYNC_ENABLED="false"

VALUE_SIZE=8192
KEY_SIZE=16

OPS_COUNT=100000  
THREADS=2 

if [ "$SYNC_ENABLED" = "true" ]; then
    SYNC_FLAG="--sync"
    SYNC_MODE="ENABLED (durable writes)"
else
    SYNC_FLAG=""
    SYNC_MODE="DISABLED (maximum performance)"
fi

# Validate benchtool exists
if [ ! -f "$BENCH" ]; then
    echo "Error: benchtool not found at $BENCH"
    echo "Please build first: mkdir -p build && cd build && cmake .. && make"
    exit 1
fi

# Initialize results file
> "$RESULTS"

echo "===================================" | tee -a "$RESULTS"
echo "Large Value (8KB) Benchmark" | tee -a "$RESULTS"
echo "TidesDB vs RocksDB Comparison" | tee -a "$RESULTS"
echo "Date: $(date)" | tee -a "$RESULTS"
echo "Sync Mode: $SYNC_MODE" | tee -a "$RESULTS"
echo "Value Size: 8KB ($VALUE_SIZE bytes)" | tee -a "$RESULTS"
echo "Key Size: $KEY_SIZE bytes" | tee -a "$RESULTS"
echo "Operations: $OPS_COUNT" | tee -a "$RESULTS"
echo "Threads: $THREADS" | tee -a "$RESULTS"
echo "===================================" | tee -a "$RESULTS"
echo "" | tee -a "$RESULTS"

# Cleanup function to ensure DB is removed
cleanup_db() {
    if [ -d "$DB_PATH" ]; then
        echo "Cleaning up $DB_PATH..." | tee -a "$RESULTS"
        rm -rf "$DB_PATH"
        if [ -d "$DB_PATH" ]; then
            echo "Warning: Failed to remove $DB_PATH" | tee -a "$RESULTS"
            return 1
        fi
    fi
    return 0
}

# Run PUT benchmark
run_put_test() {
    local test_name="$1"
    local pattern="$2"
    
    echo "" | tee -a "$RESULTS"
    echo "========================================" | tee -a "$RESULTS"
    echo "TEST: $test_name" | tee -a "$RESULTS"
    echo "========================================" | tee -a "$RESULTS"
    
    # TidesDB
    cleanup_db || exit 1
    echo "Running TidesDB PUT ($pattern)..." | tee -a "$RESULTS"
    $BENCH -e tidesdb -w write -p $pattern -k $KEY_SIZE -v $VALUE_SIZE -o $OPS_COUNT -t $THREADS $SYNC_FLAG -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"
    
    # RocksDB
    cleanup_db || exit 1
    echo "Running RocksDB PUT ($pattern)..." | tee -a "$RESULTS"
    $BENCH -e rocksdb -w write -p $pattern -k $KEY_SIZE -v $VALUE_SIZE -o $OPS_COUNT -t $THREADS $SYNC_FLAG -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"
    
    cleanup_db || exit 1
    echo "" | tee -a "$RESULTS"
}

# Run GET benchmark (requires pre-populating data)
run_get_test() {
    local test_name="$1"
    local pattern="$2"
    
    echo "" | tee -a "$RESULTS"
    echo "========================================" | tee -a "$RESULTS"
    echo "TEST: $test_name" | tee -a "$RESULTS"
    echo "========================================" | tee -a "$RESULTS"
    
    # TidesDB
    cleanup_db || exit 1
    echo "Populating TidesDB for GET test ($pattern)..." | tee -a "$RESULTS"
    $BENCH -e tidesdb -w write -p $pattern -k $KEY_SIZE -v $VALUE_SIZE -o $OPS_COUNT -t $THREADS $SYNC_FLAG -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"
    
    echo "Running TidesDB GET ($pattern)..." | tee -a "$RESULTS"
    $BENCH -e tidesdb -w read -p $pattern -k $KEY_SIZE -v $VALUE_SIZE -o $OPS_COUNT -t $THREADS $SYNC_FLAG -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"
    
    # RocksDB
    cleanup_db || exit 1
    echo "Populating RocksDB for GET test ($pattern)..." | tee -a "$RESULTS"
    $BENCH -e rocksdb -w write -p $pattern -k $KEY_SIZE -v $VALUE_SIZE -o $OPS_COUNT -t $THREADS $SYNC_FLAG -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"
    
    echo "Running RocksDB GET ($pattern)..." | tee -a "$RESULTS"
    $BENCH -e rocksdb -w read -p $pattern -k $KEY_SIZE -v $VALUE_SIZE -o $OPS_COUNT -t $THREADS $SYNC_FLAG -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"
    
    cleanup_db || exit 1
    echo "" | tee -a "$RESULTS"
}

# Run SEEK benchmark (requires pre-populating data)
run_seek_test() {
    local test_name="$1"
    local pattern="$2"
    
    echo "" | tee -a "$RESULTS"
    echo "========================================" | tee -a "$RESULTS"
    echo "TEST: $test_name" | tee -a "$RESULTS"
    echo "========================================" | tee -a "$RESULTS"
    
    # TidesDB - use same pattern for write and seek so keys exist!
    cleanup_db || exit 1
    echo "Populating TidesDB for SEEK test ($pattern)..." | tee -a "$RESULTS"
    $BENCH -e tidesdb -w write -p $pattern -k $KEY_SIZE -v $VALUE_SIZE -o $OPS_COUNT -t $THREADS $SYNC_FLAG -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"
    
    echo "Running TidesDB SEEK ($pattern)..." | tee -a "$RESULTS"
    $BENCH -e tidesdb -w seek -p $pattern -k $KEY_SIZE -v $VALUE_SIZE -o $OPS_COUNT -t $THREADS $SYNC_FLAG -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"
    
    # RocksDB - use same pattern for write and seek so keys exist!
    cleanup_db || exit 1
    echo "Populating RocksDB for SEEK test ($pattern)..." | tee -a "$RESULTS"
    $BENCH -e rocksdb -w write -p $pattern -k $KEY_SIZE -v $VALUE_SIZE -o $OPS_COUNT -t $THREADS $SYNC_FLAG -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"
    
    echo "Running RocksDB SEEK ($pattern)..." | tee -a "$RESULTS"
    $BENCH -e rocksdb -w seek -p $pattern -k $KEY_SIZE -v $VALUE_SIZE -o $OPS_COUNT -t $THREADS $SYNC_FLAG -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"
    
    cleanup_db || exit 1
    echo "" | tee -a "$RESULTS"
}

# Run RANGE/ITERATION benchmark (requires pre-populating data)
run_iteration_test() {
    local test_name="$1"
    local pattern="$2"
    local range_size="$3"
    
    echo "" | tee -a "$RESULTS"
    echo "========================================" | tee -a "$RESULTS"
    echo "TEST: $test_name" | tee -a "$RESULTS"
    echo "========================================" | tee -a "$RESULTS"
    
    # TidesDB - use same pattern for write and range so keys exist!
    cleanup_db || exit 1
    echo "Populating TidesDB for ITERATION test ($pattern)..." | tee -a "$RESULTS"
    $BENCH -e tidesdb -w write -p $pattern -k $KEY_SIZE -v $VALUE_SIZE -o $OPS_COUNT -t $THREADS $SYNC_FLAG -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"
    
    echo "Running TidesDB ITERATION ($pattern)..." | tee -a "$RESULTS"
    $BENCH -e tidesdb -w range -p $pattern -k $KEY_SIZE -v $VALUE_SIZE -o $OPS_COUNT -t $THREADS --range-size $range_size $SYNC_FLAG -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"
    
    # RocksDB - use same pattern for write and range so keys exist!
    cleanup_db || exit 1
    echo "Populating RocksDB for ITERATION test ($pattern)..." | tee -a "$RESULTS"
    $BENCH -e rocksdb -w write -p $pattern -k $KEY_SIZE -v $VALUE_SIZE -o $OPS_COUNT -t $THREADS $SYNC_FLAG -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"
    
    echo "Running RocksDB ITERATION ($pattern)..." | tee -a "$RESULTS"
    $BENCH -e rocksdb -w range -p $pattern -k $KEY_SIZE -v $VALUE_SIZE -o $OPS_COUNT -t $THREADS --range-size $range_size $SYNC_FLAG -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"
    
    cleanup_db || exit 1
    echo "" | tee -a "$RESULTS"
}

# ============================================
# Large Value Benchmark Suite
# ============================================

echo "### 1. PUT Performance - Sequential ###" | tee -a "$RESULTS"
run_put_test "8KB PUT - Sequential" "seq"

echo "### 2. PUT Performance - Random ###" | tee -a "$RESULTS"
run_put_test "8KB PUT - Random" "random"

echo "### 3. GET Performance - Sequential ###" | tee -a "$RESULTS"
run_get_test "8KB GET - Sequential" "seq"

echo "### 4. GET Performance - Random ###" | tee -a "$RESULTS"
run_get_test "8KB GET - Random" "random"

echo "### 5. SEEK Performance - Sequential ###" | tee -a "$RESULTS"
run_seek_test "8KB SEEK - Sequential" "seq"

echo "### 6. SEEK Performance - Random ###" | tee -a "$RESULTS"
run_seek_test "8KB SEEK - Random" "random"

echo "### 7. ITERATION Performance - Sequential (100 keys per range) ###" | tee -a "$RESULTS"
run_iteration_test "8KB ITERATION - Sequential" "seq" 100

echo "### 8. ITERATION Performance - Random (100 keys per range) ###" | tee -a "$RESULTS"
run_iteration_test "8KB ITERATION - Random" "random" 100

# Final cleanup
cleanup_db

echo "" | tee -a "$RESULTS"
echo "===================================" | tee -a "$RESULTS"
echo "Large Value Benchmark Complete!" | tee -a "$RESULTS"
echo "Results saved to: $RESULTS" | tee -a "$RESULTS"
echo "===================================" | tee -a "$RESULTS"
