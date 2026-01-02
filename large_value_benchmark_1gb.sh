#!/bin/bash

set -e  # Exit on error

BENCH="./build/benchtool"
DB_PATH="db-bench"
RESULTS="large_value_benchmark_1gb_results.txt"

# Set to "true" to enable fsync-fdatasync (durability), "false" for maximum performance
SYNC_ENABLED="false"

# 1GB value size for all tests
VALUE_SIZE=1073741824  # 1GB = 1024 * 1024 * 1024 bytes
KEY_SIZE=256

# Only 10 operations since each is 1GB
OPS_COUNT=10  # 10 ops * 1GB = 10GB of data
THREADS=1  # Single thread for 1GB values to avoid memory issues

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
echo "Large Value (1GB) Benchmark" | tee -a "$RESULTS"
echo "TidesDB vs RocksDB Comparison" | tee -a "$RESULTS"
echo "Date: $(date)" | tee -a "$RESULTS"
echo "Sync Mode: $SYNC_MODE" | tee -a "$RESULTS"
echo "Value Size: 1GB ($VALUE_SIZE bytes)" | tee -a "$RESULTS"
echo "Key Size: $KEY_SIZE bytes" | tee -a "$RESULTS"
echo "Operations: $OPS_COUNT" | tee -a "$RESULTS"
echo "Threads: $THREADS" | tee -a "$RESULTS"
echo "Total Data: ~10GB" | tee -a "$RESULTS"
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
    local engine="$2"
    
    echo "" | tee -a "$RESULTS"
    echo "========================================" | tee -a "$RESULTS"
    echo "TEST: $test_name" | tee -a "$RESULTS"
    echo "========================================" | tee -a "$RESULTS"
    
    cleanup_db || exit 1
    echo "Running $engine PUT (sequential, 10 x 1GB values)..." | tee -a "$RESULTS"
    $BENCH -e $engine -w write -p seq -k $KEY_SIZE -v $VALUE_SIZE -o $OPS_COUNT -t $THREADS $SYNC_FLAG -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"
    
    echo "" | tee -a "$RESULTS"
}

# Run GET benchmark (requires pre-populating data)
run_get_test() {
    local test_name="$1"
    local engine="$2"
    
    echo "" | tee -a "$RESULTS"
    echo "========================================" | tee -a "$RESULTS"
    echo "TEST: $test_name" | tee -a "$RESULTS"
    echo "========================================" | tee -a "$RESULTS"
    
    cleanup_db || exit 1
    echo "Populating $engine for GET test (sequential write)..." | tee -a "$RESULTS"
    $BENCH -e $engine -w write -p seq -k $KEY_SIZE -v $VALUE_SIZE -o $OPS_COUNT -t $THREADS $SYNC_FLAG -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"
    
    echo "Running $engine GET (sequential read, 10 x 1GB values)..." | tee -a "$RESULTS"
    $BENCH -e $engine -w read -p seq -k $KEY_SIZE -v $VALUE_SIZE -o $OPS_COUNT -t $THREADS $SYNC_FLAG -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"
    
    echo "" | tee -a "$RESULTS"
}

# Run ITERATION benchmark (requires pre-populating data)
run_iteration_test() {
    local test_name="$1"
    local engine="$2"
    local range_size="$3"
    
    echo "" | tee -a "$RESULTS"
    echo "========================================" | tee -a "$RESULTS"
    echo "TEST: $test_name" | tee -a "$RESULTS"
    echo "========================================" | tee -a "$RESULTS"
    
    cleanup_db || exit 1
    echo "Populating $engine for ITERATION test (sequential write)..." | tee -a "$RESULTS"
    $BENCH -e $engine -w write -p seq -k $KEY_SIZE -v $VALUE_SIZE -o $OPS_COUNT -t $THREADS $SYNC_FLAG -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"
    
    echo "Running $engine ITERATION (sequential range scan, $range_size keys)..." | tee -a "$RESULTS"
    $BENCH -e $engine -w range -p seq -k $KEY_SIZE -v $VALUE_SIZE -o $OPS_COUNT -t $THREADS --range-size $range_size $SYNC_FLAG -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"
    
    echo "" | tee -a "$RESULTS"
}

# ============================================
# 1GB Value Benchmark Suite
# ============================================

echo "### 1. TidesDB - PUT Performance (10 x 1GB) ###" | tee -a "$RESULTS"
run_put_test "TidesDB 1GB PUT" "tidesdb"

echo "### 2. RocksDB - PUT Performance (10 x 1GB) ###" | tee -a "$RESULTS"
run_put_test "RocksDB 1GB PUT" "rocksdb"

echo "### 3. TidesDB - GET Performance (10 x 1GB) ###" | tee -a "$RESULTS"
run_get_test "TidesDB 1GB GET" "tidesdb"

echo "### 4. RocksDB - GET Performance (10 x 1GB) ###" | tee -a "$RESULTS"
run_get_test "RocksDB 1GB GET" "rocksdb"

echo "### 5. TidesDB - ITERATION Performance (all 10 keys) ###" | tee -a "$RESULTS"
run_iteration_test "TidesDB 1GB ITERATION" "tidesdb" 10

echo "### 6. RocksDB - ITERATION Performance (all 10 keys) ###" | tee -a "$RESULTS"
run_iteration_test "RocksDB 1GB ITERATION" "rocksdb" 10

# Final cleanup
cleanup_db

echo "" | tee -a "$RESULTS"
echo "===================================" | tee -a "$RESULTS"
echo "1GB Value Benchmark Complete!" | tee -a "$RESULTS"
echo "Results saved to: $RESULTS" | tee -a "$RESULTS"
echo "===================================" | tee -a "$RESULTS"
