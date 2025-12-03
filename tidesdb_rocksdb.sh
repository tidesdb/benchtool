#!/bin/bash

set -e  # Exit on error

BENCH="./build/benchtool"
DB_PATH="/media/agpmastersystem/c794105c-0cd9-4be9-8369-ee6d6e707d68/home/db-bench"
RESULTS="benchmark_results.txt"

# Set to "true" to enable fsync-fdatasync (durability), "false" for maximum performance
SYNC_ENABLED="false"

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
echo "TidesDB vs RocksDB Comparison" | tee -a "$RESULTS"
echo "Date: $(date)" | tee -a "$RESULTS"
echo "Sync Mode: $SYNC_MODE" | tee -a "$RESULTS"
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

# Run comparison benchmark (TidesDB with RocksDB baseline)
run_comparison() {
    local test_name="$1"
    shift
    local bench_args="$@"
    
    echo "" | tee -a "$RESULTS"
    echo "========================================" | tee -a "$RESULTS"
    echo "TEST: $test_name" | tee -a "$RESULTS"
    echo "========================================" | tee -a "$RESULTS"
    
    # Run TidesDB with comparison mode (automatically runs RocksDB baseline)
    cleanup_db || exit 1
    echo "Running TidesDB (with RocksDB baseline)..." | tee -a "$RESULTS"
    $BENCH -e tidesdb -c $bench_args $SYNC_FLAG -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"
    
    cleanup_db || exit 1
    echo "" | tee -a "$RESULTS"
}

# Run read benchmark (requires pre-populating data)
run_read_comparison() {
    local test_name="$1"
    shift
    local read_args="$@"
    local write_args="${read_args/-w read/-w write}"
    
    echo "" | tee -a "$RESULTS"
    echo "========================================" | tee -a "$RESULTS"
    echo "TEST: $test_name" | tee -a "$RESULTS"
    echo "========================================" | tee -a "$RESULTS"
    
    # Prepare data with TidesDB
    cleanup_db || exit 1
    echo "Populating TidesDB for read test..." | tee -a "$RESULTS"
    $BENCH -e tidesdb $write_args $SYNC_FLAG -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"
    
    echo "Running TidesDB read test..." | tee -a "$RESULTS"
    $BENCH -e tidesdb $read_args $SYNC_FLAG -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"
    
    # Prepare data with RocksDB
    cleanup_db || exit 1
    echo "Populating RocksDB for read test..." | tee -a "$RESULTS"
    $BENCH -e rocksdb $write_args $SYNC_FLAG -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"
    
    echo "Running RocksDB read test..." | tee -a "$RESULTS"
    $BENCH -e rocksdb $read_args $SYNC_FLAG -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"
    
    cleanup_db || exit 1
    echo "" | tee -a "$RESULTS"
}

# Run delete benchmark (requires pre-populating data)
run_delete_comparison() {
    local test_name="$1"
    shift
    local delete_args="$@"
    local write_args="${delete_args/-w delete/-w write}"
    
    echo "" | tee -a "$RESULTS"
    echo "========================================" | tee -a "$RESULTS"
    echo "TEST: $test_name" | tee -a "$RESULTS"
    echo "========================================" | tee -a "$RESULTS"
    
    # Prepare data with TidesDB
    cleanup_db || exit 1
    echo "Populating TidesDB for delete test..." | tee -a "$RESULTS"
    $BENCH -e tidesdb $write_args $SYNC_FLAG -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"
    
    echo "Running TidesDB delete test..." | tee -a "$RESULTS"
    $BENCH -e tidesdb $delete_args $SYNC_FLAG -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"
    
    # Prepare data with RocksDB
    cleanup_db || exit 1
    echo "Populating RocksDB for delete test..." | tee -a "$RESULTS"
    $BENCH -e rocksdb $write_args $SYNC_FLAG -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"
    
    echo "Running RocksDB delete test..." | tee -a "$RESULTS"
    $BENCH -e rocksdb $delete_args $SYNC_FLAG -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"
    
    cleanup_db || exit 1
    echo "" | tee -a "$RESULTS"
}

# ============================================
# Benchmark Suite
# ============================================

echo "### 1. Sequential Write Performance ###" | tee -a "$RESULTS"
run_comparison "Sequential Write (10M ops, 4 threads)" \
    -w write -p seq -o 10000000 -t 4

echo "### 2. Random Write Performance ###" | tee -a "$RESULTS"
run_comparison "Random Write (10M ops, 4 threads)" \
    -w write -p random -o 10000000 -t 4

echo "### 3. Random Read Performance ###" | tee -a "$RESULTS"
run_read_comparison "Random Read (10M ops, 4 threads)" \
    -w read -p random -o 10000000 -t 4

echo "### 4. Mixed Workload (50/50 Read/Write) ###" | tee -a "$RESULTS"
run_comparison "Mixed Workload (5M ops, 4 threads)" \
    -w mixed -p random -o 5000000 -t 4

echo "### 5. Hot Key Workload (Zipfian Distribution) ###" | tee -a "$RESULTS"
run_comparison "Zipfian Write (5M ops, 4 threads)" \
    -w write -p zipfian -o 5000000 -t 4

run_comparison "Zipfian Mixed (500K ops, 4 threads)" \
    -w mixed -p zipfian -o 5000000 -t 4

echo "### 6. Delete Performance ###" | tee -a "$RESULTS"
run_delete_comparison "Random Delete (5M ops, 4 threads)" \
    -w delete -p random -o 5000000 -t 4

echo "### 7. Large Value Performance ###" | tee -a "$RESULTS"
run_comparison "Large Values (1M ops, 256B key, 4KB value)" \
    -w write -p random -k 256 -v 4096 -o 1000000 -t 4

echo "### 8. Small Value Performance ###" | tee -a "$RESULTS"
run_comparison "Small Values (50M ops, 16B key, 64B value)" \
    -w write -p random -k 16 -v 64 -o 50000000 -t 4

# Final cleanup
cleanup_db

echo "" | tee -a "$RESULTS"
echo "===================================" | tee -a "$RESULTS"
echo "Benchmark Suite Complete!" | tee -a "$RESULTS"
echo "Results saved to: $RESULTS" | tee -a "$RESULTS"
echo "===================================" | tee -a "$RESULTS"