#!/bin/bash

######################################################################################
#
# TidesDB vs RocksDB Comprehensive Benchmark Single Threaded Suite
#
######################################################################################

set -e 

BENCH="./build/benchtool"
DB_PATH="db-bench"
RESULTS="tidesdb_rocksdb_single_threaded.txt"
CSV_FILE="tidesdb_rocksdb_single_threaded.csv"

SYNC_ENABLED="false"

DEFAULT_BATCH_SIZE=1000

if [ "$SYNC_ENABLED" = "true" ]; then
    SYNC_FLAG="--sync"
    SYNC_MODE="ENABLED (durable writes)"
else
    SYNC_FLAG=""
    SYNC_MODE="DISABLED (maximum performance)"
fi

if [ ! -f "$BENCH" ]; then
    echo "Error: benchtool not found at $BENCH"
    echo "Please build first: mkdir -p build && cd build && cmake .. && make"
    exit 1
fi

> "$RESULTS"
> "$CSV_FILE"

echo "===================================" | tee -a "$RESULTS"
echo "TidesDB vs RocksDB Comparison" | tee -a "$RESULTS"
echo "Date: $(date)" | tee -a "$RESULTS"
echo "Sync Mode: $SYNC_MODE" | tee -a "$RESULTS"
echo "Default Batch Size: $DEFAULT_BATCH_SIZE" | tee -a "$RESULTS"
echo "Results will be saved to:" | tee -a "$RESULTS"
echo "  Text: $RESULTS" | tee -a "$RESULTS"
echo "  CSV:  $CSV_FILE" | tee -a "$RESULTS"
echo "===================================" | tee -a "$RESULTS"
echo "" | tee -a "$RESULTS"

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

run_comparison() {
    local test_name="$1"
    shift
    local bench_args="$@"

    echo "" | tee -a "$RESULTS"
    echo "========================================" | tee -a "$RESULTS"
    echo "TEST: $test_name" | tee -a "$RESULTS"
    echo "========================================" | tee -a "$RESULTS"

    cleanup_db || exit 1
    echo "Running TidesDB (with RocksDB baseline)..." | tee -a "$RESULTS"
    $BENCH -e tidesdb -c $bench_args $SYNC_FLAG -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"

    cleanup_db || exit 1
    echo "" | tee -a "$RESULTS"
}

run_read_comparison() {
    local test_name="$1"
    shift
    local read_args="$@"
    local write_args="${read_args/-w read/-w write}"

    local populate_args="$write_args -b $DEFAULT_BATCH_SIZE"

    echo "" | tee -a "$RESULTS"
    echo "========================================" | tee -a "$RESULTS"
    echo "TEST: $test_name" | tee -a "$RESULTS"
    echo "========================================" | tee -a "$RESULTS"

    cleanup_db || exit 1
    echo "Populating TidesDB for read test..." | tee -a "$RESULTS"
    $BENCH -e tidesdb $populate_args $SYNC_FLAG -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"

    echo "Running TidesDB read test..." | tee -a "$RESULTS"
    $BENCH -e tidesdb $read_args $SYNC_FLAG -d "$DB_PATH" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    cleanup_db || exit 1
    echo "Populating RocksDB for read test..." | tee -a "$RESULTS"
    $BENCH -e rocksdb $populate_args $SYNC_FLAG -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"

    echo "Running RocksDB read test..." | tee -a "$RESULTS"
    $BENCH -e rocksdb $read_args $SYNC_FLAG -d "$DB_PATH" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    cleanup_db || exit 1
    echo "" | tee -a "$RESULTS"
}

run_delete_comparison() {
    local test_name="$1"
    shift
    local delete_args="$@"
    local write_args="${delete_args/-w delete/-w write}"

    echo "" | tee -a "$RESULTS"
    echo "========================================" | tee -a "$RESULTS"
    echo "TEST: $test_name" | tee -a "$RESULTS"
    echo "========================================" | tee -a "$RESULTS"

    cleanup_db || exit 1
    echo "Populating TidesDB for delete test..." | tee -a "$RESULTS"
    $BENCH -e tidesdb $write_args $SYNC_FLAG -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"

    echo "Running TidesDB delete test..." | tee -a "$RESULTS"
    $BENCH -e tidesdb $delete_args $SYNC_FLAG -d "$DB_PATH" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    cleanup_db || exit 1
    echo "Populating RocksDB for delete test..." | tee -a "$RESULTS"
    $BENCH -e rocksdb $write_args $SYNC_FLAG -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"

    echo "Running RocksDB delete test..." | tee -a "$RESULTS"
    $BENCH -e rocksdb $delete_args $SYNC_FLAG -d "$DB_PATH" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    cleanup_db || exit 1
    echo "" | tee -a "$RESULTS"
}

run_seek_comparison() {
    local test_name="$1"
    shift
    local seek_args="$@"
    local write_args="${seek_args/-w seek/-w write}"

    local populate_args="$write_args -b $DEFAULT_BATCH_SIZE"

    echo "" | tee -a "$RESULTS"
    echo "========================================" | tee -a "$RESULTS"
    echo "TEST: $test_name" | tee -a "$RESULTS"
    echo "========================================" | tee -a "$RESULTS"

    cleanup_db || exit 1
    echo "Populating TidesDB for seek test..." | tee -a "$RESULTS"
    $BENCH -e tidesdb $populate_args $SYNC_FLAG -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"

    echo "Running TidesDB seek test..." | tee -a "$RESULTS"
    $BENCH -e tidesdb $seek_args $SYNC_FLAG -d "$DB_PATH" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    cleanup_db || exit 1
    echo "Populating RocksDB for seek test..." | tee -a "$RESULTS"
    $BENCH -e rocksdb $populate_args $SYNC_FLAG -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"

    echo "Running RocksDB seek test..." | tee -a "$RESULTS"
    $BENCH -e rocksdb $seek_args $SYNC_FLAG -d "$DB_PATH" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    cleanup_db || exit 1
    echo "" | tee -a "$RESULTS"
}

run_range_comparison() {
    local test_name="$1"
    shift
    local range_args="$@"
    local write_args="${range_args/-w range/-w write}"

    local populate_args="$write_args -b $DEFAULT_BATCH_SIZE"

    echo "" | tee -a "$RESULTS"
    echo "========================================" | tee -a "$RESULTS"
    echo "TEST: $test_name" | tee -a "$RESULTS"
    echo "========================================" | tee -a "$RESULTS"

    cleanup_db || exit 1
    echo "Populating TidesDB for range test..." | tee -a "$RESULTS"
    $BENCH -e tidesdb $populate_args $SYNC_FLAG -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"

    echo "Running TidesDB range test..." | tee -a "$RESULTS"
    $BENCH -e tidesdb $range_args $SYNC_FLAG -d "$DB_PATH" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    cleanup_db || exit 1
    echo "Populating RocksDB for range test..." | tee -a "$RESULTS"
    $BENCH -e rocksdb $populate_args $SYNC_FLAG -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"

    echo "Running RocksDB range test..." | tee -a "$RESULTS"
    $BENCH -e rocksdb $range_args $SYNC_FLAG -d "$DB_PATH" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    cleanup_db || exit 1
    echo "" | tee -a "$RESULTS"
}

echo "### 1. Sequential Write Performance (Batched) ###" | tee -a "$RESULTS"
run_comparison "Sequential Write (10M ops, 1 thread, batch=$DEFAULT_BATCH_SIZE)" \
    -w write -p seq -o 10000000 -t 1 -b $DEFAULT_BATCH_SIZE

echo "### 2. Random Write Performance (Batched) ###" | tee -a "$RESULTS"
run_comparison "Random Write (10M ops, 1 thread, batch=$DEFAULT_BATCH_SIZE)" \
    -w write -p random -o 10000000 -t 1 -b $DEFAULT_BATCH_SIZE

echo "### 3. Random Read Performance ###" | tee -a "$RESULTS"
run_read_comparison "Random Read (10M ops, 1 thread)" \
    -w read -p random -o 10000000 -t 1

echo "### 4. Mixed Workload (50/50 Read/Write, Batched) ###" | tee -a "$RESULTS"
run_comparison "Mixed Workload (5M ops, 1 thread, batch=$DEFAULT_BATCH_SIZE)" \
    -w mixed -p random -o 5000000 -t 1 -b $DEFAULT_BATCH_SIZE

echo "### 5. Hot Key Workload (Zipfian Distribution, Batched) ###" | tee -a "$RESULTS"
run_comparison "Zipfian Write (5M ops, 1 thread, batch=$DEFAULT_BATCH_SIZE)" \
    -w write -p zipfian -o 5000000 -t 1 -b $DEFAULT_BATCH_SIZE

run_comparison "Zipfian Mixed (5M ops, 1 thread, batch=$DEFAULT_BATCH_SIZE)" \
    -w mixed -p zipfian -o 5000000 -t 1 -b $DEFAULT_BATCH_SIZE

echo "### 6. Delete Performance (Batched) ###" | tee -a "$RESULTS"
run_delete_comparison "Random Delete (5M ops, 1 thread, batch=$DEFAULT_BATCH_SIZE)" \
    -w delete -p random -o 5000000 -t 1 -b $DEFAULT_BATCH_SIZE

echo "### 7. Large Value Performance (Batched) ###" | tee -a "$RESULTS"
run_comparison "Large Values (1M ops, 256B key, 4KB value, batch=$DEFAULT_BATCH_SIZE)" \
    -w write -p random -k 256 -v 4096 -o 1000000 -t 1 -b $DEFAULT_BATCH_SIZE

echo "### 8. Small Value Performance (Batched) ###" | tee -a "$RESULTS"
run_comparison "Small Values (50M ops, 16B key, 64B value, batch=$DEFAULT_BATCH_SIZE)" \
    -w write -p random -k 16 -v 64 -o 50000000 -t 1 -b $DEFAULT_BATCH_SIZE

echo "### 9. Batch Size Comparison ###" | tee -a "$RESULTS"
echo "Testing impact of different batch sizes on write performance" | tee -a "$RESULTS"

run_comparison "Batch Size 1 (no batching, 10M ops)" \
    -w write -p random -o 10000000 -t 1 -b 1

run_comparison "Batch Size 10 (10M ops)" \
    -w write -p random -o 10000000 -t 1 -b 10

run_comparison "Batch Size 100 (10M ops)" \
    -w write -p random -o 10000000 -t 1 -b 100

run_comparison "Batch Size 1000 (10M ops)" \
    -w write -p random -o 10000000 -t 1 -b 1000

run_comparison "Batch Size 10000 (10M ops)" \
    -w write -p random -o 10000000 -t 1 -b 10000

echo "### 10. Batch Size Impact on Deletes ###" | tee -a "$RESULTS"
run_delete_comparison "Delete Batch=1 (5M ops)" \
    -w delete -p random -o 5000000 -t 1 -b 1

run_delete_comparison "Delete Batch=100 (5M ops)" \
    -w delete -p random -o 5000000 -t 1 -b 100

run_delete_comparison "Delete Batch=1000 (5M ops)" \
    -w delete -p random -o 5000000 -t 1 -b 1000

echo "### 11. Seek Performance (Block Index Effectiveness) ###" | tee -a "$RESULTS"
run_seek_comparison "Random Seek (5M ops, 1 thread)" \
    -w seek -p random -o 5000000 -t 1

run_seek_comparison "Sequential Seek (5M ops, 1 thread)" \
    -w seek -p seq -o 5000000 -t 1

run_seek_comparison "Zipfian Seek (5M ops, 1 thread)" \
    -w seek -p zipfian -o 5000000 -t 1

echo "### 12. Range Query Performance ###" | tee -a "$RESULTS"
run_range_comparison "Range Scan 100 keys (1M ops, 1 thread)" \
    -w range -p random -o 1000000 -t 1 --range-size 100

run_range_comparison "Range Scan 1000 keys (500K ops, 1 thread)" \
    -w range -p random -o 500000 -t 1 --range-size 1000

run_range_comparison "Sequential Range Scan 100 keys (1M ops, 1 thread)" \
    -w range -p seq -o 1000000 -t 1 --range-size 100

cleanup_db

echo "" | tee -a "$RESULTS"
echo "===================================" | tee -a "$RESULTS"
echo "Benchmark Suite Complete!" | tee -a "$RESULTS"
echo "" | tee -a "$RESULTS"
echo "Results saved to:" | tee -a "$RESULTS"
echo "  Text Report: $RESULTS" | tee -a "$RESULTS"
echo "  CSV Data:    $CSV_FILE" | tee -a "$RESULTS"
echo "" | tee -a "$RESULTS"
echo "The CSV file contains detailed metrics for all benchmarks" | tee -a "$RESULTS"
echo "and can be imported into spreadsheet tools using tidesdb_rocksdb.py." | tee -a "$RESULTS"
echo "===================================" | tee -a "$RESULTS"
