#!/bin/bash

set -e

BENCH="./build/benchtool"
DB_PATH="${BENCHTOOL_DB_PATH:-db-bench}"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULTS="tidesdb_rocksdb_1gb_benchmark_results_${TIMESTAMP}.txt"
CSV_FILE="tidesdb_rocksdb_1gb_benchmark_results_${TIMESTAMP}.csv"

SYNC_ENABLED="false"
VALUE_SIZE=1073741824
KEY_SIZE=256
OPS_COUNT=10
THREADS=1

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

log() {
    echo "$1" | tee -a "$RESULTS"
}

FS_TYPE=$(df -T "$DB_PATH" 2>/dev/null | awk 'NR==2 {print $2}')
FS_TYPE=${FS_TYPE:-unknown}

log "*------------------------------------------*"
log "RUNNER: Large Value (1GB)"
log "Date: $(date)"
log "Sync Mode: $SYNC_MODE"
log "Parameters:"
log "  Value Size: 1GB ($VALUE_SIZE bytes)"
log "  Key Size: $KEY_SIZE bytes"
log "  Operations: $OPS_COUNT"
log "  Threads: $THREADS"
log "  Total Data: ~10GB"
log "Environment:"
log "  Hostname: $(hostname)"
log "  Kernel: $(uname -r)"
log "  Filesystem: $FS_TYPE"
log "  CPU: $(grep 'model name' /proc/cpuinfo | head -1 | cut -d: -f2 | xargs)"
log "  CPU Cores: $(nproc)"
log "  Memory: $(free -h | grep Mem | awk '{print $2}')"
log "Results:"
log "  Text: $RESULTS"
log "  CSV:  $CSV_FILE"
log "*------------------------------------------*"
log ""

cleanup_db() {
    if [ -d "$DB_PATH" ]; then
        rm -rf "$DB_PATH"
        if [ -d "$DB_PATH" ]; then
            log "Warning: Failed to remove $DB_PATH"
            return 1
        fi
    fi
    sync
    return 0
}

run_put_test() {
    local test_id="$1"
    local test_name="$2"
    local engine="$3"
    
    log ""
    log "*------------------------------------------*"
    log "TEST: $test_name"
    log "*------------------------------------------*"
    
    cleanup_db || exit 1
    log "Running $engine PUT (sequential, 10 x 1GB values)..."
    $BENCH -e $engine -w write -p seq -k $KEY_SIZE -v $VALUE_SIZE -o $OPS_COUNT -t $THREADS $SYNC_FLAG -d "$DB_PATH" --test-name "$test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"
    
    log ""
}

run_get_test() {
    local test_id="$1"
    local test_name="$2"
    local engine="$3"
    local populate_test_id="${test_id}_populate"
    
    log ""
    log "*------------------------------------------*"
    log "TEST: $test_name"
    log "*------------------------------------------*"
    
    cleanup_db || exit 1
    log "Populating $engine for GET test (sequential write)..."
    $BENCH -e $engine -w write -p seq -k $KEY_SIZE -v $VALUE_SIZE -o $OPS_COUNT -t $THREADS $SYNC_FLAG -d "$DB_PATH" --test-name "$populate_test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"
    
    log "Running $engine GET (sequential read, 10 x 1GB values)..."
    $BENCH -e $engine -w read -p seq -k $KEY_SIZE -v $VALUE_SIZE -o $OPS_COUNT -t $THREADS $SYNC_FLAG -d "$DB_PATH" --test-name "$test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"
    
    log ""
}

run_iteration_test() {
    local test_id="$1"
    local test_name="$2"
    local engine="$3"
    local range_size="$4"
    local populate_test_id="${test_id}_populate"
    
    log ""
    log "*------------------------------------------*"
    log "TEST: $test_name"
    log "*------------------------------------------*"
    
    cleanup_db || exit 1
    log "Populating $engine for ITERATION test (sequential write)..."
    $BENCH -e $engine -w write -p seq -k $KEY_SIZE -v $VALUE_SIZE -o $OPS_COUNT -t $THREADS $SYNC_FLAG -d "$DB_PATH" --test-name "$populate_test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"
    
    log "Running $engine ITERATION (sequential range scan, $range_size keys)..."
    $BENCH -e $engine -w range -p seq -k $KEY_SIZE -v $VALUE_SIZE -o $OPS_COUNT -t $THREADS --range-size $range_size $SYNC_FLAG -d "$DB_PATH" --test-name "$test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"
    
    log ""
}

log "### 1. TidesDB - PUT Performance (10 x 1GB) ###"
run_put_test "large_value_1gb_put" "TidesDB 1GB PUT" "tidesdb"

log "### 2. RocksDB - PUT Performance (10 x 1GB) ###"
run_put_test "large_value_1gb_put" "RocksDB 1GB PUT" "rocksdb"

log "### 3. TidesDB - GET Performance (10 x 1GB) ###"
run_get_test "large_value_1gb_get" "TidesDB 1GB GET" "tidesdb"

log "### 4. RocksDB - GET Performance (10 x 1GB) ###"
run_get_test "large_value_1gb_get" "RocksDB 1GB GET" "rocksdb"

log "### 5. TidesDB - ITERATION Performance (all 10 keys) ###"
run_iteration_test "large_value_1gb_range_10" "TidesDB 1GB ITERATION" "tidesdb" 10

log "### 6. RocksDB - ITERATION Performance (all 10 keys) ###"
run_iteration_test "large_value_1gb_range_10" "RocksDB 1GB ITERATION" "rocksdb" 10

cleanup_db

log ""
log "*------------------------------------------*"
log "RUNNER Complete"
log "Results:"
log "  Text: $RESULTS"
log "  CSV:  $CSV_FILE"
log "*------------------------------------------*"
