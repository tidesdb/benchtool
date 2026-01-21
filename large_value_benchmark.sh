#!/bin/bash

set -e

BENCH="./build/benchtool"
DB_PATH="${BENCHTOOL_DB_PATH:-db-bench}"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULTS="tidesdb_rocksdb_large_value_benchmark_results_${TIMESTAMP}.txt"
CSV_FILE="tidesdb_rocksdb_large_value_benchmark_results_${TIMESTAMP}.csv"

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
log "RUNNER: Large Value (8KB)"
log "Date: $(date)"
log "Sync Mode: $SYNC_MODE"
log "Parameters:"
log "  Value Size: 8KB ($VALUE_SIZE bytes)"
log "  Key Size: $KEY_SIZE bytes"
log "  Operations: $OPS_COUNT"
log "  Threads: $THREADS"
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
    local pattern="$3"
    
    log ""
    log "*------------------------------------------*"
    log "TEST: $test_name"
    log "*------------------------------------------*"
    
    cleanup_db || exit 1
    log "Running TidesDB PUT ($pattern)..."
    $BENCH -e tidesdb -w write -p $pattern -k $KEY_SIZE -v $VALUE_SIZE -o $OPS_COUNT -t $THREADS $SYNC_FLAG -d "$DB_PATH" --test-name "$test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"
    
    cleanup_db || exit 1
    log "Running RocksDB PUT ($pattern)..."
    $BENCH -e rocksdb -w write -p $pattern -k $KEY_SIZE -v $VALUE_SIZE -o $OPS_COUNT -t $THREADS $SYNC_FLAG -d "$DB_PATH" --test-name "$test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"
    
    cleanup_db || exit 1
    log ""
}

run_get_test() {
    local test_id="$1"
    local test_name="$2"
    local pattern="$3"
    local populate_test_id="${test_id}_populate"
    
    log ""
    log "*------------------------------------------*"
    log "TEST: $test_name"
    log "*------------------------------------------*"
    
    cleanup_db || exit 1
    log "Populating TidesDB for GET test ($pattern)..."
    $BENCH -e tidesdb -w write -p $pattern -k $KEY_SIZE -v $VALUE_SIZE -o $OPS_COUNT -t $THREADS $SYNC_FLAG -d "$DB_PATH" --test-name "$populate_test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"
    
    log "Running TidesDB GET ($pattern)..."
    $BENCH -e tidesdb -w read -p $pattern -k $KEY_SIZE -v $VALUE_SIZE -o $OPS_COUNT -t $THREADS $SYNC_FLAG -d "$DB_PATH" --test-name "$test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"
    
    cleanup_db || exit 1
    log "Populating RocksDB for GET test ($pattern)..."
    $BENCH -e rocksdb -w write -p $pattern -k $KEY_SIZE -v $VALUE_SIZE -o $OPS_COUNT -t $THREADS $SYNC_FLAG -d "$DB_PATH" --test-name "$populate_test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"
    
    log "Running RocksDB GET ($pattern)..."
    $BENCH -e rocksdb -w read -p $pattern -k $KEY_SIZE -v $VALUE_SIZE -o $OPS_COUNT -t $THREADS $SYNC_FLAG -d "$DB_PATH" --test-name "$test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"
    
    cleanup_db || exit 1
    log ""
}

run_seek_test() {
    local test_id="$1"
    local test_name="$2"
    local pattern="$3"
    local populate_test_id="${test_id}_populate"
    
    log ""
    log "*------------------------------------------*"
    log "TEST: $test_name"
    log "*------------------------------------------*"
    
    cleanup_db || exit 1
    log "Populating TidesDB for SEEK test ($pattern)..."
    $BENCH -e tidesdb -w write -p $pattern -k $KEY_SIZE -v $VALUE_SIZE -o $OPS_COUNT -t $THREADS $SYNC_FLAG -d "$DB_PATH" --test-name "$populate_test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"
    
    log "Running TidesDB SEEK ($pattern)..."
    $BENCH -e tidesdb -w seek -p $pattern -k $KEY_SIZE -v $VALUE_SIZE -o $OPS_COUNT -t $THREADS $SYNC_FLAG -d "$DB_PATH" --test-name "$test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    cleanup_db || exit 1
    log "Populating RocksDB for SEEK test ($pattern)..."
    $BENCH -e rocksdb -w write -p $pattern -k $KEY_SIZE -v $VALUE_SIZE -o $OPS_COUNT -t $THREADS $SYNC_FLAG -d "$DB_PATH" --test-name "$populate_test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"
    
    log "Running RocksDB SEEK ($pattern)..."
    $BENCH -e rocksdb -w seek -p $pattern -k $KEY_SIZE -v $VALUE_SIZE -o $OPS_COUNT -t $THREADS $SYNC_FLAG -d "$DB_PATH" --test-name "$test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"
    
    cleanup_db || exit 1
    log ""
}

run_iteration_test() {
    local test_id="$1"
    local test_name="$2"
    local pattern="$3"
    local range_size="$4"
    local populate_test_id="${test_id}_populate"
    
    log ""
    log "*------------------------------------------*"
    log "TEST: $test_name"
    log "*------------------------------------------*"
    
    cleanup_db || exit 1
    log "Populating TidesDB for ITERATION test ($pattern)..."
    $BENCH -e tidesdb -w write -p $pattern -k $KEY_SIZE -v $VALUE_SIZE -o $OPS_COUNT -t $THREADS $SYNC_FLAG -d "$DB_PATH" --test-name "$populate_test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"
    
    log "Running TidesDB ITERATION ($pattern)..."
    $BENCH -e tidesdb -w range -p $pattern -k $KEY_SIZE -v $VALUE_SIZE -o $OPS_COUNT -t $THREADS --range-size $range_size $SYNC_FLAG -d "$DB_PATH" --test-name "$test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"
    
    cleanup_db || exit 1
    log "Populating RocksDB for ITERATION test ($pattern)..."
    $BENCH -e rocksdb -w write -p $pattern -k $KEY_SIZE -v $VALUE_SIZE -o $OPS_COUNT -t $THREADS $SYNC_FLAG -d "$DB_PATH" --test-name "$populate_test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"
    
    log "Running RocksDB ITERATION ($pattern)..."
    $BENCH -e rocksdb -w range -p $pattern -k $KEY_SIZE -v $VALUE_SIZE -o $OPS_COUNT -t $THREADS --range-size $range_size $SYNC_FLAG -d "$DB_PATH" --test-name "$test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"
    
    cleanup_db || exit 1
    log ""
}

log "### 1. PUT Performance - Sequential ###"
run_put_test "large_value_8kb_put_seq" "8KB PUT - Sequential" "seq"

log "### 2. PUT Performance - Random ###"
run_put_test "large_value_8kb_put_random" "8KB PUT - Random" "random"

log "### 3. GET Performance - Sequential ###"
run_get_test "large_value_8kb_get_seq" "8KB GET - Sequential" "seq"

log "### 4. GET Performance - Random ###"
run_get_test "large_value_8kb_get_random" "8KB GET - Random" "random"

log "### 5. SEEK Performance - Sequential ###"
run_seek_test "large_value_8kb_seek_seq" "8KB SEEK - Sequential" "seq"

log "### 6. SEEK Performance - Random ###"
run_seek_test "large_value_8kb_seek_random" "8KB SEEK - Random" "random"

log "### 7. ITERATION Performance - Sequential (100 keys per range) ###"
run_iteration_test "large_value_8kb_range_seq_100" "8KB ITERATION - Sequential" "seq" 100

log "### 8. ITERATION Performance - Random (100 keys per range) ###"
run_iteration_test "large_value_8kb_range_random_100" "8KB ITERATION - Random" "random" 100

cleanup_db

log ""
log "*------------------------------------------*"
log "RUNNER Complete"
log "Results:"
log "  Text: $RESULTS"
log "  CSV:  $CSV_FILE"
log "*------------------------------------------*"
