#!/bin/bash

set -e

BENCH="./build/benchtool"
DB_PATH="${BENCHTOOL_DB_PATH:-db-bench}"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULTS="tidesdb_rocksdb_one_billion_benchmark_results_${TIMESTAMP}.txt"
CSV_FILE="tidesdb_rocksdb_one_billion_benchmark_results_${TIMESTAMP}.csv"

SYNC_ENABLED="false"
OPS_COUNT=1000000000
THREADS=8
DELETE_OPS=500000000
RANGE_SIZE=100
MEMTABLE_SIZE=134217728
BLOCK_CACHE_SIZE=8589934592

if [ "$SYNC_ENABLED" = "true" ]; then
    SYNC_FLAG="--sync"
    SYNC_MODE="ENABLED (durable writes)"
else
    SYNC_FLAG=""
    SYNC_MODE="DISABLED (maximum performance)"
fi

ENGINE_FLAGS="--memtable-size $MEMTABLE_SIZE --block-cache-size $BLOCK_CACHE_SIZE"

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
log "RUNNER: TidesDB vs RocksDB (One Billion Keys)"
log "Date: $(date)"
log "Sync Mode: $SYNC_MODE"
log "Parameters:"
log "  Operations: $OPS_COUNT"
log "  Threads: $THREADS"
log "  Delete Ops: $DELETE_OPS"
log "  Range Size: $RANGE_SIZE"
log "  Memtable Size: $MEMTABLE_SIZE"
log "  Block Cache Size: $BLOCK_CACHE_SIZE"
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

run_comparison() {
    local test_id="$1"
    local test_name="$2"
    shift 2
    local bench_args="$@"

    log ""
    log "*------------------------------------------*"
    log "TEST: $test_name"
    log "*------------------------------------------*"

    cleanup_db || exit 1
    log "Running TidesDB (with RocksDB baseline)..."
    $BENCH -e tidesdb -c $bench_args $SYNC_FLAG $ENGINE_FLAGS -d "$DB_PATH" --test-name "$test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    cleanup_db || exit 1
    log ""
}

run_read_comparison() {
    local test_id="$1"
    local test_name="$2"
    shift 2
    local read_args="$@"
    local write_args="${read_args/-w read/-w write}"
    local populate_test_id="${test_id}_populate"

    log ""
    log "*------------------------------------------*"
    log "TEST: $test_name"
    log "*------------------------------------------*"

    cleanup_db || exit 1
    log "Populating TidesDB for read test..."
    $BENCH -e tidesdb $write_args $SYNC_FLAG $ENGINE_FLAGS -d "$DB_PATH" --test-name "$populate_test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    log "Running TidesDB read test..."
    $BENCH -e tidesdb $read_args $SYNC_FLAG $ENGINE_FLAGS -d "$DB_PATH" --test-name "$test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    cleanup_db || exit 1
    log "Populating RocksDB for read test..."
    $BENCH -e rocksdb $write_args $SYNC_FLAG $ENGINE_FLAGS -d "$DB_PATH" --test-name "$populate_test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    log "Running RocksDB read test..."
    $BENCH -e rocksdb $read_args $SYNC_FLAG $ENGINE_FLAGS -d "$DB_PATH" --test-name "$test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    cleanup_db || exit 1
    log ""
}

run_delete_comparison() {
    local test_id="$1"
    local test_name="$2"
    shift 2
    local delete_args="$@"
    local write_args="${delete_args/-w delete/-w write}"
    local populate_test_id="${test_id}_populate"

    log ""
    log "*------------------------------------------*"
    log "TEST: $test_name"
    log "*------------------------------------------*"

    cleanup_db || exit 1
    log "Populating TidesDB for delete test..."
    $BENCH -e tidesdb $write_args $SYNC_FLAG $ENGINE_FLAGS -d "$DB_PATH" --test-name "$populate_test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    log "Running TidesDB delete test..."
    $BENCH -e tidesdb $delete_args $SYNC_FLAG $ENGINE_FLAGS -d "$DB_PATH" --test-name "$test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    cleanup_db || exit 1
    log "Populating RocksDB for delete test..."
    $BENCH -e rocksdb $write_args $SYNC_FLAG $ENGINE_FLAGS -d "$DB_PATH" --test-name "$populate_test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    log "Running RocksDB delete test..."
    $BENCH -e rocksdb $delete_args $SYNC_FLAG $ENGINE_FLAGS -d "$DB_PATH" --test-name "$test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    cleanup_db || exit 1
    log ""
}

run_seek_comparison() {
    local test_id="$1"
    local test_name="$2"
    shift 2
    local seek_args="$@"
    local write_args="${seek_args/-w seek/-w write}"
    local populate_test_id="${test_id}_populate"

    log ""
    log "*------------------------------------------*"
    log "TEST: $test_name"
    log "*------------------------------------------*"

    cleanup_db || exit 1
    log "Populating TidesDB for seek test..."
    $BENCH -e tidesdb $write_args $SYNC_FLAG $ENGINE_FLAGS -d "$DB_PATH" --test-name "$populate_test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    log "Running TidesDB seek test..."
    $BENCH -e tidesdb $seek_args $SYNC_FLAG $ENGINE_FLAGS -d "$DB_PATH" --test-name "$test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    cleanup_db || exit 1
    log "Populating RocksDB for seek test..."
    $BENCH -e rocksdb $write_args $SYNC_FLAG $ENGINE_FLAGS -d "$DB_PATH" --test-name "$populate_test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    log "Running RocksDB seek test..."
    $BENCH -e rocksdb $seek_args $SYNC_FLAG $ENGINE_FLAGS -d "$DB_PATH" --test-name "$test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    cleanup_db || exit 1
    log ""
}

run_range_comparison() {
    local test_id="$1"
    local test_name="$2"
    shift 2
    local range_args="$@"
    local write_args="${range_args/-w range/-w write}"
    local populate_test_id="${test_id}_populate"

    log ""
    log "*------------------------------------------*"
    log "TEST: $test_name"
    log "*------------------------------------------*"

    cleanup_db || exit 1
    log "Populating TidesDB for range test..."
    $BENCH -e tidesdb $write_args $SYNC_FLAG $ENGINE_FLAGS -d "$DB_PATH" --test-name "$populate_test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    log "Running TidesDB range test..."
    $BENCH -e tidesdb $range_args $SYNC_FLAG $ENGINE_FLAGS -d "$DB_PATH" --test-name "$test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    cleanup_db || exit 1
    log "Populating RocksDB for range test..."
    $BENCH -e rocksdb $write_args $SYNC_FLAG $ENGINE_FLAGS -d "$DB_PATH" --test-name "$populate_test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    log "Running RocksDB range test..."
    $BENCH -e rocksdb $range_args $SYNC_FLAG $ENGINE_FLAGS -d "$DB_PATH" --test-name "$test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    cleanup_db || exit 1
    log ""
}

log "### 1. Write Performance (1B ops) ###"
run_comparison "write_random_1B_t8" "Random Write (1B ops, 8 threads)" \
    -w write -p random -o $OPS_COUNT -t $THREADS

log "### 2. Read Performance (1B ops) ###"
run_read_comparison "read_random_1B_t8" "Random Read (1B ops, 8 threads)" \
    -w read -p random -o $OPS_COUNT -t $THREADS

log "### 3. Iteration/Range Scan (1B ops) ###"
run_range_comparison "range_random_1B_t8" "Range Scan (1B ops, range=$RANGE_SIZE, 8 threads)" \
    -w range -p random -o $OPS_COUNT -t $THREADS --range-size $RANGE_SIZE

log "### 4. Seek Performance (1B ops) ###"
run_seek_comparison "seek_random_1B_t8" "Random Seek (1B ops, 8 threads)" \
    -w seek -p random -o $OPS_COUNT -t $THREADS

log "### 5. Delete Performance (500M ops) ###"
run_delete_comparison "delete_random_500M_t8" "Random Delete (500M ops, 8 threads)" \
    -w delete -p random -o $DELETE_OPS -t $THREADS

cleanup_db

log ""
log "*------------------------------------------*"
log "RUNNER Complete"
log "Results:"
log "  Text: $RESULTS"
log "  CSV:  $CSV_FILE"
log "*------------------------------------------*"
