#!/bin/bash

set -e

BENCH="./build/benchtool"
DB_PATH="${BENCHTOOL_DB_PATH:-db-bench}"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULTS="tidesdb_rocksdb_no_bloom_indexes_benchmark_results_${TIMESTAMP}.txt"
CSV_FILE="tidesdb_rocksdb_no_bloom_indexes_benchmark_results_${TIMESTAMP}.csv"

SYNC_ENABLED="false"
DEFAULT_BATCH_SIZE=3
DEFAULT_THREADS=6
NO_BLOOM_BLOCK_FLAGS="--no-bloom-filters --no-block-indexes"

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
log "RUNNER: TidesDB vs RocksDB (No Bloom/Block Indexes)"
log "Date: $(date)"
log "Sync Mode: $SYNC_MODE"
log "Parameters:"
log "  Default Batch Size: $DEFAULT_BATCH_SIZE"
log "  Default Threads: $DEFAULT_THREADS"
log "  Bloom Filters: DISABLED"
log "  Block Indexes: DISABLED"
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
    $BENCH -e tidesdb -c $bench_args $SYNC_FLAG $NO_BLOOM_BLOCK_FLAGS -d "$DB_PATH" --test-name "$test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    cleanup_db || exit 1
    log ""
}

run_read_comparison() {
    local test_id="$1"
    local test_name="$2"
    shift 2
    local read_args="$@"
    local write_args="${read_args/-w read/-w write}"

    local populate_args="$write_args -b $DEFAULT_BATCH_SIZE"
    local populate_test_id="${test_id}_populate"

    log ""
    log "*------------------------------------------*"
    log "TEST: $test_name"
    log "*------------------------------------------*"

    cleanup_db || exit 1
    log "Populating TidesDB for read test..."
    $BENCH -e tidesdb $populate_args $SYNC_FLAG $NO_BLOOM_BLOCK_FLAGS -d "$DB_PATH" --test-name "$populate_test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    log "Running TidesDB read test..."
    $BENCH -e tidesdb $read_args $SYNC_FLAG $NO_BLOOM_BLOCK_FLAGS -d "$DB_PATH" --test-name "$test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    cleanup_db || exit 1
    log "Populating RocksDB for read test..."
    $BENCH -e rocksdb $populate_args $SYNC_FLAG $NO_BLOOM_BLOCK_FLAGS -d "$DB_PATH" --test-name "$populate_test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    log "Running RocksDB read test..."
    $BENCH -e rocksdb $read_args $SYNC_FLAG $NO_BLOOM_BLOCK_FLAGS -d "$DB_PATH" --test-name "$test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

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
    $BENCH -e tidesdb $write_args $SYNC_FLAG $NO_BLOOM_BLOCK_FLAGS -d "$DB_PATH" --test-name "$populate_test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    log "Running TidesDB delete test..."
    $BENCH -e tidesdb $delete_args $SYNC_FLAG $NO_BLOOM_BLOCK_FLAGS -d "$DB_PATH" --test-name "$test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    cleanup_db || exit 1
    log "Populating RocksDB for delete test..."
    $BENCH -e rocksdb $write_args $SYNC_FLAG $NO_BLOOM_BLOCK_FLAGS -d "$DB_PATH" --test-name "$populate_test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    log "Running RocksDB delete test..."
    $BENCH -e rocksdb $delete_args $SYNC_FLAG $NO_BLOOM_BLOCK_FLAGS -d "$DB_PATH" --test-name "$test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    cleanup_db || exit 1
    log ""
}

run_seek_comparison() {
    local test_id="$1"
    local test_name="$2"
    shift 2
    local seek_args="$@"
    local write_args="${seek_args/-w seek/-w write}"

    local populate_args="$write_args -b $DEFAULT_BATCH_SIZE"
    local populate_test_id="${test_id}_populate"

    log ""
    log "*------------------------------------------*"
    log "TEST: $test_name"
    log "*------------------------------------------*"

    cleanup_db || exit 1
    log "Populating TidesDB for seek test..."
    $BENCH -e tidesdb $populate_args $SYNC_FLAG $NO_BLOOM_BLOCK_FLAGS -d "$DB_PATH" --test-name "$populate_test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    log "Running TidesDB seek test..."
    $BENCH -e tidesdb $seek_args $SYNC_FLAG $NO_BLOOM_BLOCK_FLAGS -d "$DB_PATH" --test-name "$test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    cleanup_db || exit 1
    log "Populating RocksDB for seek test..."
    $BENCH -e rocksdb $populate_args $SYNC_FLAG $NO_BLOOM_BLOCK_FLAGS -d "$DB_PATH" --test-name "$populate_test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    log "Running RocksDB seek test..."
    $BENCH -e rocksdb $seek_args $SYNC_FLAG $NO_BLOOM_BLOCK_FLAGS -d "$DB_PATH" --test-name "$test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    cleanup_db || exit 1
    log ""
}

run_range_comparison() {
    local test_id="$1"
    local test_name="$2"
    shift 2
    local range_args="$@"
    local write_args="${range_args/-w range/-w write}"

    local populate_args="$write_args -b $DEFAULT_BATCH_SIZE"
    local populate_test_id="${test_id}_populate"

    log ""
    log "*------------------------------------------*"
    log "TEST: $test_name"
    log "*------------------------------------------*"

    cleanup_db || exit 1
    log "Populating TidesDB for range test..."
    $BENCH -e tidesdb $populate_args $SYNC_FLAG $NO_BLOOM_BLOCK_FLAGS -d "$DB_PATH" --test-name "$populate_test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    log "Running TidesDB range test..."
    $BENCH -e tidesdb $range_args $SYNC_FLAG $NO_BLOOM_BLOCK_FLAGS -d "$DB_PATH" --test-name "$test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    cleanup_db || exit 1
    log "Populating RocksDB for range test..."
    $BENCH -e rocksdb $populate_args $SYNC_FLAG $NO_BLOOM_BLOCK_FLAGS -d "$DB_PATH" --test-name "$populate_test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    log "Running RocksDB range test..."
    $BENCH -e rocksdb $range_args $SYNC_FLAG $NO_BLOOM_BLOCK_FLAGS -d "$DB_PATH" --test-name "$test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    cleanup_db || exit 1
    log ""
}

log "### 1. Sequential Write Performance (Batched) ###"
run_comparison "write_seq_45K_t6_b${DEFAULT_BATCH_SIZE}" "Sequential Write (45K ops, 6 threads, batch=$DEFAULT_BATCH_SIZE)" \
    -w write -p seq -o 45000 -t 6 -b $DEFAULT_BATCH_SIZE

log "### 2. Random Write Performance (Batched) ###"
run_comparison "write_random_45K_t6_b${DEFAULT_BATCH_SIZE}" "Random Write (45K ops, 6 threads, batch=$DEFAULT_BATCH_SIZE)" \
    -w write -p random -o 45000 -t 6 -b $DEFAULT_BATCH_SIZE

log "### 3. Random Read Performance ###"
run_read_comparison "read_random_45K_t6" "Random Read (45K ops, 6 threads)" \
    -w read -p random -o 45000 -t 6

log "### 4. Mixed Workload (50/50 Read/Write, Batched) ###"
run_comparison "mixed_random_45K_t6_b${DEFAULT_BATCH_SIZE}" "Mixed Workload (45K ops, 6 threads, batch=$DEFAULT_BATCH_SIZE)" \
    -w mixed -p random -o 45000 -t 6 -b $DEFAULT_BATCH_SIZE

log "### 5. Hot Key Workload (Zipfian Distribution, Batched) ###"
run_comparison "write_zipfian_45K_t6_b${DEFAULT_BATCH_SIZE}" "Zipfian Write (45K ops, 6 threads, batch=$DEFAULT_BATCH_SIZE)" \
    -w write -p zipfian -o 45000 -t 6 -b $DEFAULT_BATCH_SIZE

run_comparison "mixed_zipfian_45K_t6_b${DEFAULT_BATCH_SIZE}" "Zipfian Mixed (45K ops, 6 threads, batch=$DEFAULT_BATCH_SIZE)" \
    -w mixed -p zipfian -o 45000 -t 6 -b $DEFAULT_BATCH_SIZE

log "### 6. Delete Performance (Batched) ###"
run_delete_comparison "delete_random_45K_t6_b${DEFAULT_BATCH_SIZE}" "Random Delete (45K ops, 6 threads, batch=$DEFAULT_BATCH_SIZE)" \
    -w delete -p random -o 45000 -t 6 -b $DEFAULT_BATCH_SIZE

log "### 7. Large Value Performance (Batched) ###"
run_comparison "write_large_values_45K_k256_v4096_t6_b${DEFAULT_BATCH_SIZE}" "Large Values (45K ops, 256B key, 4KB value, batch=$DEFAULT_BATCH_SIZE)" \
    -w write -p random -k 256 -v 4096 -o 45000 -t 6 -b $DEFAULT_BATCH_SIZE

log "### 8. Small Value Performance (Batched) ###"
run_comparison "write_small_values_45K_k16_v64_t6_b${DEFAULT_BATCH_SIZE}" "Small Values (45K ops, 16B key, 64B value, batch=$DEFAULT_BATCH_SIZE)" \
    -w write -p random -k 16 -v 64 -o 45000 -t 6 -b $DEFAULT_BATCH_SIZE

log "### 9. Batch Size Comparison ###"
log "Testing impact of different batch sizes on write performance"

run_comparison "batch_1_45K_t6" "Batch Size 1 (no batching, 45K ops)" \
    -w write -p random -o 45000 -t 6 -b 1

run_comparison "batch_10_45K_t6" "Batch Size 10 (45K ops)" \
    -w write -p random -o 45000 -t 6 -b 10

run_comparison "batch_100_45K_t6" "Batch Size 100 (45K ops)" \
    -w write -p random -o 45000 -t 6 -b 100

run_comparison "batch_1000_45K_t6" "Batch Size 1000 (45K ops)" \
    -w write -p random -o 45000 -t 6 -b 1000

run_comparison "batch_10000_45K_t6" "Batch Size 10000 (45K ops)" \
    -w write -p random -o 45000 -t 6 -b 10000

log "### 10. Batch Size Impact on Deletes ###"
run_delete_comparison "delete_batch_1_45K_t6" "Delete Batch=1 (45K ops)" \
    -w delete -p random -o 45000 -t 6 -b 1

run_delete_comparison "delete_batch_100_45K_t6" "Delete Batch=100 (45K ops)" \
    -w delete -p random -o 45000 -t 6 -b 100

run_delete_comparison "delete_batch_1000_45K_t6" "Delete Batch=1000 (45K ops)" \
    -w delete -p random -o 45000 -t 6 -b 1000

log "### 11. Seek Performance ###"
run_seek_comparison "seek_random_45K_t6" "Random Seek (45K ops, 6 threads)" \
    -w seek -p random -o 45000 -t 6

run_seek_comparison "seek_seq_45K_t6" "Sequential Seek (45K ops, 6 threads)" \
    -w seek -p seq -o 45000 -t 6

run_seek_comparison "seek_zipfian_45K_t6" "Zipfian Seek (45K ops, 6 threads)" \
    -w seek -p zipfian -o 45000 -t 6

log "### 12. Range Query Performance ###"
run_range_comparison "range_random_100_45K_t6" "Range Scan 100 keys (45K ops, 6 threads)" \
    -w range -p random -o 45000 -t 6 --range-size 100

run_range_comparison "range_random_1000_45K_t6" "Range Scan 1000 keys (45K ops, 6 threads)" \
    -w range -p random -o 45000 -t 6 --range-size 1000

run_range_comparison "range_seq_100_45K_t6" "Sequential Range Scan 100 keys (45K ops, 6 threads)" \
    -w range -p seq -o 45000 -t 6 --range-size 100

cleanup_db

log ""
log "*------------------------------------------*"
log "RUNNER Complete"
log ""
log "Results:"
log "  Text: $RESULTS"
log "  CSV:  $CSV_FILE"
log "*------------------------------------------*"
