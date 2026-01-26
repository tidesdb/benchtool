#!/bin/bash

set -e

BENCH="./build/benchtool"
DB_PATH="${BENCHTOOL_DB_PATH:-db-bench}"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULTS="tidesdb_rocksdb_benchmark_results_${TIMESTAMP}.txt"
CSV_FILE="tidesdb_rocksdb_benchmark_results_${TIMESTAMP}.csv"

SYNC_ENABLED="false"
DEFAULT_BATCH_SIZE=1000
DEFAULT_THREADS=8

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
log "RUNNER: TidesDB vs RocksDB Comparison"
log "Date: $(date)"
log "Sync Mode: $SYNC_MODE"
log "Parameters:"
log "  Default Batch Size: $DEFAULT_BATCH_SIZE"
log "  Default Threads: $DEFAULT_THREADS"
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
    $BENCH -e tidesdb --debug -c $bench_args $SYNC_FLAG -d "$DB_PATH" --test-name "$test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

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
    $BENCH -e tidesdb --debug $populate_args $SYNC_FLAG -d "$DB_PATH" --test-name "$populate_test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    log "Running TidesDB read test..."
    $BENCH -e tidesdb --debug $read_args $SYNC_FLAG -d "$DB_PATH" --test-name "$test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    cleanup_db || exit 1
    log "Populating RocksDB for read test..."
    $BENCH -e rocksdb --debug $populate_args $SYNC_FLAG -d "$DB_PATH" --test-name "$populate_test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    log "Running RocksDB read test..."
    $BENCH -e rocksdb --debug $read_args $SYNC_FLAG -d "$DB_PATH" --test-name "$test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

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
    $BENCH -e tidesdb --debug $write_args $SYNC_FLAG -d "$DB_PATH" --test-name "$populate_test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    log "Running TidesDB delete test..."
    $BENCH -e tidesdb --debug $delete_args $SYNC_FLAG -d "$DB_PATH" --test-name "$test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    cleanup_db || exit 1
    log "Populating RocksDB for delete test..."
    $BENCH -e rocksdb --debug $write_args $SYNC_FLAG -d "$DB_PATH" --test-name "$populate_test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    log "Running RocksDB delete test..."
    $BENCH -e rocksdb --debug $delete_args $SYNC_FLAG -d "$DB_PATH" --test-name "$test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

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
    $BENCH -e tidesdb --debug $populate_args $SYNC_FLAG -d "$DB_PATH" --test-name "$populate_test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    log "Running TidesDB seek test..."
    $BENCH -e tidesdb --debug $seek_args $SYNC_FLAG -d "$DB_PATH" --test-name "$test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    cleanup_db || exit 1
    log "Populating RocksDB for seek test..."
    $BENCH -e rocksdb --debug $populate_args $SYNC_FLAG -d "$DB_PATH" --test-name "$populate_test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    log "Running RocksDB seek test..."
    $BENCH -e rocksdb --debug $seek_args $SYNC_FLAG -d "$DB_PATH" --test-name "$test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

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
    $BENCH -e tidesdb --debug $populate_args $SYNC_FLAG -d "$DB_PATH" --test-name "$populate_test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    log "Running TidesDB range test..."
    $BENCH -e tidesdb --debug $range_args $SYNC_FLAG -d "$DB_PATH" --test-name "$test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    cleanup_db || exit 1
    log "Populating RocksDB for range test..."
    $BENCH -e rocksdb --debug $populate_args $SYNC_FLAG -d "$DB_PATH" --test-name "$populate_test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    log "Running RocksDB range test..."
    $BENCH -e rocksdb --debug $range_args $SYNC_FLAG -d "$DB_PATH" --test-name "$test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    cleanup_db || exit 1
    log ""
}

log "### 1. Sequential Write Performance (Batched) ###"
run_comparison "write_seq_10M_t8_b${DEFAULT_BATCH_SIZE}" "Sequential Write (10M ops, 8 threads, batch=$DEFAULT_BATCH_SIZE)" \
    -w write -p seq -o 10000000 -t 8 -b $DEFAULT_BATCH_SIZE

log "### 2. Random Write Performance (Batched) ###"
run_comparison "write_random_10M_t8_b${DEFAULT_BATCH_SIZE}" "Random Write (10M ops, 8 threads, batch=$DEFAULT_BATCH_SIZE)" \
    -w write -p random -o 10000000 -t 8 -b $DEFAULT_BATCH_SIZE

log "### 3. Random Read Performance ###"
run_read_comparison "read_random_10M_t8" "Random Read (10M ops, 8 threads)" \
    -w read -p random -o 10000000 -t 8

log "### 4. Mixed Workload (50/50 Read/Write, Batched) ###"
run_comparison "mixed_random_5M_t8_b${DEFAULT_BATCH_SIZE}" "Mixed Workload (5M ops, 8 threads, batch=$DEFAULT_BATCH_SIZE)" \
    -w mixed -p random -o 5000000 -t 8 -b $DEFAULT_BATCH_SIZE

log "### 5. Hot Key Workload (Zipfian Distribution, Batched) ###"
run_comparison "write_zipfian_5M_t8_b${DEFAULT_BATCH_SIZE}" "Zipfian Write (5M ops, 8 threads, batch=$DEFAULT_BATCH_SIZE)" \
    -w write -p zipfian -o 5000000 -t 8 -b $DEFAULT_BATCH_SIZE

run_comparison "mixed_zipfian_5M_t8_b${DEFAULT_BATCH_SIZE}" "Zipfian Mixed (5M ops, 8 threads, batch=$DEFAULT_BATCH_SIZE)" \
    -w mixed -p zipfian -o 5000000 -t 8 -b $DEFAULT_BATCH_SIZE

log "### 6. Delete Performance (Batched) ###"
run_delete_comparison "delete_random_5M_t8_b${DEFAULT_BATCH_SIZE}" "Random Delete (5M ops, 8 threads, batch=$DEFAULT_BATCH_SIZE)" \
    -w delete -p random -o 5000000 -t 8 -b $DEFAULT_BATCH_SIZE

log "### 7. Large Value Performance (Batched) ###"
run_comparison "write_large_values_1M_k256_v4096_t8_b${DEFAULT_BATCH_SIZE}" "Large Values (1M ops, 256B key, 4KB value, batch=$DEFAULT_BATCH_SIZE)" \
    -w write -p random -k 256 -v 4096 -o 1000000 -t 8 -b $DEFAULT_BATCH_SIZE

log "### 8. Small Value Performance (Batched) ###"
run_comparison "write_small_values_50M_k16_v64_t8_b${DEFAULT_BATCH_SIZE}" "Small Values (50M ops, 16B key, 64B value, batch=$DEFAULT_BATCH_SIZE)" \
    -w write -p random -k 16 -v 64 -o 50000000 -t 8 -b $DEFAULT_BATCH_SIZE

log "### 9. Batch Size Comparison ###"
log "Testing impact of different batch sizes on write performance"

run_comparison "batch_1_10M_t8" "Batch Size 1 (no batching, 10M ops)" \
    -w write -p random -o 10000000 -t 8 -b 1

run_comparison "batch_10_10M_t8" "Batch Size 10 (10M ops)" \
    -w write -p random -o 10000000 -t 8 -b 10

run_comparison "batch_100_10M_t8" "Batch Size 100 (10M ops)" \
    -w write -p random -o 10000000 -t 8 -b 100

run_comparison "batch_1000_10M_t8" "Batch Size 1000 (10M ops)" \
    -w write -p random -o 10000000 -t 8 -b 1000

run_comparison "batch_10000_10M_t8" "Batch Size 10000 (10M ops)" \
    -w write -p random -o 10000000 -t 8 -b 10000

log "### 10. Batch Size Impact on Deletes ###"
run_delete_comparison "delete_batch_1_5M_t8" "Delete Batch=1 (5M ops)" \
    -w delete -p random -o 5000000 -t 8 -b 1

run_delete_comparison "delete_batch_100_5M_t8" "Delete Batch=100 (5M ops)" \
    -w delete -p random -o 5000000 -t 8 -b 100

run_delete_comparison "delete_batch_1000_5M_t8" "Delete Batch=1000 (5M ops)" \
    -w delete -p random -o 5000000 -t 8 -b 1000

log "### 11. Seek Performance (Block Index Effectiveness) ###"
run_seek_comparison "seek_random_5M_t8" "Random Seek (5M ops, 8 threads)" \
    -w seek -p random -o 5000000 -t 8

run_seek_comparison "seek_seq_5M_t8" "Sequential Seek (5M ops, 8 threads)" \
    -w seek -p seq -o 5000000 -t 8

run_seek_comparison "seek_zipfian_5M_t8" "Zipfian Seek (5M ops, 8 threads)" \
    -w seek -p zipfian -o 5000000 -t 8

log "### 12. Range Query Performance ###"
run_range_comparison "range_random_100_1M_t8" "Range Scan 100 keys (1M ops, 8 threads)" \
    -w range -p random -o 1000000 -t 8 --range-size 100

run_range_comparison "range_random_1000_500K_t8" "Range Scan 1000 keys (500K ops, 8 threads)" \
    -w range -p random -o 500000 -t 8 --range-size 1000

run_range_comparison "range_seq_100_1M_t8" "Sequential Range Scan 100 keys (1M ops, 8 threads)" \
    -w range -p seq -o 1000000 -t 8 --range-size 100

cleanup_db

log ""
log "*------------------------------------------*"
log "RUNNER Complete"
log ""
log "Results:"
log "  Text: $RESULTS"
log "  CSV:  $CSV_FILE"
log "*------------------------------------------*"
