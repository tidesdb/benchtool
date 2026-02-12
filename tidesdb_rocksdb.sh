#!/bin/bash
#
# TidesDB vs RocksDB Comparison Benchmark
# ========================================
#
# DESCRIPTION:
#   Standard benchmark suite comparing TidesDB and RocksDB across 24 test
#   categories including writes, reads, mixed workloads, deletes, seeks,
#   and range scans with various patterns and batch sizes.
#   Tests 1-12 use 64MB cache and 8 threads.
#   Tests 13-24 use 64MB cache, 16 threads, and 4x operations (large scale).
#
# FLOW:
#   1. For each test, run TidesDB then RocksDB separately
#   2. For read/seek/range/delete tests: populate DB first, then run workload
#   3. Clean up DB between engine runs for fair comparison
#   4. Results output to timestamped .txt and .csv files
#
# TEST CATEGORIES (Standard: 64MB cache, 8 threads):
#   1.  Sequential Write (10M ops)
#   2.  Random Write (10M ops)
#   3.  Random Read (10M ops)
#   4.  Mixed Workload 50/50 (5M ops)
#   5.  Zipfian Write + Mixed (5M ops each)
#   6.  Delete (5M ops)
#   7.  Large Values (1M ops, 4KB values)
#   8.  Small Values (50M ops, 64B values)
#   9.  Batch Size Scaling (1 to 10000)
#   10. Delete Batch Scaling
#   11. Seek Performance (random, seq, zipfian)
#   12. Range Scan Performance
#
# TEST CATEGORIES (Large Scale: 64MB cache, 16 threads, 4x ops):
#   13. Sequential Write (40M ops)
#   14. Random Write (40M ops)
#   15. Random Read (40M ops)
#   16. Mixed Workload (20M ops)
#   17. Zipfian Write + Mixed (20M ops each)
#   18. Delete (20M ops)
#   19. Large Values (4M ops, 4KB values)
#   20. Small Values (200M ops, 64B values)
#   21. Batch Size Scaling (1, 100, 1000)
#   22. Delete Batch Scaling
#   23. Seek Performance (random, seq, zipfian)
#   24. Range Scan Performance
#
# DURABILITY TESTS (64MB cache, sync enabled):
#   25. Synced Write Scaling (25K/1t, 50K/4t, 100K/8t, 500K/16t)
#
# USAGE:
#   ./tidesdb_rocksdb.sh
#   ./tidesdb_rocksdb.sh --use-btree
#

set -e

BENCH="./build/benchtool"
DB_PATH="${BENCHTOOL_DB_PATH:-db-bench}"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULTS="tidesdb_rocksdb_benchmark_results_${TIMESTAMP}.txt"
CSV_FILE="tidesdb_rocksdb_benchmark_results_${TIMESTAMP}.csv"

SYNC_ENABLED="false"
DEFAULT_BATCH_SIZE=1000
DEFAULT_THREADS=8
BLOCK_CACHE_SIZE=67108864
USE_BTREE="false"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --use-btree)
            USE_BTREE="true"
            shift
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--use-btree]"
            exit 1
            ;;
    esac
done

if [ "$SYNC_ENABLED" = "true" ]; then
    SYNC_FLAG="--sync"
    SYNC_MODE="ENABLED (durable writes)"
else
    SYNC_FLAG=""
    SYNC_MODE="DISABLED (maximum performance)"
fi

if [ "$USE_BTREE" = "true" ]; then
    BTREE_FLAG="--use-btree"
    BTREE_MODE="ENABLED (B+tree klog)"
else
    BTREE_FLAG=""
    BTREE_MODE="DISABLED (block-based klog)"
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
log "TidesDB B+tree: $BTREE_MODE"
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
    log "Running TidesDB..."
    $BENCH -e tidesdb $bench_args $SYNC_FLAG $BTREE_FLAG -d "$DB_PATH" --block-cache-size "$BLOCK_CACHE_SIZE" --test-name "$test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    cleanup_db || exit 1
    log "Running RocksDB..."
    $BENCH -e rocksdb $bench_args $SYNC_FLAG -d "$DB_PATH" --block-cache-size "$BLOCK_CACHE_SIZE" --test-name "$test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

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
    $BENCH -e tidesdb $populate_args $SYNC_FLAG $BTREE_FLAG -d "$DB_PATH" --block-cache-size "$BLOCK_CACHE_SIZE" --test-name "$populate_test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    log "Running TidesDB read test..."
    $BENCH -e tidesdb $read_args $SYNC_FLAG $BTREE_FLAG -d "$DB_PATH" --block-cache-size "$BLOCK_CACHE_SIZE" --test-name "$test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    cleanup_db || exit 1
    log "Populating RocksDB for read test..."
    $BENCH -e rocksdb $populate_args $SYNC_FLAG -d "$DB_PATH" --block-cache-size "$BLOCK_CACHE_SIZE" --test-name "$populate_test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    log "Running RocksDB read test..."
    $BENCH -e rocksdb $read_args $SYNC_FLAG -d "$DB_PATH" --block-cache-size "$BLOCK_CACHE_SIZE" --test-name "$test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

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
    $BENCH -e tidesdb $write_args $SYNC_FLAG $BTREE_FLAG -d "$DB_PATH" --block-cache-size "$BLOCK_CACHE_SIZE" --test-name "$populate_test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    log "Running TidesDB delete test..."
    $BENCH -e tidesdb $delete_args $SYNC_FLAG $BTREE_FLAG -d "$DB_PATH" --block-cache-size "$BLOCK_CACHE_SIZE" --test-name "$test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    cleanup_db || exit 1
    log "Populating RocksDB for delete test..."
    $BENCH -e rocksdb $write_args $SYNC_FLAG -d "$DB_PATH" --block-cache-size "$BLOCK_CACHE_SIZE" --test-name "$populate_test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    log "Running RocksDB delete test..."
    $BENCH -e rocksdb $delete_args $SYNC_FLAG -d "$DB_PATH" --block-cache-size "$BLOCK_CACHE_SIZE" --test-name "$test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

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
    $BENCH -e tidesdb $populate_args $SYNC_FLAG $BTREE_FLAG -d "$DB_PATH" --block-cache-size "$BLOCK_CACHE_SIZE" --test-name "$populate_test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    log "Running TidesDB seek test..."
    $BENCH -e tidesdb $seek_args $SYNC_FLAG $BTREE_FLAG -d "$DB_PATH" --block-cache-size "$BLOCK_CACHE_SIZE" --test-name "$test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    cleanup_db || exit 1
    log "Populating RocksDB for seek test..."
    $BENCH -e rocksdb $populate_args $SYNC_FLAG -d "$DB_PATH" --block-cache-size "$BLOCK_CACHE_SIZE" --test-name "$populate_test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    log "Running RocksDB seek test..."
    $BENCH -e rocksdb $seek_args $SYNC_FLAG -d "$DB_PATH" --block-cache-size "$BLOCK_CACHE_SIZE" --test-name "$test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    cleanup_db || exit 1
    log ""
}

run_range_comparison() {
    local test_id="$1"
    local test_name="$2"
    shift 2
    local range_args="$@"
    # Strip -w range and --range-size from args for populate phase
    local write_args="${range_args/-w range/-w write}"
    write_args=$(echo "$write_args" | sed 's/--range-size [0-9]*//g')

    local populate_args="$write_args -b $DEFAULT_BATCH_SIZE"
    local populate_test_id="${test_id}_populate"

    log ""
    log "*------------------------------------------*"
    log "TEST: $test_name"
    log "*------------------------------------------*"

    cleanup_db || exit 1
    log "Populating TidesDB for range test..."
    $BENCH -e tidesdb $populate_args $SYNC_FLAG $BTREE_FLAG -d "$DB_PATH" --block-cache-size "$BLOCK_CACHE_SIZE" --test-name "$populate_test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    log "Running TidesDB range test..."
    $BENCH -e tidesdb $range_args $SYNC_FLAG $BTREE_FLAG -d "$DB_PATH" --block-cache-size "$BLOCK_CACHE_SIZE" --test-name "$test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    cleanup_db || exit 1
    log "Populating RocksDB for range test..."
    $BENCH -e rocksdb $populate_args $SYNC_FLAG -d "$DB_PATH" --block-cache-size "$BLOCK_CACHE_SIZE" --test-name "$populate_test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    log "Running RocksDB range test..."
    $BENCH -e rocksdb $range_args $SYNC_FLAG -d "$DB_PATH" --block-cache-size "$BLOCK_CACHE_SIZE" --test-name "$test_id" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

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

log ""
log "=========================================="
log "LARGE SCALE TESTS (64MB cache, 16 threads, 4x ops)"
log "=========================================="
log ""

BLOCK_CACHE_SIZE=67108864

log "### 13. Sequential Write Performance (Large Scale) ###"
run_comparison "write_seq_40M_t16_b${DEFAULT_BATCH_SIZE}" "Sequential Write (40M ops, 16 threads, batch=$DEFAULT_BATCH_SIZE)" \
    -w write -p seq -o 40000000 -t 16 -b $DEFAULT_BATCH_SIZE

log "### 14. Random Write Performance (Large Scale) ###"
run_comparison "write_random_40M_t16_b${DEFAULT_BATCH_SIZE}" "Random Write (40M ops, 16 threads, batch=$DEFAULT_BATCH_SIZE)" \
    -w write -p random -o 40000000 -t 16 -b $DEFAULT_BATCH_SIZE

log "### 15. Random Read Performance (Large Scale) ###"
run_read_comparison "read_random_40M_t16" "Random Read (40M ops, 16 threads)" \
    -w read -p random -o 40000000 -t 16

log "### 16. Mixed Workload (Large Scale) ###"
run_comparison "mixed_random_20M_t16_b${DEFAULT_BATCH_SIZE}" "Mixed Workload (20M ops, 16 threads, batch=$DEFAULT_BATCH_SIZE)" \
    -w mixed -p random -o 20000000 -t 16 -b $DEFAULT_BATCH_SIZE

log "### 17. Hot Key Workload (Large Scale) ###"
run_comparison "write_zipfian_20M_t16_b${DEFAULT_BATCH_SIZE}" "Zipfian Write (20M ops, 16 threads, batch=$DEFAULT_BATCH_SIZE)" \
    -w write -p zipfian -o 20000000 -t 16 -b $DEFAULT_BATCH_SIZE

run_comparison "mixed_zipfian_20M_t16_b${DEFAULT_BATCH_SIZE}" "Zipfian Mixed (20M ops, 16 threads, batch=$DEFAULT_BATCH_SIZE)" \
    -w mixed -p zipfian -o 20000000 -t 16 -b $DEFAULT_BATCH_SIZE

log "### 18. Delete Performance (Large Scale) ###"
run_delete_comparison "delete_random_20M_t16_b${DEFAULT_BATCH_SIZE}" "Random Delete (20M ops, 16 threads, batch=$DEFAULT_BATCH_SIZE)" \
    -w delete -p random -o 20000000 -t 16 -b $DEFAULT_BATCH_SIZE

log "### 19. Large Value Performance (Large Scale) ###"
run_comparison "write_large_values_4M_k256_v4096_t16_b${DEFAULT_BATCH_SIZE}" "Large Values (4M ops, 256B key, 4KB value, batch=$DEFAULT_BATCH_SIZE)" \
    -w write -p random -k 256 -v 4096 -o 4000000 -t 16 -b $DEFAULT_BATCH_SIZE

log "### 20. Small Value Performance (Large Scale) ###"
run_comparison "write_small_values_200M_k16_v64_t16_b${DEFAULT_BATCH_SIZE}" "Small Values (200M ops, 16B key, 64B value, batch=$DEFAULT_BATCH_SIZE)" \
    -w write -p random -k 16 -v 64 -o 200000000 -t 16 -b $DEFAULT_BATCH_SIZE

log "### 21. Batch Size Comparison (Large Scale) ###"
run_comparison "batch_1_40M_t16" "Batch Size 1 (no batching, 40M ops, 16 threads)" \
    -w write -p random -o 40000000 -t 16 -b 1

run_comparison "batch_100_40M_t16" "Batch Size 100 (40M ops, 16 threads)" \
    -w write -p random -o 40000000 -t 16 -b 100

run_comparison "batch_1000_40M_t16" "Batch Size 1000 (40M ops, 16 threads)" \
    -w write -p random -o 40000000 -t 16 -b 1000

log "### 22. Delete Batch Scaling (Large Scale) ###"
run_delete_comparison "delete_batch_1_20M_t16" "Delete Batch=1 (20M ops, 16 threads)" \
    -w delete -p random -o 20000000 -t 16 -b 1

run_delete_comparison "delete_batch_1000_20M_t16" "Delete Batch=1000 (20M ops, 16 threads)" \
    -w delete -p random -o 20000000 -t 16 -b 1000

log "### 23. Seek Performance (Large Scale) ###"
run_seek_comparison "seek_random_20M_t16" "Random Seek (20M ops, 16 threads)" \
    -w seek -p random -o 20000000 -t 16

run_seek_comparison "seek_seq_20M_t16" "Sequential Seek (20M ops, 16 threads)" \
    -w seek -p seq -o 20000000 -t 16

run_seek_comparison "seek_zipfian_20M_t16" "Zipfian Seek (20M ops, 16 threads)" \
    -w seek -p zipfian -o 20000000 -t 16

log "### 24. Range Query Performance (Large Scale) ###"
run_range_comparison "range_random_100_4M_t16" "Range Scan 100 keys (4M ops, 16 threads)" \
    -w range -p random -o 4000000 -t 16 --range-size 100

run_range_comparison "range_random_1000_2M_t16" "Range Scan 1000 keys (2M ops, 16 threads)" \
    -w range -p random -o 2000000 -t 16 --range-size 1000

run_range_comparison "range_seq_100_4M_t16" "Sequential Range Scan 100 keys (4M ops, 16 threads)" \
    -w range -p seq -o 4000000 -t 16 --range-size 100

log ""
log "=========================================="
log "DURABILITY TESTS (sync enabled, 64MB cache)"
log "=========================================="
log ""

log "### 25. Synced Write Scaling ###"
log "Testing durable writes with sync enabled, scaling ops and threads"

run_comparison "sync_write_random_25K_t1_b${DEFAULT_BATCH_SIZE}" "Synced Random Write (25K ops, 1 thread, batch=$DEFAULT_BATCH_SIZE)" \
    -w write -p random -o 25000 -t 1 -b $DEFAULT_BATCH_SIZE --sync

run_comparison "sync_write_random_50K_t4_b${DEFAULT_BATCH_SIZE}" "Synced Random Write (50K ops, 4 threads, batch=$DEFAULT_BATCH_SIZE)" \
    -w write -p random -o 50000 -t 4 -b $DEFAULT_BATCH_SIZE --sync

run_comparison "sync_write_random_100K_t8_b${DEFAULT_BATCH_SIZE}" "Synced Random Write (100K ops, 8 threads, batch=$DEFAULT_BATCH_SIZE)" \
    -w write -p random -o 100000 -t 8 -b $DEFAULT_BATCH_SIZE --sync

run_comparison "sync_write_random_500K_t16_b${DEFAULT_BATCH_SIZE}" "Synced Random Write (500K ops, 16 threads, batch=$DEFAULT_BATCH_SIZE)" \
    -w write -p random -o 500000 -t 16 -b $DEFAULT_BATCH_SIZE --sync

cleanup_db

log ""
log "*------------------------------------------*"
log "RUNNER Complete"
log ""
log "Results:"
log "  Text: $RESULTS"
log "  CSV:  $CSV_FILE"
log "*------------------------------------------*"
