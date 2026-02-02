#!/bin/bash

set -e

BENCH="./build/benchtool"
DB_PATH="${BENCHTOOL_DB_PATH:-db-bench}"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULTS="tidesdb_btree_comparison_${TIMESTAMP}.txt"
CSV_FILE="tidesdb_btree_comparison_${TIMESTAMP}.csv"

DEFAULT_BATCH_SIZE=1000
DEFAULT_THREADS=4
DEFAULT_KEYS=10000000

# Parse command line arguments
show_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -k, --keys <count>    Number of keys to benchmark (default: 10000000)"
    echo "  -t, --threads <n>     Number of threads (default: 4)"
    echo "  -h, --help            Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                    # Run with 10M keys"
    echo "  $0 -k 25000000        # Run with 25M keys"
    echo "  $0 --keys 50000000    # Run with 50M keys"
    exit 0
}

while [[ $# -gt 0 ]]; do
    case $1 in
        -k|--keys)
            DEFAULT_KEYS="$2"
            shift 2
            ;;
        -t|--threads)
            DEFAULT_THREADS="$2"
            shift 2
            ;;
        -h|--help)
            show_usage
            ;;
        *)
            echo "Unknown option: $1"
            show_usage
            ;;
    esac
done

# Calculate range ops (10% of keys)
RANGE_OPS=$((DEFAULT_KEYS / 10))

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
log "RUNNER: TidesDB B+tree vs Block-based Comparison"
log "Date: $(date)"
log "Parameters:"
log "  Keys: $DEFAULT_KEYS"
log "  Batch Size: $DEFAULT_BATCH_SIZE"
log "  Threads: $DEFAULT_THREADS"
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

# Run write comparison: block-based vs btree
run_write_comparison() {
    local test_id="$1"
    local test_name="$2"
    shift 2
    local bench_args="$@"

    log ""
    log "*------------------------------------------*"
    log "TEST: $test_name"
    log "*------------------------------------------*"

    # Block-based (default)
    cleanup_db || exit 1
    log "Running TidesDB (block-based)..."
    $BENCH -e tidesdb $bench_args -d "$DB_PATH" --test-name "${test_id}_block" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    # B+tree
    cleanup_db || exit 1
    log "Running TidesDB (B+tree)..."
    $BENCH -e tidesdb --use-btree $bench_args -d "$DB_PATH" --test-name "${test_id}_btree" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    cleanup_db || exit 1
    log ""
}

# Run read comparison: populate then read for both formats
run_read_comparison() {
    local test_id="$1"
    local test_name="$2"
    shift 2
    local read_args="$@"
    local write_args="${read_args/-w read/-w write}"
    local populate_args="$write_args -b $DEFAULT_BATCH_SIZE"

    log ""
    log "*------------------------------------------*"
    log "TEST: $test_name"
    log "*------------------------------------------*"

    # Block-based
    cleanup_db || exit 1
    log "Populating TidesDB (block-based) for read test..."
    $BENCH -e tidesdb $populate_args -d "$DB_PATH" --test-name "${test_id}_block_populate" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    log "Running TidesDB (block-based) read test..."
    $BENCH -e tidesdb $read_args -d "$DB_PATH" --test-name "${test_id}_block" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    # B+tree
    cleanup_db || exit 1
    log "Populating TidesDB (B+tree) for read test..."
    $BENCH -e tidesdb --use-btree $populate_args -d "$DB_PATH" --test-name "${test_id}_btree_populate" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    log "Running TidesDB (B+tree) read test..."
    $BENCH -e tidesdb --use-btree $read_args -d "$DB_PATH" --test-name "${test_id}_btree" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    cleanup_db || exit 1
    log ""
}

# Run seek comparison
run_seek_comparison() {
    local test_id="$1"
    local test_name="$2"
    shift 2
    local seek_args="$@"
    local write_args="${seek_args/-w seek/-w write}"
    local populate_args="$write_args -b $DEFAULT_BATCH_SIZE"

    log ""
    log "*------------------------------------------*"
    log "TEST: $test_name"
    log "*------------------------------------------*"

    # Block-based
    cleanup_db || exit 1
    log "Populating TidesDB (block-based) for seek test..."
    $BENCH -e tidesdb $populate_args -d "$DB_PATH" --test-name "${test_id}_block_populate" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    log "Running TidesDB (block-based) seek test..."
    $BENCH -e tidesdb $seek_args -d "$DB_PATH" --test-name "${test_id}_block" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    # B+tree
    cleanup_db || exit 1
    log "Populating TidesDB (B+tree) for seek test..."
    $BENCH -e tidesdb --use-btree $populate_args -d "$DB_PATH" --test-name "${test_id}_btree_populate" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    log "Running TidesDB (B+tree) seek test..."
    $BENCH -e tidesdb --use-btree $seek_args -d "$DB_PATH" --test-name "${test_id}_btree" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    cleanup_db || exit 1
    log ""
}

# Run range comparison
run_range_comparison() {
    local test_id="$1"
    local test_name="$2"
    shift 2
    local range_args="$@"
    local write_args="${range_args/-w range/-w write}"
    local populate_args="$write_args -b $DEFAULT_BATCH_SIZE"

    log ""
    log "*------------------------------------------*"
    log "TEST: $test_name"
    log "*------------------------------------------*"

    # Block-based
    cleanup_db || exit 1
    log "Populating TidesDB (block-based) for range test..."
    $BENCH -e tidesdb $populate_args -d "$DB_PATH" --test-name "${test_id}_block_populate" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    log "Running TidesDB (block-based) range test..."
    $BENCH -e tidesdb $range_args -d "$DB_PATH" --test-name "${test_id}_block" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    # B+tree
    cleanup_db || exit 1
    log "Populating TidesDB (B+tree) for range test..."
    $BENCH -e tidesdb --use-btree $populate_args -d "$DB_PATH" --test-name "${test_id}_btree_populate" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    log "Running TidesDB (B+tree) range test..."
    $BENCH -e tidesdb --use-btree $range_args -d "$DB_PATH" --test-name "${test_id}_btree" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    cleanup_db || exit 1
    log ""
}

# Run mixed workload comparison
run_mixed_comparison() {
    local test_id="$1"
    local test_name="$2"
    shift 2
    local bench_args="$@"

    log ""
    log "*------------------------------------------*"
    log "TEST: $test_name"
    log "*------------------------------------------*"

    # Block-based
    cleanup_db || exit 1
    log "Running TidesDB (block-based) mixed workload..."
    $BENCH -e tidesdb $bench_args -d "$DB_PATH" --test-name "${test_id}_block" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    # B+tree
    cleanup_db || exit 1
    log "Running TidesDB (B+tree) mixed workload..."
    $BENCH -e tidesdb --use-btree $bench_args -d "$DB_PATH" --test-name "${test_id}_btree" --csv "$CSV_FILE" 2>&1 | tee -a "$RESULTS"

    cleanup_db || exit 1
    log ""
}

log "=============================================="
log "=== PHASE 1: 10 Million Keys ==="
log "=============================================="

log "### 1.1 Sequential Write (10M) ###"
run_write_comparison "write_seq_10M" "Sequential Write (10M keys, 8 threads, batch=$DEFAULT_BATCH_SIZE)" \
    -w write -p seq -o 10000000 -t $DEFAULT_THREADS -b $DEFAULT_BATCH_SIZE

log "### 1.2 Random Write (10M) ###"
run_write_comparison "write_random_10M" "Random Write (10M keys, 8 threads, batch=$DEFAULT_BATCH_SIZE)" \
    -w write -p random -o 10000000 -t $DEFAULT_THREADS -b $DEFAULT_BATCH_SIZE

log "### 1.3 Random Read (10M) ###"
run_read_comparison "read_random_10M" "Random Read (10M keys, 8 threads)" \
    -w read -p random -o 10000000 -t $DEFAULT_THREADS

log "### 1.4 Random Seek (10M) ###"
run_seek_comparison "seek_random_10M" "Random Seek (10M keys, 8 threads)" \
    -w seek -p random -o 10000000 -t $DEFAULT_THREADS

log "### 1.5 Mixed Workload (10M) ###"
run_mixed_comparison "mixed_random_10M" "Mixed Workload (10M ops, 8 threads, batch=$DEFAULT_BATCH_SIZE)" \
    -w mixed -p random -o 10000000 -t $DEFAULT_THREADS -b $DEFAULT_BATCH_SIZE

log "### 1.6 Range Query (10M population, 1M range ops) ###"
run_range_comparison "range_random_10M" "Range Scan 100 keys (10M population, 1M range ops)" \
    -w range -p random -o 1000000 -t $DEFAULT_THREADS --range-size 100

log "=============================================="
log "=== PHASE 2: 25 Million Keys ==="
log "=============================================="

log "### 2.1 Sequential Write (25M) ###"
run_write_comparison "write_seq_25M" "Sequential Write (25M keys, 8 threads, batch=$DEFAULT_BATCH_SIZE)" \
    -w write -p seq -o 25000000 -t $DEFAULT_THREADS -b $DEFAULT_BATCH_SIZE

log "### 2.2 Random Write (25M) ###"
run_write_comparison "write_random_25M" "Random Write (25M keys, 8 threads, batch=$DEFAULT_BATCH_SIZE)" \
    -w write -p random -o 25000000 -t $DEFAULT_THREADS -b $DEFAULT_BATCH_SIZE

log "### 2.3 Random Read (25M) ###"
run_read_comparison "read_random_25M" "Random Read (25M keys, 8 threads)" \
    -w read -p random -o 25000000 -t $DEFAULT_THREADS

log "### 2.4 Random Seek (25M) ###"
run_seek_comparison "seek_random_25M" "Random Seek (25M keys, 8 threads)" \
    -w seek -p random -o 25000000 -t $DEFAULT_THREADS

log "### 2.5 Mixed Workload (25M) ###"
run_mixed_comparison "mixed_random_25M" "Mixed Workload (25M ops, 8 threads, batch=$DEFAULT_BATCH_SIZE)" \
    -w mixed -p random -o 25000000 -t $DEFAULT_THREADS -b $DEFAULT_BATCH_SIZE

log "### 2.6 Range Query (25M population, 2.5M range ops) ###"
run_range_comparison "range_random_25M" "Range Scan 100 keys (25M population, 2.5M range ops)" \
    -w range -p random -o 2500000 -t $DEFAULT_THREADS --range-size 100

log "=============================================="
log "=== PHASE 3: 50 Million Keys ==="
log "=============================================="

log "### 3.1 Sequential Write (50M) ###"
run_write_comparison "write_seq_50M" "Sequential Write (50M keys, 8 threads, batch=$DEFAULT_BATCH_SIZE)" \
    -w write -p seq -o 50000000 -t $DEFAULT_THREADS -b $DEFAULT_BATCH_SIZE

log "### 3.2 Random Write (50M) ###"
run_write_comparison "write_random_50M" "Random Write (50M keys, 8 threads, batch=$DEFAULT_BATCH_SIZE)" \
    -w write -p random -o 50000000 -t $DEFAULT_THREADS -b $DEFAULT_BATCH_SIZE

log "### 3.3 Random Read (50M) ###"
run_read_comparison "read_random_50M" "Random Read (50M keys, 8 threads)" \
    -w read -p random -o 50000000 -t $DEFAULT_THREADS

log "### 3.4 Random Seek (50M) ###"
run_seek_comparison "seek_random_50M" "Random Seek (50M keys, 8 threads)" \
    -w seek -p random -o 50000000 -t $DEFAULT_THREADS

log "### 3.5 Mixed Workload (50M) ###"
run_mixed_comparison "mixed_random_50M" "Mixed Workload (50M ops, 8 threads, batch=$DEFAULT_BATCH_SIZE)" \
    -w mixed -p random -o 50000000 -t $DEFAULT_THREADS -b $DEFAULT_BATCH_SIZE

log "### 3.6 Range Query (50M population, 5M range ops) ###"
run_range_comparison "range_random_50M" "Range Scan 100 keys (50M population, 5M range ops)" \
    -w range -p random -o 5000000 -t $DEFAULT_THREADS --range-size 100

log "=============================================="
log "=== PHASE 4: Zipfian (Hot Keys) Comparison ==="
log "=============================================="

log "### 4.1 Zipfian Write (25M) ###"
run_write_comparison "write_zipfian_25M" "Zipfian Write (25M keys, 8 threads, batch=$DEFAULT_BATCH_SIZE)" \
    -w write -p zipfian -o 25000000 -t $DEFAULT_THREADS -b $DEFAULT_BATCH_SIZE

log "### 4.2 Zipfian Read (25M) ###"
run_read_comparison "read_zipfian_25M" "Zipfian Read (25M keys, 8 threads)" \
    -w read -p zipfian -o 25000000 -t $DEFAULT_THREADS

log "### 4.3 Zipfian Seek (25M) ###"
run_seek_comparison "seek_zipfian_25M" "Zipfian Seek (25M keys, 8 threads)" \
    -w seek -p zipfian -o 25000000 -t $DEFAULT_THREADS

cleanup_db

log ""
log "*------------------------------------------*"
log "RUNNER Complete"
log ""
log "Results:"
log "  Text: $RESULTS"
log "  CSV:  $CSV_FILE"
log "*------------------------------------------*"
