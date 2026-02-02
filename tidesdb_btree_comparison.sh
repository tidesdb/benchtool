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
log "=== B+tree vs Block-based: ${DEFAULT_KEYS} Keys ==="
log "=============================================="

log "### 1. Sequential Write ###"
run_write_comparison "write_seq" "Sequential Write (${DEFAULT_KEYS} keys, ${DEFAULT_THREADS} threads, batch=$DEFAULT_BATCH_SIZE)" \
    -w write -p seq -o $DEFAULT_KEYS -t $DEFAULT_THREADS -b $DEFAULT_BATCH_SIZE

log "### 2. Random Write ###"
run_write_comparison "write_random" "Random Write (${DEFAULT_KEYS} keys, ${DEFAULT_THREADS} threads, batch=$DEFAULT_BATCH_SIZE)" \
    -w write -p random -o $DEFAULT_KEYS -t $DEFAULT_THREADS -b $DEFAULT_BATCH_SIZE

log "### 3. Random Read ###"
run_read_comparison "read_random" "Random Read (${DEFAULT_KEYS} keys, ${DEFAULT_THREADS} threads)" \
    -w read -p random -o $DEFAULT_KEYS -t $DEFAULT_THREADS

log "### 4. Random Seek ###"
run_seek_comparison "seek_random" "Random Seek (${DEFAULT_KEYS} keys, ${DEFAULT_THREADS} threads)" \
    -w seek -p random -o $DEFAULT_KEYS -t $DEFAULT_THREADS

log "### 5. Mixed Workload ###"
run_mixed_comparison "mixed_random" "Mixed Workload (${DEFAULT_KEYS} ops, ${DEFAULT_THREADS} threads, batch=$DEFAULT_BATCH_SIZE)" \
    -w mixed -p random -o $DEFAULT_KEYS -t $DEFAULT_THREADS -b $DEFAULT_BATCH_SIZE

log "### 6. Range Query ###"
run_range_comparison "range_random" "Range Scan 100 keys (${DEFAULT_KEYS} population, ${RANGE_OPS} range ops)" \
    -w range -p random -o $RANGE_OPS -t $DEFAULT_THREADS --range-size 100

cleanup_db

log ""
log "*------------------------------------------*"
log "RUNNER Complete"
log ""
log "Results:"
log "  Text: $RESULTS"
log "  CSV:  $CSV_FILE"
log "*------------------------------------------*"
