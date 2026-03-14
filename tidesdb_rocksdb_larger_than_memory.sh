#!/bin/bash
#
# TidesDB & RocksDB - Larger-Than-Memory Benchmark
# ===================================================
#
# DESCRIPTION:
#   Benchmarks TidesDB and RocksDB with a dataset that exceeds physical RAM.
#   The script auto-detects system memory and scales the dataset size using a
#   configurable multiplier (default 2x).  The goal is to stress the engines
#   under realistic conditions where the working set cannot fit in the OS page
#   cache or the engine's block cache.
#
# STAGES (per engine):
#   1. INGEST     Bulk-load data as fast as possible (sequential writes, large
#                 batches, maximum threads).  Tagged *_ingest in CSV.
#   2. WARMUP     Random reads across the full keyspace to prime the block
#                 cache and let the engine settle (compactions, etc.).
#                 Tagged *_warmup in CSV.
#   3. ANALYSIS   General-purpose workloads over the populated database:
#                 random reads, sequential reads, seeks (random/seq/zipfian),
#                 range scans, mixed read-write, and deletes.
#                 Each test is tagged individually in the CSV.
#
# KEY SIZING:
#   key=16B  value=100B    ~116 bytes logical per record.
#   The script computes the number of operations needed to reach
#   MULTIPLIER × system_memory bytes of logical data.
#
# USAGE:
#   ./tidesdb_rocksdb_larger_than_memory.sh                     # 2x RAM (default)
#   ./tidesdb_rocksdb_larger_than_memory.sh --multiplier 4      # 4x RAM
#   ./tidesdb_rocksdb_larger_than_memory.sh --multiplier 1.5    # 1.5x RAM
#   ./tidesdb_rocksdb_larger_than_memory.sh --multiplier 8 --use-btree --threads 32
#
# OPTIONS:
#   --multiplier <n>   Dataset size as multiple of system RAM (default: 2)
#   --threads <n>      Thread count for all stages (default: nproc)
#   --batch-size <n>   Batch size for ingest stage (default: 10000)
#   --key-size <n>     Key size in bytes (default: 16)
#   --value-size <n>   Value size in bytes (default: 100)
#   --use-btree        Enable B+tree klog format (TidesDB only)
#   --warmup-pct <n>   Warmup reads as % of total keys (default: 10)
#   --analysis-pct <n> Analysis ops as % of total keys (default: 10)
#   --cache-ratio <n>  Block cache as fraction of system RAM (default: 0.25)
#   --memtable <bytes> Memtable / write-buffer size (default: 134217728 = 128MB)
#   --db-path <path>   Database directory (default: ./bench_db)
#   --skip-ingest      Skip ingest, assume DB already populated (for re-runs)
#

set -e


# Defaults

BENCH="./build/benchtool"
DB_PATH="${BENCHTOOL_DB_PATH:-db-bench}"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULTS="ltm_benchmark_results_${TIMESTAMP}.txt"
CSV_FILE="ltm_benchmark_results_${TIMESTAMP}.csv"

MULTIPLIER="2"
THREADS=$(nproc)
INGEST_BATCH=10000
KEY_SIZE=16
VALUE_SIZE=100
USE_BTREE="false"
WARMUP_PCT=10
ANALYSIS_PCT=10
CACHE_RATIO="0.25"
MEMTABLE_SIZE=134217728   # 128 MB
SKIP_INGEST="false"


# Argument parsing

while [[ $# -gt 0 ]]; do
    case $1 in
        --multiplier)    MULTIPLIER="$2";     shift 2 ;;
        --threads)       THREADS="$2";        shift 2 ;;
        --batch-size)    INGEST_BATCH="$2";   shift 2 ;;
        --key-size)      KEY_SIZE="$2";       shift 2 ;;
        --value-size)    VALUE_SIZE="$2";     shift 2 ;;
        --use-btree)     USE_BTREE="true";    shift   ;;
        --warmup-pct)    WARMUP_PCT="$2";     shift 2 ;;
        --analysis-pct)  ANALYSIS_PCT="$2";   shift 2 ;;
        --cache-ratio)   CACHE_RATIO="$2";    shift 2 ;;
        --memtable)      MEMTABLE_SIZE="$2";  shift 2 ;;
        --db-path)       DB_PATH="$2";        shift 2 ;;
        --skip-ingest)   SKIP_INGEST="true";  shift   ;;
        -h|--help)
            sed -n '2,/^$/p' "$0" | sed 's/^#//; s/^ //'
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Run with --help for usage."
            exit 1
            ;;
    esac
done


# Pre-flight checks

if [ ! -f "$BENCH" ]; then
    echo "Error: benchtool not found at $BENCH"
    echo "Build first: mkdir -p build && cd build && cmake .. && make"
    exit 1
fi


# Detect system memory (bytes) - works on Linux and macOS

if [ -f /proc/meminfo ]; then
    SYSTEM_MEM_KB=$(awk '/^MemTotal:/ {print $2}' /proc/meminfo)
    SYSTEM_MEM_BYTES=$((SYSTEM_MEM_KB * 1024))
elif command -v sysctl &>/dev/null; then
    SYSTEM_MEM_BYTES=$(sysctl -n hw.memsize 2>/dev/null || echo 0)
else
    echo "Error: Cannot detect system memory."
    exit 1
fi


# Compute dataset size and operation counts

RECORD_SIZE=$((KEY_SIZE + VALUE_SIZE))

# Use awk for float math so multipliers like 1.5 work
DATASET_BYTES=$(awk "BEGIN {printf \"%.0f\", $SYSTEM_MEM_BYTES * $MULTIPLIER}")
TOTAL_KEYS=$(awk "BEGIN {printf \"%.0f\", $DATASET_BYTES / $RECORD_SIZE}")
WARMUP_OPS=$(awk "BEGIN {printf \"%.0f\", $TOTAL_KEYS * $WARMUP_PCT / 100}")
ANALYSIS_OPS=$(awk "BEGIN {printf \"%.0f\", $TOTAL_KEYS * $ANALYSIS_PCT / 100}")

# Block cache = CACHE_RATIO × system memory
BLOCK_CACHE_SIZE=$(awk "BEGIN {printf \"%.0f\", $SYSTEM_MEM_BYTES * $CACHE_RATIO}")

# Handy human-readable sizes
fmt_bytes() {
    local b=$1
    if   (( b >= 1099511627776 )); then awk "BEGIN {printf \"%.2f TB\", $b/1099511627776}"
    elif (( b >= 1073741824 ));    then awk "BEGIN {printf \"%.2f GB\", $b/1073741824}"
    elif (( b >= 1048576 ));       then awk "BEGIN {printf \"%.2f MB\", $b/1048576}"
    else                                awk "BEGIN {printf \"%.2f KB\", $b/1024}"
    fi
}

BTREE_FLAG=""
BTREE_MODE="DISABLED (block-based klog)"
if [ "$USE_BTREE" = "true" ]; then
    BTREE_FLAG="--use-btree"
    BTREE_MODE="ENABLED (B+tree klog)"
fi


# Logging

> "$RESULTS"
> "$CSV_FILE"

log() {
    echo "$1" | tee -a "$RESULTS"
}

FS_TYPE=$(df -T "$DB_PATH" 2>/dev/null | awk 'NR==2 {print $2}' || echo "unknown")
FS_TYPE=${FS_TYPE:-unknown}
DISK_AVAIL=$(df -h "$DB_PATH" 2>/dev/null | awk 'NR==2 {print $4}' || echo "unknown")

log "*------------------------------------------*"
log "RUNNER: Larger-Than-Memory Benchmark"
log "Date: $(date)"
log ""
log "System:"
log "  Hostname:    $(hostname)"
log "  Kernel:      $(uname -r)"
log "  CPU:         $(grep 'model name' /proc/cpuinfo 2>/dev/null | head -1 | cut -d: -f2 | xargs || sysctl -n machdep.cpu.brand_string 2>/dev/null || echo 'unknown')"
log "  CPU Cores:   $(nproc)"
log "  System RAM:  $(fmt_bytes $SYSTEM_MEM_BYTES)  ($SYSTEM_MEM_BYTES bytes)"
log "  Filesystem:  $FS_TYPE"
log "  Disk Avail:  $DISK_AVAIL"
log ""
log "Parameters:"
log "  Multiplier:     ${MULTIPLIER}x system RAM"
log "  Dataset Size:   $(fmt_bytes $DATASET_BYTES)"
log "  Record Size:    ${RECORD_SIZE}B  (key=${KEY_SIZE}B + value=${VALUE_SIZE}B)"
log "  Total Keys:     $(printf "%'d" $TOTAL_KEYS)"
log "  Ingest Batch:   $INGEST_BATCH"
log "  Threads:        $THREADS"
log "  Warmup Ops:     $(printf "%'d" $WARMUP_OPS)  (${WARMUP_PCT}% of keys)"
log "  Analysis Ops:   $(printf "%'d" $ANALYSIS_OPS)  (${ANALYSIS_PCT}% of keys)"
log "  Block Cache:    $(fmt_bytes $BLOCK_CACHE_SIZE)  (${CACHE_RATIO} × RAM)"
log "  Memtable:       $(fmt_bytes $MEMTABLE_SIZE)"
log "  B+tree klog:    $BTREE_MODE"
log "  Skip Ingest:    $SKIP_INGEST"
log ""
log "Output:"
log "  Text: $RESULTS"
log "  CSV:  $CSV_FILE"
log "*------------------------------------------*"
log ""


# Helpers

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

# Common flags for every benchtool invocation
common_flags() {
    local engine="$1"
    local extra_flags="-d $DB_PATH --block-cache-size $BLOCK_CACHE_SIZE --memtable-size $MEMTABLE_SIZE -k $KEY_SIZE -v $VALUE_SIZE"
    if [ "$engine" = "tidesdb" ] && [ -n "$BTREE_FLAG" ]; then
        extra_flags="$extra_flags $BTREE_FLAG"
    fi
    echo "$extra_flags"
}

run_bench() {
    local engine="$1"
    local test_name="$2"
    shift 2
    local args="$@"

    log "  [$engine] $test_name ..."
    $BENCH -e "$engine" $(common_flags "$engine") --test-name "$test_name" --csv "$CSV_FILE" $args 2>&1 | tee -a "$RESULTS"
}

elapsed_since() {
    local start=$1
    local now=$(date +%s)
    local secs=$((now - start))
    printf "%02d:%02d:%02d" $((secs/3600)) $(((secs%3600)/60)) $((secs%60))
}


# Run the full pipeline for one engine

run_engine_pipeline() {
    local engine="$1"
    local ENGINE_START=$(date +%s)

    log ""
    log "=========================================="
    log "  ENGINE: $engine"
    log "=========================================="

    # STAGE 1 - Ingest 
    if [ "$SKIP_INGEST" = "false" ]; then
        cleanup_db || exit 1
        log ""
        log "--- STAGE 1: INGEST ($engine) ---"
        log "  Bulk-loading $(printf "%'d" $TOTAL_KEYS) keys sequentially (batch=$INGEST_BATCH, threads=$THREADS)"
        log ""

        local INGEST_START=$(date +%s)

        run_bench "$engine" "ltm_${MULTIPLIER}x_ingest" \
            -w write -p seq -o "$TOTAL_KEYS" -t "$THREADS" -b "$INGEST_BATCH"

        log "  Ingest elapsed: $(elapsed_since $INGEST_START)"
        log ""

        # Let compactions settle
        log "  Sleeping 10s for compaction quiescence..."
        sleep 10
    else
        log ""
        log "--- STAGE 1: INGEST (SKIPPED - --skip-ingest) ---"
        log ""
    fi

    # STAGE 2 - Warmup 
    log "--- STAGE 2: WARMUP ($engine) ---"
    log "  Random reads to prime block cache ($(printf "%'d" $WARMUP_OPS) ops)"
    log ""

    run_bench "$engine" "ltm_${MULTIPLIER}x_warmup" \
        -w read -p random -o "$WARMUP_OPS" -t "$THREADS"

    log ""

    # STAGE 3 - Analysis 
    log "--- STAGE 3: ANALYSIS ($engine) ---"
    log "  Running workloads over populated ${MULTIPLIER}x-RAM dataset"
    log ""

    # 3a. Random Read
    run_bench "$engine" "ltm_${MULTIPLIER}x_read_random" \
        -w read -p random -o "$ANALYSIS_OPS" -t "$THREADS"

    # 3b. Sequential Read
    run_bench "$engine" "ltm_${MULTIPLIER}x_read_seq" \
        -w read -p seq -o "$ANALYSIS_OPS" -t "$THREADS"

    # 3c. Zipfian Read (hot-key reads)
    run_bench "$engine" "ltm_${MULTIPLIER}x_read_zipfian" \
        -w read -p zipfian -o "$ANALYSIS_OPS" -t "$THREADS"

    # 3d. Random Seek
    run_bench "$engine" "ltm_${MULTIPLIER}x_seek_random" \
        -w seek -p random -o "$ANALYSIS_OPS" -t "$THREADS"

    # 3e. Sequential Seek
    run_bench "$engine" "ltm_${MULTIPLIER}x_seek_seq" \
        -w seek -p seq -o "$ANALYSIS_OPS" -t "$THREADS"

    # 3f. Zipfian Seek
    run_bench "$engine" "ltm_${MULTIPLIER}x_seek_zipfian" \
        -w seek -p zipfian -o "$ANALYSIS_OPS" -t "$THREADS"

    # 3g. Range Scan - 100 keys
    run_bench "$engine" "ltm_${MULTIPLIER}x_range_100" \
        -w range -p random -o "$ANALYSIS_OPS" -t "$THREADS" --range-size 100

    # 3h. Range Scan - 1000 keys
    local RANGE_1K_OPS=$(awk "BEGIN {printf \"%.0f\", $ANALYSIS_OPS / 2}")
    run_bench "$engine" "ltm_${MULTIPLIER}x_range_1000" \
        -w range -p random -o "$RANGE_1K_OPS" -t "$THREADS" --range-size 1000

    # 3i. Mixed Workload (50/50 read/write)
    run_bench "$engine" "ltm_${MULTIPLIER}x_mixed_random" \
        -w mixed -p random -o "$ANALYSIS_OPS" -t "$THREADS" -b 1000

    # 3j. Mixed Workload - Zipfian
    run_bench "$engine" "ltm_${MULTIPLIER}x_mixed_zipfian" \
        -w mixed -p zipfian -o "$ANALYSIS_OPS" -t "$THREADS" -b 1000

    # 3k. Random Write (append into existing larger-than-memory dataset)
    local WRITE_EXTRA_OPS=$(awk "BEGIN {printf \"%.0f\", $ANALYSIS_OPS / 2}")
    run_bench "$engine" "ltm_${MULTIPLIER}x_write_random_extra" \
        -w write -p random -o "$WRITE_EXTRA_OPS" -t "$THREADS" -b 1000

    # 3l. Delete
    run_bench "$engine" "ltm_${MULTIPLIER}x_delete_random" \
        -w delete -p random -o "$WRITE_EXTRA_OPS" -t "$THREADS" -b 1000

    log ""
    log "  $engine pipeline elapsed: $(elapsed_since $ENGINE_START)"
    log ""
}


# Disk space sanity check

# Estimate - dataset + write amplification overhead (~3x logical size is safe)
ESTIMATED_DISK=$(awk "BEGIN {printf \"%.0f\", $DATASET_BYTES * 3}")
AVAIL_BYTES=$(df --output=avail -B1 "$DB_PATH" 2>/dev/null | tail -1 || echo 0)
AVAIL_BYTES=$(echo "$AVAIL_BYTES" | tr -d ' ')

if [ "$AVAIL_BYTES" -gt 0 ] 2>/dev/null && [ "$ESTIMATED_DISK" -gt "$AVAIL_BYTES" ] 2>/dev/null; then
    log "WARNING: Estimated disk need $(fmt_bytes $ESTIMATED_DISK) exceeds"
    log "         available space $(fmt_bytes $AVAIL_BYTES)."
    log "         The benchmark may fail with ENOSPC."
    log ""
    read -p "Continue anyway? [y/N] " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi


# Main

GLOBAL_START=$(date +%s)

run_engine_pipeline "tidesdb"

# Clean up TidesDB's DB before running RocksDB
if [ "$SKIP_INGEST" = "false" ]; then
    cleanup_db || exit 1
fi

run_engine_pipeline "rocksdb"

cleanup_db

log ""
log "*------------------------------------------*"
log "RUNNER Complete"
log "  Total elapsed: $(elapsed_since $GLOBAL_START)"
log ""
log "Results:"
log "  Text: $RESULTS"
log "  CSV:  $CSV_FILE"
log ""
log "Plot with:"
log "  python3 graphgen.py $CSV_FILE"
log "*------------------------------------------*"