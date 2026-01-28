#!/bin/bash

# TidesDB Allocator Benchmark Script
# Compares performance of different memory allocators (glibc, mimalloc, tcmalloc)
#
# This script is designed to stress memory allocation patterns that reveal
# allocator performance differences:
# - High allocation churn (many small allocations/frees)
# - Multi-threaded contention
# - Mixed workloads with varied allocation sizes
# - Large block allocations
#
# Prerequisites:
# - Build TidesDB with each allocator and install to separate prefixes, OR
# - Use LD_PRELOAD to inject allocators at runtime
#
# Usage: ./tidesdb_allocator_benchmark.sh [OPTIONS]
#   --allocator <name>    Run benchmark with specific allocator (glibc|mimalloc|tcmalloc|all)
#   --preload             Use LD_PRELOAD method instead of recompiled libraries
#   --quick               Run quick benchmark (fewer operations)
#   --full                Run full benchmark suite (more operations, longer)

set -e

BENCH="./build/benchtool"
DB_PATH="${BENCHTOOL_DB_PATH:-db-alloc-bench}"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULTS_DIR="allocator_benchmark_${TIMESTAMP}"
MASTER_LOG="${RESULTS_DIR}/benchmark_summary.txt"

# Per-allocator result files will be created dynamically:
# ${RESULTS_DIR}/${allocator}_results.txt
# ${RESULTS_DIR}/${allocator}_results.csv

# Default settings
ALLOCATOR="all"
USE_PRELOAD=false
BENCHMARK_MODE="standard"

# Operation counts for different modes
QUICK_OPS=1000000
STANDARD_OPS=5000000
FULL_OPS=20000000

# Thread counts to test (allocator contention is thread-sensitive)
THREAD_COUNTS="1 4 8 16"

# Batch sizes (affects allocation patterns)
BATCH_SIZES="1 100 1000"

parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --allocator)
                ALLOCATOR="$2"
                shift 2
                ;;
            --preload)
                USE_PRELOAD=true
                shift
                ;;
            --quick)
                BENCHMARK_MODE="quick"
                shift
                ;;
            --full)
                BENCHMARK_MODE="full"
                shift
                ;;
            --help|-h)
                echo "TidesDB Allocator Benchmark"
                echo ""
                echo "Usage: ./tidesdb_allocator_benchmark.sh [OPTIONS]"
                echo ""
                echo "Options:"
                echo "  --allocator <name>  Allocator to test: glibc, mimalloc, tcmalloc, or all (default: all)"
                echo "  --preload           Use LD_PRELOAD to inject allocators (requires .so files)"
                echo "  --quick             Quick benchmark (~1M ops per test)"
                echo "  --full              Full benchmark (~20M ops per test)"
                echo "  --help, -h          Show this help message"
                echo ""
                echo "Allocator Libraries (for --preload mode):"
                echo "  mimalloc: /usr/lib/x86_64-linux-gnu/libmimalloc.so"
                echo "  tcmalloc: /usr/lib/x86_64-linux-gnu/libtcmalloc.so"
                echo ""
                echo "Without --preload, the script expects TidesDB to be compiled with"
                echo "the appropriate allocator using install_tidesdb.sh --with-mimalloc"
                echo "or --with-tcmalloc."
                exit 0
                ;;
            *)
                echo "Unknown option: $1"
                exit 1
                ;;
        esac
    done
}

parse_args "$@"

# Set operation count based on mode
case $BENCHMARK_MODE in
    quick)
        NUM_OPS=$QUICK_OPS
        THREAD_COUNTS="1 8"
        BATCH_SIZES="1 1000"
        ;;
    full)
        NUM_OPS=$FULL_OPS
        ;;
    *)
        NUM_OPS=$STANDARD_OPS
        ;;
esac

# Verify benchtool exists
if [ ! -f "$BENCH" ]; then
    echo "Error: benchtool not found at $BENCH"
    echo "Please build first: mkdir -p build && cd build && cmake .. && make"
    exit 1
fi

# Create results directory
mkdir -p "$RESULTS_DIR"

# Initialize master log
> "$MASTER_LOG"

# Current allocator's result files (set per allocator)
CURRENT_RESULTS=""
CURRENT_CSV=""

# Initialize result files for an allocator
init_allocator_files() {
    local alloc="$1"
    CURRENT_RESULTS="${RESULTS_DIR}/${alloc}_results.txt"
    CURRENT_CSV="${RESULTS_DIR}/${alloc}_results.csv"
    > "$CURRENT_RESULTS"
    > "$CURRENT_CSV"
}

log() {
    echo "$1" | tee -a "$MASTER_LOG"
    # Also log to current allocator's results if set
    if [ -n "$CURRENT_RESULTS" ]; then
        echo "$1" >> "$CURRENT_RESULTS"
    fi
}

# Log only to master (for summary info)
log_master() {
    echo "$1" | tee -a "$MASTER_LOG"
}

cleanup_db() {
    if [ -d "$DB_PATH" ]; then
        rm -rf "$DB_PATH"
    fi
    sync
}

# Detect allocator libraries for preload mode
MIMALLOC_LIB=""
TCMALLOC_LIB=""

if [ "$USE_PRELOAD" = true ]; then
    # Try common locations for mimalloc
    for lib in /usr/lib/x86_64-linux-gnu/libmimalloc.so \
               /usr/lib/libmimalloc.so \
               /usr/local/lib/libmimalloc.so; do
        if [ -f "$lib" ]; then
            MIMALLOC_LIB="$lib"
            break
        fi
    done

    # Try common locations for tcmalloc
    for lib in /usr/lib/x86_64-linux-gnu/libtcmalloc.so \
               /usr/lib/x86_64-linux-gnu/libtcmalloc_minimal.so \
               /usr/lib/libtcmalloc.so \
               /usr/local/lib/libtcmalloc.so; do
        if [ -f "$lib" ]; then
            TCMALLOC_LIB="$lib"
            break
        fi
    done
fi

get_preload_env() {
    local allocator="$1"
    case $allocator in
        glibc)
            echo ""
            ;;
        mimalloc)
            if [ -n "$MIMALLOC_LIB" ]; then
                echo "LD_PRELOAD=$MIMALLOC_LIB"
            else
                echo ""
            fi
            ;;
        tcmalloc)
            if [ -n "$TCMALLOC_LIB" ]; then
                echo "LD_PRELOAD=$TCMALLOC_LIB"
            else
                echo ""
            fi
            ;;
    esac
}

# Get system info
FS_TYPE=$(df -T "$DB_PATH" 2>/dev/null | awk 'NR==2 {print $2}' || echo "unknown")
CPU_MODEL=$(grep 'model name' /proc/cpuinfo 2>/dev/null | head -1 | cut -d: -f2 | xargs || echo "unknown")
CPU_CORES=$(nproc 2>/dev/null || echo "unknown")
TOTAL_MEM=$(free -h 2>/dev/null | grep Mem | awk '{print $2}' || echo "unknown")

log "*============================================*"
log "  TidesDB Allocator Benchmark"
log "*============================================*"
log ""
log "Date: $(date)"
log "Mode: $BENCHMARK_MODE (${NUM_OPS} ops per test)"
log "Allocator(s): $ALLOCATOR"
log "Preload Mode: $USE_PRELOAD"
log ""
log "System Information:"
log "  Hostname: $(hostname)"
log "  Kernel: $(uname -r)"
log "  CPU: $CPU_MODEL"
log "  CPU Cores: $CPU_CORES"
log "  Memory: $TOTAL_MEM"
log "  Filesystem: $FS_TYPE"
log ""
log_master "Results Directory: $RESULTS_DIR"
log_master "*============================================*"
log_master ""

# Determine which allocators to test
ALLOCATORS_TO_TEST=""
case $ALLOCATOR in
    all)
        ALLOCATORS_TO_TEST="glibc"
        if [ "$USE_PRELOAD" = true ]; then
            [ -n "$MIMALLOC_LIB" ] && ALLOCATORS_TO_TEST="$ALLOCATORS_TO_TEST mimalloc"
            [ -n "$TCMALLOC_LIB" ] && ALLOCATORS_TO_TEST="$ALLOCATORS_TO_TEST tcmalloc"
        else
            # Without preload, assume TidesDB was compiled with the allocator
            # User needs to run benchmark separately for each build
            log "Note: Without --preload, testing only glibc (default)."
            log "To test other allocators, rebuild TidesDB with --with-mimalloc or --with-tcmalloc"
            log "and run this benchmark again."
        fi
        ;;
    glibc|mimalloc|tcmalloc)
        ALLOCATORS_TO_TEST="$ALLOCATOR"
        ;;
    *)
        echo "Unknown allocator: $ALLOCATOR"
        exit 1
        ;;
esac

log_master "Testing allocators: $ALLOCATORS_TO_TEST"
log_master ""
log_master "Output files per allocator:"
for alloc in $ALLOCATORS_TO_TEST; do
    log_master "  ${alloc}: ${RESULTS_DIR}/${alloc}_results.txt, ${RESULTS_DIR}/${alloc}_results.csv"
done
log_master ""

run_allocator_test() {
    local allocator="$1"
    local test_id="$2"
    local test_name="$3"
    shift 3
    local bench_args="$@"

    local preload_env=""
    if [ "$USE_PRELOAD" = true ]; then
        preload_env=$(get_preload_env "$allocator")
    fi

    local full_test_id="${allocator}_${test_id}"

    log ""
    log "*------------------------------------------*"
    log "TEST: $test_name"
    log "Allocator: $allocator"
    log "*------------------------------------------*"

    cleanup_db

    if [ -n "$preload_env" ]; then
        log "Running with: $preload_env"
        env $preload_env $BENCH -e tidesdb $bench_args -d "$DB_PATH" --test-name "$full_test_id" --csv "$CURRENT_CSV" 2>&1 | tee -a "$CURRENT_RESULTS" | tee -a "$MASTER_LOG"
    else
        $BENCH -e tidesdb $bench_args -d "$DB_PATH" --test-name "$full_test_id" --csv "$CURRENT_CSV" 2>&1 | tee -a "$CURRENT_RESULTS" | tee -a "$MASTER_LOG"
    fi

    cleanup_db
}

run_read_test() {
    local allocator="$1"
    local test_id="$2"
    local test_name="$3"
    shift 3
    local read_args="$@"
    local write_args="${read_args/-w read/-w write}"

    local preload_env=""
    if [ "$USE_PRELOAD" = true ]; then
        preload_env=$(get_preload_env "$allocator")
    fi

    local full_test_id="${allocator}_${test_id}"
    local populate_test_id="${full_test_id}_populate"

    log ""
    log "*------------------------------------------*"
    log "TEST: $test_name"
    log "Allocator: $allocator"
    log "*------------------------------------------*"

    cleanup_db

    log "Populating database..."
    if [ -n "$preload_env" ]; then
        env $preload_env $BENCH -e tidesdb $write_args -b 1000 -d "$DB_PATH" --test-name "$populate_test_id" --csv "$CURRENT_CSV" 2>&1 | tee -a "$CURRENT_RESULTS" | tee -a "$MASTER_LOG"
    else
        $BENCH -e tidesdb $write_args -b 1000 -d "$DB_PATH" --test-name "$populate_test_id" --csv "$CURRENT_CSV" 2>&1 | tee -a "$CURRENT_RESULTS" | tee -a "$MASTER_LOG"
    fi

    log "Running read test..."
    if [ -n "$preload_env" ]; then
        env $preload_env $BENCH -e tidesdb $read_args -d "$DB_PATH" --test-name "$full_test_id" --csv "$CURRENT_CSV" 2>&1 | tee -a "$CURRENT_RESULTS" | tee -a "$MASTER_LOG"
    else
        $BENCH -e tidesdb $read_args -d "$DB_PATH" --test-name "$full_test_id" --csv "$CURRENT_CSV" 2>&1 | tee -a "$CURRENT_RESULTS" | tee -a "$MASTER_LOG"
    fi

    cleanup_db
}

# ============================================================================
# BENCHMARK SUITE
# These tests are designed to stress different allocation patterns
# ============================================================================

for alloc in $ALLOCATORS_TO_TEST; do
    # Initialize per-allocator result files
    init_allocator_files "$alloc"
    
    log ""
    log "###################################################################"
    log "# ALLOCATOR: $alloc"
    log "###################################################################"

    # -------------------------------------------------------------------------
    # 1. HIGH ALLOCATION CHURN (Small Values, No Batching)
    # This stresses malloc/free with many small allocations
    # -------------------------------------------------------------------------
    log ""
    log "### 1. High Allocation Churn (Small Values, No Batching) ###"
    log "Purpose: Stress malloc/free with many small, unbatched allocations"

    for threads in $THREAD_COUNTS; do
        run_allocator_test "$alloc" \
            "churn_small_t${threads}" \
            "Small Value Churn (${NUM_OPS} ops, ${threads} threads, batch=1)" \
            -w write -p random -k 16 -v 64 -o $NUM_OPS -t $threads -b 1
    done

    # -------------------------------------------------------------------------
    # 2. MULTI-THREADED CONTENTION
    # Tests allocator lock contention under high thread counts
    # -------------------------------------------------------------------------
    log ""
    log "### 2. Multi-Threaded Contention ###"
    log "Purpose: Test allocator lock contention with many threads"

    for threads in $THREAD_COUNTS; do
        run_allocator_test "$alloc" \
            "contention_t${threads}" \
            "Thread Contention (${NUM_OPS} ops, ${threads} threads)" \
            -w write -p random -o $NUM_OPS -t $threads -b 100
    done

    # -------------------------------------------------------------------------
    # 3. MIXED WORKLOAD (Read/Write)
    # Realistic allocation pattern with varied operations
    # -------------------------------------------------------------------------
    log ""
    log "### 3. Mixed Workload ###"
    log "Purpose: Realistic allocation patterns with mixed read/write"

    for threads in $THREAD_COUNTS; do
        run_allocator_test "$alloc" \
            "mixed_t${threads}" \
            "Mixed Workload (${NUM_OPS} ops, ${threads} threads)" \
            -w mixed -p random -o $NUM_OPS -t $threads -b 100
    done

    # -------------------------------------------------------------------------
    # 4. LARGE VALUE ALLOCATIONS
    # Tests large block allocation performance
    # -------------------------------------------------------------------------
    log ""
    log "### 4. Large Value Allocations ###"
    log "Purpose: Test large block allocation (4KB-64KB values)"

    # 4KB values
    local large_ops=$((NUM_OPS / 10))
    run_allocator_test "$alloc" \
        "large_4kb_t8" \
        "Large Values 4KB (${large_ops} ops, 8 threads)" \
        -w write -p random -k 64 -v 4096 -o $large_ops -t 8 -b 100

    # 64KB values
    local xlarge_ops=$((NUM_OPS / 50))
    run_allocator_test "$alloc" \
        "large_64kb_t8" \
        "Large Values 64KB (${xlarge_ops} ops, 8 threads)" \
        -w write -p random -k 64 -v 65536 -o $xlarge_ops -t 8 -b 10

    # -------------------------------------------------------------------------
    # 5. BATCH SIZE IMPACT
    # How batch size affects allocation patterns
    # -------------------------------------------------------------------------
    log ""
    log "### 5. Batch Size Impact ###"
    log "Purpose: How batching affects allocation patterns"

    for batch in $BATCH_SIZES; do
        run_allocator_test "$alloc" \
            "batch_${batch}_t8" \
            "Batch Size ${batch} (${NUM_OPS} ops, 8 threads)" \
            -w write -p random -o $NUM_OPS -t 8 -b $batch
    done

    # -------------------------------------------------------------------------
    # 6. SEQUENTIAL VS RANDOM ACCESS
    # Different memory access patterns
    # -------------------------------------------------------------------------
    log ""
    log "### 6. Access Patterns ###"
    log "Purpose: Sequential vs random memory access patterns"

    run_allocator_test "$alloc" \
        "seq_t8" \
        "Sequential Write (${NUM_OPS} ops, 8 threads)" \
        -w write -p seq -o $NUM_OPS -t 8 -b 1000

    run_allocator_test "$alloc" \
        "random_t8" \
        "Random Write (${NUM_OPS} ops, 8 threads)" \
        -w write -p random -o $NUM_OPS -t 8 -b 1000

    # -------------------------------------------------------------------------
    # 7. ZIPFIAN (HOT KEY) WORKLOAD
    # Skewed access pattern - some allocations reused more than others
    # -------------------------------------------------------------------------
    log ""
    log "### 7. Zipfian (Hot Key) Workload ###"
    log "Purpose: Skewed allocation reuse patterns"

    run_allocator_test "$alloc" \
        "zipfian_t8" \
        "Zipfian Write (${NUM_OPS} ops, 8 threads)" \
        -w write -p zipfian -o $NUM_OPS -t 8 -b 1000

    run_allocator_test "$alloc" \
        "zipfian_mixed_t8" \
        "Zipfian Mixed (${NUM_OPS} ops, 8 threads)" \
        -w mixed -p zipfian -o $NUM_OPS -t 8 -b 100

    # -------------------------------------------------------------------------
    # 8. READ-HEAVY WORKLOAD
    # Tests memory access patterns during reads
    # -------------------------------------------------------------------------
    log ""
    log "### 8. Read-Heavy Workload ###"
    log "Purpose: Memory access patterns during read operations"

    run_read_test "$alloc" \
        "read_random_t8" \
        "Random Read (${NUM_OPS} ops, 8 threads)" \
        -w read -p random -o $NUM_OPS -t 8

    # -------------------------------------------------------------------------
    # 9. DELETE WORKLOAD (Memory Reclamation)
    # Tests how allocator handles freed memory
    # -------------------------------------------------------------------------
    log ""
    log "### 9. Delete Workload (Memory Reclamation) ###"
    log "Purpose: Test allocator memory reclamation behavior"

    # First populate, then delete
    local delete_ops=$((NUM_OPS / 2))
    cleanup_db
    log "Populating for delete test..."
    if [ "$USE_PRELOAD" = true ]; then
        preload_env=$(get_preload_env "$alloc")
        if [ -n "$preload_env" ]; then
            env $preload_env $BENCH -e tidesdb -w write -p seq -o $delete_ops -t 8 -b 1000 -d "$DB_PATH" --test-name "${alloc}_delete_populate" --csv "$CURRENT_CSV" 2>&1 | tee -a "$CURRENT_RESULTS" | tee -a "$MASTER_LOG"
        else
            $BENCH -e tidesdb -w write -p seq -o $delete_ops -t 8 -b 1000 -d "$DB_PATH" --test-name "${alloc}_delete_populate" --csv "$CURRENT_CSV" 2>&1 | tee -a "$CURRENT_RESULTS" | tee -a "$MASTER_LOG"
        fi
    else
        $BENCH -e tidesdb -w write -p seq -o $delete_ops -t 8 -b 1000 -d "$DB_PATH" --test-name "${alloc}_delete_populate" --csv "$CURRENT_CSV" 2>&1 | tee -a "$CURRENT_RESULTS" | tee -a "$MASTER_LOG"
    fi

    log "Running delete test..."
    if [ "$USE_PRELOAD" = true ]; then
        preload_env=$(get_preload_env "$alloc")
        if [ -n "$preload_env" ]; then
            env $preload_env $BENCH -e tidesdb -w delete -p seq -o $delete_ops -t 8 -b 1000 -d "$DB_PATH" --test-name "${alloc}_delete_t8" --csv "$CURRENT_CSV" 2>&1 | tee -a "$CURRENT_RESULTS" | tee -a "$MASTER_LOG"
        else
            $BENCH -e tidesdb -w delete -p seq -o $delete_ops -t 8 -b 1000 -d "$DB_PATH" --test-name "${alloc}_delete_t8" --csv "$CURRENT_CSV" 2>&1 | tee -a "$CURRENT_RESULTS" | tee -a "$MASTER_LOG"
        fi
    else
        $BENCH -e tidesdb -w delete -p seq -o $delete_ops -t 8 -b 1000 -d "$DB_PATH" --test-name "${alloc}_delete_t8" --csv "$CURRENT_CSV" 2>&1 | tee -a "$CURRENT_RESULTS" | tee -a "$MASTER_LOG"
    fi
    cleanup_db

done

# ============================================================================
# SUMMARY
# ============================================================================

log_master ""
log_master "*============================================*"
log_master "  Benchmark Complete"
log_master "*============================================*"
log_master ""
log_master "Results saved to:"
log_master "  Summary: $MASTER_LOG"
log_master ""
log_master "Per-allocator results:"
for alloc in $ALLOCATORS_TO_TEST; do
    log_master "  ${alloc}:"
    log_master "    Text: ${RESULTS_DIR}/${alloc}_results.txt"
    log_master "    CSV:  ${RESULTS_DIR}/${alloc}_results.csv"
done
log_master ""
log_master "To generate comparison graphs for each allocator:"
for alloc in $ALLOCATORS_TO_TEST; do
    log_master "  python3 graphgen.py ${RESULTS_DIR}/${alloc}_results.csv --output ${RESULTS_DIR}/${alloc}_graphs"
done
log_master ""

# If we tested multiple allocators, provide comparison hints
if [ $(echo "$ALLOCATORS_TO_TEST" | wc -w) -gt 1 ]; then
    log_master "Comparison Tips:"
    log_master "  - Look for ops/sec differences in high-thread tests (contention)"
    log_master "  - Compare memory usage (peak RSS) across allocators"
    log_master "  - Check latency percentiles (p99) for allocation spikes"
    log_master "  - Compare CSV files side-by-side using test_name column (prefixed with allocator)"
    log_master ""
fi

log_master "*============================================*"
