#!/bin/bash
#
# TidesDB vs RocksDB Extensive Benchmark Suite
# =============================================
#
# DESCRIPTION:
#   Comprehensive multi-run benchmark comparing TidesDB and RocksDB performance
#   across 13 test categories with statistical rigor (3 runs per test), warm-up
#   phases, and cold-cache testing via OS page cache drops.
#
# FLOW:
#   1. Initialize configuration (900M keys, 32GB block cache, 16 threads, batch 1000)
#   2. For each test category:
#      a. For each engine (tidesdb, rocksdb):
#         i.   Clean database directory
#         ii.  Drop OS page caches (for cold-start accuracy)
#         iii. Run warm-up phase (100K ops, discarded)
#         iv.  Run measurement phase (results recorded to CSV)
#         v.   Repeat for NUM_RUNS iterations
#   3. Output results to timestamped .txt and .csv files
#
# TEST CATEGORIES:
#   1.  Write Performance (seq, random, zipfian patterns)
#   2.  Read Performance - Warm Cache
#   3.  Read Performance - Cold Cache (page cache dropped)
#   4.  Overwrite Performance
#   5.  Mixed Workloads (50/50 read/write)
#   6.  Seek Performance (point lookups)
#   7.  Range Scan Performance (10, 100, 1000 key scans)
#   8.  Delete Performance
#   9.  Fill-Then-Scan (bulk load + sequential scan)
#   10. Value Size Impact (64B to 16KB)
#   11. Batch Size Scaling (1 to 10000)
#   12. Thread Scaling (1 to 16 threads)
#   13. Key Size Impact (8B to 128B)
#
# OUTPUT:
#   - tidesdb_rocksdb_extensive_benchmark_results_YYYYMMDD_HHMMSS.txt (human-readable)
#   - tidesdb_rocksdb_extensive_benchmark_results_YYYYMMDD_HHMMSS.csv (for graphing)
#
# USAGE:
#   ./tidesdb_rocksdb_extensive.sh
#   BENCHTOOL_DB_PATH=/mnt/nvme ./tidesdb_rocksdb_extensive.sh  # custom DB path
#

set -e 

BENCH="./build/benchtool"
DB_PATH="${BENCHTOOL_DB_PATH:-db-bench}"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
CSV_OUTPUT="tidesdb_rocksdb_extensive_benchmark_results_${TIMESTAMP}.csv"
RESULTS_TXT="tidesdb_rocksdb_extensive_benchmark_results_${TIMESTAMP}.txt"

SYNC_ENABLED="false"
DEFAULT_BATCH_SIZE=1000
DEFAULT_THREADS=16
DEFAULT_OPS=900000000
BLOCK_CACHE_SIZE=34359738368
NUM_RUNS=1  
WARMUP_OPS=100000 
CURRENT_TEST_NAME=""
DROP_CACHES_FAILED=0

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


> "$CSV_OUTPUT"

FS_TYPE=$(df -T "$DB_PATH" 2>/dev/null | awk 'NR==2 {print $2}')
FS_TYPE=${FS_TYPE:-unknown}

log() {
    echo "$1" | tee -a "$RESULTS_TXT"
}

log "*------------------------------------------*"
log "RUNNER: TidesDB vs RocksDB (Extensive)"
log "Date: $(date)"
log "Sync Mode: $SYNC_MODE"
log "Parameters:"
log "  Default operations: $DEFAULT_OPS"
log "  Block cache size: $BLOCK_CACHE_SIZE bytes ($(echo "scale=2; $BLOCK_CACHE_SIZE / 1024 / 1024 / 1024" | bc) GB)"
log "  Runs per test: $NUM_RUNS"
log "  Warm-up operations: $WARMUP_OPS"
log "Environment:"
log "  Hostname: $(hostname)"
log "  Kernel: $(uname -r)"
log "  Filesystem: $FS_TYPE"
log "  CPU: $(grep 'model name' /proc/cpuinfo | head -1 | cut -d: -f2 | xargs)"
log "  CPU Cores: $(nproc)"
log "  Memory: $(free -h | grep Mem | awk '{print $2}')"
log "Results:"
log "  Text: $RESULTS_TXT"
log "  CSV:  $CSV_OUTPUT"
log "*------------------------------------------*"
log ""

cleanup_db() {
    if [ -d "$DB_PATH" ]; then
        rm -rf "$DB_PATH"
    fi
    sync
}

drop_caches() {
    local success=0
    if [ -w /proc/sys/vm/drop_caches ]; then
        if echo 3 > /proc/sys/vm/drop_caches 2>/dev/null; then
            success=1
        fi
    else
        if sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches' 2>/dev/null; then
            success=1
        fi
    fi
    
    if [ $success -eq 0 ]; then
        if [ $DROP_CACHES_FAILED -eq 0 ]; then
            log "WARNING: Failed to drop caches. Cold-read results may be skewed."
            log "         Run as root or configure sudo for accurate cold-cache benchmarks."
            DROP_CACHES_FAILED=1
        fi
    fi
    sync
    sleep 1
}

run_benchmark() {
    local engine="$1"
    local workload="$2"
    local pattern="$3"
    local ops="$4"
    local threads="$5"
    local batch="$6"
    local key_size="$7"
    local value_size="$8"
    shift 8
    local extra_args="$@"
    
    if ! $BENCH -e "$engine" \
        -w "$workload" \
        -p "$pattern" \
        -o "$ops" \
        -t "$threads" \
        -b "$batch" \
        -k "$key_size" \
        -v "$value_size" \
        $SYNC_FLAG \
        -d "$DB_PATH" \
        --block-cache-size "$BLOCK_CACHE_SIZE" \
        --test-name "$CURRENT_TEST_NAME" \
        --csv "$CSV_OUTPUT" \
        $extra_args 2>&1 | tee -a "$RESULTS_TXT"; then
        log "ERROR: Benchmark failed for $engine ($workload, $pattern)"
        return 1
    fi
}

benchmark_write() {
    local test_name="$1"
    local pattern="$2"
    local ops="$3"
    local threads="$4"
    local batch="$5"
    local key_size="$6"
    local value_size="$7"
    
    log ""
    log "*------------------------------------------*"
    log "TEST: $test_name (Write Benchmark)"
    log "Pattern: $pattern, Ops: $ops, Threads: $threads, Batch: $batch"
    log "*------------------------------------------*"
    
    CURRENT_TEST_NAME="$test_name"

    for engine in tidesdb rocksdb; do
        log ""
        log "--- Engine: $engine ---"
        
        for run in $(seq 1 $NUM_RUNS); do
            log "  Run $run/$NUM_RUNS..."
            
            cleanup_db
            drop_caches
            
            log "    Warm-up phase..."
            $BENCH -e "$engine" -w write -p "$pattern" -o "$WARMUP_OPS" -t "$threads" -b "$batch" -k "$key_size" -v "$value_size" $SYNC_FLAG -d "$DB_PATH" > /dev/null 2>&1
            cleanup_db
            
            log "    Measurement phase..."
            run_benchmark "$engine" "write" "$pattern" "$ops" "$threads" "$batch" "$key_size" "$value_size"
            
            cleanup_db
        done
    done
}

benchmark_read() {
    local test_name="$1"
    local pattern="$2"
    local ops="$3"
    local threads="$4"
    local batch="$5"
    local key_size="$6"
    local value_size="$7"
    
    log ""
    log "*------------------------------------------*"
    log "TEST: $test_name (Read Benchmark)"
    log "Pattern: $pattern, Ops: $ops, Threads: $threads"
    log "*------------------------------------------*"
    
    CURRENT_TEST_NAME="$test_name"

    for engine in tidesdb rocksdb; do
        log ""
        log "--- Engine: $engine ---"
        
        for run in $(seq 1 $NUM_RUNS); do
            log "  Run $run/$NUM_RUNS..."
            
            cleanup_db
            drop_caches
            
            log "    Populating database..."
            $BENCH -e "$engine" -w write -p "$pattern" -o "$ops" -t "$threads" -b "$batch" -k "$key_size" -v "$value_size" $SYNC_FLAG -d "$DB_PATH" > /dev/null 2>&1
            
            log "    Cache warm-up..."
            $BENCH -e "$engine" -w read -p "$pattern" -o "$WARMUP_OPS" -t "$threads" -b "$batch" -k "$key_size" -v "$value_size" $SYNC_FLAG -d "$DB_PATH" > /dev/null 2>&1
            
            log "    Measurement (warm cache)..."
            run_benchmark "$engine" "read" "$pattern" "$ops" "$threads" "$batch" "$key_size" "$value_size"
            
            cleanup_db
        done
    done
}

benchmark_cold_read() {
    local test_name="$1"
    local pattern="$2"
    local ops="$3"
    local threads="$4"
    local batch="$5"
    local key_size="$6"
    local value_size="$7"
    
    log ""
    log "*------------------------------------------*"
    log "TEST: $test_name (Cold-Start Read Benchmark)"
    log "Pattern: $pattern, Ops: $ops, Threads: $threads"
    log "*------------------------------------------*"
    
    CURRENT_TEST_NAME="$test_name"

    for engine in tidesdb rocksdb; do
        log ""
        log "--- Engine: $engine ---"
        
        for run in $(seq 1 $NUM_RUNS); do
            log "  Run $run/$NUM_RUNS..."
            
            cleanup_db

            log "    Populating database..."
            $BENCH -e "$engine" -w write -p "$pattern" -o "$ops" -t "$threads" -b "$batch" -k "$key_size" -v "$value_size" $SYNC_FLAG -d "$DB_PATH" > /dev/null 2>&1
            
            log "    Dropping caches (cold start)..."
            drop_caches
            sleep 2
            
            log "    Measurement (cold cache)..."
            run_benchmark "$engine" "read" "$pattern" "$ops" "$threads" "$batch" "$key_size" "$value_size"
            
            cleanup_db
        done
    done
}

benchmark_overwrite() {
    local test_name="$1"
    local pattern="$2"
    local ops="$3"
    local threads="$4"
    local batch="$5"
    local key_size="$6"
    local value_size="$7"
    
    log ""
    log "*------------------------------------------*"
    log "TEST: $test_name (Overwrite Benchmark)"
    log "Pattern: $pattern, Ops: $ops, Threads: $threads"
    log "*------------------------------------------*"
    
    CURRENT_TEST_NAME="$test_name"

    for engine in tidesdb rocksdb; do
        log ""
        log "--- Engine: $engine ---"
        
        for run in $(seq 1 $NUM_RUNS); do
            log "  Run $run/$NUM_RUNS..."
            
            cleanup_db
            drop_caches
            
            log "    Initial population..."
            $BENCH -e "$engine" -w write -p seq -o "$ops" -t "$threads" -b "$batch" -k "$key_size" -v "$value_size" $SYNC_FLAG -d "$DB_PATH" > /dev/null 2>&1
            
            log "    Overwrite measurement..."
            run_benchmark "$engine" "write" "seq" "$ops" "$threads" "$batch" "$key_size" "$value_size"
            
            cleanup_db
        done
    done
}

benchmark_mixed() {
    local test_name="$1"
    local pattern="$2"
    local ops="$3"
    local threads="$4"
    local batch="$5"
    local key_size="$6"
    local value_size="$7"
    
    log ""
    log "*------------------------------------------*"
    log "TEST: $test_name (Mixed Workload)"
    log "Pattern: $pattern, Ops: $ops, Threads: $threads"
    log "*------------------------------------------*"
    
    CURRENT_TEST_NAME="$test_name"

    for engine in tidesdb rocksdb; do
        log ""
        log "--- Engine: $engine ---"
        
        for run in $(seq 1 $NUM_RUNS); do
            log "  Run $run/$NUM_RUNS..."
            
            cleanup_db
            drop_caches
            
            log "    Warm-up..."
            $BENCH -e "$engine" -w mixed -p "$pattern" -o "$WARMUP_OPS" -t "$threads" -b "$batch" -k "$key_size" -v "$value_size" $SYNC_FLAG -d "$DB_PATH" > /dev/null 2>&1
            cleanup_db
            
            log "    Measurement..."
            run_benchmark "$engine" "mixed" "$pattern" "$ops" "$threads" "$batch" "$key_size" "$value_size"
            
            cleanup_db
        done
    done
}

benchmark_seek() {
    local test_name="$1"
    local pattern="$2"
    local ops="$3"
    local threads="$4"
    local batch="$5"
    local key_size="$6"
    local value_size="$7"
    
    log ""
    log "*------------------------------------------*"
    log "TEST: $test_name (Seek Benchmark)"
    log "Pattern: $pattern, Ops: $ops, Threads: $threads"
    log "*------------------------------------------*"
    
    CURRENT_TEST_NAME="$test_name"

    for engine in tidesdb rocksdb; do
        log ""
        log "--- Engine: $engine ---"
        
        for run in $(seq 1 $NUM_RUNS); do
            log "  Run $run/$NUM_RUNS..."
            
            cleanup_db
            drop_caches
            
            log "    Populating..."
            $BENCH -e "$engine" -w write -p "$pattern" -o "$ops" -t "$threads" -b "$batch" -k "$key_size" -v "$value_size" $SYNC_FLAG -d "$DB_PATH" > /dev/null 2>&1
            
            log "    Seek warm-up..."
            $BENCH -e "$engine" -w seek -p "$pattern" -o "$WARMUP_OPS" -t "$threads" -b "$batch" -k "$key_size" -v "$value_size" $SYNC_FLAG -d "$DB_PATH" > /dev/null 2>&1
            
            log "    Measurement..."
            run_benchmark "$engine" "seek" "$pattern" "$ops" "$threads" "$batch" "$key_size" "$value_size"
            
            cleanup_db
        done
    done
}

benchmark_range() {
    local test_name="$1"
    local pattern="$2"
    local ops="$3"
    local threads="$4"
    local batch="$5"
    local key_size="$6"
    local value_size="$7"
    local range_size="$8"
    
    log ""
    log "*------------------------------------------*"
    log "TEST: $test_name (Range Scan Benchmark)"
    log "Pattern: $pattern, Ops: $ops, Range Size: $range_size"
    log "*------------------------------------------*"
    
    CURRENT_TEST_NAME="$test_name"

    for engine in tidesdb rocksdb; do
        log ""
        log "--- Engine: $engine ---"
        
        for run in $(seq 1 $NUM_RUNS); do
            log "  Run $run/$NUM_RUNS..."
            
            cleanup_db
            drop_caches
            
            local pop_ops=$((ops * 10))
            log "    Populating ($pop_ops keys)..."
            $BENCH -e "$engine" -w write -p seq -o "$pop_ops" -t "$threads" -b "$batch" -k "$key_size" -v "$value_size" $SYNC_FLAG -d "$DB_PATH" > /dev/null 2>&1
            
            log "    Measurement..."
            run_benchmark "$engine" "range" "$pattern" "$ops" "$threads" "$batch" "$key_size" "$value_size" "--range-size $range_size"
            
            cleanup_db
        done
    done
}

benchmark_delete() {
    local test_name="$1"
    local pattern="$2"
    local ops="$3"
    local threads="$4"
    local batch="$5"
    local key_size="$6"
    local value_size="$7"
    
    log ""
    log "*------------------------------------------*"
    log "TEST: $test_name (Delete Benchmark)"
    log "Pattern: $pattern, Ops: $ops, Threads: $threads"
    log "*------------------------------------------*"
    
    CURRENT_TEST_NAME="$test_name"

    for engine in tidesdb rocksdb; do
        log ""
        log "--- Engine: $engine ---"
        
        for run in $(seq 1 $NUM_RUNS); do
            log "  Run $run/$NUM_RUNS..."
            
            cleanup_db
            drop_caches
            
            log "    Populating..."
            $BENCH -e "$engine" -w write -p "$pattern" -o "$ops" -t "$threads" -b "$batch" -k "$key_size" -v "$value_size" $SYNC_FLAG -d "$DB_PATH" > /dev/null 2>&1
            
            log "    Delete measurement..."
            run_benchmark "$engine" "delete" "$pattern" "$ops" "$threads" "$batch" "$key_size" "$value_size"
            
            cleanup_db
        done
    done
}

benchmark_fill_scan() {
    local test_name="$1"
    local ops="$2"
    local threads="$3"
    local batch="$4"
    local key_size="$5"
    local value_size="$6"
    
    log ""
    log "*------------------------------------------*"
    log "TEST: $test_name (Fill-then-Scan Benchmark)"
    log "Ops: $ops, Threads: $threads"
    log "*------------------------------------------*"

    for engine in tidesdb rocksdb; do
        log ""
        log "--- Engine: $engine ---"
        
        for run in $(seq 1 $NUM_RUNS); do
            log "  Run $run/$NUM_RUNS..."
            
            cleanup_db
            drop_caches
            
            log "    Fill phase..."
            CURRENT_TEST_NAME="${test_name}_fill"
            run_benchmark "$engine" "write" "seq" "$ops" "$threads" "$batch" "$key_size" "$value_size"
            
            log "    Scan phase..."
            CURRENT_TEST_NAME="${test_name}_scan"
            local scan_ops=$((ops / 1000))  
            run_benchmark "$engine" "range" "seq" "$scan_ops" "$threads" "$batch" "$key_size" "$value_size" "--range-size 1000"
            
            cleanup_db
        done
    done
}

log ""
log "### 1. Write Performance ###"

benchmark_write "write_seq" "seq" $DEFAULT_OPS $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 16 100
benchmark_write "write_random" "random" $DEFAULT_OPS $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 16 100
benchmark_write "write_zipfian" "zipfian" $DEFAULT_OPS $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 16 100

log ""
log "### 2. Read Performance (Warm Cache) ###"

benchmark_read "read_random_warm" "random" $DEFAULT_OPS $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 16 100
benchmark_read "read_seq_warm" "seq" $DEFAULT_OPS $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 16 100
benchmark_read "read_zipfian_warm" "zipfian" $DEFAULT_OPS $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 16 100

log ""
log "### 3. Read Performance (Cold Cache) ###"

benchmark_cold_read "read_random_cold" "random" $DEFAULT_OPS $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 16 100
benchmark_cold_read "read_seq_cold" "seq" $DEFAULT_OPS $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 16 100

log ""
log "### 4. Overwrite Performance ###"

benchmark_overwrite "overwrite" "seq" $DEFAULT_OPS $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 16 100

log ""
log "### 5. Mixed Workloads ###"

benchmark_mixed "mixed_random" "random" $DEFAULT_OPS $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 16 100
benchmark_mixed "mixed_zipfian" "zipfian" $DEFAULT_OPS $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 16 100

log ""
log "### 6. Seek Performance ###"

benchmark_seek "seek_random" "random" $DEFAULT_OPS $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 16 100
benchmark_seek "seek_seq" "seq" $DEFAULT_OPS $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 16 100

log ""
log "### 7. Range Scan Performance ###"

benchmark_range "range_10" "random" $((DEFAULT_OPS / 100)) $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 16 100 10
benchmark_range "range_100" "random" $((DEFAULT_OPS / 100)) $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 16 100 100
benchmark_range "range_1000" "random" $((DEFAULT_OPS / 500)) $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 16 100 1000

log ""
log "### 8. Delete Performance ###"

benchmark_delete "delete_random" "random" $DEFAULT_OPS $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 16 100

log ""
log "### 9. Fill-Then-Scan ###"

benchmark_fill_scan "fill_scan" $DEFAULT_OPS $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 16 100

log ""
log "### 10. Value Size Impact ###"

benchmark_write "write_small_value" "random" $DEFAULT_OPS $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 16 64
benchmark_write "write_medium_value" "random" $((DEFAULT_OPS / 10)) $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 16 1024
benchmark_write "write_large_value" "random" $((DEFAULT_OPS / 100)) $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 256 4096
benchmark_write "write_xlarge_value" "random" $((DEFAULT_OPS / 1000)) $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 256 16384

log ""
log "### 11. Batch Size Scaling ###"

for batch in 1 10 100 1000 10000; do
    benchmark_write "batch_${batch}" "random" $((DEFAULT_OPS / 10)) $DEFAULT_THREADS $batch 16 100
done

log ""
log "### 12. Thread Scaling ###"

for threads in 1 2 4 8 16; do
    benchmark_write "threads_${threads}" "random" $((DEFAULT_OPS / 10)) $threads $DEFAULT_BATCH_SIZE 16 100
done

log ""
log "### 13. Key Size Impact ###"

benchmark_write "key_8B" "random" $DEFAULT_OPS $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 8 100
benchmark_write "key_32B" "random" $DEFAULT_OPS $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 32 100
benchmark_write "key_64B" "random" $DEFAULT_OPS $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 64 100
benchmark_write "key_128B" "random" $DEFAULT_OPS $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 128 100

cleanup_db

log ""
log "*------------------------------------------*"
log "RUNNER Complete"
log "Results:"
log "  Text: $RESULTS_TXT"
log "  CSV:  $CSV_OUTPUT"
log "*------------------------------------------*"
