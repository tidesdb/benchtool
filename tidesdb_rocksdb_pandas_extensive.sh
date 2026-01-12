#!/bin/bash

set -e  # Exit on error

###############################################################################
# -- Multiple runs per test for statistical significance
# -- Warm-up phases before measurement
# -- Cold-start tests (drop caches, restart)
# -- Proper cache warming sequences
# -- Controlled test isolation
###############################################################################

BENCH="./build/benchtool"
DB_PATH="db-bench"
RESULTS_DIR="benchmark_results"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
CSV_OUTPUT="${RESULTS_DIR}/scientific_${TIMESTAMP}.csv"
RESULTS_TXT="${RESULTS_DIR}/scientific_${TIMESTAMP}.txt"

SYNC_ENABLED="false"
DEFAULT_BATCH_SIZE=1000
DEFAULT_THREADS=8
NUM_RUNS=3  # Number of runs per test for statistical significance
WARMUP_OPS=100000  # Warm-up operations before measurement

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

mkdir -p "$RESULTS_DIR"

echo "test_name,run_number,phase,engine,operation,ops_per_sec,duration_sec,avg_latency_us,stddev_us,cv_percent,p50_us,p95_us,p99_us,min_us,max_us,peak_rss_mb,peak_vms_mb,disk_read_mb,disk_write_mb,cpu_user_sec,cpu_sys_sec,cpu_percent,db_size_mb,write_amp,read_amp,space_amp,workload,pattern,ops_count,threads,batch_size,key_size,value_size" > "$CSV_OUTPUT"

log() {
    echo "$1" | tee -a "$RESULTS_TXT"
}

log "============================================================================="
log "TidesDB vs RocksDB Scientific Benchmark Suite"
log "============================================================================="
log "Date: $(date)"
log "Hostname: $(hostname)"
log "Kernel: $(uname -r)"
log "CPU: $(grep 'model name' /proc/cpuinfo | head -1 | cut -d: -f2 | xargs)"
log "CPU Cores: $(nproc)"
log "Memory: $(free -h | grep Mem | awk '{print $2}')"
log "Sync Mode: $SYNC_MODE"
log "Runs per test: $NUM_RUNS"
log "Warm-up operations: $WARMUP_OPS"
log "CSV Output: $CSV_OUTPUT"
log "============================================================================="
log ""

cleanup_db() {
    if [ -d "$DB_PATH" ]; then
        rm -rf "$DB_PATH"
    fi
    # Sync filesystem
    sync
}

drop_caches() {
    if [ -w /proc/sys/vm/drop_caches ]; then
        echo 3 > /proc/sys/vm/drop_caches 2>/dev/null || true
    else
        # Try with sudo if available
        sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches' 2>/dev/null || true
    fi
    sync
    sleep 1
}

# Append benchmark result to CSV
# Args -- test_name, run_number, phase, temp_csv, workload, pattern, ops, threads, batch, key_size, value_size
append_csv() {
    local test_name="$1"
    local run_number="$2"
    local phase="$3"
    local temp_csv="$4"
    local workload="$5"
    local pattern="$6"
    local ops="$7"
    local threads="$8"
    local batch="$9"
    local key_size="${10}"
    local value_size="${11}"
    
    if [ -f "$temp_csv" ]; then
        tail -n +2 "$temp_csv" | while IFS=, read -r eng op ops_sec dur avg std cv p50 p95 p99 min max rss vms dr dw cu cs cp db wa ra sa; do
            echo "$test_name,$run_number,$phase,$eng,$op,$ops_sec,$dur,$avg,$std,$cv,$p50,$p95,$p99,$min,$max,$rss,$vms,$dr,$dw,$cu,$cs,$cp,$db,$wa,$ra,$sa,$workload,$pattern,$ops,$threads,$batch,$key_size,$value_size" >> "$CSV_OUTPUT"
        done
        rm -f "$temp_csv"
    fi
}

# Run a single benchmark with proper isolation
# Args -- test_name, run_number, phase, engine, workload, pattern, ops, threads, batch, key_size, value_size, extra_args
run_isolated_benchmark() {
    local test_name="$1"
    local run_number="$2"
    local phase="$3"
    local engine="$4"
    local workload="$5"
    local pattern="$6"
    local ops="$7"
    local threads="$8"
    local batch="$9"
    local key_size="${10}"
    local value_size="${11}"
    shift 11
    local extra_args="$@"
    
    local temp_csv=$(mktemp)
    
    $BENCH -e "$engine" \
        -w "$workload" \
        -p "$pattern" \
        -o "$ops" \
        -t "$threads" \
        -b "$batch" \
        -k "$key_size" \
        -v "$value_size" \
        $SYNC_FLAG \
        -d "$DB_PATH" \
        --csv "$temp_csv" \
        $extra_args 2>&1 | tee -a "$RESULTS_TXT"
    
    append_csv "$test_name" "$run_number" "$phase" "$temp_csv" "$workload" "$pattern" "$ops" "$threads" "$batch" "$key_size" "$value_size"
}


# Standard write benchmark with warm-up and multiple runs
# Args -- test_name, pattern, ops, threads, batch, key_size, value_size
benchmark_write() {
    local test_name="$1"
    local pattern="$2"
    local ops="$3"
    local threads="$4"
    local batch="$5"
    local key_size="$6"
    local value_size="$7"
    
    log ""
    log "========================================================================"
    log "TEST: $test_name (Write Benchmark)"
    log "Pattern: $pattern, Ops: $ops, Threads: $threads, Batch: $batch"
    log "========================================================================"
    
    for engine in tidesdb rocksdb; do
        log ""
        log "--- Engine: $engine ---"
        
        for run in $(seq 1 $NUM_RUNS); do
            log "  Run $run/$NUM_RUNS..."
            
            cleanup_db
            drop_caches
            
            # Warm-up phase (not measured, smaller dataset)
            log "    Warm-up phase..."
            $BENCH -e "$engine" -w write -p "$pattern" -o "$WARMUP_OPS" -t "$threads" -b "$batch" -k "$key_size" -v "$value_size" $SYNC_FLAG -d "$DB_PATH" > /dev/null 2>&1
            cleanup_db
            
            # Measurement phase
            log "    Measurement phase..."
            run_isolated_benchmark "$test_name" "$run" "measurement" "$engine" "write" "$pattern" "$ops" "$threads" "$batch" "$key_size" "$value_size"
            
            cleanup_db
        done
    done
}

# Read benchmark with proper data population and cache states
# Args -- test_name, pattern, ops, threads, batch, key_size, value_size
benchmark_read() {
    local test_name="$1"
    local pattern="$2"
    local ops="$3"
    local threads="$4"
    local batch="$5"
    local key_size="$6"
    local value_size="$7"
    
    log ""
    log "========================================================================"
    log "TEST: $test_name (Read Benchmark)"
    log "Pattern: $pattern, Ops: $ops, Threads: $threads"
    log "========================================================================"
    
    for engine in tidesdb rocksdb; do
        log ""
        log "--- Engine: $engine ---"
        
        for run in $(seq 1 $NUM_RUNS); do
            log "  Run $run/$NUM_RUNS..."
            
            cleanup_db
            drop_caches
            
            log "    Populating database..."
            $BENCH -e "$engine" -w write -p "$pattern" -o "$ops" -t "$threads" -b "$batch" -k "$key_size" -v "$value_size" $SYNC_FLAG -d "$DB_PATH" > /dev/null 2>&1
            
            # Warm cache read (not measured)
            log "    Cache warm-up..."
            $BENCH -e "$engine" -w read -p "$pattern" -o "$WARMUP_OPS" -t "$threads" -k "$key_size" -v "$value_size" $SYNC_FLAG -d "$DB_PATH" > /dev/null 2>&1
            
            # Measurement phase (warm cache)
            log "    Measurement (warm cache)..."
            run_isolated_benchmark "$test_name" "$run" "warm_cache" "$engine" "read" "$pattern" "$ops" "$threads" "$batch" "$key_size" "$value_size"
            
            cleanup_db
        done
    done
}

# Cold-start read benchmark (drop caches, measure cold read performance)
# Args -- test_name, pattern, ops, threads, batch, key_size, value_size
benchmark_cold_read() {
    local test_name="$1"
    local pattern="$2"
    local ops="$3"
    local threads="$4"
    local batch="$5"
    local key_size="$6"
    local value_size="$7"
    
    log ""
    log "========================================================================"
    log "TEST: $test_name (Cold-Start Read Benchmark)"
    log "Pattern: $pattern, Ops: $ops, Threads: $threads"
    log "========================================================================"
    
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
            
            # Measurement phase (cold cache)
            log "    Measurement (cold cache)..."
            run_isolated_benchmark "$test_name" "$run" "cold_cache" "$engine" "read" "$pattern" "$ops" "$threads" "$batch" "$key_size" "$value_size"
            
            cleanup_db
        done
    done
}

# Overwrite benchmark (update existing keys)
# Args -- test_name, pattern, ops, threads, batch, key_size, value_size
benchmark_overwrite() {
    local test_name="$1"
    local pattern="$2"
    local ops="$3"
    local threads="$4"
    local batch="$5"
    local key_size="$6"
    local value_size="$7"
    
    log ""
    log "========================================================================"
    log "TEST: $test_name (Overwrite Benchmark)"
    log "Pattern: $pattern, Ops: $ops, Threads: $threads"
    log "========================================================================"
    
    for engine in tidesdb rocksdb; do
        log ""
        log "--- Engine: $engine ---"
        
        for run in $(seq 1 $NUM_RUNS); do
            log "  Run $run/$NUM_RUNS..."
            
            cleanup_db
            drop_caches
            
            log "    Initial population..."
            $BENCH -e "$engine" -w write -p seq -o "$ops" -t "$threads" -b "$batch" -k "$key_size" -v "$value_size" $SYNC_FLAG -d "$DB_PATH" > /dev/null 2>&1
            
            # Overwrite phase (write same keys again with sequential pattern)
            log "    Overwrite measurement..."
            run_isolated_benchmark "$test_name" "$run" "overwrite" "$engine" "write" "seq" "$ops" "$threads" "$batch" "$key_size" "$value_size"
            
            cleanup_db
        done
    done
}

# Mixed workload with different read/write ratios
# Args -- test_name, pattern, ops, threads, batch, key_size, value_size
benchmark_mixed() {
    local test_name="$1"
    local pattern="$2"
    local ops="$3"
    local threads="$4"
    local batch="$5"
    local key_size="$6"
    local value_size="$7"
    
    log ""
    log "========================================================================"
    log "TEST: $test_name (Mixed Workload)"
    log "Pattern: $pattern, Ops: $ops, Threads: $threads"
    log "========================================================================"
    
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
            
            # Measurement
            log "    Measurement..."
            run_isolated_benchmark "$test_name" "$run" "measurement" "$engine" "mixed" "$pattern" "$ops" "$threads" "$batch" "$key_size" "$value_size"
            
            cleanup_db
        done
    done
}

# Seek benchmark
# Args -- test_name, pattern, ops, threads, batch, key_size, value_size
benchmark_seek() {
    local test_name="$1"
    local pattern="$2"
    local ops="$3"
    local threads="$4"
    local batch="$5"
    local key_size="$6"
    local value_size="$7"
    
    log ""
    log "========================================================================"
    log "TEST: $test_name (Seek Benchmark)"
    log "Pattern: $pattern, Ops: $ops, Threads: $threads"
    log "========================================================================"
    
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
            $BENCH -e "$engine" -w seek -p "$pattern" -o "$WARMUP_OPS" -t "$threads" -k "$key_size" -v "$value_size" $SYNC_FLAG -d "$DB_PATH" > /dev/null 2>&1
            
            # Measurement
            log "    Measurement..."
            run_isolated_benchmark "$test_name" "$run" "measurement" "$engine" "seek" "$pattern" "$ops" "$threads" "$batch" "$key_size" "$value_size"
            
            cleanup_db
        done
    done
}

# Range scan benchmark
# Args -- test_name, pattern, ops, threads, batch, key_size, value_size, range_size
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
    log "========================================================================"
    log "TEST: $test_name (Range Scan Benchmark)"
    log "Pattern: $pattern, Ops: $ops, Range Size: $range_size"
    log "========================================================================"
    
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
            
            # Measurement
            log "    Measurement..."
            run_isolated_benchmark "$test_name" "$run" "measurement" "$engine" "range" "$pattern" "$ops" "$threads" "$batch" "$key_size" "$value_size" "--range-size $range_size"
            
            cleanup_db
        done
    done
}

# Delete benchmark
# Args -- test_name, pattern, ops, threads, batch, key_size, value_size
benchmark_delete() {
    local test_name="$1"
    local pattern="$2"
    local ops="$3"
    local threads="$4"
    local batch="$5"
    local key_size="$6"
    local value_size="$7"
    
    log ""
    log "========================================================================"
    log "TEST: $test_name (Delete Benchmark)"
    log "Pattern: $pattern, Ops: $ops, Threads: $threads"
    log "========================================================================"
    
    for engine in tidesdb rocksdb; do
        log ""
        log "--- Engine: $engine ---"
        
        for run in $(seq 1 $NUM_RUNS); do
            log "  Run $run/$NUM_RUNS..."
            
            cleanup_db
            drop_caches
            
            log "    Populating..."
            $BENCH -e "$engine" -w write -p "$pattern" -o "$ops" -t "$threads" -b "$batch" -k "$key_size" -v "$value_size" $SYNC_FLAG -d "$DB_PATH" > /dev/null 2>&1
            
            # Measurement
            log "    Delete measurement..."
            run_isolated_benchmark "$test_name" "$run" "measurement" "$engine" "delete" "$pattern" "$ops" "$threads" "$batch" "$key_size" "$value_size"
            
            cleanup_db
        done
    done
}

# Fill-then-scan benchmark (write N keys, then full iteration)
# Args -- test_name, ops, threads, batch, key_size, value_size
benchmark_fill_scan() {
    local test_name="$1"
    local ops="$2"
    local threads="$3"
    local batch="$4"
    local key_size="$5"
    local value_size="$6"
    
    log ""
    log "========================================================================"
    log "TEST: $test_name (Fill-then-Scan Benchmark)"
    log "Ops: $ops, Threads: $threads"
    log "========================================================================"
    
    for engine in tidesdb rocksdb; do
        log ""
        log "--- Engine: $engine ---"
        
        for run in $(seq 1 $NUM_RUNS); do
            log "  Run $run/$NUM_RUNS..."
            
            cleanup_db
            drop_caches
            
            # Fill phase (measured)
            log "    Fill phase..."
            run_isolated_benchmark "$test_name" "$run" "fill" "$engine" "write" "seq" "$ops" "$threads" "$batch" "$key_size" "$value_size"
            
            # Full scan (iteration is automatic in benchmark output)
            log "    Scan complete (see ITER in results)"
            
            cleanup_db
        done
    done
}

log ""
log "############################################################################"
log "# SECTION 1 - WRITE PERFORMANCE"
log "############################################################################"

benchmark_write "write_seq_5M" "seq" 5000000 $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 16 100
benchmark_write "write_random_5M" "random" 5000000 $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 16 100
benchmark_write "write_zipfian_5M" "zipfian" 5000000 $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 16 100

log ""
log "############################################################################"
log "# SECTION 2 - READ PERFORMANCE (WARM CACHE)"
log "############################################################################"

benchmark_read "read_random_warm_5M" "random" 5000000 $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 16 100
benchmark_read "read_seq_warm_5M" "seq" 5000000 $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 16 100
benchmark_read "read_zipfian_warm_5M" "zipfian" 5000000 $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 16 100

log ""
log "############################################################################"
log "# SECTION 3 - READ PERFORMANCE (COLD CACHE)"
log "############################################################################"

benchmark_cold_read "read_random_cold_5M" "random" 5000000 $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 16 100
benchmark_cold_read "read_seq_cold_5M" "seq" 5000000 $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 16 100

log ""
log "############################################################################"
log "# SECTION 4 - OVERWRITE PERFORMANCE"
log "############################################################################"

benchmark_overwrite "overwrite_5M" "seq" 5000000 $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 16 100

log ""
log "############################################################################"
log "# SECTION 5 - MIXED WORKLOADS"
log "############################################################################"

benchmark_mixed "mixed_random_5M" "random" 5000000 $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 16 100
benchmark_mixed "mixed_zipfian_5M" "zipfian" 5000000 $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 16 100

log ""
log "############################################################################"
log "# SECTION 6 - SEEK PERFORMANCE"
log "############################################################################"

benchmark_seek "seek_random_5M" "random" 5000000 $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 16 100
benchmark_seek "seek_seq_5M" "seq" 5000000 $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 16 100

log ""
log "############################################################################"
log "# SECTION 7 - RANGE SCAN PERFORMANCE"
log "############################################################################"

benchmark_range "range_10" "random" 500000 $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 16 100 10
benchmark_range "range_100" "random" 500000 $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 16 100 100
benchmark_range "range_1000" "random" 200000 $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 16 100 1000

log ""
log "############################################################################"
log "# SECTION 8 - DELETE PERFORMANCE"
log "############################################################################"

benchmark_delete "delete_random_5M" "random" 5000000 $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 16 100

log ""
log "############################################################################"
log "# SECTION 9 - FILL-THEN-SCAN"
log "############################################################################"

benchmark_fill_scan "fill_scan_10M" 10000000 $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 16 100

log ""
log "############################################################################"
log "# SECTION 10 - VALUE SIZE IMPACT"
log "############################################################################"

benchmark_write "write_small_value" "random" 10000000 $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 16 64
benchmark_write "write_medium_value" "random" 2000000 $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 16 1024
benchmark_write "write_large_value" "random" 500000 $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 256 4096
benchmark_write "write_xlarge_value" "random" 100000 $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 256 16384

log ""
log "############################################################################"
log "# SECTION 11 - BATCH SIZE SCALING"
log "############################################################################"

for batch in 1 10 100 1000 10000; do
    benchmark_write "batch_${batch}" "random" 2000000 $DEFAULT_THREADS $batch 16 100
done

log ""
log "############################################################################"
log "# SECTION 12 - THREAD SCALING"
log "############################################################################"

for threads in 1 2 4 8 16; do
    benchmark_write "threads_${threads}" "random" 2000000 $threads $DEFAULT_BATCH_SIZE 16 100
done

log ""
log "############################################################################"
log "# SECTION 13 - KEY SIZE IMPACT"
log "############################################################################"

benchmark_write "key_8B" "random" 5000000 $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 8 100
benchmark_write "key_32B" "random" 5000000 $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 32 100
benchmark_write "key_64B" "random" 5000000 $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 64 100
benchmark_write "key_128B" "random" 5000000 $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 128 100

cleanup_db

log ""
log "============================================================================="
log "Benchmark Suite Complete!"
log "CSV Results: $CSV_OUTPUT"
log "Text Results: $RESULTS_TXT"
log "============================================================================="
log ""
log "To generate graphs, run:"
log "  python3 visualize_scientific.py $CSV_OUTPUT"
