#!/bin/bash

set -e  # Exit on error

BENCH="./build/benchtool"
DB_PATH="db-bench"
RESULTS_DIR="benchmark_results"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
CSV_OUTPUT="${RESULTS_DIR}/benchmark_${TIMESTAMP}.csv"
RESULTS_TXT="${RESULTS_DIR}/benchmark_${TIMESTAMP}.txt"

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

mkdir -p "$RESULTS_DIR"

echo "test_name,engine,operation,ops_per_sec,duration_sec,avg_latency_us,stddev_us,cv_percent,p50_us,p95_us,p99_us,min_us,max_us,peak_rss_mb,peak_vms_mb,disk_read_mb,disk_write_mb,cpu_user_sec,cpu_sys_sec,cpu_percent,db_size_mb,write_amp,read_amp,space_amp,workload,pattern,ops_count,threads,batch_size,key_size,value_size" > "$CSV_OUTPUT"

echo "===================================" | tee "$RESULTS_TXT"
echo "TidesDB vs RocksDB Benchmark Suite" | tee -a "$RESULTS_TXT"
echo "Date: $(date)" | tee -a "$RESULTS_TXT"
echo "Sync Mode: $SYNC_MODE" | tee -a "$RESULTS_TXT"
echo "Default Batch Size: $DEFAULT_BATCH_SIZE" | tee -a "$RESULTS_TXT"
echo "Default Threads: $DEFAULT_THREADS" | tee -a "$RESULTS_TXT"
echo "CSV Output: $CSV_OUTPUT" | tee -a "$RESULTS_TXT"
echo "===================================" | tee -a "$RESULTS_TXT"
echo "" | tee -a "$RESULTS_TXT"

cleanup_db() {
    if [ -d "$DB_PATH" ]; then
        rm -rf "$DB_PATH"
    fi
}

# Run a single benchmark and append to CSV
# Args -- test_name, engine, workload, pattern, ops, threads, batch, key_size, value_size, extra_args
run_single_benchmark() {
    local test_name="$1"
    local engine="$2"
    local workload="$3"
    local pattern="$4"
    local ops="$5"
    local threads="$6"
    local batch="$7"
    local key_size="$8"
    local value_size="$9"
    shift 9
    local extra_args="$@"
    
    local temp_csv=$(mktemp)
    
    echo "  Running $engine..." | tee -a "$RESULTS_TXT"
    
    cleanup_db
    
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
    
    # Append CSV data (skip header) with test metadata
    if [ -f "$temp_csv" ]; then
        tail -n +2 "$temp_csv" | while IFS=, read -r eng op ops_sec dur avg std cv p50 p95 p99 min max rss vms dr dw cu cs cp db wa ra sa; do
            echo "$test_name,$eng,$op,$ops_sec,$dur,$avg,$std,$cv,$p50,$p95,$p99,$min,$max,$rss,$vms,$dr,$dw,$cu,$cs,$cp,$db,$wa,$ra,$sa,$workload,$pattern,$ops,$threads,$batch,$key_size,$value_size" >> "$CSV_OUTPUT"
        done
        rm -f "$temp_csv"
    fi
    
    cleanup_db
}

# Run comparison benchmark (both engines)
# Args -- test_name, workload, pattern, ops, threads, batch, key_size, value_size, extra_args
run_comparison() {
    local test_name="$1"
    local workload="$2"
    local pattern="$3"
    local ops="$4"
    local threads="$5"
    local batch="$6"
    local key_size="$7"
    local value_size="$8"
    shift 8
    local extra_args="$@"
    
    echo "" | tee -a "$RESULTS_TXT"
    echo "========================================" | tee -a "$RESULTS_TXT"
    echo "TEST: $test_name" | tee -a "$RESULTS_TXT"
    echo "========================================" | tee -a "$RESULTS_TXT"
    
    run_single_benchmark "$test_name" "tidesdb" "$workload" "$pattern" "$ops" "$threads" "$batch" "$key_size" "$value_size" $extra_args
    run_single_benchmark "$test_name" "rocksdb" "$workload" "$pattern" "$ops" "$threads" "$batch" "$key_size" "$value_size" $extra_args
}

# Run read/seek/range benchmark (requires pre-population)
# Args -- test_name, workload, pattern, ops, threads, batch, key_size, value_size, extra_args
run_read_workload() {
    local test_name="$1"
    local workload="$2"
    local pattern="$3"
    local ops="$4"
    local threads="$5"
    local batch="$6"
    local key_size="$7"
    local value_size="$8"
    shift 8
    local extra_args="$@"
    
    local temp_csv=$(mktemp)
    
    echo "" | tee -a "$RESULTS_TXT"
    echo "========================================" | tee -a "$RESULTS_TXT"
    echo "TEST: $test_name" | tee -a "$RESULTS_TXT"
    echo "========================================" | tee -a "$RESULTS_TXT"
    
    # TidesDB
    echo "  Populating TidesDB..." | tee -a "$RESULTS_TXT"
    cleanup_db
    $BENCH -e tidesdb -w write -p "$pattern" -o "$ops" -t "$threads" -b "$batch" -k "$key_size" -v "$value_size" $SYNC_FLAG -d "$DB_PATH" 2>&1 | tee -a "$RESULTS_TXT"
    
    echo "  Running TidesDB $workload..." | tee -a "$RESULTS_TXT"
    $BENCH -e tidesdb -w "$workload" -p "$pattern" -o "$ops" -t "$threads" -k "$key_size" -v "$value_size" $SYNC_FLAG -d "$DB_PATH" --csv "$temp_csv" $extra_args 2>&1 | tee -a "$RESULTS_TXT"
    
    if [ -f "$temp_csv" ]; then
        tail -n +2 "$temp_csv" | while IFS=, read -r eng op ops_sec dur avg std cv p50 p95 p99 min max rss vms dr dw cu cs cp db wa ra sa; do
            echo "$test_name,tidesdb,$op,$ops_sec,$dur,$avg,$std,$cv,$p50,$p95,$p99,$min,$max,$rss,$vms,$dr,$dw,$cu,$cs,$cp,$db,$wa,$ra,$sa,$workload,$pattern,$ops,$threads,$batch,$key_size,$value_size" >> "$CSV_OUTPUT"
        done
        rm -f "$temp_csv"
    fi
    
    # RocksDB
    echo "  Populating RocksDB..." | tee -a "$RESULTS_TXT"
    cleanup_db
    $BENCH -e rocksdb -w write -p "$pattern" -o "$ops" -t "$threads" -b "$batch" -k "$key_size" -v "$value_size" $SYNC_FLAG -d "$DB_PATH" 2>&1 | tee -a "$RESULTS_TXT"
    
    echo "  Running RocksDB $workload..." | tee -a "$RESULTS_TXT"
    $BENCH -e rocksdb -w "$workload" -p "$pattern" -o "$ops" -t "$threads" -k "$key_size" -v "$value_size" $SYNC_FLAG -d "$DB_PATH" --csv "$temp_csv" $extra_args 2>&1 | tee -a "$RESULTS_TXT"
    
    if [ -f "$temp_csv" ]; then
        tail -n +2 "$temp_csv" | while IFS=, read -r eng op ops_sec dur avg std cv p50 p95 p99 min max rss vms dr dw cu cs cp db wa ra sa; do
            echo "$test_name,rocksdb,$op,$ops_sec,$dur,$avg,$std,$cv,$p50,$p95,$p99,$min,$max,$rss,$vms,$dr,$dw,$cu,$cs,$cp,$db,$wa,$ra,$sa,$workload,$pattern,$ops,$threads,$batch,$key_size,$value_size" >> "$CSV_OUTPUT"
        done
        rm -f "$temp_csv"
    fi
    
    cleanup_db
}

# Run delete benchmark (requires pre-population)
run_delete_workload() {
    local test_name="$1"
    local pattern="$2"
    local ops="$3"
    local threads="$4"
    local batch="$5"
    local key_size="$6"
    local value_size="$7"
    
    local temp_csv=$(mktemp)
    
    echo "" | tee -a "$RESULTS_TXT"
    echo "========================================" | tee -a "$RESULTS_TXT"
    echo "TEST: $test_name" | tee -a "$RESULTS_TXT"
    echo "========================================" | tee -a "$RESULTS_TXT"
    
    # TidesDB
    echo "  Populating TidesDB..." | tee -a "$RESULTS_TXT"
    cleanup_db
    $BENCH -e tidesdb -w write -p "$pattern" -o "$ops" -t "$threads" -b "$batch" -k "$key_size" -v "$value_size" $SYNC_FLAG -d "$DB_PATH" 2>&1 | tee -a "$RESULTS_TXT"
    
    echo "  Running TidesDB delete..." | tee -a "$RESULTS_TXT"
    $BENCH -e tidesdb -w delete -p "$pattern" -o "$ops" -t "$threads" -b "$batch" -k "$key_size" -v "$value_size" $SYNC_FLAG -d "$DB_PATH" --csv "$temp_csv" 2>&1 | tee -a "$RESULTS_TXT"
    
    if [ -f "$temp_csv" ]; then
        tail -n +2 "$temp_csv" | while IFS=, read -r eng op ops_sec dur avg std cv p50 p95 p99 min max rss vms dr dw cu cs cp db wa ra sa; do
            echo "$test_name,tidesdb,$op,$ops_sec,$dur,$avg,$std,$cv,$p50,$p95,$p99,$min,$max,$rss,$vms,$dr,$dw,$cu,$cs,$cp,$db,$wa,$ra,$sa,delete,$pattern,$ops,$threads,$batch,$key_size,$value_size" >> "$CSV_OUTPUT"
        done
        rm -f "$temp_csv"
    fi
    
    # RocksDB
    echo "  Populating RocksDB..." | tee -a "$RESULTS_TXT"
    cleanup_db
    $BENCH -e rocksdb -w write -p "$pattern" -o "$ops" -t "$threads" -b "$batch" -k "$key_size" -v "$value_size" $SYNC_FLAG -d "$DB_PATH" 2>&1 | tee -a "$RESULTS_TXT"
    
    echo "  Running RocksDB delete..." | tee -a "$RESULTS_TXT"
    $BENCH -e rocksdb -w delete -p "$pattern" -o "$ops" -t "$threads" -b "$batch" -k "$key_size" -v "$value_size" $SYNC_FLAG -d "$DB_PATH" --csv "$temp_csv" 2>&1 | tee -a "$RESULTS_TXT"
    
    if [ -f "$temp_csv" ]; then
        tail -n +2 "$temp_csv" | while IFS=, read -r eng op ops_sec dur avg std cv p50 p95 p99 min max rss vms dr dw cu cs cp db wa ra sa; do
            echo "$test_name,rocksdb,$op,$ops_sec,$dur,$avg,$std,$cv,$p50,$p95,$p99,$min,$max,$rss,$vms,$dr,$dw,$cu,$cs,$cp,$db,$wa,$ra,$sa,delete,$pattern,$ops,$threads,$batch,$key_size,$value_size" >> "$CSV_OUTPUT"
        done
        rm -f "$temp_csv"
    fi
    
    cleanup_db
}

echo ""
echo "### SECTION 1 - Write Performance ###" | tee -a "$RESULTS_TXT"

# 1.1 Sequential Write (batched)
run_comparison "seq_write_10M" "write" "seq" 10000000 $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 16 100

# 1.2 Random Write (batched)
run_comparison "random_write_10M" "write" "random" 10000000 $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 16 100

# 1.3 Zipfian Write (hot keys)
run_comparison "zipfian_write_5M" "write" "zipfian" 5000000 $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 16 100

echo ""
echo "### SECTION 2 - Read Performance ###" | tee -a "$RESULTS_TXT"

# 2.1 Random Read
run_read_workload "random_read_10M" "read" "random" 10000000 $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 16 100

# 2.2 Sequential Read
run_read_workload "seq_read_10M" "read" "seq" 10000000 $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 16 100

echo ""
echo "### SECTION 3 - Mixed Workload ###" | tee -a "$RESULTS_TXT"

# 3.1 Mixed Random (50/50 read/write)
run_comparison "mixed_random_5M" "mixed" "random" 5000000 $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 16 100

# 3.2 Mixed Zipfian
run_comparison "mixed_zipfian_5M" "mixed" "zipfian" 5000000 $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 16 100

echo ""
echo "### SECTION 4 - Delete Performance ###" | tee -a "$RESULTS_TXT"

# 4.1 Random Delete
run_delete_workload "random_delete_5M" "random" 5000000 $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 16 100

echo ""
echo "### SECTION 5 - Seek Performance ###" | tee -a "$RESULTS_TXT"

# 5.1 Random Seek
run_read_workload "random_seek_5M" "seek" "random" 5000000 $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 16 100

# 5.2 Sequential Seek
run_read_workload "seq_seek_5M" "seek" "seq" 5000000 $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 16 100

echo ""
echo "### SECTION 6 - Range Query Performance ###" | tee -a "$RESULTS_TXT"

# 6.1 Range Scan 100 keys
run_read_workload "range_100_1M" "range" "random" 1000000 $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 16 100 "--range-size 100"

# 6.2 Range Scan 1000 keys
run_read_workload "range_1000_500K" "range" "random" 500000 $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 16 100 "--range-size 1000"

echo ""
echo "### SECTION 7 - Value Size Impact ###" | tee -a "$RESULTS_TXT"

# 7.1 Small Values (64B)
run_comparison "small_value_write" "write" "random" 20000000 $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 16 64

# 7.2 Medium Values (1KB)
run_comparison "medium_value_write" "write" "random" 5000000 $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 16 1024

# 7.3 Large Values (4KB)
run_comparison "large_value_write" "write" "random" 1000000 $DEFAULT_THREADS $DEFAULT_BATCH_SIZE 256 4096

echo ""
echo "### SECTION 8 - Batch Size Impact ###" | tee -a "$RESULTS_TXT"

# 8.1 No batching
run_comparison "batch_1" "write" "random" 5000000 $DEFAULT_THREADS 1 16 100

# 8.2 Small batch
run_comparison "batch_10" "write" "random" 5000000 $DEFAULT_THREADS 10 16 100

# 8.3 Medium batch
run_comparison "batch_100" "write" "random" 5000000 $DEFAULT_THREADS 100 16 100

# 8.4 Large batch
run_comparison "batch_1000" "write" "random" 5000000 $DEFAULT_THREADS 1000 16 100

# 8.5 Very large batch
run_comparison "batch_10000" "write" "random" 5000000 $DEFAULT_THREADS 10000 16 100

echo ""
echo "### SECTION 9 - Thread Scaling ###" | tee -a "$RESULTS_TXT"

# 9.1 Single thread
run_comparison "threads_1" "write" "random" 5000000 1 $DEFAULT_BATCH_SIZE 16 100

# 9.2 4 threads
run_comparison "threads_4" "write" "random" 5000000 4 $DEFAULT_BATCH_SIZE 16 100

# 9.3 8 threads
run_comparison "threads_8" "write" "random" 5000000 8 $DEFAULT_BATCH_SIZE 16 100

# 9.4 16 threads
run_comparison "threads_16" "write" "random" 5000000 16 $DEFAULT_BATCH_SIZE 16 100

cleanup_db

echo "" | tee -a "$RESULTS_TXT"
echo "===================================" | tee -a "$RESULTS_TXT"
echo "Benchmark Suite Complete!" | tee -a "$RESULTS_TXT"
echo "CSV Results: $CSV_OUTPUT" | tee -a "$RESULTS_TXT"
echo "Text Results: $RESULTS_TXT" | tee -a "$RESULTS_TXT"
echo "===================================" | tee -a "$RESULTS_TXT"
echo ""
echo "To generate graphs, run:"
echo "  python3 visualize_benchmark.py $CSV_OUTPUT"
