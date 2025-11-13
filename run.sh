#!/bin/bash

BENCH="./benchtool"
DB_PATH="/media/agpmastersystem/0e938ae2-7b41-437b-a5ac-a27e9f111a4e/db-bench"
RESULTS="benchmark_results.txt"


> "$RESULTS"

echo "===================================" | tee -a "$RESULTS"
echo "TidesDB vs RocksDB Benchmark Suite" | tee -a "$RESULTS"
echo "Date: $(date)" | tee -a "$RESULTS"
echo "===================================" | tee -a "$RESULTS"
echo "" | tee -a "$RESULTS"

run_bench() {
    echo "" | tee -a "$RESULTS"
    echo "Running: $BENCH $@" | tee -a "$RESULTS"
    rm -rf "$DB_PATH"
    $BENCH "$@" -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"
    echo "---" | tee -a "$RESULTS"
}

# Function for delete workloads - writes data first, then deletes
run_bench_delete() {
    local delete_args="$@"
    # Extract operation count and other params for write phase
    local write_args="${delete_args/-w delete/-w write}"
    
    echo "" | tee -a "$RESULTS"
    echo "Preparing data for delete test..." | tee -a "$RESULTS"
    rm -rf "$DB_PATH"
    $BENCH $write_args -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"
    
    echo "Running delete: $BENCH $delete_args" | tee -a "$RESULTS"
    $BENCH $delete_args -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"
    echo "---" | tee -a "$RESULTS"
}

echo "### 1. Single-Threaded Performance ###" | tee -a "$RESULTS"
run_bench -e tidesdb -c -w write -p seq -o 1000000
run_bench -e tidesdb -c -w write -p random -o 1000000
run_bench -e tidesdb -c -w mixed -p random -o 1000000

echo "### 2. Multi-Threaded Scalability ###" | tee -a "$RESULTS"
for threads in 2 4 8; do
   run_bench -e tidesdb -c -w write -t $threads -o 1000000
done

echo "### 3. Delete Workload Performance ###" | tee -a "$RESULTS"
run_bench_delete -e tidesdb -c -w delete -p seq -o 500000
run_bench_delete -e tidesdb -c -w delete -p random -o 500000 -t 4

echo "### 4. Mixed Workload Performance ###" | tee -a "$RESULTS"
run_bench -e tidesdb -c -w mixed -t 4 -o 1000000

echo "### 5. Key Pattern Performance ###" | tee -a "$RESULTS"
run_bench -e tidesdb -c -w mixed -p zipfian -o 500000 -t 4
run_bench -e tidesdb -c -w mixed -p timestamp -o 500000 -t 4

echo "### 6. Value Size Tests ###" | tee -a "$RESULTS"
run_bench -e tidesdb -c -w mixed -v 1024 -o 500000 -t 4
run_bench -e tidesdb -c -w mixed -k 8 -v 32 -o 1000000 -t 4

echo "### 7. High Concurrency (5M ops, 8 threads) ###" | tee -a "$RESULTS"
run_bench -e tidesdb -c -w write -o 5000000 -t 8

# Single-threaded Delete-only
echo "### 8. Single-threaded Delete-only ###" | tee -a "$RESULTS"
run_bench_delete -e tidesdb -c -w delete -p random -o 1000000

# Multi-threaded Delete (4 threads)
echo "### 9. Multi-threaded Delete (4 threads) ###" | tee -a "$RESULTS"
run_bench_delete -e tidesdb -c -w delete -p random -o 1000000 -t 4

# Hot Key Deletion (Zipfian)
echo "### 10. Hot Key Deletion (Zipfian) ###" | tee -a "$RESULTS"
run_bench_delete -e tidesdb -c -w delete -p zipfian -o 500000 -t 4

# Batch Size Tests - Writes
echo "### 11. Batch Size Tests - Writes ###" | tee -a "$RESULTS"
rm -rf "$DB_PATH"
$BENCH -e tidesdb -c -w write -p random -o 1000000 -b 10 -t 4 -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"
rm -rf "$DB_PATH"
$BENCH -e tidesdb -c -w write -p random -o 1000000 -b 100 -t 4 -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"
rm -rf "$DB_PATH"
$BENCH -e tidesdb -c -w write -p random -o 1000000 -b 1000 -t 4 -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"

# Batch Size Tests - Deletes
echo "### 12. Batch Size Tests - Deletes ###" | tee -a "$RESULTS"
run_bench_delete -e tidesdb -c -w delete -p random -o 1000000 -b 10 -t 4
run_bench_delete -e tidesdb -c -w delete -p random -o 1000000 -b 100 -t 4
run_bench_delete -e tidesdb -c -w delete -p random -o 1000000 -b 1000 -t 4

# Batch Size Tests - Mixed
echo "### 13. Batch Size Tests - Mixed ###" | tee -a "$RESULTS"
rm -rf "$DB_PATH"
$BENCH -e tidesdb -c -w mixed -p random -o 1000000 -b 100 -t 4 -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"
rm -rf "$DB_PATH"
$BENCH -e tidesdb -c -w mixed -p random -o 1000000 -b 1000 -t 4 -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"

# Batch Size Tests - Sequential
echo "### 14. Batch Size Tests - Sequential ###" | tee -a "$RESULTS"
rm -rf "$DB_PATH"
$BENCH -e tidesdb -c -w write -p seq -o 1000000 -b 100 -t 4 -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"
rm -rf "$DB_PATH"
$BENCH -e tidesdb -c -w write -p random -o 1000000 -b 100 -t 4 -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"

echo "" | tee -a "$RESULTS"
echo "===================================" | tee -a "$RESULTS"
echo "Benchmark Suite Complete!" | tee -a "$RESULTS"
echo "Results saved to: $RESULTS" | tee -a "$RESULTS"
echo "===================================" | tee -a "$RESULTS"