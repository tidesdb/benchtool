#!/bin/bash

BENCH="./build/benchtool"
DB_PATH="db-bench"
RESULTS="benchmark_results.txt"

# Set to "true" to enable fsync-fdatasync (durability), "false" for maximum performance
SYNC_ENABLED="false"

if [ "$SYNC_ENABLED" = "true" ]; then
    SYNC_FLAG="--sync"
    SYNC_MODE="ENABLED (durable writes)"
else
    SYNC_FLAG=""
    SYNC_MODE="DISABLED (maximum performance)"
fi

> "$RESULTS"

echo "===================================" | tee -a "$RESULTS"
echo "TidesDB-RocksDB Benchtool Runner" | tee -a "$RESULTS"
echo "Date: $(date)" | tee -a "$RESULTS"
echo "Sync Mode: $SYNC_MODE" | tee -a "$RESULTS"
echo "===================================" | tee -a "$RESULTS"
echo "" | tee -a "$RESULTS"

run_bench() {
    echo "" | tee -a "$RESULTS"
    echo "Running: $BENCH $@ $SYNC_FLAG" | tee -a "$RESULTS"
    rm -rf "$DB_PATH"
    $BENCH "$@" $SYNC_FLAG -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"
    echo "---" | tee -a "$RESULTS"
}

run_bench_delete() {
    local delete_args="$@"
    local write_args="${delete_args/-w delete/-w write}"
    
    echo "" | tee -a "$RESULTS"
    echo "Preparing data for delete test..." | tee -a "$RESULTS"
    rm -rf "$DB_PATH"
    $BENCH $write_args $SYNC_FLAG -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"
    
    echo "Running delete: $BENCH $delete_args $SYNC_FLAG" | tee -a "$RESULTS"
    $BENCH $delete_args $SYNC_FLAG -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"
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

echo "### 8. Single-threaded Delete-only ###" | tee -a "$RESULTS"
run_bench_delete -e tidesdb -c -w delete -p random -o 1000000

echo "### 9. Multi-threaded Delete (4 threads) ###" | tee -a "$RESULTS"
run_bench_delete -e tidesdb -c -w delete -p random -o 1000000 -t 4

echo "### 10. Hot Key Deletion (Zipfian) ###" | tee -a "$RESULTS"
run_bench_delete -e tidesdb -c -w delete -p zipfian -o 500000 -t 4

echo "### 11. Batch Size Tests - Writes ###" | tee -a "$RESULTS"
rm -rf "$DB_PATH"
$BENCH -e tidesdb -c -w write -p random -o 1000000 -b 10 -t 4 -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"
rm -rf "$DB_PATH"
$BENCH -e tidesdb -c -w write -p random -o 1000000 -b 100 -t 4 -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"
rm -rf "$DB_PATH"
$BENCH -e tidesdb -c -w write -p random -o 1000000 -b 1000 -t 4 -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"

echo "### 12. Batch Size Tests - Deletes ###" | tee -a "$RESULTS"
run_bench_delete -e tidesdb -c -w delete -p random -o 1000000 -b 10 -t 4
run_bench_delete -e tidesdb -c -w delete -p random -o 1000000 -b 100 -t 4
run_bench_delete -e tidesdb -c -w delete -p random -o 1000000 -b 1000 -t 4

echo "### 13. Batch Size Tests - Mixed ###" | tee -a "$RESULTS"
rm -rf "$DB_PATH"
$BENCH -e tidesdb -c -w mixed -p random -o 1000000 -b 100 -t 4 -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"
rm -rf "$DB_PATH"
$BENCH -e tidesdb -c -w mixed -p random -o 1000000 -b 1000 -t 4 -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"

echo "### 14. Batch Size Tests - Sequential ###" | tee -a "$RESULTS"
rm -rf "$DB_PATH"
$BENCH -e tidesdb -c -w write -p seq -o 1000000 -b 100 -t 4 -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"
rm -rf "$DB_PATH"
$BENCH -e tidesdb -c -w write -p random -o 1000000 -b 100 -t 4 -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"

echo "### 15. Large-Scale Workloads (10M keys) ###" | tee -a "$RESULTS"
run_bench -e tidesdb -c -w write -p random -o 10000000 -t 8
run_bench -e tidesdb -c -w mixed -p random -o 10000000 -t 8
run_bench -e tidesdb -c -w read -p random -o 10000000 -t 8

echo "### 16. Large-Scale Workloads (50M keys) ###" | tee -a "$RESULTS"
run_bench -e tidesdb -c -w write -p random -o 50000000 -t 8
run_bench -e tidesdb -c -w mixed -p random -o 50000000 -t 8

echo "### 17. Large-Scale Workloads (100M keys) ###" | tee -a "$RESULTS"
run_bench -e tidesdb -c -w write -p seq -o 100000000 -t 8
run_bench -e tidesdb -c -w write -p random -o 100000000 -t 8

echo "### 18. Large-Scale Workloads (250M keys) ###" | tee -a "$RESULTS"
run_bench -e tidesdb -c -w write -p seq -o 250000000 -t 8

echo "### 19. Large-Scale Delete (10M keys) ###" | tee -a "$RESULTS"
run_bench_delete -e tidesdb -c -w delete -p random -o 10000000 -t 8

echo "### 20. Large Value Size Tests (4KB values) ###" | tee -a "$RESULTS"
run_bench -e tidesdb -c -w write -p random -o 500000 -v 4096 -t 4
run_bench -e tidesdb -c -w mixed -p random -o 500000 -v 4096 -t 4
run_bench -e tidesdb -c -w read -p random -o 500000 -v 4096 -t 4

echo "### 21. Large Value Size Tests (16KB values) ###" | tee -a "$RESULTS"
run_bench -e tidesdb -c -w write -p random -o 250000 -v 16384 -t 4
run_bench -e tidesdb -c -w mixed -p random -o 250000 -v 16384 -t 4

echo "### 22. Large Value Size Tests (64KB values) ###" | tee -a "$RESULTS"
run_bench -e tidesdb -c -w write -p random -o 100000 -v 65536 -t 4
run_bench -e tidesdb -c -w read -p random -o 100000 -v 65536 -t 4

echo "### 23. Read-Heavy Workloads (90% read / 10% write simulation) ###" | tee -a "$RESULTS"
echo "Note: Using read workload as approximation for read-heavy pattern" | tee -a "$RESULTS"
run_bench -e tidesdb -c -w read -p zipfian -o 5000000 -t 8
run_bench -e tidesdb -c -w read -p random -o 5000000 -t 8
run_bench -e tidesdb -c -w mixed -p zipfian -o 2000000 -t 8

echo "### 24. Iteration Performance at Scale ###" | tee -a "$RESULTS"
echo "Testing full scan performance on large datasets" | tee -a "$RESULTS"
run_bench -e tidesdb -c -w write -p seq -o 10000000 -t 8
echo "Note: Iteration stats included in above write benchmark" | tee -a "$RESULTS"
run_bench -e tidesdb -c -w write -p seq -o 50000000 -t 8

echo "### 25. Sustained Write Performance (500M keys) ###" | tee -a "$RESULTS"
echo "Long-running test to detect performance degradation" | tee -a "$RESULTS"
run_bench -e tidesdb -c -w write -p seq -o 500000000 -t 8

echo "### 26. Sustained Write Performance (1B keys) ###" | tee -a "$RESULTS"
echo "Maximum scale test - may take significant time" | tee -a "$RESULTS"
run_bench -e tidesdb -c -w write -p seq -o 1000000000 -t 8

echo "### 27. Hot Key Scenarios (Zipfian at Scale) ###" | tee -a "$RESULTS"
echo "80/20 access pattern on large dataset" | tee -a "$RESULTS"
run_bench -e tidesdb -c -w write -p zipfian -o 50000000 -t 8
run_bench -e tidesdb -c -w mixed -p zipfian -o 50000000 -t 8
run_bench -e tidesdb -c -w read -p zipfian -o 50000000 -t 8

echo "### 28. Recovery/Reopen Tests ###" | tee -a "$RESULTS"
echo "Testing database reopen and read performance after write" | tee -a "$RESULTS"
echo "Step 1: Write large dataset" | tee -a "$RESULTS"
rm -rf "$DB_PATH"
$BENCH -e tidesdb -w write -p random -o 10000000 -t 8 -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"
echo "Step 2: Reopen and read (simulated by new benchmark run)" | tee -a "$RESULTS"
$BENCH -e tidesdb -w read -p random -o 10000000 -t 8 -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"
echo "Step 3: Compare with RocksDB" | tee -a "$RESULTS"
rm -rf "$DB_PATH"
$BENCH -e rocksdb -w write -p random -o 10000000 -t 8 -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"
$BENCH -e rocksdb -w read -p random -o 10000000 -t 8 -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"

echo "### 29. Concurrent Mixed Operations (High Contention) ###" | tee -a "$RESULTS"
echo "Simultaneous reads and writes with varying thread counts" | tee -a "$RESULTS"
for threads in 4 8 16; do
    run_bench -e tidesdb -c -w mixed -p zipfian -o 5000000 -t $threads
done

echo "### 30. Space Efficiency Comparison ###" | tee -a "$RESULTS"
echo "Testing space amplification with different key/value sizes" | tee -a "$RESULTS"
echo "Small keys, small values (8 bytes / 32 bytes)" | tee -a "$RESULTS"
run_bench -e tidesdb -c -w write -k 8 -v 32 -o 5000000 -t 8
echo "Medium keys, medium values (64 bytes / 256 bytes)" | tee -a "$RESULTS"
run_bench -e tidesdb -c -w write -k 64 -v 256 -o 2000000 -t 8
echo "Large keys, large values (256 bytes / 4096 bytes)" | tee -a "$RESULTS"
run_bench -e tidesdb -c -w write -k 256 -v 4096 -o 500000 -t 8
echo "Variable size comparison complete - check space amplification metrics" | tee -a "$RESULTS"

echo "" | tee -a "$RESULTS"
echo "===================================" | tee -a "$RESULTS"
echo "Comprehensive Benchmark Suite Complete!" | tee -a "$RESULTS"
echo "Results saved to: $RESULTS" | tee -a "$RESULTS"
echo "===================================" | tee -a "$RESULTS"