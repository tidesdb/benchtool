#!/bin/bash
# TidesDB Stall Inspection Script
# There are paths in TidesDB that have caused latency spikes, these benchmarks are designed to reproduce those scenarios.

set -e

BENCH_BIN="./build/benchtool"
DB_PATH="tidesdb_latency_test"
RESULTS="latency_inspection_results.txt"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

cleanup() {
    rm -rf "$DB_PATH"
}

print_header() {
    echo ""
    echo "========================================"
    echo -e "${YELLOW}$1${NC}"
    echo "========================================"
}

> "$RESULTS"

echo "===================================" | tee -a "$RESULTS"
echo "TidesDB Latency Stall Inspection" | tee -a "$RESULTS"
echo "Date: $(date)" | tee -a "$RESULTS"
echo "===================================" | tee -a "$RESULTS"

cleanup

print_header "TidesDB Latency Stall Inspection"
echo "This script runs targeted tests to identify latency spikes." | tee -a "$RESULTS"
echo "Enable TidesDB debug mode for detailed flush/compaction logs." | tee -a "$RESULTS"
echo "" | tee -a "$RESULTS"

echo "" | tee -a "$RESULTS"
echo "========================================" | tee -a "$RESULTS"
echo "Random Write 10M ops (REPRODUCE HIGH CV)" | tee -a "$RESULTS"
echo "========================================" | tee -a "$RESULTS"
echo "10M ops, 8 threads, batch=1000, random keys" | tee -a "$RESULTS"
cleanup

$BENCH_BIN \
    -e tidesdb \
    -o 10000000 \
    -k 16 \
    -v 100 \
    -t 8 \
    -b 1000 \
    -p random \
    -w write \
    -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"

echo "" | tee -a "$RESULTS"

echo "" | tee -a "$RESULTS"
echo "========================================" | tee -a "$RESULTS"
echo "No Batching 10M ops (REPRODUCE EXTREME CV)" | tee -a "$RESULTS"
echo "========================================" | tee -a "$RESULTS"
echo "10M ops, 8 threads, batch=1, random keys" | tee -a "$RESULTS"
cleanup

$BENCH_BIN \
    -e tidesdb \
    -o 10000000 \
    -k 16 \
    -v 100 \
    -t 8 \
    -b 1 \
    -p random \
    -w write \
    -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"

echo "" | tee -a "$RESULTS"

echo "" | tee -a "$RESULTS"
echo "========================================" | tee -a "$RESULTS"
echo "Small Values 50M ops (REPRODUCE WORST CASE)" | tee -a "$RESULTS"
echo "========================================" | tee -a "$RESULTS"
echo "50M ops, 8 threads, batch=1000, 64B values" | tee -a "$RESULTS"
cleanup

$BENCH_BIN \
    -e tidesdb \
    -o 50000000 \
    -k 16 \
    -v 64 \
    -t 8 \
    -b 1000 \
    -p random \
    -w write \
    -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"

echo "" | tee -a "$RESULTS"


echo "" | tee -a "$RESULTS"
echo "========================================" | tee -a "$RESULTS"
echo "Mixed Workload 5M ops" | tee -a "$RESULTS"
echo "========================================" | tee -a "$RESULTS"
cleanup

$BENCH_BIN \
    -e tidesdb \
    -o 5000000 \
    -k 16 \
    -v 100 \
    -t 8 \
    -b 1000 \
    -p random \
    -w mixed \
    -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"

echo "" | tee -a "$RESULTS"

echo "" | tee -a "$RESULTS"
echo "========================================" | tee -a "$RESULTS"
echo "Small Memtable Sustained Pressure (5M ops)" | tee -a "$RESULTS"
echo "========================================" | tee -a "$RESULTS"
cleanup

$BENCH_BIN \
    -e tidesdb \
    -o 5000000 \
    -k 16 \
    -v 100 \
    -t 8 \
    -b 1000 \
    -p random \
    -w write \
    --memtable-size 4194304 \
    -d "$DB_PATH" 2>&1 | tee -a "$RESULTS"


echo "" | tee -a "$RESULTS"
echo "========================================" | tee -a "$RESULTS"
echo "Inspection Complete" | tee -a "$RESULTS"
echo "========================================" | tee -a "$RESULTS"
echo "Results saved to: $RESULTS" | tee -a "$RESULTS"

cleanup
