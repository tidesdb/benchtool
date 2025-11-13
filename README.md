
## Benchtool

Pluggable storage engine benchmarking tool with support for TidesDB and RocksDB.

## Features

Pluggable storage engine benchmarking tool supporting multiple storage engines including TidesDB and RocksDB. Configure workloads for write-only, read-only, delete-only, or mixed operations. Benchmark with multiple concurrent threads to test scalability. Choose from various key patterns including sequential, random, zipfian, uniform, timestamp, or reverse key generation. Get detailed metrics including throughput, latency percentiles (p50, p95, p99), and min/max values. Compare engines side-by-side and export results to file for analysis.

> [!NOTE]
> TidesDB and RocksDB are configured to match each other's configurations

## Build

```bash
mkdir build && cd build
cmake ..
make
```

## Command Line Options

```
Usage: bench [OPTIONS]

Options:
  -e, --engine <name>       Storage engine to benchmark (tidesdb, rocksdb)
  -o, --operations <num>    Number of operations (default: 100000)
  -k, --key-size <bytes>    Key size in bytes (default: 16)
  -v, --value-size <bytes>  Value size in bytes (default: 100)
  -t, --threads <num>       Number of threads (default: 1)
  -b, --batch-size <num>    Batch size for operations (default: 1)
  -d, --db-path <path>      Database path (default: ./bench_db)
  -c, --compare             Compare against RocksDB baseline
  -r, --report <file>       Output report to file (default: stdout)
  -s, --sequential          Use sequential keys instead of random
  -p, --pattern <type>      Key pattern: seq, random, zipfian, uniform, timestamp, reverse (default: random)
  -w, --workload <type>     Workload type: write, read, mixed, delete (default: mixed)
  -h, --help                Show help message
```

## Usage Examples

### Basic Benchmarks

```bash
# Benchmark TidesDB with default settings
./bench -e tidesdb

# Benchmark with 1 million operations
./bench -e tidesdb -o 1000000

# Benchmark with custom key/value sizes
./bench -e tidesdb -o 500000 -k 32 -v 1024
```

### Multi-threaded Benchmarks

```bash
# 4 threads, 500K operations
./bench -e tidesdb -t 4 -o 500000

# 8 threads, 1M operations
./bench -e tidesdb -t 8 -o 1000000
```

### Workload Types

```bash
# Write-only workload
./bench -e tidesdb -w write -o 1000000

# Read-only workload
./bench -e tidesdb -w read -o 1000000

# Delete-only workload
./bench -e tidesdb -w delete -o 1000000

# Mixed workload (default - writes then reads)
./bench -e tidesdb -w mixed -o 1000000
```

### Key Patterns

```bash
# Random keys (default)
./bench -e tidesdb -o 500000

# Sequential keys
./bench -e tidesdb -p seq -o 500000

# Zipfian distribution (hot keys - 80/20 rule)
./bench -e tidesdb -p zipfian -o 500000

# Uniform random distribution
./bench -e tidesdb -p uniform -o 500000

# Timestamp-based keys
./bench -e tidesdb -p timestamp -o 500000

# Reverse sequential keys
./bench -e tidesdb -p reverse -o 500000
```

### Comparison Mode

```bash
# Compare TidesDB vs RocksDB
./bench -e tidesdb -c -o 500000 -t 4

# Compare with custom settings
./bench -e tidesdb -c -o 1000000 -k 32 -v 512 -t 8
```

### Report Generation

```bash
# Generate report file
./bench -e tidesdb -o 1000000 -r results.txt

# Compare and save to file
./bench -e tidesdb -c -o 500000 -t 4 -r comparison.txt
```

## Benchmark Results

### TidesDB vs RocksDB (500K ops, 4 threads)

```
Configuration:
  Operations: 500,000
  Key Size: 16 bytes
  Value Size: 100 bytes
  Threads: 4
  Workload: Mixed

Results:
                TidesDB         RocksDB         Ratio
  PUT:          291K ops/sec    423K ops/sec    0.69x
  GET:          7.7M ops/sec    1.6M ops/sec    4.71x
  ITER:         12.8M ops/sec   5.3M ops/sec    2.42x

Latency (TidesDB):
  PUT avg:      13.38 μs
  PUT p50:      3.00 μs
  PUT p95:      93.00 μs
  PUT p99:      169.00 μs
  
  GET avg:      0.39 μs
  GET p50:      0.00 μs
  GET p95:      1.00 μs
  GET p99:      1.00 μs
```

## Adding New Engines
1. Create `engine_yourengine.c` implementing storage_engine_ops_t.  
2. Add to `engine_registry.c`
3. Update CMakeLists.txt