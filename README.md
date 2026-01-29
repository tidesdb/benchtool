
# Benchtool

A comprehensive storage engine benchmarking tool supporting TidesDB and RocksDB with pluggable architecture. Benchtool measures performance across write-only, read-only, delete-only, and mixed workloads with configurable concurrency for scalability testing. Key generation patterns include sequential, random, zipfian (hot keys), uniform, timestamp-based, and reverse sequential access. 

The tool provides detailed performance metrics including throughput, latency distributions (p50, p95, p99), and operation durations. Resource monitoring tracks memory usage (RSS/VMS), disk I/O, CPU utilization, and database size. 

Amplification metrics reveal write, read, and space efficiency in which is critical for understanding SSD wear and storage overhead. Side-by-side engine comparisons and exportable reports enable thorough performance analysis.

> [!NOTE]
> TidesDB and RocksDB are configured to match each other's configurations for a fair comparison.

## Build
```bash
rm -rf build && mkdir build && cd build
cmake .. -DENABLE_ASAN=OFF -DENABLE_UBSAN=OFF
make
cd ..
```

### Custom Library Paths

If TidesDB or RocksDB are installed in non-standard locations, specify their paths:

```bash
cmake -B build \
  -DTIDESDB_BUILD_DIR=/path/to/tidesdb/build \
  -DTIDESDB_INCLUDE_DIR=/path/to/tidesdb/include \
  -DROCKSDB_DIR=/path/to/rocksdb
```

| Flag | Description |
|------|-------------|
| `TIDESDB_BUILD_DIR` | Directory containing `libtidesdb.so` / `libtidesdb.a` |
| `TIDESDB_INCLUDE_DIR` | Directory containing TidesDB headers |
| `ROCKSDB_DIR` | RocksDB root directory (searches `lib/` and `include/` subdirs) |

## Command Line Options
```
Usage: benchtool [OPTIONS]

Options:
  -e, --engine <name>            Storage engine to benchmark (tidesdb, rocksdb)
  -o, --operations <num>         Number of operations (default: 10000000)
  -k, --key-size <bytes>         Key size in bytes (default: 16)
  -v, --value-size <bytes>       Value size in bytes (default: 100)
  -t, --threads <num>            Number of threads (default: 4)
  -b, --batch-size <num>         Batch size for operations (default: 1)
  -d, --db-path <path>           Database path (default: ./bench_db)
  -s, --sequential               Shortcut for sequential key pattern
  -c, --compare                  Compare against RocksDB baseline
  -r, --report <file>            Output report to file (default: stdout)
  --csv <file>                   Export results to CSV file for graphing
  -p, --pattern <type>           Key pattern: seq, random, zipfian, uniform, timestamp, reverse (default: random)
  -w, --workload <type>          Workload type: write, read, mixed, delete, seek, range (default: mixed)
  --range-size <num>             Number of keys to iterate in range queries (default: 100)
  --sync                         Enable fsync for durable writes (slower)
  --memtable-size <bytes>        Engine write-buffer / memtable size (0 = engine default)
  --block-cache-size <bytes>     Block cache size (0 = engine default)
  --test-name <name>             Tag results with a test name in CSV output
  --rocksdb-blobdb / --no-rocksdb-blobdb  Force-enable/disable RocksDB BlobDB
  --bloom-filters / --no-bloom-filters    Force-enable/disable bloom filters
  --block-indexes / --no-block-indexes    Force-enable/disable block indexes
  --bloom-fp <ratio>             Bloom filter false-positive rate (default 0.01)
  --l0_queue_stall_threshold <n> TidesDB L0 stall threshold (default 10)
  --l1_file_count_trigger <n>    TidesDB L1 file trigger count (default 4)
  --dividing_level_offset <n>    TidesDB dividing level offset (default 2)
  --min_levels <n>               TidesDB minimum tree levels (default 5)
  --index_sample_ratio <n>       TidesDB index sample ratio (default 1)
  --block_index_prefix_len <n>   TidesDB block index prefix length (default 16)
  --klog_value_threshold <bytes> TidesDB/ RocksDB (BlobDB) KLog value threshold (default 512)
  -h, --help                     Show help message
```

## Runners
The benchtool has default runners such as

- `large_value_benchmark.sh` - 8KB value suite (PUT/GET/SEEK/RANGE) with both engines
- `large_value_benchmark_1gb.sh` - 1GB value suite (PUT/GET/RANGE) with both engines
- `tidesdb_rocksdb.sh` - main mixed workload comparison suite
- `tidesdb_rocksdb_synced.sh` - synced (durable) write suite with reduced ops for practicality
- `tidesdb_rocksdb_single_threaded.sh` - single-threaded comparison suite
- `tidesdb_rocksdb_no_bloom_indexes.sh` - comparison suite with bloom filters and block indexes disabled
- `tidesdb_rocksdb_extensive.sh` - extensive multi-run suite with warm/cold, scaling, and sweeps
- `tidesdb_rocksdb_one_billion.sh` - 1B key write/read/seek/range + 500M delete, 8 threads, memtable 128MB, cache 8GB
- `tidesdb_allocator_benchmark.sh` - allocator comparison suite (i.e `./tidesdb_allocator_benchmark.sh --preload --allocator all`)

## Graphs
Generate image-only graphs from any benchtool CSV with `graphgen.py`:

```bash
python3 graphgen.py <csv_file> [output_dir]
```

Notes:
- CSV includes `test_name` (from `--test-name`) to uniquely identify each run, including populate steps tagged with `_populate`.
- Graphgen adapts to any CSV by checking available columns; it outputs throughput, latency averages + percentiles, variability (CV%) + stddev, resource usage (CPU/IO/memory/duration), amplification, and parameter sweep plots when the data exists.


## Usage Examples

### Basic Benchmarks

```bash
# Benchmark TidesDB with default settings
./benchtool -e tidesdb

# Benchmark with 1 million operations
./benchtool -e tidesdb -o 1000000

# Benchmark with custom key/value sizes
./benchtool -e tidesdb -o 500000 -k 32 -v 1024
```

### Multi-threaded Benchmarks

```bash
# 4 threads, 500K operations
./benchtool -e tidesdb -t 4 -o 500000

# 8 threads, 1M operations
./benchtool -e tidesdb -t 8 -o 1000000
```

### Workload Types

```bash
# Write-only workload
./benchtool -e tidesdb -w write -o 1000000

# Read-only workload
./benchtool -e tidesdb -w read -o 1000000

# Delete-only workload
./benchtool -e tidesdb -w delete -o 1000000

# Mixed workload (default - writes then reads)
./benchtool -e tidesdb -w mixed -o 1000000

# Seek workload (point seeks to specific keys)
./benchtool -e tidesdb -w seek -o 1000000

# Range query workload (seek + iterate N keys)
./benchtool -e tidesdb -w range -o 500000 --range-size 100
```

### Key Patterns

```bash
# Random keys (default)
./benchtool -e tidesdb -o 500000

# Sequential keys
./benchtool -e tidesdb -p seq -o 500000

# Zipfian distribution (hot keys - 80/20 rule)
./benchtool -e tidesdb -p zipfian -o 500000

# Uniform random distribution
./benchtool -e tidesdb -p uniform -o 500000

# Timestamp-based keys
./benchtool -e tidesdb -p timestamp -o 500000

# Reverse sequential keys
./benchtool -e tidesdb -p reverse -o 500000
```

### Seek and Range Query Benchmarks

```bash
# Random point seeks (tests block index effectiveness)
./benchtool -e tidesdb -w seek -p random -o 1000000 -t 8

# Sequential seeks
./benchtool -e tidesdb -w seek -p seq -o 1000000 -t 8

# Hot key seeks (Zipfian distribution)
./benchtool -e tidesdb -w seek -p zipfian -o 1000000 -t 8

# Range queries - scan 100 keys per operation
./benchtool -e tidesdb -w range -p random -o 500000 -t 8 --range-size 100

# Range queries - scan 1000 keys per operation
./benchtool -e tidesdb -w range -p random -o 100000 -t 8 --range-size 1000

# Sequential range scans (best case for iterators)
./benchtool -e tidesdb -w range -p seq -o 500000 -t 8 --range-size 100

# Compare seek performance: TidesDB vs RocksDB
./benchtool -e tidesdb -c -w seek -p random -o 1000000 -t 8

# Compare range query performance
./benchtool -e tidesdb -c -w range -p random -o 500000 -t 8 --range-size 100
```

Seek benchmarks test the effectiveness of block indexes and bloom filters for point lookups. Range queries measure iterator performance and cache effectiveness for scanning multiple consecutive keys. The `--range-size` parameter controls how many keys are iterated per range operation, allowing you to test different scan lengths.

### Comparison Mode

```bash
# Compare TidesDB vs RocksDB
./benchtool -e tidesdb -c -o 500000 -t 4

# Compare with custom settings
./benchtool -e tidesdb -c -o 1000000 -k 32 -v 512 -t 8
```

### Report Generation

```bash
# Generate report file
./benchtool -e tidesdb -o 1000000 -r results.txt

# Compare and save to file
./benchtool -e tidesdb -c -o 500000 -t 4 -r comparison.txt

# Export results to CSV for graphing
./benchtool -e tidesdb -c -o 500000 -t 4 --csv results.csv

# Generate both report and CSV
./benchtool -e tidesdb -c -o 500000 -t 4 -r report.txt --csv results.csv
```

### Durability Options

```bash
# Default: Fast mode (no fsync)
./benchtool -e tidesdb -w write -o 1000000

# Durable mode: Enable fsync for crash-safe writes (slower)
./benchtool -e tidesdb -w write -o 1000000 --sync

# Compare performance impact of sync
./benchtool -e tidesdb -c -w write -o 1000000 --sync
```

The `--sync` flag enables fsync after each write operation, ensuring data is persisted to disk. This provides durability guarantees but significantly reduces write throughput. Use this to test worst-case performance or when crash recovery is critical.

## RocksDB Optimizations

The RocksDB engine configuration has been optimized for fair and accurate benchmarking. The cache uses HyperClockCache instead of LRU (recommended by RocksDB team for better concurrency) with 64 MB size to match TidesDB configuration. Index configuration employs binary search index rather than two-level index for better read performance, pins L0 index and filter blocks in cache for faster hot data access, and reduces index lookup overhead during benchmarks. Checksums use the default CRC32c (XXH3 would be ideal to match TidesDB but is not exposed in RocksDB's C API). Bloom filters are configured at 10 bits per key to match TidesDB, reducing unnecessary disk reads for non-existent keys. Compression and memtable settings include LZ4 compression to match TidesDB, 64 MB write buffer (memtable) size, and 8 background jobs for flush and compaction operations.

These optimizations ensure RocksDB runs at peak performance for fair comparison with TidesDB.

## Metrics

Benchtool provides performance and resource metrics

### Performance Metrics

The benchmark measures throughput as operations per second for PUT, GET, DELETE, and ITER operations, providing a clear picture of how fast each storage engine can handle different workload types. Latency statistics capture the complete distribution of operation times, including average latency, standard deviation, coefficient of variation (CV%), median (p50), 95th percentile (p95), 99th percentile (p99), as well as minimum and maximum values in microseconds. The coefficient of variation (stddev/mean × 100) helps identify inconsistent performance—high CV% indicates variable latency. Duration tracking shows the total wall-clock time spent on each operation type, helping identify which operations dominate the overall benchmark runtime.

### Resource Metrics

Resource monitoring tracks actual system-level consumption throughout the benchmark. Memory usage is measured through peak RSS (Resident Set Size), which represents the actual physical memory used by the process, and peak VMS (Virtual Memory Size), which shows the total virtual memory allocated. Disk I/O metrics capture bytes read from and written to disk via `/proc/self/io`, providing accurate system-level measurements that reflect the true storage cost of operations. CPU usage is broken down into user time (spent executing application code) and system time (spent in kernel operations), with an overall CPU utilization percentage showing how efficiently the benchmark uses available CPU resources. The total on-disk database size is measured after all operations complete, revealing the actual storage footprint.

### Amplification Factors

Amplification metrics help understand the efficiency of storage engines by measuring the overhead of database operations. Write amplification is the ratio of bytes written to disk versus logical data written, calculated as `disk_bytes_written / (num_operations × (key_size + value_size))`. Lower values are better, with 1.0x representing ideal performance with no amplification. This metric is particularly important for SSD wear and write performance, as excessive write amplification can significantly reduce SSD lifespan. Read amplification measures the ratio of bytes read from disk versus logical data read (`disk_bytes_read / logical_bytes_read`), indicating how efficiently the storage engine retrieves data. Space amplification is the ratio of disk space used versus logical data size (`db_size_on_disk / logical_data_size`), with lower values being better and 1.0x representing no overhead. This metric includes the cost of indexes, metadata, and fragmentation, revealing the true storage efficiency of each engine.

### Comparison Mode

When using `-c` flag, benchtool compares TidesDB against RocksDB and provides:

**Full results for both engines**
- Complete latency statistics (avg, stddev, CV%, p50, p95, p99, min, max)
- Throughput (ops/sec) and duration
- Resource usage (memory, disk I/O, CPU, database size)
- Amplification factors (write, read, space)

**Side-by-side comparisons**
- Throughput comparison with speedup ratios
- Latency comparison (avg, p99, max, CV%) for each operation type
- Resource usage comparison
- Amplification factor comparison

## Adding New Engines
1. Create `engine_yourengine.c` implementing storage_engine_ops_t.  
2. Add to `engine_registry.c`
3. Update CMakeLists.txt
