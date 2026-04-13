
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
| `TIDESDB_BUILD_DIR` | Directory containing `libtidesdb.so` / `libtidesdb.a` (auto-creates symlink for `tidesdb_version.h`) |
| `TIDESDB_INCLUDE_DIR` | Directory containing TidesDB headers |
| `ROCKSDB_DIR` | RocksDB root directory (searches `lib/` and `include/` subdirs) |
| `ROCKSDB_MAJOR` | RocksDB major version number (for version display) |
| `ROCKSDB_MINOR` | RocksDB minor version number |
| `ROCKSDB_PATCH` | RocksDB patch version number |
| `BENCHTOOL_WITH_S3` | Enable the `--object-store s3` option. Requires TidesDB to be built with `-DTIDESDB_WITH_S3=ON` so that `tidesdb_objstore_s3_create` is exported. |

> **Note:** When `TIDESDB_BUILD_DIR` contains `tidesdb_version.h`, CMake automatically creates a symlink so the include path `<tidesdb/tidesdb_version.h>` resolves correctly.

Example with all options:
```bash
cmake -B build \
  -DTIDESDB_BUILD_DIR=/path/to/tidesdb/build \
  -DTIDESDB_INCLUDE_DIR=/path/to/tidesdb/include \
  -DROCKSDB_DIR=/path/to/rocksdb \
  -DROCKSDB_MAJOR=10 -DROCKSDB_MINOR=11 -DROCKSDB_PATCH=0
```

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
  --debug                        Enable debug logging for storage engines
  --use-btree                    Use B+tree format for klog (TidesDB only)
  -h, --help                     Show help message
```

### TidesDB Engine-Level Configuration

These map onto fields of `tidesdb_config_t` and are passed to `tidesdb_open`. When omitted, benchtool keeps its existing defaults (single flush/compaction thread, log-to-file enabled) for backward compatibility with prior runs.

| Option | TidesDB field | Default |
|--------|---------------|---------|
| `--num-flush-threads <n>` | `num_flush_threads` | benchtool: `1` (TidesDB lib default: `2`) |
| `--num-compaction-threads <n>` | `num_compaction_threads` | benchtool: `1` (TidesDB lib default: `2`) |
| `--max-open-sstables <n>` | `max_open_sstables` | `256` |
| `--max-memory-usage <bytes>` | `max_memory_usage` | `0` (auto, 50% of RAM) |
| `--log-to-file <0\|1>` | `log_to_file` | `1` (write `LOG` file in db dir) |

### TidesDB Column-Family Tuning

Additional knobs on `tidesdb_column_family_config_t` not previously exposed:

| Option | TidesDB field | Default |
|--------|---------------|---------|
| `--compression <algo>` | `compression_algorithm` | `lz4` (also: `none`, `lz4fast`, `zstd`, `snappy`) |
| `--skip-list-max-level <n>` | `skip_list_max_level` | `12` |
| `--skip-list-probability <p>` | `skip_list_probability` | `0.25` |
| `--level-size-ratio <n>` | `level_size_ratio` | `10` |
| `--sync-mode <mode>` | `sync_mode` | derived from `--sync` (use `none`, `full`, or `interval`) |
| `--sync-interval-us <us>` | `sync_interval_us` | `128000` (only relevant for `interval`) |

### TidesDB Unified Memtable Mode

Unified memtable replaces all per-CF skip lists and WALs with a single shared skip list and single WAL at the database level, reducing N WAL writes per multi-CF transaction to one.

| Option | TidesDB field | Default |
|--------|---------------|---------|
| `--unified-memtable` | `unified_memtable = 1` | off |
| `--unified-memtable-size <bytes>` | `unified_memtable_write_buffer_size` | `0` → 64 MB |
| `--unified-memtable-skip-list-max-level <n>` | `unified_memtable_skip_list_max_level` | `0` → 12 |
| `--unified-memtable-skip-list-probability <p>` | `unified_memtable_skip_list_probability` | `0` → 0.25 |
| `--unified-memtable-sync-mode <mode>` | `unified_memtable_sync_mode` | `none` |
| `--unified-memtable-sync-interval-us <us>` | `unified_memtable_sync_interval_us` | `0` |

```bash
# Run a write workload with unified memtable enabled
./benchtool -e tidesdb --unified-memtable -w write -o 5000000

# Pair with a custom sync mode + interval
./benchtool -e tidesdb --unified-memtable \
  --unified-memtable-sync-mode interval \
  --unified-memtable-sync-interval-us 200000 -o 1000000
```

> **Note:** Even though benchtool only uses one column family, unified memtable still affects WAL layout and write batching, so the option is useful for measuring its overhead in the single-CF case.

### TidesDB Object Store Mode

Object store mode places SSTables in a remote (or filesystem-backed) object store and uses local disk as a cache. Setting `--object-store` automatically enables unified memtable mode (as required by TidesDB).

Two backends are supported:
- `fs` — local filesystem connector, useful for testing (always available).
- `s3` — S3 / MinIO / GCS-compatible connector, requires `-DBENCHTOOL_WITH_S3=ON` at CMake time **and** TidesDB built with `-DTIDESDB_WITH_S3=ON`.

| Option | Where it lands | Default |
|--------|---------------|---------|
| `--object-store <none\|fs\|s3>` | selects connector | `none` |
| `--object-store-fs-path <dir>` | `tidesdb_objstore_fs_create(root_dir)` | required for `fs` |
| `--s3-endpoint <host[:port]>` | `tidesdb_objstore_s3_create` | required for `s3` |
| `--s3-bucket <name>` | s3 connector | required |
| `--s3-prefix <prefix>` | s3 connector | unset |
| `--s3-access-key <key>` | s3 connector | required |
| `--s3-secret-key <key>` | s3 connector | required |
| `--s3-region <region>` | s3 connector | unset (use for AWS, leave unset for MinIO) |
| `--s3-no-ssl` | `use_ssl = 0` | HTTPS by default |
| `--s3-path-style` | `use_path_style = 1` | virtual-hosted (set for MinIO) |
| `--object-local-cache-path <dir>` | `tidesdb_objstore_config_t.local_cache_path` | db_path |
| `--object-local-cache-max-bytes <b>` | `local_cache_max_bytes` | `0` (unlimited) |
| `--object-cache-on-read <0\|1>` | `cache_on_read` | `1` |
| `--object-cache-on-write <0\|1>` | `cache_on_write` | `1` |
| `--object-max-concurrent-uploads <n>` | `max_concurrent_uploads` | `4` |
| `--object-max-concurrent-downloads <n>` | `max_concurrent_downloads` | `8` |
| `--object-multipart-threshold <bytes>` | `multipart_threshold` | 64 MB |
| `--object-multipart-part-size <bytes>` | `multipart_part_size` | 8 MB |
| `--object-sync-manifest <0\|1>` | `sync_manifest_to_object` | `1` |
| `--object-replicate-wal <0\|1>` | `replicate_wal` | `1` |
| `--object-wal-upload-sync <0\|1>` | `wal_upload_sync` | `0` |
| `--object-wal-sync-threshold <bytes>` | `wal_sync_threshold_bytes` | 1 MB |
| `--object-wal-sync-on-commit <0\|1>` | `wal_sync_on_commit` | `0` (RPO=0 when `1`) |
| `--object-replica-mode <0\|1>` | `replica_mode` | `0` |
| `--object-replica-sync-interval-us <us>` | `replica_sync_interval_us` | 5 000 000 (5 s) |
| `--object-replica-replay-wal <0\|1>` | `replica_replay_wal` | `1` |
| `--object-lazy-compaction <0\|1>` | per-CF `object_lazy_compaction` | `0` |
| `--object-prefetch-compaction <0\|1>` | per-CF `object_prefetch_compaction` | `1` |

```bash
# Local filesystem connector — exercises the object store code path without needing S3
./benchtool -e tidesdb \
  --object-store fs --object-store-fs-path /var/tmp/tidesdb-objs \
  -w write -o 1000000

# MinIO (local, HTTP, path-style URLs)
./benchtool -e tidesdb \
  --object-store s3 \
  --s3-endpoint localhost:9000 --s3-bucket tidesdb-bench \
  --s3-access-key minioadmin --s3-secret-key minioadmin \
  --s3-no-ssl --s3-path-style \
  --object-local-cache-max-bytes $((512 * 1024 * 1024)) \
  --object-max-concurrent-uploads 8 \
  -w write -o 100000

# AWS S3 with sync-on-commit replication (RPO=0)
./benchtool -e tidesdb \
  --object-store s3 \
  --s3-endpoint s3.amazonaws.com --s3-bucket my-tidesdb-bucket \
  --s3-region us-east-1 \
  --s3-access-key $AWS_ACCESS_KEY_ID --s3-secret-key $AWS_SECRET_ACCESS_KEY \
  --object-wal-sync-on-commit 1 \
  -w write -o 500000
```

## Runners
The benchtool has default runners such as

- `large_value_benchmark.sh` - 8KB value suite (PUT/GET/SEEK/RANGE) with both engines, 100K ops, 2 threads
- `large_value_benchmark_1gb.sh` - 1GB value suite (PUT/GET/RANGE) with both engines, 10 ops, ~10GB total data
- `tidesdb_rocksdb.sh` - main comparison suite with 25 tests across 3 categories:
  - **Standard scale** (tests 1–12): 64MB cache, 8 threads — sequential/random/zipfian writes (10M), random reads (10M), mixed workloads (5M), deletes (5M), large values (4KB, 1M ops), small values (64B, 50M ops), batch size scaling (1–10000), delete batch scaling, seek performance (random/seq/zipfian), range scans
  - **Large scale** (tests 13–24): 6GB cache, 16 threads, 4x operations — same workload categories at higher concurrency and volume
  - **Durability** (test 25): synced writes with scaling threads and ops (25K/1t, 50K/4t, 100K/8t, 500K/16t)
- `tidesdb_rocksdb_quick.sh` - fast benchmark inspired by RocksDB wiki (100M bulkload, 50M read/write, 16 threads, 32GB cache, ~1-2 hours)
- `tidesdb_rocksdb_synced.sh` - synced (durable) write suite with reduced ops for practicality
- `tidesdb_rocksdb_single_threaded.sh` - single-threaded comparison suite
- `tidesdb_rocksdb_no_bloom_indexes.sh` - comparison suite with bloom filters and block indexes disabled
- `tidesdb_rocksdb_extensive.sh` - extensive multi-run suite (900M keys, 32GB cache, 16 threads, 3 runs per test, warm/cold cache, scaling sweeps)
- `tidesdb_rocksdb_one_billion.sh` - 1B key write/read/seek/range + 500M delete, 8 threads, memtable 128MB, cache 8GB
- `tidesdb_lmdb.sh` - TidesDB vs LMDB comparison suite (10M ops, 8 threads)
- `tidesdb_rocksdb_lmdb.sh` - three-way comparison (TidesDB vs RocksDB vs LMDB), configurable via `--engines`
- `tidesdb_allocator_benchmark.sh` - allocator comparison suite (i.e `./tidesdb_allocator_benchmark.sh --preload --allocator all`)
- `tidesdb_btree_comparison.sh` - B+tree vs block-based klog format comparison (default 10M keys, configurable via `-k`)
- `tidesdb_rocksdb_larger_than_memory.sh` - comparison suite with larger than memory data

## Graphs

### TidesDB vs RocksDB Comparison Plots
Generate detailed comparison plots from `tidesdb_rocksdb.sh` CSV output with `plot_tidesdb_rocksdb.py`:

```bash
python3 -m venv venv && source venv/bin/activate && pip install pandas matplotlib numpy
python3 plot_tidesdb_rocksdb.py <csv_file>
```

Outputs 17 PNG plots to `benchmark_plots/` (TidesDB = blue, RocksDB = grey):

| Plot | Description |
|------|-------------|
| `00_speedup_summary` | Horizontal bar chart of TidesDB/RocksDB throughput ratio across all workloads |
| `01_write_throughput` | Sequential, random, zipfian write throughput (standard + large scale) |
| `02_read_mixed_throughput` | Read throughput + mixed workload PUT/GET sides |
| `03_delete_throughput` | Delete throughput with batch size scaling |
| `04_seek_throughput` | Random, sequential, zipfian seek throughput |
| `05_range_scan_throughput` | Range scan throughput (100/1000 keys, random/sequential) |
| `06_batch_size_scaling` | Line chart of throughput vs batch size (1 to 10K) |
| `07_value_size_impact` | 64B vs 100B vs 4KB value write performance |
| `08_latency_overview` | 4-panel average latency: writes, reads, seeks, ranges |
| `09_latency_percentiles` | 6-panel p50/p95/p99 for key workloads |
| `10_write_amplification` | Write amplification factor comparison |
| `11_space_efficiency` | On-disk DB size + space amplification |
| `12_resource_usage` | 4-panel: memory (RSS/VMS), disk writes, CPU% |
| `13_tail_latency` | Average vs p99 latency side-by-side |
| `14_duration_comparison` | Wall-clock duration for key tests |
| `15_latency_variability` | CV% (coefficient of variation) comparison |
| `16_sync_write_performance` | Synced (durable) write throughput and latency scaling |


### TidesDB Version-to-Version Comparison Plots
Compare benchmark results between two TidesDB versions to identify regressions and performance gains with `compare_tidesdb_versions.py`:

```bash
python3 compare_tidesdb_versions.py <newer_csv> <older_csv>
```

Example:
```bash
python3 compare_tidesdb_versions.py \
  tidesdb_rocksdb_benchmark_results_20260217_113922.csv \
  tidesdb_rocksdb_benchmark_results_20260216_061038.csv
```

Outputs 12 PNG plots and a text report to `version_comparison_plots/` (Newer = blue, Older = grey, Improvement = green, Regression = red):

| Plot | Description |
|------|-------------|
| `00_change_summary` | Horizontal bar chart of throughput % change across all key benchmarks |
| `01_write_comparison` | Sequential, random, zipfian write throughput side-by-side |
| `02_mixed_comparison` | Mixed workload write and read side throughput |
| `03_delete_comparison` | Delete throughput with batch size scaling |
| `04_seek_comparison` | Random, sequential, zipfian seek throughput |
| `05_range_comparison` | Range scan throughput (100/1000 keys, random/sequential) |
| `06_batch_comparison` | Line chart of throughput vs batch size (1 to 10K) |
| `07_value_size_comparison` | 64B vs 100B vs 4KB value write performance |
| `08_latency_comparison` | Average latency across writes, reads, seeks, ranges, deletes |
| `09_latency_percentiles_comparison` | p50/p95/p99 for random write, seek, and delete |
| `10_write_amp_comparison` | Write amplification factor comparison |
| `11_resource_comparison` | 4-panel: memory (RSS), disk writes, CPU%, database size |
| `version_comparison_report.txt` | Text summary listing improvements, regressions, and unchanged benchmarks |

Notes:
- Dates are auto-extracted from filenames (e.g. `*_20260217_113922.csv`) for labeling.
- Only TidesDB entries are compared; RocksDB rows and populate steps are filtered out.

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
