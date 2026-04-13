/*
 * Copyright 2024 Alex Gaetano Padula (TidesDB)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <dirent.h>
#include <getopt.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "benchmark.h"

static void print_usage(const char *prog)
{
    if (prog == NULL)
    {
        return;
    }

    printf("TidesDB Storage Engine Benchmarker\n\n");
    printf("Usage: %s [OPTIONS]\n\n", prog);
    printf("Options:\n");
    printf(
        "  -e, --engine <name>       Storage engine to benchmark (tidesdb, "
        "rocksdb, lmdb)\n");
    printf("  -o, --operations <num>    Number of operations (default: 100000)\n");
    printf("  -k, --key-size <bytes>    Key size in bytes (default: 16)\n");
    printf("  -v, --value-size <bytes>  Value size in bytes (default: 100)\n");
    printf("  -t, --threads <num>       Number of threads (default: 1)\n");
    printf("  -b, --batch-size <num>    Batch size for operations (default: 1)\n");
    printf("  -d, --db-path <path>      Database path (default: ./bench_db)\n");
    printf("  -c, --compare             Compare against RocksDB baseline\n");
    printf("  -r, --report <file>       Output report to file (default: stdout)\n");
    printf("  --csv <file>              Export results to CSV file for graphing\n");
    printf("  --test-name <name>        Tag results with a test name in CSV output\n");
    printf("  -s, --sequential          Use sequential keys instead of random\n");
    printf(
        "  -p, --pattern <type>      Key pattern: seq, random, zipfian, "
        "uniform, timestamp, "
        "reverse (default: random)\n");
    printf(
        "  -w, --workload <type>     Workload type: write, read, mixed, "
        "delete, seek, range (default: mixed)\n");
    printf("  --sync                    Enable fsync for durable writes (slower)\n");
    printf("  --range-size <num>        Number of keys per range query (default: 100)\n");
    printf("  --memtable-size <bytes>   Memtable/write buffer size in bytes (0 = default)\n");
    printf("  --block-cache-size <bytes> Block cache size in bytes (0 = default)\n");
    printf("  --rocksdb-blobdb          Enable RocksDB BlobDB for large values\n");
    printf("  --no-rocksdb-blobdb       Disable RocksDB BlobDB\n");
    printf("  --bloom-fp <fp>           Bloom filter false positive rate (0.01 = default)\n");
    printf(
        "  --l0_queue_stall_threshold <num> L0 queue stall threshold (10 = default) (TidesDB) \n");
    printf("  --l1_file_count_trigger <num> L1 file count trigger (4 = default) (TidesDB) \n");
    printf("  --bloom-filters           Enable bloom filters\n");
    printf("  --klog_value_threshold <bytes> Klog value threshold (512 bytes = default)\n");
    printf("  --dividing_level_offset <num> Dividing level offset (TidesDB) (2 = default)\n");
    printf("  --min_levels <num> Minimum levels (TidesDB) (5 = default)\n");
    printf("  --index_sample_ratio <num> Sample ratio for block indexes (TidesDB) (1 = default)\n");
    printf(
        "  --block_index_prefix_len <num> Sample prefix length for min-max block indexes (TidesDB) "
        "(16 = default)\n");
    printf("  --no-bloom-filters        Disable bloom filters\n");
    printf("  --block-indexes           Enable block indexes\n");
    printf("  --no-block-indexes        Disable block indexes\n");
    printf("  --debug                   Enable debug logging for storage engines\n");
    printf("  --use-btree               Use B+tree format for klog (TidesDB)\n");
    printf("\nTidesDB engine-level configuration:\n");
    printf("  --num-flush-threads <n>          Flush thread pool size (default 1)\n");
    printf("  --num-compaction-threads <n>     Compaction thread pool size (default 1)\n");
    printf("  --max-open-sstables <n>          Max cached SSTable structures (default 256)\n");
    printf("  --max-memory-usage <bytes>       Global memory limit (0 = auto, 50%% of RAM)\n");
    printf("  --log-to-file <0|1>              Log to file vs stderr (default 1)\n");
    printf("\nTidesDB column-family tuning:\n");
    printf("  --compression <algo>             none|lz4|lz4fast|zstd|snappy (default lz4)\n");
    printf("  --skip-list-max-level <n>        Skip list max level (default 12)\n");
    printf("  --skip-list-probability <p>      Skip list probability (default 0.25)\n");
    printf("  --level-size-ratio <n>           Level size multiplier (default 10)\n");
    printf("  --sync-mode <mode>               none|full|interval (overrides --sync)\n");
    printf("  --sync-interval-us <us>          Sync interval (default 128000us)\n");
    printf("\nTidesDB unified memtable mode:\n");
    printf("  --unified-memtable               Enable unified memtable (single shared WAL)\n");
    printf("  --unified-memtable-size <bytes>  Unified write buffer size (0 = default 64MB)\n");
    printf("  --unified-memtable-skip-list-max-level <n>     0 = default 12\n");
    printf("  --unified-memtable-skip-list-probability <p>   0 = default 0.25\n");
    printf("  --unified-memtable-sync-mode <mode>            none|full|interval\n");
    printf("  --unified-memtable-sync-interval-us <us>       Sync interval (microseconds)\n");
    printf("\nTidesDB object store mode (auto-enables unified memtable):\n");
    printf("  --object-store <none|fs|s3>      Backend (default none)\n");
    printf("  --object-store-fs-path <dir>     Root dir for filesystem backend\n");
    printf("  --s3-endpoint <host[:port]>      e.g. s3.amazonaws.com or minio.local:9000\n");
    printf("  --s3-bucket <name>               Bucket name\n");
    printf("  --s3-prefix <prefix>             Optional key prefix (e.g. 'production/db1/')\n");
    printf("  --s3-access-key <key>            Access key ID\n");
    printf("  --s3-secret-key <key>            Secret access key\n");
    printf("  --s3-region <region>             AWS region (NULL for MinIO)\n");
    printf("  --s3-no-ssl                      Use HTTP instead of HTTPS\n");
    printf("  --s3-path-style                  Use path-style URLs (required for MinIO)\n");
    printf("  --object-local-cache-path <dir>  Local cache directory (default db_path)\n");
    printf("  --object-local-cache-max-bytes <b>  Max local cache size (0 = unlimited)\n");
    printf("  --object-cache-on-read <0|1>     Cache downloaded files (default 1)\n");
    printf("  --object-cache-on-write <0|1>    Keep local copy after upload (default 1)\n");
    printf("  --object-max-concurrent-uploads <n>     (default 4)\n");
    printf("  --object-max-concurrent-downloads <n>   (default 8)\n");
    printf("  --object-multipart-threshold <bytes>    (default 64MB)\n");
    printf("  --object-multipart-part-size <bytes>    (default 8MB)\n");
    printf("  --object-sync-manifest <0|1>            Upload MANIFEST after compaction\n");
    printf("  --object-replicate-wal <0|1>            Upload closed WAL segments\n");
    printf("  --object-wal-upload-sync <0|1>          Block flush on WAL upload\n");
    printf("  --object-wal-sync-threshold <bytes>     Active-WAL sync threshold (default 1MB)\n");
    printf("  --object-wal-sync-on-commit <0|1>       RPO=0 replication\n");
    printf("  --object-replica-mode <0|1>             Read-only replica mode\n");
    printf("  --object-replica-sync-interval-us <us>  MANIFEST poll interval (default 5s)\n");
    printf("  --object-replica-replay-wal <0|1>       Replay WAL on replicas (default 1)\n");
    printf("  --object-lazy-compaction <0|1>          Less aggressive compaction (per-CF)\n");
    printf("  --object-prefetch-compaction <0|1>      Parallel input prefetch (per-CF)\n");
    printf("\n  -h, --help                Show this help message\n\n");
    printf("Examples:\n");
    printf("  %s -e tidesdb -o 1000000 -k 16 -v 100\n", prog);
    printf("  %s -e tidesdb -c -o 500000 -t 4\n", prog);
    printf("  %s -e rocksdb -w write -o 1000000\n", prog);
    printf("  %s -e tidesdb --unified-memtable -o 1000000\n", prog);
    printf("  %s -e tidesdb --object-store fs --object-store-fs-path /tmp/objs -o 100000\n", prog);
    printf("  %s -e tidesdb --compression zstd --sync-mode interval --sync-interval-us 500000\n",
           prog);
}

int main(int argc, char **argv)
{
    benchmark_config_t config = {.engine_name = "tidesdb",
                                 .num_operations = 10000000,
                                 .key_size = 16,
                                 .value_size = 100,
                                 .num_threads = 4,
                                 .batch_size = 1,
                                 .db_path = "./bench_db",
                                 .compare_mode = 0,
                                 .report_file = NULL,
                                 .csv_file = NULL,
                                 .test_name = NULL,
                                 .key_pattern = KEY_PATTERN_RANDOM,
                                 .workload_type = WORKLOAD_MIXED,
                                 .sync_enabled = 0,
                                 .range_size = 100,
                                 .memtable_size = 0,
                                 .block_cache_size = 0,
                                 .enable_blobdb = -1,
                                 .enable_bloom_filter = -1,
                                 .enable_block_indexes = -1,
                                 .bloom_fpr = 0.01,
                                 .l0_queue_stall_threshold = 10,
                                 .l1_file_count_trigger = 4,
                                 .dividing_level_offset = 2,
                                 .min_levels = 5,
                                 .index_sample_ratio = 1,
                                 .block_index_prefix_len = 16,
                                 .klog_value_threshold = 512,
                                 .debug_logging = 0,
                                 .use_btree = 0,
                                 .num_flush_threads = 0,
                                 .num_compaction_threads = 0,
                                 .max_open_sstables = 0,
                                 .max_memory_usage = 0,
                                 .log_to_file = 0,
                                 .compression_algorithm = NULL,
                                 .skip_list_max_level = 0,
                                 .skip_list_probability = 0.0f,
                                 .level_size_ratio = 0,
                                 .sync_mode = NULL,
                                 .sync_interval_us = 0,
                                 .unified_memtable = 0,
                                 .unified_memtable_write_buffer_size = 0,
                                 .unified_memtable_skip_list_max_level = 0,
                                 .unified_memtable_skip_list_probability = 0.0f,
                                 .unified_memtable_sync_mode = NULL,
                                 .unified_memtable_sync_interval_us = 0,
                                 .object_store_backend = NULL,
                                 .object_store_fs_path = NULL,
                                 .s3_endpoint = NULL,
                                 .s3_bucket = NULL,
                                 .s3_prefix = NULL,
                                 .s3_access_key = NULL,
                                 .s3_secret_key = NULL,
                                 .s3_region = NULL,
                                 .s3_use_ssl = 1,
                                 .s3_use_path_style = 0,
                                 .object_local_cache_path = NULL,
                                 .object_local_cache_max_bytes = 0,
                                 .object_cache_on_read = -1,
                                 .object_cache_on_write = -1,
                                 .object_max_concurrent_uploads = 0,
                                 .object_max_concurrent_downloads = 0,
                                 .object_multipart_threshold = 0,
                                 .object_multipart_part_size = 0,
                                 .object_sync_manifest = -1,
                                 .object_replicate_wal = -1,
                                 .object_wal_upload_sync = -1,
                                 .object_wal_sync_threshold_bytes = 0,
                                 .object_wal_sync_on_commit = -1,
                                 .object_replica_mode = -1,
                                 .object_replica_sync_interval_us = 0,
                                 .object_replica_replay_wal = -1,
                                 .object_lazy_compaction = -1,
                                 .object_prefetch_compaction = -1};

    enum
    {
        OPT_TEST_NAME = 1000,
        OPT_BLOOM_FPR,
        OPT_L0_QUEUE_STALL_THRESHOLD,
        OPT_L1_FILE_COUNT_TRIGGER,
        OPT_DIVIDING_LEVEL_OFFSET,
        OPT_MIN_LEVELS,
        OPT_INDEX_SAMPLE_RATIO,
        OPT_BLOCK_INDEX_PREFIX_LEN,
        OPT_KLOG_VALUE_THRESHOLD,
        OPT_DEBUG,
        OPT_USE_BTREE,
        OPT_NUM_FLUSH_THREADS,
        OPT_NUM_COMPACTION_THREADS,
        OPT_MAX_OPEN_SSTABLES,
        OPT_MAX_MEMORY_USAGE,
        OPT_LOG_TO_FILE,
        OPT_COMPRESSION,
        OPT_SKIP_LIST_MAX_LEVEL,
        OPT_SKIP_LIST_PROBABILITY,
        OPT_LEVEL_SIZE_RATIO,
        OPT_SYNC_MODE,
        OPT_SYNC_INTERVAL_US,
        OPT_UNIFIED_MEMTABLE,
        OPT_UNIFIED_MEMTABLE_SIZE,
        OPT_UNIFIED_MEMTABLE_SKIP_LIST_MAX_LEVEL,
        OPT_UNIFIED_MEMTABLE_SKIP_LIST_PROBABILITY,
        OPT_UNIFIED_MEMTABLE_SYNC_MODE,
        OPT_UNIFIED_MEMTABLE_SYNC_INTERVAL_US,
        OPT_OBJECT_STORE,
        OPT_OBJECT_STORE_FS_PATH,
        OPT_S3_ENDPOINT,
        OPT_S3_BUCKET,
        OPT_S3_PREFIX,
        OPT_S3_ACCESS_KEY,
        OPT_S3_SECRET_KEY,
        OPT_S3_REGION,
        OPT_S3_NO_SSL,
        OPT_S3_PATH_STYLE,
        OPT_OBJECT_LOCAL_CACHE_PATH,
        OPT_OBJECT_LOCAL_CACHE_MAX_BYTES,
        OPT_OBJECT_CACHE_ON_READ,
        OPT_OBJECT_CACHE_ON_WRITE,
        OPT_OBJECT_MAX_CONCURRENT_UPLOADS,
        OPT_OBJECT_MAX_CONCURRENT_DOWNLOADS,
        OPT_OBJECT_MULTIPART_THRESHOLD,
        OPT_OBJECT_MULTIPART_PART_SIZE,
        OPT_OBJECT_SYNC_MANIFEST,
        OPT_OBJECT_REPLICATE_WAL,
        OPT_OBJECT_WAL_UPLOAD_SYNC,
        OPT_OBJECT_WAL_SYNC_THRESHOLD,
        OPT_OBJECT_WAL_SYNC_ON_COMMIT,
        OPT_OBJECT_REPLICA_MODE,
        OPT_OBJECT_REPLICA_SYNC_INTERVAL_US,
        OPT_OBJECT_REPLICA_REPLAY_WAL,
        OPT_OBJECT_LAZY_COMPACTION,
        OPT_OBJECT_PREFETCH_COMPACTION
    };

    static struct option long_options[] = {
        {"engine", required_argument, 0, 'e'},
        {"operations", required_argument, 0, 'o'},
        {"key-size", required_argument, 0, 'k'},
        {"value-size", required_argument, 0, 'v'},
        {"threads", required_argument, 0, 't'},
        {"batch-size", required_argument, 0, 'b'},
        {"db-path", required_argument, 0, 'd'},
        {"compare", no_argument, 0, 'c'},
        {"report", required_argument, 0, 'r'},
        {"csv", required_argument, 0, 'X'},
        {"test-name", required_argument, 0, OPT_TEST_NAME},
        {"pattern", required_argument, 0, 'p'},
        {"workload", required_argument, 0, 'w'},
        {"sync", no_argument, 0, 'S'},
        {"range-size", required_argument, 0, 'R'},
        {"memtable-size", required_argument, 0, 'M'},
        {"block-cache-size", required_argument, 0, 'C'},
        {"rocksdb-blobdb", no_argument, 0, 'B'},
        {"no-rocksdb-blobdb", no_argument, 0, 'N'},
        {"bloom-filters", no_argument, 0, 'F'},
        {"no-bloom-filters", no_argument, 0, 'G'},
        {"block-indexes", no_argument, 0, 'I'},
        {"no-block-indexes", no_argument, 0, 'J'},
        {"bloom-fpr", required_argument, 0, OPT_BLOOM_FPR},
        {"l0_queue_stall_threshold", required_argument, 0, OPT_L0_QUEUE_STALL_THRESHOLD},
        {"l1_file_count_trigger", required_argument, 0, OPT_L1_FILE_COUNT_TRIGGER},
        {"dividing_level_offset", required_argument, 0, OPT_DIVIDING_LEVEL_OFFSET},
        {"min_levels", required_argument, 0, OPT_MIN_LEVELS},
        {"index_sample_ratio", required_argument, 0, OPT_INDEX_SAMPLE_RATIO},
        {"block_index_prefix_len", required_argument, 0, OPT_BLOCK_INDEX_PREFIX_LEN},
        {"klog_value_threshold", required_argument, 0, OPT_KLOG_VALUE_THRESHOLD},
        {"debug", no_argument, 0, OPT_DEBUG},
        {"use-btree", no_argument, 0, OPT_USE_BTREE},
        {"num-flush-threads", required_argument, 0, OPT_NUM_FLUSH_THREADS},
        {"num-compaction-threads", required_argument, 0, OPT_NUM_COMPACTION_THREADS},
        {"max-open-sstables", required_argument, 0, OPT_MAX_OPEN_SSTABLES},
        {"max-memory-usage", required_argument, 0, OPT_MAX_MEMORY_USAGE},
        {"log-to-file", required_argument, 0, OPT_LOG_TO_FILE},
        {"compression", required_argument, 0, OPT_COMPRESSION},
        {"skip-list-max-level", required_argument, 0, OPT_SKIP_LIST_MAX_LEVEL},
        {"skip-list-probability", required_argument, 0, OPT_SKIP_LIST_PROBABILITY},
        {"level-size-ratio", required_argument, 0, OPT_LEVEL_SIZE_RATIO},
        {"sync-mode", required_argument, 0, OPT_SYNC_MODE},
        {"sync-interval-us", required_argument, 0, OPT_SYNC_INTERVAL_US},
        {"unified-memtable", no_argument, 0, OPT_UNIFIED_MEMTABLE},
        {"unified-memtable-size", required_argument, 0, OPT_UNIFIED_MEMTABLE_SIZE},
        {"unified-memtable-skip-list-max-level", required_argument, 0,
         OPT_UNIFIED_MEMTABLE_SKIP_LIST_MAX_LEVEL},
        {"unified-memtable-skip-list-probability", required_argument, 0,
         OPT_UNIFIED_MEMTABLE_SKIP_LIST_PROBABILITY},
        {"unified-memtable-sync-mode", required_argument, 0, OPT_UNIFIED_MEMTABLE_SYNC_MODE},
        {"unified-memtable-sync-interval-us", required_argument, 0,
         OPT_UNIFIED_MEMTABLE_SYNC_INTERVAL_US},
        {"object-store", required_argument, 0, OPT_OBJECT_STORE},
        {"object-store-fs-path", required_argument, 0, OPT_OBJECT_STORE_FS_PATH},
        {"s3-endpoint", required_argument, 0, OPT_S3_ENDPOINT},
        {"s3-bucket", required_argument, 0, OPT_S3_BUCKET},
        {"s3-prefix", required_argument, 0, OPT_S3_PREFIX},
        {"s3-access-key", required_argument, 0, OPT_S3_ACCESS_KEY},
        {"s3-secret-key", required_argument, 0, OPT_S3_SECRET_KEY},
        {"s3-region", required_argument, 0, OPT_S3_REGION},
        {"s3-no-ssl", no_argument, 0, OPT_S3_NO_SSL},
        {"s3-path-style", no_argument, 0, OPT_S3_PATH_STYLE},
        {"object-local-cache-path", required_argument, 0, OPT_OBJECT_LOCAL_CACHE_PATH},
        {"object-local-cache-max-bytes", required_argument, 0, OPT_OBJECT_LOCAL_CACHE_MAX_BYTES},
        {"object-cache-on-read", required_argument, 0, OPT_OBJECT_CACHE_ON_READ},
        {"object-cache-on-write", required_argument, 0, OPT_OBJECT_CACHE_ON_WRITE},
        {"object-max-concurrent-uploads", required_argument, 0, OPT_OBJECT_MAX_CONCURRENT_UPLOADS},
        {"object-max-concurrent-downloads", required_argument, 0,
         OPT_OBJECT_MAX_CONCURRENT_DOWNLOADS},
        {"object-multipart-threshold", required_argument, 0, OPT_OBJECT_MULTIPART_THRESHOLD},
        {"object-multipart-part-size", required_argument, 0, OPT_OBJECT_MULTIPART_PART_SIZE},
        {"object-sync-manifest", required_argument, 0, OPT_OBJECT_SYNC_MANIFEST},
        {"object-replicate-wal", required_argument, 0, OPT_OBJECT_REPLICATE_WAL},
        {"object-wal-upload-sync", required_argument, 0, OPT_OBJECT_WAL_UPLOAD_SYNC},
        {"object-wal-sync-threshold", required_argument, 0, OPT_OBJECT_WAL_SYNC_THRESHOLD},
        {"object-wal-sync-on-commit", required_argument, 0, OPT_OBJECT_WAL_SYNC_ON_COMMIT},
        {"object-replica-mode", required_argument, 0, OPT_OBJECT_REPLICA_MODE},
        {"object-replica-sync-interval-us", required_argument, 0,
         OPT_OBJECT_REPLICA_SYNC_INTERVAL_US},
        {"object-replica-replay-wal", required_argument, 0, OPT_OBJECT_REPLICA_REPLAY_WAL},
        {"object-lazy-compaction", required_argument, 0, OPT_OBJECT_LAZY_COMPACTION},
        {"object-prefetch-compaction", required_argument, 0, OPT_OBJECT_PREFETCH_COMPACTION},
        {"help", no_argument, 0, 'h'},
        {0, 0, 0, 0}};

    int opt;
    int option_index = 0;

    while ((opt = getopt_long(argc, argv, "e:o:k:v:t:b:d:cr:sp:w:R:M:C:h", long_options,
                              &option_index)) != -1)
    {
        switch (opt)
        {
            case 'e':
                config.engine_name = optarg;
                break;
            case 'o':
                config.num_operations = atoll(optarg);
                break;
            case 'k':
                config.key_size = atoi(optarg);
                break;
            case 'v':
                config.value_size = atoi(optarg);
                break;
            case 't':
                config.num_threads = atoi(optarg);
                break;
            case 'b':
                config.batch_size = atoi(optarg);
                break;
            case 'd':
                config.db_path = optarg;
                break;
            case 's':
                config.key_pattern = KEY_PATTERN_SEQUENTIAL;
                break;
            case 'c':
                config.compare_mode = 1;
                break;
            case 'r':
                config.report_file = optarg;
                break;
            case 'X':
                config.csv_file = optarg;
                break;
            case OPT_TEST_NAME:
                config.test_name = optarg;
                break;
            case 'p':
                if (strcmp(optarg, "seq") == 0 || strcmp(optarg, "sequential") == 0)
                    config.key_pattern = KEY_PATTERN_SEQUENTIAL;
                else if (strcmp(optarg, "random") == 0)
                    config.key_pattern = KEY_PATTERN_RANDOM;
                else if (strcmp(optarg, "zipfian") == 0)
                    config.key_pattern = KEY_PATTERN_ZIPFIAN;
                else if (strcmp(optarg, "uniform") == 0)
                    config.key_pattern = KEY_PATTERN_UNIFORM;
                else if (strcmp(optarg, "timestamp") == 0)
                    config.key_pattern = KEY_PATTERN_TIMESTAMP;
                else if (strcmp(optarg, "reverse") == 0)
                    config.key_pattern = KEY_PATTERN_REVERSE;
                else
                {
                    fprintf(stderr, "Invalid key pattern: %s\n", optarg);
                    return 1;
                }
                break;
            case 'w':
                if (strcmp(optarg, "write") == 0)
                    config.workload_type = WORKLOAD_WRITE;
                else if (strcmp(optarg, "read") == 0)
                    config.workload_type = WORKLOAD_READ;
                else if (strcmp(optarg, "mixed") == 0)
                    config.workload_type = WORKLOAD_MIXED;
                else if (strcmp(optarg, "delete") == 0)
                    config.workload_type = WORKLOAD_DELETE;
                else if (strcmp(optarg, "seek") == 0)
                    config.workload_type = WORKLOAD_SEEK;
                else if (strcmp(optarg, "range") == 0)
                    config.workload_type = WORKLOAD_RANGE;
                else
                {
                    fprintf(stderr, "Invalid workload type: %s\n", optarg);
                    return 1;
                }
                break;
            case 'S':
                config.sync_enabled = 1;
                break;
            case 'R':
                config.range_size = atoi(optarg);
                break;
            case 'M':
                config.memtable_size = (size_t)atoll(optarg);
                break;
            case 'C':
                config.block_cache_size = (size_t)atoll(optarg);
                break;
            case 'B':
                config.enable_blobdb = 1;
                break;
            case 'N':
                config.enable_blobdb = 0;
                break;
            case 'F':
                config.enable_bloom_filter = 1;
                break;
            case 'G':
                config.enable_bloom_filter = 0;
                break;
            case 'I':
                config.enable_block_indexes = 1;
                break;
            case 'J':
                config.enable_block_indexes = 0;
                break;
            case 'h':
                print_usage(argv[0]);
                return 0;
            case OPT_BLOOM_FPR:
                config.bloom_fpr = atof(optarg);
                break;
            case OPT_L0_QUEUE_STALL_THRESHOLD:
                config.l0_queue_stall_threshold = atoi(optarg);
                break;
            case OPT_L1_FILE_COUNT_TRIGGER:
                config.l1_file_count_trigger = atoi(optarg);
                break;
            case OPT_DIVIDING_LEVEL_OFFSET:
                config.dividing_level_offset = atoi(optarg);
                break;
            case OPT_MIN_LEVELS:
                config.min_levels = atoi(optarg);
                break;
            case OPT_INDEX_SAMPLE_RATIO:
                config.index_sample_ratio = atoi(optarg);
                break;
            case OPT_BLOCK_INDEX_PREFIX_LEN:
                config.block_index_prefix_len = atoi(optarg);
                break;
            case OPT_KLOG_VALUE_THRESHOLD:
                config.klog_value_threshold = (size_t)atoll(optarg);
                break;
            case OPT_DEBUG:
                config.debug_logging = 1;
                break;
            case OPT_USE_BTREE:
                config.use_btree = 1;
                break;
            case OPT_NUM_FLUSH_THREADS:
                config.num_flush_threads = atoi(optarg);
                break;
            case OPT_NUM_COMPACTION_THREADS:
                config.num_compaction_threads = atoi(optarg);
                break;
            case OPT_MAX_OPEN_SSTABLES:
                config.max_open_sstables = (size_t)atoll(optarg);
                break;
            case OPT_MAX_MEMORY_USAGE:
                config.max_memory_usage = (size_t)atoll(optarg);
                break;
            case OPT_LOG_TO_FILE:
                config.log_to_file = atoi(optarg) ? 1 : 0;
                break;
            case OPT_COMPRESSION:
                config.compression_algorithm = optarg;
                break;
            case OPT_SKIP_LIST_MAX_LEVEL:
                config.skip_list_max_level = atoi(optarg);
                break;
            case OPT_SKIP_LIST_PROBABILITY:
                config.skip_list_probability = (float)atof(optarg);
                break;
            case OPT_LEVEL_SIZE_RATIO:
                config.level_size_ratio = (size_t)atoll(optarg);
                break;
            case OPT_SYNC_MODE:
                config.sync_mode = optarg;
                break;
            case OPT_SYNC_INTERVAL_US:
                config.sync_interval_us = (uint64_t)strtoull(optarg, NULL, 10);
                break;
            case OPT_UNIFIED_MEMTABLE:
                config.unified_memtable = 1;
                break;
            case OPT_UNIFIED_MEMTABLE_SIZE:
                config.unified_memtable_write_buffer_size = (size_t)atoll(optarg);
                break;
            case OPT_UNIFIED_MEMTABLE_SKIP_LIST_MAX_LEVEL:
                config.unified_memtable_skip_list_max_level = atoi(optarg);
                break;
            case OPT_UNIFIED_MEMTABLE_SKIP_LIST_PROBABILITY:
                config.unified_memtable_skip_list_probability = (float)atof(optarg);
                break;
            case OPT_UNIFIED_MEMTABLE_SYNC_MODE:
                config.unified_memtable_sync_mode = optarg;
                break;
            case OPT_UNIFIED_MEMTABLE_SYNC_INTERVAL_US:
                config.unified_memtable_sync_interval_us = (uint64_t)strtoull(optarg, NULL, 10);
                break;
            case OPT_OBJECT_STORE:
                config.object_store_backend = optarg;
                break;
            case OPT_OBJECT_STORE_FS_PATH:
                config.object_store_fs_path = optarg;
                break;
            case OPT_S3_ENDPOINT:
                config.s3_endpoint = optarg;
                break;
            case OPT_S3_BUCKET:
                config.s3_bucket = optarg;
                break;
            case OPT_S3_PREFIX:
                config.s3_prefix = optarg;
                break;
            case OPT_S3_ACCESS_KEY:
                config.s3_access_key = optarg;
                break;
            case OPT_S3_SECRET_KEY:
                config.s3_secret_key = optarg;
                break;
            case OPT_S3_REGION:
                config.s3_region = optarg;
                break;
            case OPT_S3_NO_SSL:
                config.s3_use_ssl = 0;
                break;
            case OPT_S3_PATH_STYLE:
                config.s3_use_path_style = 1;
                break;
            case OPT_OBJECT_LOCAL_CACHE_PATH:
                config.object_local_cache_path = optarg;
                break;
            case OPT_OBJECT_LOCAL_CACHE_MAX_BYTES:
                config.object_local_cache_max_bytes = (size_t)atoll(optarg);
                break;
            case OPT_OBJECT_CACHE_ON_READ:
                config.object_cache_on_read = atoi(optarg);
                break;
            case OPT_OBJECT_CACHE_ON_WRITE:
                config.object_cache_on_write = atoi(optarg);
                break;
            case OPT_OBJECT_MAX_CONCURRENT_UPLOADS:
                config.object_max_concurrent_uploads = atoi(optarg);
                break;
            case OPT_OBJECT_MAX_CONCURRENT_DOWNLOADS:
                config.object_max_concurrent_downloads = atoi(optarg);
                break;
            case OPT_OBJECT_MULTIPART_THRESHOLD:
                config.object_multipart_threshold = (size_t)atoll(optarg);
                break;
            case OPT_OBJECT_MULTIPART_PART_SIZE:
                config.object_multipart_part_size = (size_t)atoll(optarg);
                break;
            case OPT_OBJECT_SYNC_MANIFEST:
                config.object_sync_manifest = atoi(optarg);
                break;
            case OPT_OBJECT_REPLICATE_WAL:
                config.object_replicate_wal = atoi(optarg);
                break;
            case OPT_OBJECT_WAL_UPLOAD_SYNC:
                config.object_wal_upload_sync = atoi(optarg);
                break;
            case OPT_OBJECT_WAL_SYNC_THRESHOLD:
                config.object_wal_sync_threshold_bytes = (size_t)atoll(optarg);
                break;
            case OPT_OBJECT_WAL_SYNC_ON_COMMIT:
                config.object_wal_sync_on_commit = atoi(optarg);
                break;
            case OPT_OBJECT_REPLICA_MODE:
                config.object_replica_mode = atoi(optarg);
                break;
            case OPT_OBJECT_REPLICA_SYNC_INTERVAL_US:
                config.object_replica_sync_interval_us = (uint64_t)strtoull(optarg, NULL, 10);
                break;
            case OPT_OBJECT_REPLICA_REPLAY_WAL:
                config.object_replica_replay_wal = atoi(optarg);
                break;
            case OPT_OBJECT_LAZY_COMPACTION:
                config.object_lazy_compaction = atoi(optarg);
                break;
            case OPT_OBJECT_PREFETCH_COMPACTION:
                config.object_prefetch_compaction = atoi(optarg);
                break;
            default:
                print_usage(argv[0]);
                return 1;
        }
    }

    if (config.num_operations <= 0LL || config.key_size <= 0 || config.value_size <= 0 ||
        config.num_threads <= 0 || config.batch_size <= 0)
    {
        fprintf(stderr, "Error: All numeric parameters must be positive\n");
        return 1;
    }

    printf("=== TidesDB Storage Engine Benchmarker ===\n\n");
    printf("Configuration:\n");
    const char *version = get_engine_version(config.engine_name);
    printf("  Engine: %s (v%s)\n", config.engine_name, version);
    printf("  Operations: %" PRId64 "\n", config.num_operations);
    printf("  Key Size: %d bytes\n", config.key_size);
    printf("  Value Size: %d bytes\n", config.value_size);
    printf("  Threads: %d\n", config.num_threads);
    printf("  Batch Size: %d\n", config.batch_size);
    if (config.test_name)
    {
        printf("  Test Name: %s\n", config.test_name);
    }
    const char *pattern_name;
    switch (config.key_pattern)
    {
        case KEY_PATTERN_SEQUENTIAL:
            pattern_name = "Sequential";
            break;
        case KEY_PATTERN_RANDOM:
            pattern_name = "Random";
            break;
        case KEY_PATTERN_ZIPFIAN:
            pattern_name = "Zipfian (hot keys)";
            break;
        case KEY_PATTERN_UNIFORM:
            pattern_name = "Uniform Random";
            break;
        case KEY_PATTERN_TIMESTAMP:
            pattern_name = "Timestamp";
            break;
        case KEY_PATTERN_REVERSE:
            pattern_name = "Reverse Sequential";
            break;
        default:
            pattern_name = "Unknown";
            break;
    }
    printf("  Key Pattern: %s\n", pattern_name);
    printf("  Workload: %s\n", config.workload_type == WORKLOAD_WRITE    ? "Write-only"
                               : config.workload_type == WORKLOAD_READ   ? "Read-only"
                               : config.workload_type == WORKLOAD_DELETE ? "Delete-only"
                               : config.workload_type == WORKLOAD_SEEK   ? "Seek"
                               : config.workload_type == WORKLOAD_RANGE  ? "Range Query"
                                                                         : "Mixed");
    printf("  Sync Mode: %s\n", config.sync_enabled ? "Enabled (durable)" : "Disabled (fast)");
    printf("\n");

    benchmark_results_t *results = NULL;
    benchmark_results_t *baseline_results = NULL;

    if (run_benchmark(&config, &results) != 0)
    {
        fprintf(stderr, "Benchmark failed\n");
        return 1;
    }

    if (config.compare_mode)
    {
        const char *baseline_engine = NULL;
        if (strcmp(config.engine_name, "rocksdb") == 0)
        {
            baseline_engine = "tidesdb";
        }
        else if (strcmp(config.engine_name, "tidesdb") == 0)
        {
            baseline_engine = "rocksdb";
        }

        if (baseline_engine)
        {
            printf("\n=== Cleaning database for baseline comparison ===\n");

            char rm_cmd[2048];
            snprintf(rm_cmd, sizeof(rm_cmd), "rm -rf %s", config.db_path);
            int rm_result = system(rm_cmd);
            if (rm_result != 0)
            {
                fprintf(stderr, "Warning: Failed to clean database path for baseline\n");
            }

            printf("\n=== Running %s Baseline ===\n\n", baseline_engine);
            benchmark_config_t baseline_config = config;
            baseline_config.engine_name = baseline_engine;

            if (run_benchmark(&baseline_config, &baseline_results) != 0)
            {
                fprintf(stderr, "Baseline benchmark failed\n");
            }
        }
    }

    FILE *report_fp = stdout;
    if (config.report_file)
    {
        report_fp = fopen(config.report_file, "w");
        if (!report_fp)
        {
            fprintf(stderr, "Failed to open report file: %s\n", config.report_file);
            report_fp = stdout;
        }
    }

    printf("\n");   /* ensure newline before report */
    fflush(stdout); /* flush any buffered output */
    generate_report(report_fp, results, baseline_results);
    fflush(report_fp); /* ensure report is written */

    if (report_fp != stdout)
    {
        fclose(report_fp);
        printf("\nReport written to: %s\n", config.report_file);
    }

    if (config.csv_file)
    {
        /* we need to check if CSV file is empty to determine if we need to write header */
        int write_header = 0;
        FILE *check_fp = fopen(config.csv_file, "r");
        if (check_fp)
        {
            fseek(check_fp, 0, SEEK_END);
            if (ftell(check_fp) == 0)
            {
                write_header = 1;
            }
            fclose(check_fp);
        }
        else
        {
            /* file doesnt exist yet, we need to write header */
            write_header = 1;
        }

        FILE *csv_fp = fopen(config.csv_file, "a");
        if (csv_fp)
        {
            generate_csv(csv_fp, results, baseline_results, write_header);
            fclose(csv_fp);
            printf("CSV exported to: %s\n", config.csv_file);
        }
        else
        {
            fprintf(stderr, "Failed to open CSV file: %s\n", config.csv_file);
        }
    }

    free_results(results);
    if (baseline_results) free_results(baseline_results);

    return 0;
}
