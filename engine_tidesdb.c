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
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <tidesdb/objstore.h>
#include <tidesdb/tidesdb.h>
#ifdef BENCHTOOL_WITH_S3
#include <tidesdb/objstore_s3.h>
#endif

#include "benchmark.h"

typedef struct
{
    tidesdb_t *db;
    tidesdb_column_family_t *cf;
    tidesdb_sync_mode_t sync_mode;
    tidesdb_column_family_config_t cf_config; /* store config to avoid duplication */
    pthread_key_t txn_key;                    /* thread-local key for reusable transactions */
    int txn_key_initialized;                  /* flag to track if key was created */
    tidesdb_objstore_config_t os_cfg;         /* object store config (when active) */
    int os_cfg_initialized;                   /* 1 when os_cfg is populated for this db */
} tidesdb_handle_t;

static int parse_sync_mode(const char *name, tidesdb_sync_mode_t *out)
{
    if (!name) return -1;
    if (strcmp(name, "none") == 0)
    {
        *out = TDB_SYNC_NONE;
        return 0;
    }
    if (strcmp(name, "full") == 0)
    {
        *out = TDB_SYNC_FULL;
        return 0;
    }
    if (strcmp(name, "interval") == 0)
    {
        *out = TDB_SYNC_INTERVAL;
        return 0;
    }
    return -1;
}

static int parse_compression(const char *name, compression_algorithm *out)
{
    if (!name) return -1;
    if (strcmp(name, "none") == 0)
    {
        *out = TDB_COMPRESS_NONE;
        return 0;
    }
    if (strcmp(name, "lz4") == 0)
    {
        *out = TDB_COMPRESS_LZ4;
        return 0;
    }
    if (strcmp(name, "lz4fast") == 0 || strcmp(name, "lz4_fast") == 0)
    {
        *out = TDB_COMPRESS_LZ4_FAST;
        return 0;
    }
    if (strcmp(name, "zstd") == 0)
    {
        *out = TDB_COMPRESS_ZSTD;
        return 0;
    }
    if (strcmp(name, "snappy") == 0)
    {
        *out = TDB_COMPRESS_SNAPPY;
        return 0;
    }
    return -1;
}

/* thread-local transaction wrapper for reuse via tidesdb_txn_reset */
typedef struct
{
    tidesdb_txn_t *txn;
    int committed; /* 1 if txn was committed/aborted and can be reset */
} thread_local_txn_t;

typedef struct
{
    tidesdb_iter_t *iter;
    tidesdb_txn_t *txn; /* read-only transaction for consistent iteration */
} tidesdb_iter_wrapper_t;

static const storage_engine_ops_t tidesdb_ops;

/* destructor for thread-local transaction storage - called when thread exits */
static void thread_local_txn_destructor(void *data)
{
    thread_local_txn_t *tl_txn = (thread_local_txn_t *)data;
    if (tl_txn)
    {
        if (tl_txn->txn)
        {
            tidesdb_txn_free(tl_txn->txn);
        }
        free(tl_txn);
    }
}

static int tidesdb_open_impl(storage_engine_t **engine, const char *path,
                             const benchmark_config_t *config)
{
    *engine = malloc(sizeof(storage_engine_t));
    if (!*engine) return -1;

    tidesdb_handle_t *handle = malloc(sizeof(tidesdb_handle_t));
    if (!handle)
    {
        free(*engine);
        return -1;
    }

    handle->os_cfg_initialized = 0;

    tidesdb_config_t tdb_config = tidesdb_default_config();
    tdb_config.db_path = (char *)path; /* tidesdb_open makes its own copy */

    /** because we are using 1 column family, we don't really need many threads as flushes and
    compactions are done serially, the only time it parallelizes is when there are many column
    families flushing and compacting. These can be overridden via CLI. */
    tdb_config.num_flush_threads = config->num_flush_threads > 0 ? config->num_flush_threads : 1;
    tdb_config.num_compaction_threads =
        config->num_compaction_threads > 0 ? config->num_compaction_threads : 1;
    tdb_config.log_to_file = config->log_to_file != 0 ? config->log_to_file : 1;
    tdb_config.log_level = config->debug_logging ? TDB_LOG_DEBUG : TDB_LOG_NONE;

    tdb_config.block_cache_size =
        config->block_cache_size > 0 ? config->block_cache_size : TDB_DEFAULT_BLOCK_CACHE_SIZE;

    if (config->max_open_sstables > 0)
    {
        tdb_config.max_open_sstables = config->max_open_sstables;
    }
    if (config->max_memory_usage > 0)
    {
        tdb_config.max_memory_usage = config->max_memory_usage;
    }

    /* unified memtable mode */
    tdb_config.unified_memtable = config->unified_memtable ? 1 : 0;
    tdb_config.unified_memtable_write_buffer_size = config->unified_memtable_write_buffer_size;
    tdb_config.unified_memtable_skip_list_max_level = config->unified_memtable_skip_list_max_level;
    tdb_config.unified_memtable_skip_list_probability =
        config->unified_memtable_skip_list_probability;
    if (config->unified_memtable_sync_mode)
    {
        tidesdb_sync_mode_t mode;
        if (parse_sync_mode(config->unified_memtable_sync_mode, &mode) != 0)
        {
            fprintf(stderr, "Invalid unified_memtable_sync_mode: %s\n",
                    config->unified_memtable_sync_mode);
            free(handle);
            free(*engine);
            return -1;
        }
        tdb_config.unified_memtable_sync_mode = (int)mode;
    }
    tdb_config.unified_memtable_sync_interval_us = config->unified_memtable_sync_interval_us;

    /* object store mode */
    if (config->object_store_backend && strcmp(config->object_store_backend, "none") != 0)
    {
        tidesdb_objstore_t *os = NULL;
        if (strcmp(config->object_store_backend, "fs") == 0)
        {
            if (!config->object_store_fs_path)
            {
                fprintf(stderr, "object_store=fs requires --object-store-fs-path\n");
                free(handle);
                free(*engine);
                return -1;
            }
            os = tidesdb_objstore_fs_create(config->object_store_fs_path);
        }
        else if (strcmp(config->object_store_backend, "s3") == 0)
        {
#ifdef BENCHTOOL_WITH_S3
            if (!config->s3_endpoint || !config->s3_bucket || !config->s3_access_key ||
                !config->s3_secret_key)
            {
                fprintf(stderr,
                        "object_store=s3 requires --s3-endpoint, --s3-bucket, --s3-access-key, "
                        "--s3-secret-key\n");
                free(handle);
                free(*engine);
                return -1;
            }
            os = tidesdb_objstore_s3_create(
                config->s3_endpoint, config->s3_bucket, config->s3_prefix, config->s3_access_key,
                config->s3_secret_key, config->s3_region, config->s3_use_ssl ? 1 : 0,
                config->s3_use_path_style ? 1 : 0);
#else
            fprintf(stderr,
                    "S3 object store requires building benchtool with -DBENCHTOOL_WITH_S3=ON "
                    "and TidesDB built with -DTIDESDB_WITH_S3=ON\n");
            free(handle);
            free(*engine);
            return -1;
#endif
        }
        else
        {
            fprintf(stderr, "Unknown object store backend: %s (expected none|fs|s3)\n",
                    config->object_store_backend);
            free(handle);
            free(*engine);
            return -1;
        }

        if (!os)
        {
            fprintf(stderr, "Failed to create object store connector (%s)\n",
                    config->object_store_backend);
            free(handle);
            free(*engine);
            return -1;
        }

        handle->os_cfg = tidesdb_objstore_default_config();
        if (config->object_local_cache_path)
            handle->os_cfg.local_cache_path = config->object_local_cache_path;
        if (config->object_local_cache_max_bytes)
            handle->os_cfg.local_cache_max_bytes = config->object_local_cache_max_bytes;
        if (config->object_cache_on_read >= 0)
            handle->os_cfg.cache_on_read = config->object_cache_on_read;
        if (config->object_cache_on_write >= 0)
            handle->os_cfg.cache_on_write = config->object_cache_on_write;
        if (config->object_max_concurrent_uploads > 0)
            handle->os_cfg.max_concurrent_uploads = config->object_max_concurrent_uploads;
        if (config->object_max_concurrent_downloads > 0)
            handle->os_cfg.max_concurrent_downloads = config->object_max_concurrent_downloads;
        if (config->object_multipart_threshold)
            handle->os_cfg.multipart_threshold = config->object_multipart_threshold;
        if (config->object_multipart_part_size)
            handle->os_cfg.multipart_part_size = config->object_multipart_part_size;
        if (config->object_sync_manifest >= 0)
            handle->os_cfg.sync_manifest_to_object = config->object_sync_manifest;
        if (config->object_replicate_wal >= 0)
            handle->os_cfg.replicate_wal = config->object_replicate_wal;
        if (config->object_wal_upload_sync >= 0)
            handle->os_cfg.wal_upload_sync = config->object_wal_upload_sync;
        if (config->object_wal_sync_threshold_bytes)
            handle->os_cfg.wal_sync_threshold_bytes = config->object_wal_sync_threshold_bytes;
        if (config->object_wal_sync_on_commit >= 0)
            handle->os_cfg.wal_sync_on_commit = config->object_wal_sync_on_commit;
        if (config->object_replica_mode >= 0)
            handle->os_cfg.replica_mode = config->object_replica_mode;
        if (config->object_replica_sync_interval_us)
            handle->os_cfg.replica_sync_interval_us = config->object_replica_sync_interval_us;
        if (config->object_replica_replay_wal >= 0)
            handle->os_cfg.replica_replay_wal = config->object_replica_replay_wal;

        handle->os_cfg_initialized = 1;
        tdb_config.object_store = os;
        tdb_config.object_store_config = &handle->os_cfg;
    }

    if (tidesdb_open(&tdb_config, &handle->db) != 0)
    {
        free(handle);
        free(*engine);
        return -1;
    }

    handle->cf_config = tidesdb_default_column_family_config();

    /* compression algorithm (defaults to LZ4 for parity with existing benchtool behavior) */
    {
        compression_algorithm algo = TDB_COMPRESS_LZ4;
        if (config->compression_algorithm &&
            parse_compression(config->compression_algorithm, &algo) != 0)
        {
            fprintf(stderr, "Invalid compression algorithm: %s\n", config->compression_algorithm);
            tidesdb_close(handle->db);
            free(handle);
            free(*engine);
            return -1;
        }
        handle->cf_config.compression_algorithm = algo;
    }

    handle->cf_config.bloom_fpr = config->bloom_fpr;
    handle->cf_config.l0_queue_stall_threshold = config->l0_queue_stall_threshold;
    handle->cf_config.l1_file_count_trigger = config->l1_file_count_trigger;
    handle->cf_config.dividing_level_offset = config->dividing_level_offset;
    handle->cf_config.min_levels = config->min_levels;
    handle->cf_config.index_sample_ratio = config->index_sample_ratio;
    handle->cf_config.block_index_prefix_len = config->block_index_prefix_len;
    handle->cf_config.klog_value_threshold = config->klog_value_threshold;
    handle->cf_config.use_btree = config->use_btree;

    if (config->level_size_ratio > 0) handle->cf_config.level_size_ratio = config->level_size_ratio;
    if (config->skip_list_max_level > 0)
        handle->cf_config.skip_list_max_level = config->skip_list_max_level;
    if (config->skip_list_probability > 0.0f)
        handle->cf_config.skip_list_probability = config->skip_list_probability;

    handle->cf_config.enable_bloom_filter =
        config->enable_bloom_filter >= 0 ? config->enable_bloom_filter : 1;

    handle->cf_config.enable_block_indexes =
        config->enable_block_indexes >= 0 ? config->enable_block_indexes : 1;

    /* per-CF object store tuning */
    if (config->object_lazy_compaction >= 0)
        handle->cf_config.object_lazy_compaction = config->object_lazy_compaction;
    if (config->object_prefetch_compaction >= 0)
        handle->cf_config.object_prefetch_compaction = config->object_prefetch_compaction;

    /* explicit string wins, else fall back to the sync_enabled boolean */
    if (config->sync_mode)
    {
        tidesdb_sync_mode_t mode;
        if (parse_sync_mode(config->sync_mode, &mode) != 0)
        {
            fprintf(stderr, "Invalid sync_mode: %s\n", config->sync_mode);
            tidesdb_close(handle->db);
            free(handle);
            free(*engine);
            return -1;
        }
        handle->cf_config.sync_mode = mode;
    }
    else
    {
        handle->cf_config.sync_mode = config->sync_enabled ? TDB_SYNC_FULL : TDB_SYNC_NONE;
    }
    if (config->sync_interval_us > 0) handle->cf_config.sync_interval_us = config->sync_interval_us;

    handle->cf_config.write_buffer_size =
        config->memtable_size > 0 ? config->memtable_size : TDB_DEFAULT_WRITE_BUFFER_SIZE;
    if (tidesdb_create_column_family(handle->db, "default", &handle->cf_config) != 0)
    {
        /* column family might already exist, which is fine */
    }

    handle->cf = tidesdb_get_column_family(handle->db, "default");
    if (!handle->cf)
    {
        tidesdb_close(handle->db);
        free(handle);
        free(*engine);
        return -1;
    }

    /* we initialize thread-local storage for reusable transactions */
    if (pthread_key_create(&handle->txn_key, thread_local_txn_destructor) == 0)
    {
        handle->txn_key_initialized = 1;
    }
    else
    {
        handle->txn_key_initialized = 0;
    }

    (*engine)->handle = handle;
    (*engine)->ops = &tidesdb_ops;

    return 0;
}

/* helper to set sync mode dynamically */
static void tidesdb_set_sync_mode(storage_engine_t *engine, int sync_enabled)
{
    tidesdb_handle_t *handle = (tidesdb_handle_t *)engine->handle;
    handle->sync_mode = sync_enabled ? TDB_SYNC_FULL : TDB_SYNC_NONE;

    /* we update column family config with new sync mode */
    handle->cf_config.sync_mode = handle->sync_mode;

    int result = tidesdb_cf_update_runtime_config(handle->cf, &handle->cf_config, 0);
    if (result != 0)
    {
        fprintf(stderr, "Warning: Failed to update sync mode to %s\n",
                sync_enabled ? "FULL" : "NONE");
    }
}

static int tidesdb_close_impl(storage_engine_t *engine)
{
    tidesdb_handle_t *handle = (tidesdb_handle_t *)engine->handle;

    /* thread-local transactions are cleaned up by thread_local_txn_destructor
     * when each thread exits, so we only need to delete the key here */
    if (handle->txn_key_initialized)
    {
        pthread_key_delete(handle->txn_key);
    }

    /* tidesdb_close() frees db_path internally, so we don't free it here */
    tidesdb_close(handle->db);
    free(handle);
    free(engine);
    return 0;
}

/* helper to get or create a reusable transaction for the current thread */
static tidesdb_txn_t *get_or_create_txn(tidesdb_handle_t *handle, thread_local_txn_t **tl_out)
{
    if (!handle->txn_key_initialized)
    {
        *tl_out = NULL;
        tidesdb_txn_t *txn = NULL;
        if (tidesdb_txn_begin(handle->db, &txn) != 0) return NULL;
        return txn;
    }

    thread_local_txn_t *tl_txn = pthread_getspecific(handle->txn_key);
    if (!tl_txn)
    {
        /* first call from this thread, thus we allocate thread-local storage */
        tl_txn = malloc(sizeof(thread_local_txn_t));
        if (!tl_txn)
        {
            *tl_out = NULL;
            return NULL;
        }
        tl_txn->txn = NULL;
        tl_txn->committed = 1; /* mark as needing new txn */
        pthread_setspecific(handle->txn_key, tl_txn);
    }

    *tl_out = tl_txn;

    if (tl_txn->txn && tl_txn->committed)
    {
        /* we reuse existing transaction via reset */
        if (tidesdb_txn_reset(tl_txn->txn, TDB_ISOLATION_READ_COMMITTED) == 0)
        {
            tl_txn->committed = 0;
            return tl_txn->txn;
        }
        /* reset failed, we free and create new */
        tidesdb_txn_free(tl_txn->txn);
        tl_txn->txn = NULL;
    }

    if (!tl_txn->txn)
    {
        if (tidesdb_txn_begin(handle->db, &tl_txn->txn) != 0)
        {
            return NULL;
        }
        tl_txn->committed = 0;
    }

    return tl_txn->txn;
}

static int tidesdb_put_impl(storage_engine_t *engine, const uint8_t *key, size_t key_size,
                            const uint8_t *value, size_t value_size)
{
    tidesdb_handle_t *handle = (tidesdb_handle_t *)engine->handle;
    thread_local_txn_t *tl_txn = NULL;
    tidesdb_txn_t *txn = get_or_create_txn(handle, &tl_txn);
    if (!txn) return -1;

    int result = tidesdb_txn_put(txn, handle->cf, key, key_size, value, value_size, 0);
    if (result == 0) result = tidesdb_txn_commit(txn);

    if (tl_txn)
    {
        /* we mark as committed so it can be reset on next use */
        tl_txn->committed = 1;
    }
    else
    {
        /* fallback path is to free the transaction */
        tidesdb_txn_free(txn);
    }

    return result;
}

static int tidesdb_get_impl(storage_engine_t *engine, const uint8_t *key, size_t key_size,
                            uint8_t **value, size_t *value_size)
{
    tidesdb_handle_t *handle = (tidesdb_handle_t *)engine->handle;
    thread_local_txn_t *tl_txn = NULL;
    tidesdb_txn_t *txn = get_or_create_txn(handle, &tl_txn);
    if (!txn) return -1;

    int result = tidesdb_txn_get(txn, handle->cf, key, key_size, value, value_size);

    /* GET doesn't need commit, but we mark as committed so reset works */
    if (tl_txn)
    {
        tl_txn->committed = 1;
    }
    else
    {
        tidesdb_txn_free(txn);
    }

    return result;
}

static int tidesdb_del_impl(storage_engine_t *engine, const uint8_t *key, size_t key_size)
{
    tidesdb_handle_t *handle = (tidesdb_handle_t *)engine->handle;
    thread_local_txn_t *tl_txn = NULL;
    tidesdb_txn_t *txn = get_or_create_txn(handle, &tl_txn);
    if (!txn) return -1;

    int result = tidesdb_txn_delete(txn, handle->cf, key, key_size);
    if (result == 0) result = tidesdb_txn_commit(txn);

    if (tl_txn)
    {
        tl_txn->committed = 1;
    }
    else
    {
        tidesdb_txn_free(txn);
    }

    return result;
}

static int tidesdb_batch_begin_impl(storage_engine_t *engine, void **batch_ctx)
{
    tidesdb_handle_t *handle = (tidesdb_handle_t *)engine->handle;
    tidesdb_txn_t *txn = NULL;

    if (tidesdb_txn_begin(handle->db, &txn) != 0) return -1;

    *batch_ctx = txn;
    return 0;
}

static int tidesdb_batch_put_impl(void *batch_ctx, storage_engine_t *engine, const uint8_t *key,
                                  size_t key_size, const uint8_t *value, size_t value_size)
{
    tidesdb_handle_t *handle = (tidesdb_handle_t *)engine->handle;
    tidesdb_txn_t *txn = (tidesdb_txn_t *)batch_ctx;

    return tidesdb_txn_put(txn, handle->cf, key, key_size, value, value_size, 0);
}

static int tidesdb_batch_commit_impl(void *batch_ctx)
{
    tidesdb_txn_t *txn = (tidesdb_txn_t *)batch_ctx;

    int result = tidesdb_txn_commit(txn);
    tidesdb_txn_free(txn);

    return result;
}

static int tidesdb_batch_delete_impl(void *batch_ctx, storage_engine_t *engine, const uint8_t *key,
                                     size_t key_size)
{
    tidesdb_handle_t *handle = (tidesdb_handle_t *)engine->handle;
    tidesdb_txn_t *txn = (tidesdb_txn_t *)batch_ctx;

    return tidesdb_txn_delete(txn, handle->cf, key, key_size);
}

static int tidesdb_iter_new_impl(storage_engine_t *engine, void **iter)
{
    tidesdb_handle_t *handle = (tidesdb_handle_t *)engine->handle;

    /* we allocate wrapper to hold both iterator and transaction */
    tidesdb_iter_wrapper_t *wrapper = malloc(sizeof(tidesdb_iter_wrapper_t));
    if (!wrapper) return -1;

    /* we create a fresh read-only transaction for this iteration */
    if (tidesdb_txn_begin(handle->db, &wrapper->txn) != 0)
    {
        free(wrapper);
        return -1;
    }

    /* we create iterator from the transaction for the specific CF */
    if (tidesdb_iter_new(wrapper->txn, handle->cf, &wrapper->iter) != 0)
    {
        tidesdb_txn_free(wrapper->txn);
        free(wrapper);
        return -1;
    }

    *iter = wrapper;
    return 0;
}

static int tidesdb_iter_seek_to_first_impl(void *iter)
{
    if (!iter) return -1;
    tidesdb_iter_wrapper_t *wrapper = (tidesdb_iter_wrapper_t *)iter;
    if (!wrapper->iter) return -1;
    return tidesdb_iter_seek_to_first(wrapper->iter);
}

static int tidesdb_iter_seek_impl(void *iter, const uint8_t *key, size_t key_size)
{
    if (!iter) return -1;
    tidesdb_iter_wrapper_t *wrapper = (tidesdb_iter_wrapper_t *)iter;
    if (!wrapper->iter) return -1;
    return tidesdb_iter_seek(wrapper->iter, key, key_size);
}

static int tidesdb_iter_valid_impl(void *iter)
{
    if (!iter) return 0;
    tidesdb_iter_wrapper_t *wrapper = (tidesdb_iter_wrapper_t *)iter;
    if (!wrapper->iter) return 0;
    return tidesdb_iter_valid(wrapper->iter);
}

static int tidesdb_iter_next_impl(void *iter)
{
    if (!iter) return -1;
    tidesdb_iter_wrapper_t *wrapper = (tidesdb_iter_wrapper_t *)iter;
    if (!wrapper->iter) return -1;
    return tidesdb_iter_next(wrapper->iter);
}

static int tidesdb_iter_key_impl(void *iter, uint8_t **key, size_t *key_size)
{
    if (!iter || !key || !key_size) return -1;
    tidesdb_iter_wrapper_t *wrapper = (tidesdb_iter_wrapper_t *)iter;
    if (!wrapper->iter) return -1;
    return tidesdb_iter_key(wrapper->iter, key, key_size);
}

static int tidesdb_iter_value_impl(void *iter, uint8_t **value, size_t *value_size)
{
    if (!iter || !value || !value_size) return -1;
    tidesdb_iter_wrapper_t *wrapper = (tidesdb_iter_wrapper_t *)iter;
    if (!wrapper->iter) return -1;
    return tidesdb_iter_value(wrapper->iter, value, value_size);
}

static int tidesdb_iter_free_impl(void *iter)
{
    if (!iter) return 0;

    tidesdb_iter_wrapper_t *wrapper = (tidesdb_iter_wrapper_t *)iter;

    if (wrapper->iter)
    {
        tidesdb_iter_free(wrapper->iter);
        wrapper->iter = NULL;
    }

    if (wrapper->txn)
    {
        tidesdb_txn_free(wrapper->txn);
        wrapper->txn = NULL;
    }

    free(wrapper);

    return 0;
}

static const storage_engine_ops_t tidesdb_ops = {
    .open = tidesdb_open_impl,
    .close = tidesdb_close_impl,
    .put = tidesdb_put_impl,
    .get = tidesdb_get_impl,
    .del = tidesdb_del_impl,
    .batch_begin = tidesdb_batch_begin_impl,
    .batch_put = tidesdb_batch_put_impl,
    .batch_delete = tidesdb_batch_delete_impl,
    .batch_commit = tidesdb_batch_commit_impl,
    .iter_new = tidesdb_iter_new_impl,
    .iter_seek_to_first = tidesdb_iter_seek_to_first_impl,
    .iter_seek = tidesdb_iter_seek_impl,
    .iter_valid = tidesdb_iter_valid_impl,
    .iter_next = tidesdb_iter_next_impl,
    .iter_key = tidesdb_iter_key_impl,
    .iter_value = tidesdb_iter_value_impl,
    .iter_free = tidesdb_iter_free_impl,
    .set_sync = tidesdb_set_sync_mode,
    .name = "TidesDB"};

const storage_engine_ops_t *get_tidesdb_ops(void)
{
    return &tidesdb_ops;
}
