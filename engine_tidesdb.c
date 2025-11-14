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
#include <stdlib.h>
#include <string.h>
#include <tidesdb/tidesdb.h>

#include "benchmark.h"

typedef struct
{
    tidesdb_t *db;
    tidesdb_column_family_t *cf;
    tidesdb_sync_mode_t sync_mode; 
} tidesdb_handle_t;

typedef struct
{
    tidesdb_iter_t *iter;
    tidesdb_txn_t *txn;
} tidesdb_iter_wrapper_t;

static const storage_engine_ops_t tidesdb_ops;

static int tidesdb_open_impl(storage_engine_t **engine, const char *path)
{
    *engine = malloc(sizeof(storage_engine_t));
    if (!*engine) return -1;

    tidesdb_handle_t *handle = malloc(sizeof(tidesdb_handle_t));
    if (!handle)
    {
        free(*engine);
        return -1;
    }

    tidesdb_config_t config;
    memset(&config, 0, sizeof(tidesdb_config_t));
    strncpy(config.db_path, path, sizeof(config.db_path) - 1);
    config.db_path[sizeof(config.db_path) - 1] = '\0';
    config.num_flush_threads = 4;
    config.num_compaction_threads = 4;
    config.enable_debug_logging = 0;

    if (tidesdb_open(&config, &handle->db) != 0)
    {
        free(handle);
        free(*engine);
        return -1;
    }
    tidesdb_column_family_config_t cf_config = tidesdb_default_column_family_config();
    cf_config.enable_compression = 1;
    cf_config.compression_algorithm = COMPRESS_LZ4;
    cf_config.enable_bloom_filter = 1;
    cf_config.enable_block_indexes = 1;
    cf_config.block_manager_cache_size = 64 * 1024 * 1024;
    cf_config.sync_mode = TDB_SYNC_NONE; /* default */
    cf_config.memtable_flush_size = 64 * 1024 * 1024;


    if (tidesdb_create_column_family(handle->db, "default", &cf_config) != 0)
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

    (*engine)->handle = handle;
    (*engine)->ops = &tidesdb_ops;

    return 0;
}

/* helper to set sync mode dynamically */
static void tidesdb_set_sync_mode(storage_engine_t *engine, int sync_enabled)
{
    tidesdb_handle_t *handle = (tidesdb_handle_t *)engine->handle;
    /* TidesDB sync modes TDB_SYNC_NONE, TDB_SYNC_FSYNC, TDB_SYNC_FDATASYNC */
    handle->sync_mode = sync_enabled ? TDB_SYNC_FDATASYNC : TDB_SYNC_NONE;
    
    /* get current column family stats to safely read config */
    tidesdb_column_family_stat_t *stats = NULL;
    if (tidesdb_get_column_family_stats(handle->db, "default", &stats) != 0)
    {
        return; /* failed to get stats, cannot update */
    }
    
    /* populate update config from current stats */
    tidesdb_column_family_update_config_t update_config = {
        .memtable_flush_size = stats->config.memtable_flush_size,
        .max_sstables_before_compaction = stats->config.max_sstables_before_compaction,
        .compaction_threads = stats->config.compaction_threads,
        .sl_max_level = stats->config.sl_max_level,
        .sl_probability = stats->config.sl_probability,
        .enable_bloom_filter = stats->config.enable_bloom_filter,
        .bloom_filter_fp_rate = stats->config.bloom_filter_fp_rate,
        .enable_background_compaction = stats->config.enable_background_compaction,
        .background_compaction_interval = stats->config.background_compaction_interval,
        .block_manager_cache_size = stats->config.block_manager_cache_size,
        .sync_mode = handle->sync_mode
    };
    
    free(stats);
    
    (void)tidesdb_update_column_family_config(handle->db, "default", &update_config);
}

static int tidesdb_close_impl(storage_engine_t *engine)
{
    tidesdb_handle_t *handle = (tidesdb_handle_t *)engine->handle;
    tidesdb_close(handle->db);
    free(handle);
    free(engine);
    return 0;
}

static int tidesdb_put_impl(storage_engine_t *engine, const uint8_t *key, size_t key_size,
                            const uint8_t *value, size_t value_size)
{
    tidesdb_handle_t *handle = (tidesdb_handle_t *)engine->handle;
    tidesdb_txn_t *txn = NULL;

    tidesdb_txn_begin(handle->db, handle->cf, &txn);
    int result = tidesdb_txn_put(txn, key, key_size, value, value_size, -1);
    tidesdb_txn_commit(txn);
    tidesdb_txn_free(txn);

    return result;
}

static int tidesdb_get_impl(storage_engine_t *engine, const uint8_t *key, size_t key_size,
                            uint8_t **value, size_t *value_size)
{
    tidesdb_handle_t *handle = (tidesdb_handle_t *)engine->handle;
    tidesdb_txn_t *txn = NULL;

    tidesdb_txn_begin_read(handle->db, handle->cf, &txn);
    int result = tidesdb_txn_get(txn, key, key_size, value, value_size);
    tidesdb_txn_free(txn);

    return result;
}

static int tidesdb_del_impl(storage_engine_t *engine, const uint8_t *key, size_t key_size)
{
    tidesdb_handle_t *handle = (tidesdb_handle_t *)engine->handle;
    tidesdb_txn_t *txn = NULL;

    tidesdb_txn_begin(handle->db, handle->cf, &txn);
    int result = tidesdb_txn_delete(txn, key, key_size);
    tidesdb_txn_commit(txn);
    tidesdb_txn_free(txn);

    return result;
}

static int tidesdb_iter_new_impl(storage_engine_t *engine, void **iter)
{
    tidesdb_handle_t *handle = (tidesdb_handle_t *)engine->handle;

    /* allocate wrapper to hold both iterator and transaction */
    tidesdb_iter_wrapper_t *wrapper = malloc(sizeof(tidesdb_iter_wrapper_t));
    if (!wrapper) return -1;

    /* create a fresh read transaction for this iteration */
    if (tidesdb_txn_begin_read(handle->db, handle->cf, &wrapper->txn) != 0)
    {
        free(wrapper);
        return -1;
    }

    /* create iterator from the transaction */
    if (tidesdb_iter_new(wrapper->txn, &wrapper->iter) != 0)
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
    tidesdb_iter_wrapper_t *wrapper = (tidesdb_iter_wrapper_t *)iter;
    return tidesdb_iter_seek_to_first(wrapper->iter);
}

static int tidesdb_iter_valid_impl(void *iter)
{
    tidesdb_iter_wrapper_t *wrapper = (tidesdb_iter_wrapper_t *)iter;
    return tidesdb_iter_valid(wrapper->iter);
}

static int tidesdb_iter_next_impl(void *iter)
{
    tidesdb_iter_wrapper_t *wrapper = (tidesdb_iter_wrapper_t *)iter;
    return tidesdb_iter_next(wrapper->iter);
}

static int tidesdb_iter_key_impl(void *iter, uint8_t **key, size_t *key_size)
{
    tidesdb_iter_wrapper_t *wrapper = (tidesdb_iter_wrapper_t *)iter;
    return tidesdb_iter_key(wrapper->iter, key, key_size);
}

static int tidesdb_iter_value_impl(void *iter, uint8_t **value, size_t *value_size)
{
    tidesdb_iter_wrapper_t *wrapper = (tidesdb_iter_wrapper_t *)iter;
    return tidesdb_iter_value(wrapper->iter, value, value_size);
}

static int tidesdb_iter_free_impl(void *iter)
{
    tidesdb_iter_wrapper_t *wrapper = (tidesdb_iter_wrapper_t *)iter;

    tidesdb_iter_free(wrapper->iter);

    tidesdb_txn_free(wrapper->txn);

    free(wrapper);

    return 0;
}

static const storage_engine_ops_t tidesdb_ops = {
    .open = tidesdb_open_impl,
    .close = tidesdb_close_impl,
    .put = tidesdb_put_impl,
    .get = tidesdb_get_impl,
    .del = tidesdb_del_impl,
    .iter_new = tidesdb_iter_new_impl,
    .iter_seek_to_first = tidesdb_iter_seek_to_first_impl,
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