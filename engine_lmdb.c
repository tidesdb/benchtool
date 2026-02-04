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

#include "benchmark.h"

#ifdef HAVE_LMDB
#include <lmdb.h>

const char *lmdb_version_str = MDB_VERSION_STRING;

typedef struct
{
    MDB_env *env;
    MDB_dbi dbi;
    int sync_enabled;
} lmdb_handle_t;

typedef struct
{
    MDB_txn *txn;
    lmdb_handle_t *handle;
} lmdb_batch_context_t;

typedef struct
{
    MDB_txn *txn;
    MDB_cursor *cursor;
    MDB_val key;
    MDB_val value;
    int valid;
} lmdb_iter_t;

static const storage_engine_ops_t lmdb_ops;

static int lmdb_open_impl(storage_engine_t **engine, const char *path,
                          const benchmark_config_t *config)
{
    *engine = malloc(sizeof(storage_engine_t));
    if (!*engine) return -1;

    lmdb_handle_t *handle = malloc(sizeof(lmdb_handle_t));
    if (!handle)
    {
        free(*engine);
        return -1;
    }

    int rc = mdb_env_create(&handle->env);
    if (rc != 0)
    {
        free(handle);
        free(*engine);
        return -1;
    }

    size_t map_size = config->memtable_size > 0 ? config->memtable_size : (size_t)10 * 1024 * 1024 * 1024;
    mdb_env_set_mapsize(handle->env, map_size);

    mdb_env_set_maxreaders(handle->env, config->num_threads > 0 ? config->num_threads * 2 : 128);

    unsigned int env_flags = MDB_NOSUBDIR;
    if (!config->sync_enabled)
    {
        env_flags |= MDB_NOSYNC | MDB_WRITEMAP;
    }

    rc = mdb_env_open(handle->env, path, env_flags, 0664);
    if (rc != 0)
    {
        mdb_env_close(handle->env);
        free(handle);
        free(*engine);
        return -1;
    }

    MDB_txn *txn;
    rc = mdb_txn_begin(handle->env, NULL, 0, &txn);
    if (rc != 0)
    {
        mdb_env_close(handle->env);
        free(handle);
        free(*engine);
        return -1;
    }

    rc = mdb_dbi_open(txn, NULL, 0, &handle->dbi);
    if (rc != 0)
    {
        mdb_txn_abort(txn);
        mdb_env_close(handle->env);
        free(handle);
        free(*engine);
        return -1;
    }

    rc = mdb_txn_commit(txn);
    if (rc != 0)
    {
        mdb_env_close(handle->env);
        free(handle);
        free(*engine);
        return -1;
    }

    handle->sync_enabled = config->sync_enabled;

    (*engine)->handle = handle;
    (*engine)->ops = &lmdb_ops;

    return 0;
}

static void lmdb_set_sync_mode(storage_engine_t *engine, int sync_enabled)
{
    lmdb_handle_t *handle = (lmdb_handle_t *)engine->handle;
    handle->sync_enabled = sync_enabled;

    unsigned int flags = 0;
    if (!sync_enabled)
    {
        flags = MDB_NOSYNC;
    }
    mdb_env_set_flags(handle->env, MDB_NOSYNC, !sync_enabled);
}

static int lmdb_close_impl(storage_engine_t *engine)
{
    lmdb_handle_t *handle = (lmdb_handle_t *)engine->handle;
    mdb_dbi_close(handle->env, handle->dbi);
    mdb_env_close(handle->env);
    free(handle);
    free(engine);
    return 0;
}

static int lmdb_put_impl(storage_engine_t *engine, const uint8_t *key, size_t key_size,
                         const uint8_t *value, size_t value_size)
{
    lmdb_handle_t *handle = (lmdb_handle_t *)engine->handle;

    MDB_txn *txn;
    int rc = mdb_txn_begin(handle->env, NULL, 0, &txn);
    if (rc != 0) return -1;

    MDB_val mdb_key = {.mv_size = key_size, .mv_data = (void *)key};
    MDB_val mdb_value = {.mv_size = value_size, .mv_data = (void *)value};

    rc = mdb_put(txn, handle->dbi, &mdb_key, &mdb_value, 0);
    if (rc != 0)
    {
        mdb_txn_abort(txn);
        return -1;
    }

    rc = mdb_txn_commit(txn);
    return rc == 0 ? 0 : -1;
}

static int lmdb_get_impl(storage_engine_t *engine, const uint8_t *key, size_t key_size,
                         uint8_t **value, size_t *value_size)
{
    lmdb_handle_t *handle = (lmdb_handle_t *)engine->handle;

    MDB_txn *txn;
    int rc = mdb_txn_begin(handle->env, NULL, MDB_RDONLY, &txn);
    if (rc != 0) return -1;

    MDB_val mdb_key = {.mv_size = key_size, .mv_data = (void *)key};
    MDB_val mdb_value;

    rc = mdb_get(txn, handle->dbi, &mdb_key, &mdb_value);
    if (rc != 0)
    {
        mdb_txn_abort(txn);
        return -1;
    }

    *value = malloc(mdb_value.mv_size);
    if (!*value)
    {
        mdb_txn_abort(txn);
        return -1;
    }

    memcpy(*value, mdb_value.mv_data, mdb_value.mv_size);
    *value_size = mdb_value.mv_size;

    mdb_txn_abort(txn);
    return 0;
}

static int lmdb_del_impl(storage_engine_t *engine, const uint8_t *key, size_t key_size)
{
    lmdb_handle_t *handle = (lmdb_handle_t *)engine->handle;

    MDB_txn *txn;
    int rc = mdb_txn_begin(handle->env, NULL, 0, &txn);
    if (rc != 0) return -1;

    MDB_val mdb_key = {.mv_size = key_size, .mv_data = (void *)key};

    rc = mdb_del(txn, handle->dbi, &mdb_key, NULL);
    if (rc != 0 && rc != MDB_NOTFOUND)
    {
        mdb_txn_abort(txn);
        return -1;
    }

    rc = mdb_txn_commit(txn);
    return rc == 0 ? 0 : -1;
}

static int lmdb_batch_begin_impl(storage_engine_t *engine, void **batch_ctx)
{
    lmdb_handle_t *handle = (lmdb_handle_t *)engine->handle;

    lmdb_batch_context_t *ctx = malloc(sizeof(lmdb_batch_context_t));
    if (!ctx) return -1;

    int rc = mdb_txn_begin(handle->env, NULL, 0, &ctx->txn);
    if (rc != 0)
    {
        free(ctx);
        return -1;
    }

    ctx->handle = handle;
    *batch_ctx = ctx;
    return 0;
}

static int lmdb_batch_put_impl(void *batch_ctx, storage_engine_t *engine, const uint8_t *key,
                               size_t key_size, const uint8_t *value, size_t value_size)
{
    (void)engine;
    lmdb_batch_context_t *ctx = (lmdb_batch_context_t *)batch_ctx;

    MDB_val mdb_key = {.mv_size = key_size, .mv_data = (void *)key};
    MDB_val mdb_value = {.mv_size = value_size, .mv_data = (void *)value};

    int rc = mdb_put(ctx->txn, ctx->handle->dbi, &mdb_key, &mdb_value, 0);
    return rc == 0 ? 0 : -1;
}

static int lmdb_batch_delete_impl(void *batch_ctx, storage_engine_t *engine, const uint8_t *key,
                                  size_t key_size)
{
    (void)engine;
    lmdb_batch_context_t *ctx = (lmdb_batch_context_t *)batch_ctx;

    MDB_val mdb_key = {.mv_size = key_size, .mv_data = (void *)key};

    int rc = mdb_del(ctx->txn, ctx->handle->dbi, &mdb_key, NULL);
    /* MDB_NOTFOUND is acceptable for delete */
    return (rc == 0 || rc == MDB_NOTFOUND) ? 0 : -1;
}

static int lmdb_batch_commit_impl(void *batch_ctx)
{
    lmdb_batch_context_t *ctx = (lmdb_batch_context_t *)batch_ctx;

    int rc = mdb_txn_commit(ctx->txn);
    free(ctx);
    return rc == 0 ? 0 : -1;
}

static int lmdb_iter_new_impl(storage_engine_t *engine, void **iter)
{
    lmdb_handle_t *handle = (lmdb_handle_t *)engine->handle;

    lmdb_iter_t *it = malloc(sizeof(lmdb_iter_t));
    if (!it) return -1;

    int rc = mdb_txn_begin(handle->env, NULL, MDB_RDONLY, &it->txn);
    if (rc != 0)
    {
        free(it);
        return -1;
    }

    rc = mdb_cursor_open(it->txn, handle->dbi, &it->cursor);
    if (rc != 0)
    {
        mdb_txn_abort(it->txn);
        free(it);
        return -1;
    }

    it->valid = 0;
    memset(&it->key, 0, sizeof(it->key));
    memset(&it->value, 0, sizeof(it->value));

    *iter = it;
    return 0;
}

static int lmdb_iter_seek_to_first_impl(void *iter)
{
    lmdb_iter_t *it = (lmdb_iter_t *)iter;

    int rc = mdb_cursor_get(it->cursor, &it->key, &it->value, MDB_FIRST);
    it->valid = (rc == 0);
    return 0;
}

static int lmdb_iter_seek_impl(void *iter, const uint8_t *key, size_t key_size)
{
    lmdb_iter_t *it = (lmdb_iter_t *)iter;

    it->key.mv_size = key_size;
    it->key.mv_data = (void *)key;

    int rc = mdb_cursor_get(it->cursor, &it->key, &it->value, MDB_SET_RANGE);
    it->valid = (rc == 0);
    return 0;
}

static int lmdb_iter_valid_impl(void *iter)
{
    lmdb_iter_t *it = (lmdb_iter_t *)iter;
    return it->valid;
}

static int lmdb_iter_next_impl(void *iter)
{
    lmdb_iter_t *it = (lmdb_iter_t *)iter;

    int rc = mdb_cursor_get(it->cursor, &it->key, &it->value, MDB_NEXT);
    it->valid = (rc == 0);
    return 0;
}

static int lmdb_iter_key_impl(void *iter, uint8_t **key, size_t *key_size)
{
    lmdb_iter_t *it = (lmdb_iter_t *)iter;

    if (!it->valid) return -1;

    *key = malloc(it->key.mv_size);
    if (!*key) return -1;

    memcpy(*key, it->key.mv_data, it->key.mv_size);
    *key_size = it->key.mv_size;
    return 0;
}

static int lmdb_iter_value_impl(void *iter, uint8_t **value, size_t *value_size)
{
    lmdb_iter_t *it = (lmdb_iter_t *)iter;

    if (!it->valid) return -1;

    *value = malloc(it->value.mv_size);
    if (!*value) return -1;

    memcpy(*value, it->value.mv_data, it->value.mv_size);
    *value_size = it->value.mv_size;
    return 0;
}

static int lmdb_iter_free_impl(void *iter)
{
    lmdb_iter_t *it = (lmdb_iter_t *)iter;

    mdb_cursor_close(it->cursor);
    mdb_txn_abort(it->txn);
    free(it);
    return 0;
}

static const storage_engine_ops_t lmdb_ops = {
    .open = lmdb_open_impl,
    .close = lmdb_close_impl,
    .put = lmdb_put_impl,
    .get = lmdb_get_impl,
    .del = lmdb_del_impl,
    .batch_begin = lmdb_batch_begin_impl,
    .batch_put = lmdb_batch_put_impl,
    .batch_delete = lmdb_batch_delete_impl,
    .batch_commit = lmdb_batch_commit_impl,
    .iter_new = lmdb_iter_new_impl,
    .iter_seek_to_first = lmdb_iter_seek_to_first_impl,
    .iter_seek = lmdb_iter_seek_impl,
    .iter_valid = lmdb_iter_valid_impl,
    .iter_next = lmdb_iter_next_impl,
    .iter_key = lmdb_iter_key_impl,
    .iter_value = lmdb_iter_value_impl,
    .iter_free = lmdb_iter_free_impl,
    .set_sync = lmdb_set_sync_mode,
    .name = "lmdb",
};

const storage_engine_ops_t *get_lmdb_ops(void)
{
    return &lmdb_ops;
}

#else /* !HAVE_LMDB */

const storage_engine_ops_t *get_lmdb_ops(void)
{
    return NULL;
}

#endif /* HAVE_LMDB */
