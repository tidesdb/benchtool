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

#ifdef HAVE_ROCKSDB
#include <rocksdb/c.h>
typedef struct {
  rocksdb_t *db;
  rocksdb_options_t *options;
  rocksdb_readoptions_t *roptions;
  rocksdb_writeoptions_t *woptions;
} rocksdb_handle_t;

static const storage_engine_ops_t rocksdb_ops;

static int rocksdb_open_impl(storage_engine_t **engine, const char *path) {
  *engine = malloc(sizeof(storage_engine_t));
  if (!*engine)
    return -1;

  rocksdb_handle_t *handle = malloc(sizeof(rocksdb_handle_t));
  if (!handle) {
    free(*engine);
    return -1;
  }

  handle->options = rocksdb_options_create();
  rocksdb_options_set_create_if_missing(handle->options, 1);

  /* match TidesDB compression settings */
  rocksdb_options_set_compression(handle->options, rocksdb_lz4_compression);

  /* match TidesDB block cache size 64 MB */
  rocksdb_cache_t *cache = rocksdb_cache_create_lru(64 * 1024 * 1024);
  rocksdb_block_based_table_options_t *table_options =
      rocksdb_block_based_options_create();
  rocksdb_block_based_options_set_block_cache(table_options, cache);

  /* match TidesDB bloom filter settings */
  rocksdb_block_based_options_set_filter_policy(
      table_options, rocksdb_filterpolicy_create_bloom(10));

  /* match TidesDB block indexes */
  rocksdb_block_based_options_set_index_type(
      table_options,
      rocksdb_block_based_table_index_type_two_level_index_search);

  rocksdb_options_set_block_based_table_factory(handle->options, table_options);

  /* match TidesDB memtable flush size: 64 MB */
  rocksdb_options_set_write_buffer_size(handle->options, 64 * 1024 * 1024);

  /* match TidesDB thread configuration */
  rocksdb_options_set_max_background_jobs(handle->options,
                                          8); /* 4 flush + 4 compaction */

  handle->roptions = rocksdb_readoptions_create();
  handle->woptions = rocksdb_writeoptions_create();

  /* match TidesDB sync mode TDB_SYNC_NONE (no fsync) */
  rocksdb_writeoptions_set_sync(handle->woptions, 0);

  char *err = NULL;
  handle->db = rocksdb_open(handle->options, path, &err);
  if (err) {
    free(err);
    rocksdb_options_destroy(handle->options);
    rocksdb_readoptions_destroy(handle->roptions);
    rocksdb_writeoptions_destroy(handle->woptions);
    free(handle);
    free(*engine);
    return -1;
  }

  (*engine)->handle = handle;
  (*engine)->ops = &rocksdb_ops;
  return 0;
}

static int rocksdb_close_impl(storage_engine_t *engine) {
  rocksdb_handle_t *handle = (rocksdb_handle_t *)engine->handle;
  rocksdb_close(handle->db);
  rocksdb_options_destroy(handle->options);
  rocksdb_readoptions_destroy(handle->roptions);
  rocksdb_writeoptions_destroy(handle->woptions);
  rocksdb_block_based_options_destroy(handle->table_options);
  rocksdb_filterpolicy_destroy(handle->filter_policy);
  rocksdb_cache_destroy(handle->cache);
  free(handle);
  free(engine);
  return 0;
}

static int rocksdb_put_impl(storage_engine_t *engine, const uint8_t *key,
                            size_t key_size, const uint8_t *value,
                            size_t value_size) {
  rocksdb_handle_t *handle = (rocksdb_handle_t *)engine->handle;
  char *err = NULL;

  rocksdb_put(handle->db, handle->woptions, (const char *)key, key_size,
              (const char *)value, value_size, &err);

  if (err) {
    free(err);
    return -1;
  }
  return 0;
}

static int rocksdb_get_impl(storage_engine_t *engine, const uint8_t *key,
                            size_t key_size, uint8_t **value,
                            size_t *value_size) {
  rocksdb_handle_t *handle = (rocksdb_handle_t *)engine->handle;
  char *err = NULL;

  char *val = rocksdb_get(handle->db, handle->roptions, (const char *)key,
                          key_size, value_size, &err);

  if (err) {
    free(err);
    return -1;
  }

  if (!val)
    return -1;

  *value = (uint8_t *)val;
  return 0;
}

static int rocksdb_del_impl(storage_engine_t *engine, const uint8_t *key,
                            size_t key_size) {
  rocksdb_handle_t *handle = (rocksdb_handle_t *)engine->handle;
  char *err = NULL;

  rocksdb_delete(handle->db, handle->woptions, (const char *)key, key_size,
                 &err);

  if (err) {
    free(err);
    return -1;
  }
  return 0;
}

static int rocksdb_iter_new_impl(storage_engine_t *engine, void **iter) {
  rocksdb_handle_t *handle = (rocksdb_handle_t *)engine->handle;
  *iter = rocksdb_create_iterator(handle->db, handle->roptions);
  return *iter ? 0 : -1;
}

static int rocksdb_iter_seek_to_first_impl(void *iter) {
  rocksdb_iter_seek_to_first((rocksdb_iterator_t *)iter);
  return 0;
}

static int rocksdb_iter_valid_impl(void *iter) {
  return rocksdb_iter_valid((rocksdb_iterator_t *)iter) ? 1 : 0;
}

static int rocksdb_iter_next_impl(void *iter) {
  rocksdb_iter_next((rocksdb_iterator_t *)iter);
  return 0;
}

static int rocksdb_iter_key_impl(void *iter, uint8_t **key, size_t *key_size) {
  *key = (uint8_t *)rocksdb_iter_key((rocksdb_iterator_t *)iter, key_size);
  return 0;
}

static int rocksdb_iter_value_impl(void *iter, uint8_t **value,
                                   size_t *value_size) {
  *value =
      (uint8_t *)rocksdb_iter_value((rocksdb_iterator_t *)iter, value_size);
  return 0;
}

static int rocksdb_iter_free_impl(void *iter) {
  rocksdb_iter_destroy((rocksdb_iterator_t *)iter);
  return 0;
}

static const storage_engine_ops_t rocksdb_ops = {
    .open = rocksdb_open_impl,
    .close = rocksdb_close_impl,
    .put = rocksdb_put_impl,
    .get = rocksdb_get_impl,
    .del = rocksdb_del_impl,
    .iter_new = rocksdb_iter_new_impl,
    .iter_seek_to_first = rocksdb_iter_seek_to_first_impl,
    .iter_valid = rocksdb_iter_valid_impl,
    .iter_next = rocksdb_iter_next_impl,
    .iter_key = rocksdb_iter_key_impl,
    .iter_value = rocksdb_iter_value_impl,
    .iter_free = rocksdb_iter_free_impl,
    .name = "RocksDB"};

const storage_engine_ops_t *get_rocksdb_ops(void) { return &rocksdb_ops; }

#else

/* stubski */
const storage_engine_ops_t *get_rocksdb_ops(void) { return NULL; }

#endif /* HAVE_ROCKSDB */
