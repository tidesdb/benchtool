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
#ifndef __BENCHMARK_H__
#define __BENCHMARK_H__

#include <stdint.h>
#include <time.h>

#ifndef TIDESDB_VERSION
#define TIDESDB_VERSION "unknown"
#endif

const char *get_engine_version(const char *engine_name);
#include <stddef.h>
#include <stdio.h>

typedef enum {
  WORKLOAD_WRITE,
  WORKLOAD_READ,
  WORKLOAD_MIXED,
  WORKLOAD_DELETE
} workload_type_t;

typedef enum {
  KEY_PATTERN_SEQUENTIAL,
  KEY_PATTERN_RANDOM,
  KEY_PATTERN_ZIPFIAN,   /* hot keys (80/20 distribution) */
  KEY_PATTERN_UNIFORM,   /* true uniform random */
  KEY_PATTERN_TIMESTAMP, /* monotonically increasing timestamp-like */
  KEY_PATTERN_REVERSE    /* reverse sequential */
} key_pattern_t;

typedef struct {
  const char *engine_name;
  int num_operations;
  int key_size;
  int value_size;
  int num_threads;
  int batch_size;
  const char *db_path;
  int compare_mode;
  const char *report_file;
  key_pattern_t key_pattern;
  workload_type_t workload_type;
} benchmark_config_t;

typedef struct {
  double duration_seconds;
  double ops_per_second;
  double avg_latency_us;
  double p50_latency_us;
  double p95_latency_us;
  double p99_latency_us;
  double min_latency_us;
  double max_latency_us;
} operation_stats_t;

typedef struct {
  const char *engine_name;
  benchmark_config_t config;
  operation_stats_t put_stats;
  operation_stats_t get_stats;
  operation_stats_t delete_stats;
  operation_stats_t iteration_stats;
  size_t total_bytes_written;
  size_t total_bytes_read;
} benchmark_results_t;

/* storage eng interface */
typedef struct storage_engine_t storage_engine_t;

typedef struct {
  int (*open)(storage_engine_t **engine, const char *path);

  int (*close)(storage_engine_t *engine);

  int (*put)(storage_engine_t *engine, const uint8_t *key, size_t key_size,
             const uint8_t *value, size_t value_size);

  int (*get)(storage_engine_t *engine, const uint8_t *key, size_t key_size,
             uint8_t **value, size_t *value_size);

  int (*del)(storage_engine_t *engine, const uint8_t *key, size_t key_size);

  int (*iter_new)(storage_engine_t *engine, void **iter);
  int (*iter_seek_to_first)(void *iter);
  int (*iter_valid)(void *iter);
  int (*iter_next)(void *iter);
  int (*iter_key)(void *iter, uint8_t **key, size_t *key_size);
  int (*iter_value)(void *iter, uint8_t **value, size_t *value_size);
  int (*iter_free)(void *iter);

  const char *name;
} storage_engine_ops_t;

struct storage_engine_t {
  const storage_engine_ops_t *ops;
  void *handle;
};

int run_benchmark(benchmark_config_t *config, benchmark_results_t **results);
void generate_report(FILE *fp, benchmark_results_t *results,
                     benchmark_results_t *baseline);
void free_results(benchmark_results_t *results);

const storage_engine_ops_t *get_engine_ops(const char *engine_name);

#endif /* __BENCHMARK_H__ */