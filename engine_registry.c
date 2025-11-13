#include <string.h>

#include "benchmark.h"

extern const storage_engine_ops_t *get_tidesdb_ops(void);
extern const storage_engine_ops_t *get_rocksdb_ops(void);

const storage_engine_ops_t *get_engine_ops(const char *engine_name)
{
    if (strcmp(engine_name, "tidesdb") == 0)
    {
        return get_tidesdb_ops();
    }
    else if (strcmp(engine_name, "rocksdb") == 0)
    {
        const storage_engine_ops_t *ops = get_rocksdb_ops();
        if (!ops)
        {
            return NULL;
        }
        return ops;
    }

    return NULL;
}