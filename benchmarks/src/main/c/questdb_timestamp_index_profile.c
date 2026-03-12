#define _POSIX_C_SOURCE 200809L

#include <dlfcn.h>
#include <errno.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

typedef struct {
    uint64_t ts;
    uint64_t i;
} index_t;

/*
 * Kernels benchmarked here:
 *
 * copyFromTimestampIndex(env, cl, pIndex, indexLo, indexHi, pTs)
 *   - extract ts fields from index[lo..hi] into pTs
 *
 * oooCopyIndex(env, cl, pIndex, index_size, pDest)
 *   - extract ts fields from index[0..size-1] into pDest
 *
 * shiftTimestampIndex(env, cl, pSrc, count, pDest)
 *   - copy ts from src, write sequential i = 0..count-1 into dest
 *
 * flattenIndex(env, cl, pIndex, count)
 *   - in-place: preserve ts, overwrite i with sequential 0..count-1
 */

typedef void (*copy_ts_index_fn)(void *, void *, int64_t, int64_t, int64_t, int64_t);
typedef void (*ooo_copy_index_fn)(void *, void *, int64_t, int64_t, int64_t);
typedef void (*shift_ts_index_fn)(void *, void *, int64_t, int64_t, int64_t);
typedef void (*flatten_index_fn)(void *, void *, int64_t, int64_t);

enum kernel_id {
    KERNEL_COPY_TS_INDEX,
    KERNEL_OOO_COPY_INDEX,
    KERNEL_SHIFT_TS_INDEX,
    KERNEL_FLATTEN_INDEX,
};

typedef struct {
    const char *lib_path;
    int rows;
    int warmup;
    int iterations;
    double seconds;
    enum kernel_id kernel;
    const char *kernel_name;
} options_t;

typedef struct {
    index_t *src_index;
    index_t *dst_index;
    int64_t *dst_ts;
    int64_t *expected_ts;
    index_t *expected_index;
} buffers_t;

static void usage(const char *argv0) {
    fprintf(stderr,
            "Usage: %s --lib /path/to/libquestdb.so --kernel NAME [options]\n"
            "Kernels:\n"
            "  copyFromTimestampIndex\n"
            "  oooCopyIndex\n"
            "  shiftTimestampIndex\n"
            "  flattenIndex\n"
            "Options:\n"
            "  --rows N           rows per call (default: 1048576)\n"
            "  --warmup N         warmup calls before timing (default: 10)\n"
            "  --iterations N     timed calls to execute (default: 2000)\n"
            "  --seconds S        timed run duration in seconds (overrides iterations)\n",
            argv0);
}

static int parse_int(const char *arg, const char *name) {
    char *end = NULL;
    errno = 0;
    long value = strtol(arg, &end, 10);
    if (errno != 0 || end == arg || *end != '\0') {
        fprintf(stderr, "invalid %s: %s\n", name, arg);
        exit(2);
    }
    if (value <= 0 || value > INT32_MAX) {
        fprintf(stderr, "%s out of range: %s\n", name, arg);
        exit(2);
    }
    return (int) value;
}

static double parse_double(const char *arg, const char *name) {
    char *end = NULL;
    errno = 0;
    double value = strtod(arg, &end);
    if (errno != 0 || end == arg || *end != '\0') {
        fprintf(stderr, "invalid %s: %s\n", name, arg);
        exit(2);
    }
    if (value <= 0.0) {
        fprintf(stderr, "%s must be > 0: %s\n", name, arg);
        exit(2);
    }
    return value;
}

static options_t parse_options(int argc, char **argv) {
    options_t opts = {
            .lib_path = NULL,
            .rows = 1024 * 1024,
            .warmup = 10,
            .iterations = 2000,
            .seconds = 0.0,
            .kernel = -1,
            .kernel_name = NULL,
    };

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--lib") == 0 && i + 1 < argc) {
            opts.lib_path = argv[++i];
        } else if (strcmp(argv[i], "--rows") == 0 && i + 1 < argc) {
            opts.rows = parse_int(argv[++i], "rows");
        } else if (strcmp(argv[i], "--kernel") == 0 && i + 1 < argc) {
            const char *k = argv[++i];
            if (strcmp(k, "copyFromTimestampIndex") == 0) {
                opts.kernel = KERNEL_COPY_TS_INDEX;
            } else if (strcmp(k, "oooCopyIndex") == 0) {
                opts.kernel = KERNEL_OOO_COPY_INDEX;
            } else if (strcmp(k, "shiftTimestampIndex") == 0) {
                opts.kernel = KERNEL_SHIFT_TS_INDEX;
            } else if (strcmp(k, "flattenIndex") == 0) {
                opts.kernel = KERNEL_FLATTEN_INDEX;
            } else {
                fprintf(stderr, "unknown kernel: %s\n", k);
                usage(argv[0]);
                exit(2);
            }
            opts.kernel_name = k;
        } else if (strcmp(argv[i], "--warmup") == 0 && i + 1 < argc) {
            opts.warmup = parse_int(argv[++i], "warmup");
        } else if (strcmp(argv[i], "--iterations") == 0 && i + 1 < argc) {
            opts.iterations = parse_int(argv[++i], "iterations");
        } else if (strcmp(argv[i], "--seconds") == 0 && i + 1 < argc) {
            opts.seconds = parse_double(argv[++i], "seconds");
        } else {
            usage(argv[0]);
            exit(2);
        }
    }

    if (opts.lib_path == NULL || opts.kernel_name == NULL) {
        usage(argv[0]);
        exit(2);
    }
    return opts;
}

static void *xaligned_alloc(size_t alignment, size_t size) {
    void *ptr = NULL;
    const int rc = posix_memalign(&ptr, alignment, size);
    if (rc != 0) {
        fprintf(stderr, "posix_memalign failed: %s\n", strerror(rc));
        exit(1);
    }
    memset(ptr, 0, size);
    return ptr;
}

static void build_buffers(buffers_t *buffers, int rows, enum kernel_id kernel) {
    buffers->src_index = xaligned_alloc(64, (size_t) rows * sizeof(index_t));

    /* Fill source index with realistic data: increasing timestamps, varied row ids */
    for (int i = 0; i < rows; i++) {
        buffers->src_index[i].ts = (uint64_t) (1000000 + i * 1000);
        buffers->src_index[i].i = (uint64_t) (rows - 1 - i);  /* reverse order row ids */
    }

    switch (kernel) {
        case KERNEL_COPY_TS_INDEX:
        case KERNEL_OOO_COPY_INDEX:
            buffers->dst_ts = xaligned_alloc(64, (size_t) rows * sizeof(int64_t));
            buffers->expected_ts = xaligned_alloc(64, (size_t) rows * sizeof(int64_t));
            for (int i = 0; i < rows; i++) {
                buffers->expected_ts[i] = (int64_t) buffers->src_index[i].ts;
            }
            break;

        case KERNEL_SHIFT_TS_INDEX:
            buffers->dst_index = xaligned_alloc(64, (size_t) rows * sizeof(index_t));
            buffers->expected_index = xaligned_alloc(64, (size_t) rows * sizeof(index_t));
            for (int i = 0; i < rows; i++) {
                buffers->expected_index[i].ts = buffers->src_index[i].ts;
                buffers->expected_index[i].i = (uint64_t) i;
            }
            break;

        case KERNEL_FLATTEN_INDEX:
            buffers->dst_index = xaligned_alloc(64, (size_t) rows * sizeof(index_t));
            buffers->expected_index = xaligned_alloc(64, (size_t) rows * sizeof(index_t));
            for (int i = 0; i < rows; i++) {
                buffers->expected_index[i].ts = buffers->src_index[i].ts;
                buffers->expected_index[i].i = (uint64_t) i;
            }
            break;
    }
}

static void reset_for_flatten(buffers_t *buffers, int rows) {
    memcpy(buffers->dst_index, buffers->src_index, (size_t) rows * sizeof(index_t));
}

static uint64_t now_ns(void) {
    struct timespec ts;
    clockid_t clock_id = CLOCK_MONOTONIC;
#ifdef CLOCK_MONOTONIC_RAW
    clock_id = CLOCK_MONOTONIC_RAW;
#endif
    if (clock_gettime(clock_id, &ts) != 0) {
        perror("clock_gettime");
        exit(1);
    }
    return (uint64_t) ts.tv_sec * UINT64_C(1000000000) + (uint64_t) ts.tv_nsec;
}

/* ---- kernel dispatch ---- */

typedef struct {
    void *fn;
    buffers_t *buffers;
    int rows;
    enum kernel_id kernel;
} dispatch_ctx_t;

static void invoke_once(dispatch_ctx_t *ctx) {
    switch (ctx->kernel) {
        case KERNEL_COPY_TS_INDEX: {
            copy_ts_index_fn fn = (copy_ts_index_fn) ctx->fn;
            fn(NULL, NULL,
               (int64_t) (intptr_t) ctx->buffers->src_index,
               0,
               ctx->rows - 1,
               (int64_t) (intptr_t) ctx->buffers->dst_ts);
            break;
        }
        case KERNEL_OOO_COPY_INDEX: {
            ooo_copy_index_fn fn = (ooo_copy_index_fn) ctx->fn;
            fn(NULL, NULL,
               (int64_t) (intptr_t) ctx->buffers->src_index,
               ctx->rows,
               (int64_t) (intptr_t) ctx->buffers->dst_ts);
            break;
        }
        case KERNEL_SHIFT_TS_INDEX: {
            shift_ts_index_fn fn = (shift_ts_index_fn) ctx->fn;
            fn(NULL, NULL,
               (int64_t) (intptr_t) ctx->buffers->src_index,
               ctx->rows,
               (int64_t) (intptr_t) ctx->buffers->dst_index);
            break;
        }
        case KERNEL_FLATTEN_INDEX: {
            flatten_index_fn fn = (flatten_index_fn) ctx->fn;
            reset_for_flatten(ctx->buffers, ctx->rows);
            fn(NULL, NULL,
               (int64_t) (intptr_t) ctx->buffers->dst_index,
               ctx->rows);
            break;
        }
    }
}

static void validate_once(dispatch_ctx_t *ctx) {
    invoke_once(ctx);

    switch (ctx->kernel) {
        case KERNEL_COPY_TS_INDEX:
        case KERNEL_OOO_COPY_INDEX:
            if (memcmp(ctx->buffers->dst_ts, ctx->buffers->expected_ts,
                       (size_t) ctx->rows * sizeof(int64_t)) != 0) {
                for (int i = 0; i < ctx->rows; i++) {
                    if (ctx->buffers->dst_ts[i] != ctx->buffers->expected_ts[i]) {
                        fprintf(stderr, "validation failed at row %d: got=%" PRId64 " expected=%" PRId64 "\n",
                                i, ctx->buffers->dst_ts[i], ctx->buffers->expected_ts[i]);
                        break;
                    }
                }
                exit(1);
            }
            break;

        case KERNEL_SHIFT_TS_INDEX:
        case KERNEL_FLATTEN_INDEX:
            if (memcmp(ctx->buffers->dst_index, ctx->buffers->expected_index,
                       (size_t) ctx->rows * sizeof(index_t)) != 0) {
                for (int i = 0; i < ctx->rows; i++) {
                    if (ctx->buffers->dst_index[i].ts != ctx->buffers->expected_index[i].ts
                        || ctx->buffers->dst_index[i].i != ctx->buffers->expected_index[i].i) {
                        fprintf(stderr, "validation failed at row %d: "
                                "got={ts=%" PRIu64 ",i=%" PRIu64 "} expected={ts=%" PRIu64 ",i=%" PRIu64 "}\n",
                                i,
                                ctx->buffers->dst_index[i].ts, ctx->buffers->dst_index[i].i,
                                ctx->buffers->expected_index[i].ts, ctx->buffers->expected_index[i].i);
                        break;
                    }
                }
                exit(1);
            }
            break;
    }
}

static uint64_t run_iterations(dispatch_ctx_t *ctx, int iterations) {
    volatile uint64_t checksum = 0;
    for (int i = 0; i < iterations; i++) {
        invoke_once(ctx);
        /* Touch output to prevent dead-code elimination */
        switch (ctx->kernel) {
            case KERNEL_COPY_TS_INDEX:
            case KERNEL_OOO_COPY_INDEX:
                checksum += (uint64_t) ctx->buffers->dst_ts[(i + ctx->rows - 1) % ctx->rows];
                break;
            case KERNEL_SHIFT_TS_INDEX:
            case KERNEL_FLATTEN_INDEX:
                checksum += ctx->buffers->dst_index[(i + ctx->rows - 1) % ctx->rows].ts;
                break;
        }
    }
    return checksum;
}

static uint64_t run_seconds(dispatch_ctx_t *ctx, double seconds, int *completed) {
    volatile uint64_t checksum = 0;
    const uint64_t deadline = now_ns() + (uint64_t) (seconds * 1000000000.0);
    int iterations = 0;
    while (now_ns() < deadline) {
        invoke_once(ctx);
        switch (ctx->kernel) {
            case KERNEL_COPY_TS_INDEX:
            case KERNEL_OOO_COPY_INDEX:
                checksum += (uint64_t) ctx->buffers->dst_ts[(iterations + ctx->rows - 1) % ctx->rows];
                break;
            case KERNEL_SHIFT_TS_INDEX:
            case KERNEL_FLATTEN_INDEX:
                checksum += ctx->buffers->dst_index[(iterations + ctx->rows - 1) % ctx->rows].ts;
                break;
        }
        iterations++;
    }
    *completed = iterations;
    return checksum;
}

static void free_buffers(buffers_t *buffers) {
    free(buffers->src_index);
    free(buffers->dst_index);
    free(buffers->dst_ts);
    free(buffers->expected_ts);
    free(buffers->expected_index);
}

static const char *sym_for_kernel(enum kernel_id kernel) {
    switch (kernel) {
        case KERNEL_COPY_TS_INDEX:
            return "Java_io_questdb_std_Vect_copyFromTimestampIndex";
        case KERNEL_OOO_COPY_INDEX:
            return "Java_io_questdb_std_Vect_oooCopyIndex";
        case KERNEL_SHIFT_TS_INDEX:
            return "Java_io_questdb_std_Vect_shiftTimestampIndex";
        case KERNEL_FLATTEN_INDEX:
            return "Java_io_questdb_std_Vect_flattenIndex";
    }
    return NULL;
}

int main(int argc, char **argv) {
    const options_t opts = parse_options(argc, argv);
    buffers_t buffers = {0};
    build_buffers(&buffers, opts.rows, opts.kernel);

    void *handle = dlopen(opts.lib_path, RTLD_NOW | RTLD_LOCAL);
    if (handle == NULL) {
        fprintf(stderr, "dlopen failed: %s\n", dlerror());
        free_buffers(&buffers);
        return 1;
    }

    const char *sym_name = sym_for_kernel(opts.kernel);
    dlerror();
    void *fn = dlsym(handle, sym_name);
    const char *error = dlerror();
    if (error != NULL) {
        fprintf(stderr, "dlsym(%s) failed: %s\n", sym_name, error);
        dlclose(handle);
        free_buffers(&buffers);
        return 1;
    }

    dispatch_ctx_t ctx = {
            .fn = fn,
            .buffers = &buffers,
            .rows = opts.rows,
            .kernel = opts.kernel,
    };

    validate_once(&ctx);

    if (opts.warmup > 0) {
        run_iterations(&ctx, opts.warmup);
    }

    const uint64_t start = now_ns();
    int iterations = opts.iterations;
    uint64_t checksum;
    if (opts.seconds > 0.0) {
        checksum = run_seconds(&ctx, opts.seconds, &iterations);
    } else {
        checksum = run_iterations(&ctx, iterations);
    }
    const uint64_t elapsed = now_ns() - start;

    printf("lib=%s\n", opts.lib_path);
    printf("kernel=%s\n", opts.kernel_name);
    printf("rows=%d iterations=%d\n", opts.rows, iterations);
    printf("elapsed_ns=%" PRIu64 "\n", elapsed);
    printf("ns_per_call=%.3f\n", iterations > 0 ? (double) elapsed / iterations : 0.0);
    printf("rows_per_sec=%.3f\n", elapsed > 0 ? ((double) opts.rows * iterations * 1000000000.0) / elapsed : 0.0);
    printf("checksum=%" PRIu64 "\n", checksum);

    dlclose(handle);
    free_buffers(&buffers);
    return 0;
}
