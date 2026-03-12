#define _POSIX_C_SOURCE 200809L

#include <dlfcn.h>
#include <errno.h>
#include <float.h>
#include <inttypes.h>
#include <math.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/*
 * Benchmark harness for vectorized aggregation kernels in hwy_vec_agg.cpp.
 *
 * All JNI entry points share one of these signatures:
 *   jlong   fn(env, cl, pData, count)
 *   jdouble fn(env, cl, pData, count)
 *   jint    fn(env, cl, pData, count)
 *   jdouble fn(env, cl, pData, count, pAccCount)
 *
 * Null sentinels:
 *   double -> NaN
 *   int32  -> INT32_MIN
 *   int64  -> INT64_MIN
 *   int16  -> INT16_MIN  (-32768)
 */

/* JNI-compatible function pointer types */
typedef int64_t (*agg_long_fn)(void *, void *, int64_t, int64_t);
typedef double (*agg_double_fn)(void *, void *, int64_t, int64_t);
typedef int32_t (*agg_int_fn)(void *, void *, int64_t, int64_t);
typedef double (*agg_double_acc_fn)(void *, void *, int64_t, int64_t, int64_t);

enum elem_type { ELEM_DOUBLE, ELEM_INT, ELEM_LONG, ELEM_SHORT };
enum ret_type { RET_LONG, RET_DOUBLE, RET_INT };

typedef struct {
    const char *name;
    const char *jni_symbol;
    enum elem_type elem;
    enum ret_type ret;
    bool has_acc;
} kernel_info_t;

static const kernel_info_t KERNELS[] = {
        {"countDouble",       "Java_io_questdb_std_Vect_countDouble",       ELEM_DOUBLE, RET_LONG,   false},
        {"sumDouble",         "Java_io_questdb_std_Vect_sumDouble",         ELEM_DOUBLE, RET_DOUBLE, false},
        {"sumDoubleAcc",      "Java_io_questdb_std_Vect_sumDoubleAcc",      ELEM_DOUBLE, RET_DOUBLE, true},
        {"sumDoubleKahan",    "Java_io_questdb_std_Vect_sumDoubleKahan",    ELEM_DOUBLE, RET_DOUBLE, false},
        {"sumDoubleNeumaier", "Java_io_questdb_std_Vect_sumDoubleNeumaier", ELEM_DOUBLE, RET_DOUBLE, false},
        {"minDouble",         "Java_io_questdb_std_Vect_minDouble",         ELEM_DOUBLE, RET_DOUBLE, false},
        {"maxDouble",         "Java_io_questdb_std_Vect_maxDouble",         ELEM_DOUBLE, RET_DOUBLE, false},
        {"countInt",          "Java_io_questdb_std_Vect_countInt",          ELEM_INT,    RET_LONG,   false},
        {"sumInt",            "Java_io_questdb_std_Vect_sumInt",            ELEM_INT,    RET_LONG,   false},
        {"sumIntAcc",         "Java_io_questdb_std_Vect_sumIntAcc",         ELEM_INT,    RET_DOUBLE, true},
        {"minInt",            "Java_io_questdb_std_Vect_minInt",            ELEM_INT,    RET_INT,    false},
        {"maxInt",            "Java_io_questdb_std_Vect_maxInt",            ELEM_INT,    RET_INT,    false},
        {"countLong",         "Java_io_questdb_std_Vect_countLong",         ELEM_LONG,   RET_LONG,   false},
        {"sumLong",           "Java_io_questdb_std_Vect_sumLong",           ELEM_LONG,   RET_LONG,   false},
        {"sumLongAcc",        "Java_io_questdb_std_Vect_sumLongAcc",        ELEM_LONG,   RET_DOUBLE, true},
        {"minLong",           "Java_io_questdb_std_Vect_minLong",           ELEM_LONG,   RET_LONG,   false},
        {"maxLong",           "Java_io_questdb_std_Vect_maxLong",           ELEM_LONG,   RET_LONG,   false},
        {"sumShort",          "Java_io_questdb_std_Vect_sumShort",          ELEM_SHORT,  RET_LONG,   false},
        {"minShort",          "Java_io_questdb_std_Vect_minShort",          ELEM_SHORT,  RET_INT,    false},
        {"maxShort",          "Java_io_questdb_std_Vect_maxShort",          ELEM_SHORT,  RET_INT,    false},
};

static const int NUM_KERNELS = sizeof(KERNELS) / sizeof(KERNELS[0]);

typedef struct {
    const char *lib_path;
    int rows;
    int null_percent;
    int warmup;
    int iterations;
    const kernel_info_t *kernel;
} options_t;

typedef struct {
    void *data;
    int64_t acc_count;
} buffers_t;

static void usage(const char *argv0) {
    fprintf(stderr,
            "Usage: %s --lib /path/to/libquestdb.so --kernel NAME [options]\n"
            "Kernels:\n",
            argv0);
    for (int i = 0; i < NUM_KERNELS; i++) {
        fprintf(stderr, "  %s\n", KERNELS[i].name);
    }
    fprintf(stderr,
            "Options:\n"
            "  --rows N           elements per call (default: 1048576)\n"
            "  --null-percent N   null element percentage (default: 5)\n"
            "  --warmup N         warmup calls before timing (default: 10)\n"
            "  --iterations N     timed calls to execute (default: 2000)\n");
}

static int parse_int(const char *arg, const char *name) {
    char *end = NULL;
    errno = 0;
    long value = strtol(arg, &end, 10);
    if (errno != 0 || end == arg || *end != '\0') {
        fprintf(stderr, "invalid %s: %s\n", name, arg);
        exit(2);
    }
    if (value < 0 || value > INT32_MAX) {
        fprintf(stderr, "%s out of range: %s\n", name, arg);
        exit(2);
    }
    return (int) value;
}

static const kernel_info_t *find_kernel(const char *name) {
    for (int i = 0; i < NUM_KERNELS; i++) {
        if (strcmp(KERNELS[i].name, name) == 0) {
            return &KERNELS[i];
        }
    }
    return NULL;
}

static options_t parse_options(int argc, char **argv) {
    options_t opts = {
            .lib_path = NULL,
            .rows = 1024 * 1024,
            .null_percent = 5,
            .warmup = 10,
            .iterations = 2000,
            .kernel = NULL,
    };

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--lib") == 0 && i + 1 < argc) {
            opts.lib_path = argv[++i];
        } else if (strcmp(argv[i], "--rows") == 0 && i + 1 < argc) {
            opts.rows = parse_int(argv[++i], "rows");
            if (opts.rows <= 0) {
                fprintf(stderr, "rows must be > 0\n");
                exit(2);
            }
        } else if (strcmp(argv[i], "--null-percent") == 0 && i + 1 < argc) {
            opts.null_percent = parse_int(argv[++i], "null-percent");
            if (opts.null_percent > 100) {
                fprintf(stderr, "null-percent must be <= 100\n");
                exit(2);
            }
        } else if (strcmp(argv[i], "--kernel") == 0 && i + 1 < argc) {
            opts.kernel = find_kernel(argv[++i]);
            if (opts.kernel == NULL) {
                fprintf(stderr, "unknown kernel: %s\n", argv[i]);
                usage(argv[0]);
                exit(2);
            }
        } else if (strcmp(argv[i], "--warmup") == 0 && i + 1 < argc) {
            opts.warmup = parse_int(argv[++i], "warmup");
        } else if (strcmp(argv[i], "--iterations") == 0 && i + 1 < argc) {
            opts.iterations = parse_int(argv[++i], "iterations");
            if (opts.iterations <= 0) {
                fprintf(stderr, "iterations must be > 0\n");
                exit(2);
            }
        } else {
            usage(argv[0]);
            exit(2);
        }
    }

    if (opts.lib_path == NULL || opts.kernel == NULL) {
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
    return ptr;
}

/* Simple splitmix64 PRNG */
static uint64_t prng_state = UINT64_C(0xDEADBEEFCAFEBABE);

static uint64_t prng_next(void) {
    uint64_t z = (prng_state += UINT64_C(0x9E3779B97F4A7C15));
    z = (z ^ (z >> 30)) * UINT64_C(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)) * UINT64_C(0x94D049BB133111EB);
    return z ^ (z >> 31);
}

static bool is_null_slot(int null_percent) {
    if (null_percent <= 0) return false;
    return (int) (prng_next() % 100) < null_percent;
}

static void build_buffers(buffers_t *buffers, int rows, enum elem_type elem, int null_percent) {
    prng_state = UINT64_C(0xDEADBEEFCAFEBABE);

    switch (elem) {
        case ELEM_DOUBLE: {
            double *d = xaligned_alloc(64, (size_t) rows * sizeof(double));
            for (int i = 0; i < rows; i++) {
                if (is_null_slot(null_percent)) {
                    d[i] = NAN;
                } else {
                    /* Generate values in [-1000, 1000] range */
                    uint64_t r = prng_next();
                    d[i] = ((double) (int64_t) r / (double) INT64_MAX) * 1000.0;
                }
            }
            buffers->data = d;
            break;
        }
        case ELEM_INT: {
            int32_t *p = xaligned_alloc(64, (size_t) rows * sizeof(int32_t));
            for (int i = 0; i < rows; i++) {
                if (is_null_slot(null_percent)) {
                    p[i] = INT32_MIN;
                } else {
                    p[i] = (int32_t) (prng_next() % 2000001) - 1000000;
                }
            }
            buffers->data = p;
            break;
        }
        case ELEM_LONG: {
            int64_t *p = xaligned_alloc(64, (size_t) rows * sizeof(int64_t));
            for (int i = 0; i < rows; i++) {
                if (is_null_slot(null_percent)) {
                    p[i] = INT64_MIN;
                } else {
                    p[i] = (int64_t) (prng_next() % UINT64_C(2000001)) - 1000000;
                }
            }
            buffers->data = p;
            break;
        }
        case ELEM_SHORT: {
            int16_t *p = xaligned_alloc(64, (size_t) rows * sizeof(int16_t));
            for (int i = 0; i < rows; i++) {
                if (is_null_slot(null_percent)) {
                    p[i] = INT16_MIN;
                } else {
                    p[i] = (int16_t) ((int) (prng_next() % 60001) - 30000);
                }
            }
            buffers->data = p;
            break;
        }
    }
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

static uint64_t invoke_kernel(void *fn, const kernel_info_t *ki, buffers_t *bufs, int rows) {
    int64_t pData = (int64_t) (intptr_t) bufs->data;
    uint64_t result = 0;

    if (ki->has_acc) {
        agg_double_acc_fn f = (agg_double_acc_fn) fn;
        double r = f(NULL, NULL, pData, rows, (int64_t) (intptr_t) &bufs->acc_count);
        uint64_t tmp;
        memcpy(&tmp, &r, sizeof(tmp));
        result = tmp ^ (uint64_t) bufs->acc_count;
    } else {
        switch (ki->ret) {
            case RET_LONG: {
                agg_long_fn f = (agg_long_fn) fn;
                result = (uint64_t) f(NULL, NULL, pData, rows);
                break;
            }
            case RET_DOUBLE: {
                agg_double_fn f = (agg_double_fn) fn;
                double r = f(NULL, NULL, pData, rows);
                memcpy(&result, &r, sizeof(result));
                break;
            }
            case RET_INT: {
                agg_int_fn f = (agg_int_fn) fn;
                result = (uint64_t) (uint32_t) f(NULL, NULL, pData, rows);
                break;
            }
        }
    }
    return result;
}

static uint64_t run_iterations(void *fn, const kernel_info_t *ki, buffers_t *bufs, int rows, int iterations) {
    volatile uint64_t checksum = 0;
    for (int i = 0; i < iterations; i++) {
        checksum ^= invoke_kernel(fn, ki, bufs, rows);
    }
    return checksum;
}

static void free_buffers(buffers_t *buffers) {
    free(buffers->data);
}

int main(int argc, char **argv) {
    const options_t opts = parse_options(argc, argv);
    buffers_t buffers = {0};
    build_buffers(&buffers, opts.rows, opts.kernel->elem, opts.null_percent);

    void *handle = dlopen(opts.lib_path, RTLD_NOW | RTLD_LOCAL);
    if (handle == NULL) {
        fprintf(stderr, "dlopen failed: %s\n", dlerror());
        free_buffers(&buffers);
        return 1;
    }

    dlerror();
    void *fn = dlsym(handle, opts.kernel->jni_symbol);
    const char *error = dlerror();
    if (error != NULL) {
        fprintf(stderr, "dlsym(%s) failed: %s\n", opts.kernel->jni_symbol, error);
        dlclose(handle);
        free_buffers(&buffers);
        return 1;
    }

    /* Quick validation: call once and print result for sanity checking */
    uint64_t sanity = invoke_kernel(fn, opts.kernel, &buffers, opts.rows);
    (void) sanity;

    if (opts.warmup > 0) {
        run_iterations(fn, opts.kernel, &buffers, opts.rows, opts.warmup);
    }

    const uint64_t start = now_ns();
    uint64_t checksum = run_iterations(fn, opts.kernel, &buffers, opts.rows, opts.iterations);
    const uint64_t elapsed = now_ns() - start;

    printf("lib=%s\n", opts.lib_path);
    printf("kernel=%s\n", opts.kernel->name);
    printf("rows=%d null_percent=%d iterations=%d\n", opts.rows, opts.null_percent, opts.iterations);
    printf("elapsed_ns=%" PRIu64 "\n", elapsed);
    printf("ns_per_call=%.3f\n", opts.iterations > 0 ? (double) elapsed / opts.iterations : 0.0);
    printf("rows_per_sec=%.3f\n", elapsed > 0 ? ((double) opts.rows * opts.iterations * 1000000000.0) / elapsed : 0.0);
    printf("checksum=%" PRIu64 "\n", checksum);

    dlclose(handle);
    free_buffers(&buffers);
    return 0;
}
