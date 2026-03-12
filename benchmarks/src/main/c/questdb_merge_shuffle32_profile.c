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

typedef void (*merge_shuffle32_jni_fn)(void *, void *, int64_t, int64_t, int64_t, int64_t, int64_t);

enum {
    INDEX_ENTRY_BYTES = (int) (sizeof(uint64_t) * 2),
};

static const uint64_t IN_ORDER_BIT = UINT64_C(1) << 63;

typedef struct {
    const char *lib_path;
    int rows;
    int ooo_percent;
    int warmup;
    int iterations;
    double seconds;
} options_t;

typedef struct {
    index_t *merge_index;
    int32_t *src_in_order;
    int32_t *src_ooo;
    int32_t *dst;
    int32_t *expected;
    int in_order_rows;
    int ooo_rows;
} buffers_t;

static void usage(const char *argv0) {
    fprintf(stderr,
            "Usage: %s --lib /path/to/libquestdb.so [options]\n"
            "Options:\n"
            "  --rows N           merged rows per call (default: 1048576)\n"
            "  --ooo-percent N    out-of-order share in percent (default: 25)\n"
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
            .ooo_percent = 25,
            .warmup = 10,
            .iterations = 2000,
            .seconds = 0.0,
    };

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--lib") == 0 && i + 1 < argc) {
            opts.lib_path = argv[++i];
        } else if (strcmp(argv[i], "--rows") == 0 && i + 1 < argc) {
            opts.rows = parse_int(argv[++i], "rows");
        } else if (strcmp(argv[i], "--ooo-percent") == 0 && i + 1 < argc) {
            opts.ooo_percent = parse_int(argv[++i], "ooo-percent");
            if (opts.ooo_percent >= 100) {
                fprintf(stderr, "ooo-percent must be < 100\n");
                exit(2);
            }
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

    if (opts.lib_path == NULL) {
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

static int32_t mix32(int value) {
    uint32_t x = (uint32_t) value * UINT32_C(0x9E3779B9);
    x ^= x >> 16;
    x *= UINT32_C(0x85EBCA6B);
    x ^= x >> 13;
    return (int32_t) x;
}

static void build_merge_index(buffers_t *buffers, int rows, int ooo_percent) {
    buffers->ooo_rows = rows * ooo_percent / 100;
    if (buffers->ooo_rows <= 0) {
        buffers->ooo_rows = 1;
    }
    if (buffers->ooo_rows >= rows) {
        buffers->ooo_rows = rows / 2;
    }
    buffers->in_order_rows = rows - buffers->ooo_rows;

    buffers->merge_index = xaligned_alloc(64, (size_t) rows * sizeof(index_t));
    buffers->src_in_order = xaligned_alloc(64, (size_t) buffers->in_order_rows * sizeof(int32_t));
    buffers->src_ooo = xaligned_alloc(64, (size_t) buffers->ooo_rows * sizeof(int32_t));
    buffers->dst = xaligned_alloc(64, (size_t) rows * sizeof(int32_t));
    buffers->expected = xaligned_alloc(64, (size_t) rows * sizeof(int32_t));

    for (int i = 0; i < buffers->in_order_rows; i++) {
        buffers->src_in_order[i] = mix32(i);
    }
    for (int i = 0; i < buffers->ooo_rows; i++) {
        buffers->src_ooo[i] = mix32(i + buffers->in_order_rows);
    }

    int in_order_row = 0;
    int ooo_row = 0;
    for (int i = 0; i < rows; i++) {
        buffers->merge_index[i].ts = (uint64_t) (1000000 + i);
        const bool pick_ooo = ooo_row < buffers->ooo_rows
                              && (in_order_row >= buffers->in_order_rows
                                  || ((int64_t) (i + 1) * buffers->ooo_rows / rows) > ooo_row);
        if (pick_ooo) {
            buffers->merge_index[i].i = (uint64_t) ooo_row++;
        } else {
            buffers->merge_index[i].i = IN_ORDER_BIT | (uint64_t) in_order_row++;
        }
    }
}

static void build_expected(buffers_t *buffers, int rows) {
    for (int i = 0; i < rows; i++) {
        const uint64_t encoded = buffers->merge_index[i].i;
        const uint64_t row = encoded & ~IN_ORDER_BIT;
        const uint64_t pick = encoded >> 63u;
        buffers->expected[i] = pick ? buffers->src_in_order[row] : buffers->src_ooo[row];
    }
}

static void validate_once(merge_shuffle32_jni_fn fn, buffers_t *buffers, int rows) {
    memset(buffers->dst, 0, (size_t) rows * sizeof(int32_t));
    fn(
            NULL,
            NULL,
            (int64_t) (intptr_t) buffers->src_in_order,
            (int64_t) (intptr_t) buffers->src_ooo,
            (int64_t) (intptr_t) buffers->dst,
            (int64_t) (intptr_t) buffers->merge_index,
            rows
    );
    if (memcmp(buffers->dst, buffers->expected, (size_t) rows * sizeof(int32_t)) != 0) {
        for (int i = 0; i < rows; i++) {
            if (buffers->dst[i] != buffers->expected[i]) {
                fprintf(stderr, "validation failed at row %d: got=%d expected=%d\n",
                        i, buffers->dst[i], buffers->expected[i]);
                break;
            }
        }
        exit(1);
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

static uint64_t run_iterations(merge_shuffle32_jni_fn fn, buffers_t *buffers, int rows, int iterations) {
    volatile uint64_t checksum = 0;
    for (int i = 0; i < iterations; i++) {
        fn(
                NULL,
                NULL,
                (int64_t) (intptr_t) buffers->src_in_order,
                (int64_t) (intptr_t) buffers->src_ooo,
                (int64_t) (intptr_t) buffers->dst,
                (int64_t) (intptr_t) buffers->merge_index,
                rows
        );
        checksum += (uint32_t) buffers->dst[(i + rows - 1) % rows];
    }
    return checksum;
}

static uint64_t run_seconds(merge_shuffle32_jni_fn fn, buffers_t *buffers, int rows, double seconds, int *completed) {
    volatile uint64_t checksum = 0;
    const uint64_t deadline = now_ns() + (uint64_t) (seconds * 1000000000.0);
    int iterations = 0;
    while (now_ns() < deadline) {
        fn(
                NULL,
                NULL,
                (int64_t) (intptr_t) buffers->src_in_order,
                (int64_t) (intptr_t) buffers->src_ooo,
                (int64_t) (intptr_t) buffers->dst,
                (int64_t) (intptr_t) buffers->merge_index,
                rows
        );
        checksum += (uint32_t) buffers->dst[(iterations + rows - 1) % rows];
        iterations++;
    }
    *completed = iterations;
    return checksum;
}

static void free_buffers(buffers_t *buffers) {
    free(buffers->merge_index);
    free(buffers->src_in_order);
    free(buffers->src_ooo);
    free(buffers->dst);
    free(buffers->expected);
}

int main(int argc, char **argv) {
    const options_t opts = parse_options(argc, argv);
    buffers_t buffers = {0};
    build_merge_index(&buffers, opts.rows, opts.ooo_percent);
    build_expected(&buffers, opts.rows);

    void *handle = dlopen(opts.lib_path, RTLD_NOW | RTLD_LOCAL);
    if (handle == NULL) {
        fprintf(stderr, "dlopen failed: %s\n", dlerror());
        free_buffers(&buffers);
        return 1;
    }

    dlerror();
    merge_shuffle32_jni_fn merge32 = (merge_shuffle32_jni_fn) dlsym(handle, "Java_io_questdb_std_Vect_mergeShuffle32Bit");
    const char *error = dlerror();
    if (error != NULL) {
        fprintf(stderr, "dlsym failed: %s\n", error);
        dlclose(handle);
        free_buffers(&buffers);
        return 1;
    }

    validate_once(merge32, &buffers, opts.rows);

    if (opts.warmup > 0) {
        run_iterations(merge32, &buffers, opts.rows, opts.warmup);
    }

    const uint64_t start = now_ns();
    int iterations = opts.iterations;
    uint64_t checksum;
    if (opts.seconds > 0.0) {
        checksum = run_seconds(merge32, &buffers, opts.rows, opts.seconds, &iterations);
    } else {
        checksum = run_iterations(merge32, &buffers, opts.rows, iterations);
    }
    const uint64_t elapsed = now_ns() - start;

    printf("lib=%s\n", opts.lib_path);
    printf("rows=%d ooo_percent=%d iterations=%d\n", opts.rows, opts.ooo_percent, iterations);
    printf("elapsed_ns=%" PRIu64 "\n", elapsed);
    printf("ns_per_call=%.3f\n", iterations > 0 ? (double) elapsed / iterations : 0.0);
    printf("rows_per_sec=%.3f\n", elapsed > 0 ? ((double) opts.rows * iterations * 1000000000.0) / elapsed : 0.0);
    printf("checksum=%" PRIu64 "\n", checksum);

    dlclose(handle);
    free_buffers(&buffers);
    return 0;
}
