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

/* indexReshuffle32Bit and indexReshuffle64Bit share the same JNI signature:
 *   (JNIEnv *env, jclass cl, jlong pSrc, jlong pDest, jlong pIndex, jlong count) */
typedef void (*reshuffle_jni_fn)(void *, void *, int64_t, int64_t, int64_t, int64_t);

typedef struct {
    const char *lib_path;
    int rows;
    int width;  /* 32 or 64 */
    int warmup;
    int iterations;
    double seconds;
    bool sequential;  /* sequential vs random permutation index */
} options_t;

typedef struct {
    index_t *index;
    void *src;
    void *dst;
    void *expected;
} buffers_t;

static void usage(const char *argv0) {
    fprintf(stderr,
            "Usage: %s --lib /path/to/libquestdb.so --width 32|64 [options]\n"
            "Options:\n"
            "  --rows N           rows per call (default: 1048576)\n"
            "  --width N          element width: 32 or 64 (required)\n"
            "  --sequential       use sequential index instead of random permutation\n"
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
            .width = 0,
            .warmup = 10,
            .iterations = 2000,
            .seconds = 0.0,
            .sequential = false,
    };

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--lib") == 0 && i + 1 < argc) {
            opts.lib_path = argv[++i];
        } else if (strcmp(argv[i], "--rows") == 0 && i + 1 < argc) {
            opts.rows = parse_int(argv[++i], "rows");
        } else if (strcmp(argv[i], "--width") == 0 && i + 1 < argc) {
            opts.width = parse_int(argv[++i], "width");
            if (opts.width != 32 && opts.width != 64) {
                fprintf(stderr, "width must be 32 or 64\n");
                exit(2);
            }
        } else if (strcmp(argv[i], "--sequential") == 0) {
            opts.sequential = true;
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

    if (opts.lib_path == NULL || opts.width == 0) {
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

/* Fisher-Yates shuffle seeded from a simple splitmix state */
static void shuffle_indices(index_t *index, int count, uint64_t seed) {
    uint64_t state = seed;
    for (int i = count - 1; i > 0; i--) {
        state ^= state >> 30;
        state *= UINT64_C(0xBF58476D1CE4E5B9);
        state ^= state >> 27;
        int j = (int) (state % (uint64_t) (i + 1));
        uint64_t tmp = index[i].i;
        index[i].i = index[j].i;
        index[j].i = tmp;
    }
}

static void build_buffers(buffers_t *buffers, int rows, int width, bool sequential) {
    size_t elem_size = width == 64 ? sizeof(int64_t) : sizeof(int32_t);

    buffers->index = xaligned_alloc(64, (size_t) rows * sizeof(index_t));
    buffers->src = xaligned_alloc(64, (size_t) rows * elem_size);
    buffers->dst = xaligned_alloc(64, (size_t) rows * elem_size);
    buffers->expected = xaligned_alloc(64, (size_t) rows * elem_size);

    /* Fill source with distinct values */
    if (width == 64) {
        int64_t *src = buffers->src;
        for (int i = 0; i < rows; i++) {
            uint64_t x = (uint64_t) i * UINT64_C(0x9E3779B97F4A7C15);
            x ^= x >> 30;
            src[i] = (int64_t) x;
        }
    } else {
        int32_t *src = buffers->src;
        for (int i = 0; i < rows; i++) {
            uint32_t x = (uint32_t) i * UINT32_C(0x9E3779B9);
            x ^= x >> 16;
            src[i] = (int32_t) x;
        }
    }

    /* Build index: sequential or random permutation */
    for (int i = 0; i < rows; i++) {
        buffers->index[i].ts = (uint64_t) (1000000 + i);
        buffers->index[i].i = (uint64_t) i;
    }
    if (!sequential) {
        shuffle_indices(buffers->index, rows, UINT64_C(0xDEADBEEFCAFEBABE));
    }

    /* Compute expected output: dest[j] = src[index[j].i] */
    if (width == 64) {
        int64_t *src = buffers->src;
        int64_t *exp = buffers->expected;
        for (int i = 0; i < rows; i++) {
            exp[i] = src[buffers->index[i].i];
        }
    } else {
        int32_t *src = buffers->src;
        int32_t *exp = buffers->expected;
        for (int i = 0; i < rows; i++) {
            exp[i] = src[buffers->index[i].i];
        }
    }
}

static void validate_once(reshuffle_jni_fn fn, buffers_t *buffers, int rows, int width) {
    size_t elem_size = width == 64 ? sizeof(int64_t) : sizeof(int32_t);
    memset(buffers->dst, 0, (size_t) rows * elem_size);
    fn(
            NULL,
            NULL,
            (int64_t) (intptr_t) buffers->src,
            (int64_t) (intptr_t) buffers->dst,
            (int64_t) (intptr_t) buffers->index,
            rows
    );
    if (memcmp(buffers->dst, buffers->expected, (size_t) rows * elem_size) != 0) {
        if (width == 64) {
            int64_t *dst = buffers->dst;
            int64_t *exp = buffers->expected;
            for (int i = 0; i < rows; i++) {
                if (dst[i] != exp[i]) {
                    fprintf(stderr, "validation failed at row %d: got=%" PRId64 " expected=%" PRId64 "\n",
                            i, dst[i], exp[i]);
                    break;
                }
            }
        } else {
            int32_t *dst = buffers->dst;
            int32_t *exp = buffers->expected;
            for (int i = 0; i < rows; i++) {
                if (dst[i] != exp[i]) {
                    fprintf(stderr, "validation failed at row %d: got=%d expected=%d\n",
                            i, dst[i], exp[i]);
                    break;
                }
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

static uint64_t read_dst_element(const void *dst, int width, int idx) {
    if (width == 64) {
        return (uint64_t) ((const int64_t *) dst)[idx];
    }
    return (uint64_t) ((const uint32_t *) dst)[idx];
}

static uint64_t run_iterations(reshuffle_jni_fn fn, buffers_t *buffers, int rows, int width, int iterations) {
    volatile uint64_t checksum = 0;
    for (int i = 0; i < iterations; i++) {
        fn(
                NULL,
                NULL,
                (int64_t) (intptr_t) buffers->src,
                (int64_t) (intptr_t) buffers->dst,
                (int64_t) (intptr_t) buffers->index,
                rows
        );
        checksum += read_dst_element(buffers->dst, width, (i + rows - 1) % rows);
    }
    return checksum;
}

static uint64_t run_seconds(reshuffle_jni_fn fn, buffers_t *buffers, int rows, int width, double seconds, int *completed) {
    volatile uint64_t checksum = 0;
    const uint64_t deadline = now_ns() + (uint64_t) (seconds * 1000000000.0);
    int iterations = 0;
    while (now_ns() < deadline) {
        fn(
                NULL,
                NULL,
                (int64_t) (intptr_t) buffers->src,
                (int64_t) (intptr_t) buffers->dst,
                (int64_t) (intptr_t) buffers->index,
                rows
        );
        checksum += read_dst_element(buffers->dst, width, (iterations + rows - 1) % rows);
        iterations++;
    }
    *completed = iterations;
    return checksum;
}

static void free_buffers(buffers_t *buffers) {
    free(buffers->index);
    free(buffers->src);
    free(buffers->dst);
    free(buffers->expected);
}

int main(int argc, char **argv) {
    const options_t opts = parse_options(argc, argv);
    buffers_t buffers = {0};
    build_buffers(&buffers, opts.rows, opts.width, opts.sequential);

    void *handle = dlopen(opts.lib_path, RTLD_NOW | RTLD_LOCAL);
    if (handle == NULL) {
        fprintf(stderr, "dlopen failed: %s\n", dlerror());
        free_buffers(&buffers);
        return 1;
    }

    const char *sym_name = opts.width == 64
        ? "Java_io_questdb_std_Vect_indexReshuffle64Bit"
        : "Java_io_questdb_std_Vect_indexReshuffle32Bit";

    dlerror();
    reshuffle_jni_fn reshuffle = (reshuffle_jni_fn) dlsym(handle, sym_name);
    const char *error = dlerror();
    if (error != NULL) {
        fprintf(stderr, "dlsym(%s) failed: %s\n", sym_name, error);
        dlclose(handle);
        free_buffers(&buffers);
        return 1;
    }

    validate_once(reshuffle, &buffers, opts.rows, opts.width);

    if (opts.warmup > 0) {
        run_iterations(reshuffle, &buffers, opts.rows, opts.width, opts.warmup);
    }

    const uint64_t start = now_ns();
    int iterations = opts.iterations;
    uint64_t checksum;
    if (opts.seconds > 0.0) {
        checksum = run_seconds(reshuffle, &buffers, opts.rows, opts.width, opts.seconds, &iterations);
    } else {
        checksum = run_iterations(reshuffle, &buffers, opts.rows, opts.width, iterations);
    }
    const uint64_t elapsed = now_ns() - start;

    printf("lib=%s\n", opts.lib_path);
    printf("kernel=indexReshuffle%dBit\n", opts.width);
    printf("rows=%d pattern=%s iterations=%d\n", opts.rows, opts.sequential ? "sequential" : "random", iterations);
    printf("elapsed_ns=%" PRIu64 "\n", elapsed);
    printf("ns_per_call=%.3f\n", iterations > 0 ? (double) elapsed / iterations : 0.0);
    printf("rows_per_sec=%.3f\n", elapsed > 0 ? ((double) opts.rows * iterations * 1000000000.0) / elapsed : 0.0);
    printf("checksum=%" PRIu64 "\n", checksum);

    dlclose(handle);
    free_buffers(&buffers);
    return 0;
}
