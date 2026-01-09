/*
 * This library forces jemalloc background thread initialization before
 * the JVM starts. It works around a race condition where jemalloc's
 * background threads may not start reliably.
 *
 * The issue: Even with env LD_PRELOAD (avoiding shell fork issues),
 * jemalloc's background thread creation can still fail intermittently.
 * Toggling background_thread off then on via mallctl forces reliable
 * thread creation.
 *
 * Compile with:
 *   gcc -shared -fPIC -O2 -D_GNU_SOURCE -o libjemalloc_preload.so jemalloc_preload.c -ldl
 */

#include <stdlib.h>
#include <stdbool.h>
#include <dlfcn.h>

__attribute__((constructor))
static void init_jemalloc_early(void) {
    /* Trigger jemalloc initialization */
    void *ptr = malloc(1);
    if (ptr) {
        free(ptr);
    }

    /*
     * Toggle background threads off then on to force reliable creation.
     * Only do this if background threads are already enabled in the config,
     * to avoid enabling them when the user explicitly disabled them.
     */
    int (*mallctl_fn)(const char *, void *, size_t *, void *, size_t);
    mallctl_fn = dlsym(RTLD_DEFAULT, "mallctl");

    if (mallctl_fn) {
        bool current_enabled = false;
        size_t sz = sizeof(current_enabled);

        /* Query current background_thread setting */
        if (mallctl_fn("background_thread", &current_enabled, &sz, NULL, 0) == 0
                && current_enabled) {
            /* Toggle off then on to force reliable thread creation */
            bool enabled = false;
            mallctl_fn("background_thread", NULL, NULL, &enabled, sizeof(enabled));
            enabled = true;
            mallctl_fn("background_thread", NULL, NULL, &enabled, sizeof(enabled));
        }
    }
}
