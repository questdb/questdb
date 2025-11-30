if (OS_WINDOWS)
    return()
endif ()

if (OS_LINUX)
    # ThreadPool select job randomly, and there can be some threads that have been
    # performed some memory-heavy tasks before and will be inactive for some time,
    # but until it becomes active again, the memory will not be freed since, by
    # default, each thread has its arena, but there should be no more than
    # 4*CPU arenas (see opt.nareans description).
    #
    # By enabling percpu_arena number of arenas is limited to the number of CPUs, and hence
    # this problem should go away.
    #
    # muzzy_decay_ms -- use MADV_FREE when available on newer Linuxes, to
    # avoid spurious latencies and additional work associated with
    # MADV_DONTNEED. See
    # https://github.com/ClickHouse/ClickHouse/issues/11121 for motivation.
    set(JEMALLOC_CONFIG_MALLOC_CONF "percpu_arena:percpu,oversize_threshold:0,muzzy_decay_ms:0,dirty_decay_ms:5000,background_thread:true")
else ()
    set(JEMALLOC_CONFIG_MALLOC_CONF "oversize_threshold:0,muzzy_decay_ms:0,dirty_decay_ms:5000")
endif ()
# CACHE variable is empty to allow changing defaults without the necessity
# to purge cache
set(JEMALLOC_CONFIG_MALLOC_CONF_OVERRIDE "" CACHE STRING "Change default configuration string of JEMalloc")
if (JEMALLOC_CONFIG_MALLOC_CONF_OVERRIDE)
    set(JEMALLOC_CONFIG_MALLOC_CONF "${JEMALLOC_CONFIG_MALLOC_CONF_OVERRIDE}")
endif ()

message(STATUS "jemalloc malloc_conf: ${JEMALLOC_CONFIG_MALLOC_CONF}")

set(LIBRARY_DIR "src/main/c/share/jemalloc")
set(LIBRARY_CMAKE_DIR "src/main/c/share/jemalloc-cmake")

set(SRCS
        "${LIBRARY_DIR}/src/arena.c"
        "${LIBRARY_DIR}/src/background_thread.c"
        "${LIBRARY_DIR}/src/base.c"
        "${LIBRARY_DIR}/src/bin.c"
        "${LIBRARY_DIR}/src/bin_info.c"
        "${LIBRARY_DIR}/src/bitmap.c"
        "${LIBRARY_DIR}/src/buf_writer.c"
        "${LIBRARY_DIR}/src/cache_bin.c"
        "${LIBRARY_DIR}/src/ckh.c"
        "${LIBRARY_DIR}/src/counter.c"
        "${LIBRARY_DIR}/src/ctl.c"
        "${LIBRARY_DIR}/src/decay.c"
        "${LIBRARY_DIR}/src/div.c"
        "${LIBRARY_DIR}/src/ecache.c"
        "${LIBRARY_DIR}/src/edata.c"
        "${LIBRARY_DIR}/src/edata_cache.c"
        "${LIBRARY_DIR}/src/ehooks.c"
        "${LIBRARY_DIR}/src/emap.c"
        "${LIBRARY_DIR}/src/eset.c"
        "${LIBRARY_DIR}/src/exp_grow.c"
        "${LIBRARY_DIR}/src/extent.c"
        "${LIBRARY_DIR}/src/extent_dss.c"
        "${LIBRARY_DIR}/src/extent_mmap.c"
        "${LIBRARY_DIR}/src/fxp.c"
        "${LIBRARY_DIR}/src/hook.c"
        "${LIBRARY_DIR}/src/hpa.c"
        "${LIBRARY_DIR}/src/hpa_hooks.c"
        "${LIBRARY_DIR}/src/hpdata.c"
        "${LIBRARY_DIR}/src/inspect.c"
        "${LIBRARY_DIR}/src/jemalloc.c"
        "${LIBRARY_DIR}/src/large.c"
        "${LIBRARY_DIR}/src/log.c"
        "${LIBRARY_DIR}/src/malloc_io.c"
        "${LIBRARY_DIR}/src/mutex.c"
        "${LIBRARY_DIR}/src/nstime.c"
        "${LIBRARY_DIR}/src/pa.c"
        "${LIBRARY_DIR}/src/pac.c"
        "${LIBRARY_DIR}/src/pa_extra.c"
        "${LIBRARY_DIR}/src/pages.c"
        "${LIBRARY_DIR}/src/pai.c"
        "${LIBRARY_DIR}/src/peak_event.c"
        "${LIBRARY_DIR}/src/prof.c"
        "${LIBRARY_DIR}/src/prof_data.c"
        "${LIBRARY_DIR}/src/prof_log.c"
        "${LIBRARY_DIR}/src/prof_recent.c"
        "${LIBRARY_DIR}/src/prof_stats.c"
        "${LIBRARY_DIR}/src/prof_sys.c"
        "${LIBRARY_DIR}/src/psset.c"
        "${LIBRARY_DIR}/src/rtree.c"
        "${LIBRARY_DIR}/src/safety_check.c"
        "${LIBRARY_DIR}/src/san_bump.c"
        "${LIBRARY_DIR}/src/san.c"
        "${LIBRARY_DIR}/src/sc.c"
        "${LIBRARY_DIR}/src/sec.c"
        "${LIBRARY_DIR}/src/stats.c"
        "${LIBRARY_DIR}/src/sz.c"
        "${LIBRARY_DIR}/src/tcache.c"
        "${LIBRARY_DIR}/src/test_hooks.c"
        "${LIBRARY_DIR}/src/thread_event.c"
        "${LIBRARY_DIR}/src/ticker.c"
        "${LIBRARY_DIR}/src/tsd.c"
        "${LIBRARY_DIR}/src/witness.c"
        "${LIBRARY_DIR}/src/util.c"
)
if (OS_DARWIN)
    list(APPEND SRCS "${LIBRARY_DIR}/src/zone.c")
endif ()

add_library(jemalloc SHARED ${SRCS})

# First include jemalloc-cmake files, to override anything that jemalloc has.
target_include_directories(jemalloc PUBLIC "${LIBRARY_CMAKE_DIR}/include")
target_include_directories(jemalloc PUBLIC "${LIBRARY_DIR}/include")

set(JEMALLOC_INCLUDE_PREFIX)
# OS_
if (OS_LINUX)
    set(JEMALLOC_INCLUDE_PREFIX "include_linux")
elseif (OS_FREEBSD)
    set(JEMALLOC_INCLUDE_PREFIX "include_freebsd")
elseif (OS_DARWIN)
    set(JEMALLOC_INCLUDE_PREFIX "include_darwin")
else ()
    message(FATAL_ERROR "internal jemalloc: This OS is not supported")
endif ()
# ARCH_
if (ARCH_AMD64)
    set(JEMALLOC_INCLUDE_PREFIX "${JEMALLOC_INCLUDE_PREFIX}_x86_64")
elseif (ARCH_AARCH64)
    set(JEMALLOC_INCLUDE_PREFIX "${JEMALLOC_INCLUDE_PREFIX}_aarch64")
else ()
    message(FATAL_ERROR "internal jemalloc: This arch is not supported")
endif ()

message(STATUS "jemalloc include prefix: ${JEMALLOC_INCLUDE_PREFIX}")
message(STATUS "jemalloc _aarch64: ${ARCH_AARCH64}")
message(STATUS "jemalloc amd64: ${ARCH_AMD4}")

set(JEMALLOC_CMAKE_INCLUDE_PREFIX ${LIBRARY_CMAKE_DIR}/${JEMALLOC_INCLUDE_PREFIX})

# configure jemalloc-cmake headers
configure_file(${JEMALLOC_CMAKE_INCLUDE_PREFIX}/jemalloc/internal/jemalloc_internal_defs.h.in
        ${JEMALLOC_CMAKE_INCLUDE_PREFIX}/jemalloc/internal/jemalloc_internal_defs.h)

target_include_directories(jemalloc PUBLIC
        "${CMAKE_CURRENT_BINARY_DIR}/${JEMALLOC_CMAKE_INCLUDE_PREFIX}/jemalloc/internal")

target_include_directories(jemalloc PUBLIC
        "${LIBRARY_CMAKE_DIR}/include")

target_compile_definitions(jemalloc PUBLIC -DJEMALLOC_NO_PRIVATE_NAMESPACE)

# Because our coverage callbacks call malloc, and recursive call of malloc could not work.
target_compile_options(jemalloc PRIVATE ${WITHOUT_COVERAGE_FLAGS_LIST})

if (CMAKE_BUILD_TYPE_UC STREQUAL "DEBUG")
    target_compile_definitions(jemalloc PUBLIC
            -DJEMALLOC_DEBUG=1
            # Usage examples:
            # - MALLOC_CONF=log:.
            # - MALLOC_CONF='log:core.malloc.exit|core.sallocx.entry|core.sdallocx.entry'
            -DJEMALLOC_LOG=1)
endif ()

target_compile_definitions(jemalloc PUBLIC -DJEMALLOC_PROF=1)

# jemalloc provides support for two different libunwind flavors: the original HP libunwind and the one coming with gcc / g++ / libstdc++.
# The latter is identified by `JEMALLOC_PROF_LIBGCC` and uses `_Unwind_Backtrace` method instead of `unw_backtrace`.
# At the time ClickHouse uses LLVM libunwind which follows libgcc's way of backtracking.
#
# ClickHouse has to provide `unw_backtrace` method by the means of [commit 8e2b31e](https://github.com/ClickHouse/libunwind/commit/8e2b31e766dd502f6df74909e04a7dbdf5182eb1).
#target_compile_definitions (jemalloc PRIVATE -DJEMALLOC_PROF_LIBGCC=1)
#target_link_libraries (jemalloc PRIVATE unwind)

# for RTLD_NEXT
target_compile_options(jemalloc PUBLIC -D_GNU_SOURCE)
