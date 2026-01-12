cmake_minimum_required(VERSION 3.5)
project(questdb)

include(ExternalProject)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

set(CMAKE_CXX_VISIBILITY_PRESET hidden)
set(CMAKE_C_VISIBILITY_PRESET hidden)

# use this is for debug purposes
#set(CMAKE_VERBOSE_MAKEFILE ON)

# deal with windows slashes in JAVA_HOME
if ($ENV{JAVA_HOME})
    FILE(TO_CMAKE_PATH $ENV{JAVA_HOME} JAVA_HOME)
endif ($ENV{JAVA_HOME})

if (CMAKE_SYSTEM_PROCESSOR MATCHES "^arm64" OR CMAKE_SYSTEM_PROCESSOR MATCHES "^aarch64")
    set(ARCH_AARCH64 1)
else ()
    set(ARCH_AMD64 1)
endif ()

if (APPLE)
    set(OS_DARWIN 1)
elseif (UNIX)
    if (CMAKE_SYSTEM_NAME MATCHES "FreeBSD")
        set(OS_FREEBSD 1)
    elseif (CMAKE_SYSTEM_NAME MATCHES "Linux")
        set(OS_LINUX 1)
    endif ()
elseif (WIN32)
    set(OS_WINDOWS 1)
endif (APPLE)

set(
        VCL_FILES
        src/main/c/share/vec_agg.cpp
        src/main/c/share/ooo_dispatch.cpp
        src/main/c/share/geohash_dispatch.cpp
)

set(
        VCL_FILES_SSE2
        src/main/c/share/vcl/instrset_detect.cpp
        src/main/c/share/rosti.cpp
        src/main/c/share/vec_agg_vanilla.cpp
        src/main/c/share/vec_agg.cpp
        src/main/c/share/vec_int_key_agg.cpp
        src/main/c/share/ooo_dispatch.cpp
        src/main/c/share/geohash_dispatch.cpp
)

set(
        SOURCE_FILES
        src/main/c/share/simd.h
        src/main/c/share/files.h
        src/main/c/share/zip.h
        src/main/c/share/net.h
        src/main/c/share/zip.c
        src/main/c/share/os.h
        src/main/c/share/vec_agg_vanilla.h
        src/main/c/share/util.cpp
        src/main/c/share/ooo.cpp
        src/main/c/share/bitmap_index_utils.h
        src/main/c/share/bitmap_index_utils.cpp
        src/main/c/share/geohash.cpp
        src/main/c/share/jit/compiler.h
        src/main/c/share/jit/compiler.cpp
        src/main/c/share/cpprt_overrides.h
        src/main/c/share/cpprt_overrides.cpp
        src/main/c/share/dedup.cpp
        src/main/c/share/byte_sink.cpp
        src/main/c/share/byte_sink.h
        src/main/c/share/converters.cpp
        src/main/c/share/converters.h
        src/main/c/share/json.cpp
)

# JNI includes
include_directories($ENV{JAVA_HOME}/include/)

# Current directory includes and build directory includes
include_directories(${CMAKE_CURRENT_BINARY_DIR} ${CMAKE_SOURCE_DIR})

if (OS_DARWIN)
    include_directories($ENV{JAVA_HOME}/include/darwin/)
    set(
            SOURCE_FILES ${SOURCE_FILES}
            src/main/c/osx/kqueue.c
            src/main/c/share/net.c
            src/main/c/osx/affinity.c
            src/main/c/osx/accept.c
            src/main/c/freebsd/files.c
    )
elseif (OS_FREEBSD)
    MESSAGE("Building for FreeBSD")
    include_directories($ENV{JAVA_HOME}/include/freebsd/)
    set(
            SOURCE_FILES ${SOURCE_FILES}
            src/main/c/freebsd/kqueue.c
            src/main/c/share/net.c
            src/main/c/freebsd/affinity.c
            src/main/c/freebsd/accept.c
            src/main/c/freebsd/files.c
    )
elseif (OS_LINUX)
    MESSAGE("Building for GNU/Linux")
    include_directories($ENV{JAVA_HOME}/include/linux/)
    set(
            SOURCE_FILES ${SOURCE_FILES}
            src/main/c/share/net.c
            src/main/c/linux/epoll.c
            src/main/c/linux/recvmmsg.c
            src/main/c/linux/affinity.c
            src/main/c/linux/accept.c
            src/main/c/linux/files.c
            src/main/c/linux/io_uring.c
            src/main/c/linux/inotify.c
    )
endif ()

if (OS_WINDOWS)
    include_directories($ENV{JAVA_HOME}/include/win32/)
    set(
            SOURCE_FILES ${SOURCE_FILES}
            src/main/c/windows/files.c
            src/main/c/windows/files.h
            src/main/c/windows/os.c
            src/main/c/windows/net.c
            src/main/c/windows/select.h
            src/main/c/windows/errno.h
            src/main/c/windows/select.c
            src/main/c/windows/timer.c
            src/main/c/windows/timer.h
            src/main/c/windows/accept.c
            src/main/c/windows/filewatch.c
            src/main/c/share/fs.h
    )
else ()
    set(
            SOURCE_FILES ${SOURCE_FILES}
            src/main/c/share/files.c
            src/main/c/share/os.c
            src/main/c/share/ooo.h
    )
endif ()

set(OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/target/classes/io/questdb/bin-local/)

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${OUTPUT})
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${OUTPUT})
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${OUTPUT})

#asmjit

if (NOT DEFINED ASMJIT_EMBED)
    set(ASMJIT_EMBED TRUE)
endif ()

include(FetchContent)
FetchContent_Declare(
        asmjit
        GIT_REPOSITORY https://github.com/questdb/asmjit.git
        GIT_TAG f64c90818ff2ef87ec4f73f44d0a7e73fbff3229 # Improved overhead benchmarks
)

FetchContent_MakeAvailable(asmjit)
include_directories(${asmjit_SOURCE_DIR} SYSTEM)
add_compile_definitions(ASMJIT_NO_STDCXX)

# use this is for debug purposes
#add_compile_definitions(ASMJIT_BUILD_DEBUG)

if (NOT DEFINED JIT_TEST)
    set(JIT_TEST FALSE)
endif ()

if (JIT_TEST)
    include_directories(${asmjit_SOURCE_DIR}/test SYSTEM)
    include_directories(${asmjit_SOURCE_DIR}/asmjit-testing/commons SYSTEM)
    add_executable(jittests src/test/c/jittests/test_app.cpp)
    target_link_libraries(jittests questdb)
endif ()

#liburing

if (OS_LINUX)
    MESSAGE("Downloading io_uring")
    ExternalProject_Add(
            liburing_git
            GIT_REPOSITORY http://github.com/axboe/liburing.git
            GIT_TAG liburing-2.2
            BUILD_IN_SOURCE 1
            BUILD_BYPRODUCTS "<SOURCE_DIR>/src/liburing.a"
            BUILD_COMMAND make "CC=${CMAKE_C_COMPILER}" "CXX=${CMAKE_CXX_COMPILER}" "AR=${CMAKE_AR}" "RANLIB=${CMAKE_RANLIB}"
            CONFIGURE_COMMAND ""
            INSTALL_COMMAND ""
            TEST_COMMAND ""
            LOG_BUILD ON
    )
    ExternalProject_Get_Property(liburing_git SOURCE_DIR)
    add_library(liburing INTERFACE)
    add_dependencies(liburing liburing_git)
    target_include_directories(liburing INTERFACE ${SOURCE_DIR}/src/include)
    target_link_libraries(liburing INTERFACE ${SOURCE_DIR}/src/liburing.a)
endif ()

add_library(questdb SHARED ${SOURCE_FILES})
target_link_libraries(questdb asmjit::asmjit)

set(COMMON_OPTIONS "-Wno-gnu-anonymous-struct;-Wno-nested-anon-types;-Wno-unused-parameter;-fPIC;-fno-rtti;-fno-exceptions")

set(DEBUG_OPTIONS "-Wall;-pedantic;-Wextra;-g;-O0")
set(RELEASE_OPTIONS "-O3")

list(APPEND DEBUG_OPTIONS "${ASMJIT_CFLAGS};${COMMON_OPTIONS}")
list(APPEND RELEASE_OPTIONS "${ASMJIT_CFLAGS};${COMMON_OPTIONS}")

#zlib

set(SKIP_INSTALL_ALL OFF CACHE BOOL "Disable install" FORCE)
set(ZLIB_BUILD_EXAMPLES OFF CACHE BOOL "Disable examples" FORCE)
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/src/main/c/share/zlib)
target_link_libraries(questdb zlibstatic)

if (OS_LINUX)
    target_link_libraries(questdb liburing)
    add_dependencies(questdb liburing)
endif ()

if (ARCH_AARCH64)
    add_compile_definitions(__AARCH64__)

    ## on ARM64 we use vanilla arithmetic functions for now
    set(
            AARCH64_FILES
            src/main/c/share/rosti.cpp
            src/main/c/aarch64/vect.cpp
            src/main/c/share/vec_int_key_agg.cpp
            src/main/c/share/vec_agg_vanilla.cpp
            src/main/c/share/ooo_dispatch_vanilla.cpp
            src/main/c/share/geohash_dispatch_vanilla.cpp
    )

    add_library(questdb-aarch64 OBJECT ${AARCH64_FILES})
    target_compile_options(questdb-aarch64 PRIVATE "-fPIC"
            "$<$<CONFIG:DEBUG>:${DEBUG_OPTIONS}>"
            "$<$<CONFIG:RELEASE>:${RELEASE_OPTIONS}>")

    target_link_libraries(
            questdb
            questdb-aarch64
    )

    target_compile_options(questdb PRIVATE
            "$<$<CONFIG:DEBUG>:${DEBUG_OPTIONS}>"
            "$<$<CONFIG:RELEASE>:${RELEASE_OPTIONS}>")

else ()

    #assembler subroutines by Agner Fog
    set(ASMLIB_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/src/main/c/share/asmlib/)
    include(${ASMLIB_SOURCE_DIR}CMakeLists.txt)

    ## We need to compile VCL four different times with different CXX options
    ## for different instruction sets. As vect.cpp compiles it will produce
    ## different function names for corresponding instruction sets.
    ## Results of these compilations are then combined together in a single library.
    ## This way same library will have a set of functions for each instruction sets.

    ## Java will then call a dispatcher, which will check instruction set for the runtime
    ## and fetch method pointer to the relevant implementation.
    # SSE 4.1 lib
    add_library(questdb-sse4 OBJECT ${VCL_FILES})
    target_compile_options(questdb-sse4 PRIVATE "-m64;-msse4.1;-fPIC"
            "$<$<CONFIG:DEBUG>:${DEBUG_OPTIONS}>"
            "$<$<CONFIG:RELEASE>:${RELEASE_OPTIONS}>")

    # AVX2 lib
    add_library(questdb-avx2 OBJECT ${VCL_FILES})
    target_compile_options(questdb-avx2 PRIVATE "-m64;-mavx2;-fPIC;-mfma"
            "$<$<CONFIG:DEBUG>:${DEBUG_OPTIONS}>"
            "$<$<CONFIG:RELEASE>:${RELEASE_OPTIONS}>")

    #AVX512 lib
    add_library(questdb-avx512 OBJECT ${VCL_FILES})
    # A_memset, A_memcpy are faster on avx512 but slower on other CPU's on Linux
    target_compile_definitions(questdb-avx512 PRIVATE ENABLE_ASMLIB)
    target_compile_options(questdb-avx512 PRIVATE "-m64;-mavx512f;-fPIC;-mfma;-mavx512vl;-mavx512bw;-mavx512dq"
            "$<$<CONFIG:DEBUG>:${DEBUG_OPTIONS}>"
            "$<$<CONFIG:RELEASE>:${RELEASE_OPTIONS}>")

    #SSE2 lib
    add_library(questdb-sse2 OBJECT ${VCL_FILES_SSE2})
    target_link_libraries(
            questdb-sse2
            questdb-sse4
            questdb-avx2
            questdb-avx512
    )

    if (WIN32)
        target_compile_options(questdb-sse2 PRIVATE "-m64;-march=core2;-msse2;-Wno-attributes"
                "$<$<CONFIG:DEBUG>:${DEBUG_OPTIONS}>"
                "$<$<CONFIG:RELEASE>:${RELEASE_OPTIONS}>")
    else ()
        target_compile_options(questdb-sse2 PRIVATE "-m64;-march=core2;-msse2;-fPIC"
                "$<$<CONFIG:DEBUG>:${DEBUG_OPTIONS}>"
                "$<$<CONFIG:RELEASE>:${RELEASE_OPTIONS}>")
    endif (WIN32)
    target_compile_options(questdb PRIVATE "-m64"
            "$<$<CONFIG:DEBUG>:${DEBUG_OPTIONS}>"
            "$<$<CONFIG:RELEASE>:${RELEASE_OPTIONS}>")

    ## Uncomment to measure time of ooo.cpp methods
    ## add_compile_definitions(OOO_CPP_PROFILE_TIMING)

    target_link_libraries(
            questdb
            questdb-sse2
            questdb-sse4
            questdb-avx2
            questdb-avx512
            asm
    )
endif (ARCH_AARCH64)

#simdjson
set(SIMDJSON_ENABLE_THREADS OFF CACHE BOOL "Link with thread support" FORCE)
include(${CMAKE_CURRENT_SOURCE_DIR}/src/main/c/share/simdjson-cmake/CMakeLists.txt)

if (OS_WINDOWS)
    target_link_libraries(questdb wsock32 ws2_32 secur32 shlwapi psapi -s -static-libgcc -static-libstdc++ _simdjson)
    target_compile_options(questdb PRIVATE -fno-threadsafe-statics)
elseif (OS_DARWIN)
    target_link_libraries(questdb -nostdlib++ -dead_strip -lc++ _simdjson)
else ()
    target_compile_options(questdb PRIVATE -fno-threadsafe-statics -ffunction-sections -fdata-sections)
    target_link_libraries(questdb rt -Wl,--gc-sections -Wl,--exclude-libs=ALL -static-libgcc _simdjson)
endif ()

# create output directory in case it does not exist, e.g. after mvn clean
add_custom_command(TARGET _simdjson PRE_LINK
        COMMAND ${CMAKE_COMMAND} -E make_directory ${OUTPUT}
)

# Define the secondary output directory

set(SECONDARY_OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/questdb/bin-local/)
set(SECONDARY_OUTPUT2 ${CMAKE_CURRENT_SOURCE_DIR}/target/classes/io/questdb/bin-local/)

add_custom_command(TARGET questdb POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E make_directory ${SECONDARY_OUTPUT}
        COMMAND ${CMAKE_COMMAND} -E copy_directory ${OUTPUT} ${SECONDARY_OUTPUT}
        COMMENT "Copying all build outputs from ${OUTPUT} to ${SECONDARY_OUTPUT}"
)

add_custom_command(TARGET questdb POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E make_directory ${SECONDARY_OUTPUT2}
        COMMAND ${CMAKE_COMMAND} -E copy_directory ${OUTPUT} ${SECONDARY_OUTPUT2}
        COMMENT "Copying all build outputs from ${OUTPUT} to ${SECONDARY_OUTPUT2}"
)

#jemalloc
include(${CMAKE_CURRENT_SOURCE_DIR}/src/main/c/share/jemalloc-cmake/CMakeLists.txt)
