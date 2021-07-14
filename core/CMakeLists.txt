cmake_minimum_required(VERSION 3.5)
project(questdb)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# deal with windows slashes in JAVA_HOME
if ($ENV{JAVA_HOME})
    FILE(TO_CMAKE_PATH $ENV{JAVA_HOME} JAVA_HOME)
endif ($ENV{JAVA_HOME})

set(
        VCL_FILES
        src/main/c/share/vec_agg.cpp
        src/main/c/share/vec_ts_agg.cpp
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
        src/main/c/share/vec_ts_agg.cpp
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
        src/main/c/share/txn_board.cpp
        src/main/c/share/bitmap_index_utils.h
        src/main/c/share/bitmap_index_utils.cpp
        src/main/c/share/geohash.cpp
)

# JNI includes
include_directories($ENV{JAVA_HOME}/include/)

if (APPLE)
    include_directories($ENV{JAVA_HOME}/include/darwin/)
    if (CMAKE_SYSTEM_PROCESSOR MATCHES "^arm64")
        set(AARCH64 1)
        set(OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/src/main/resources/io/questdb/bin/armosx)
    else()
        set(OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/src/main/resources/io/questdb/bin/osx)
    endif()
    set(
            SOURCE_FILES ${SOURCE_FILES}
            src/main/c/osx/kqueue.c
            src/main/c/share/net.c
            src/main/c/osx/affinity.c
            src/main/c/osx/accept.c
            src/main/c/freebsd/files.c
    )
elseif (UNIX)
    if (CMAKE_SYSTEM_NAME MATCHES "FreeBSD")
        MESSAGE("Building for FreeBSD")
        include_directories($ENV{JAVA_HOME}/include/freebsd/)
        set(OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/src/main/resources/io/questdb/bin/freebsd)
        set(
                SOURCE_FILES ${SOURCE_FILES}
                src/main/c/freebsd/kqueue.c
                src/main/c/share/net.c
                src/main/c/freebsd/affinity.c
                src/main/c/freebsd/accept.c
                src/main/c/freebsd/files.c
        )
    else (CMAKE_SYSTEM_NAME MATCHES "FreeBSD")
        MESSAGE("Building for GNU/Linux")
        include_directories($ENV{JAVA_HOME}/include/linux/)
        if (CMAKE_SYSTEM_PROCESSOR MATCHES "^aarch64")
            set(OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/src/main/resources/io/questdb/bin/armlinux)
            set(AARCH64 1)
        else ()
            set(OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/src/main/resources/io/questdb/bin/linux)
        endif ()
        set(
                SOURCE_FILES ${SOURCE_FILES}
                src/main/c/share/net.c
                src/main/c/linux/epoll.c
                src/main/c/linux/recvmmsg.c
                src/main/c/linux/affinity.c
                src/main/c/linux/accept.c
                src/main/c/linux/files.c
        )

    endif (CMAKE_SYSTEM_NAME MATCHES "FreeBSD")
endif (APPLE)

if (WIN32)
    include_directories($ENV{JAVA_HOME}/include/win32/)
    set(OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/src/main/resources/io/questdb/bin/windows)
    set(
            SOURCE_FILES ${SOURCE_FILES}
            src/main/c/windows/files.c
            src/main/c/windows/os.c
            src/main/c/windows/net.c
            src/main/c/windows/select.h
            src/main/c/windows/errno.h
            src/main/c/windows/select.c
            src/main/c/windows/timer.c
            src/main/c/windows/timer.h
            src/main/c/windows/accept.c
            src/main/c/share/fs.h
    )
else ()
    set(
            SOURCE_FILES ${SOURCE_FILES}
            src/main/c/share/files.c
            src/main/c/share/os.c
    )
endif (WIN32)

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${OUTPUT})
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${OUTPUT})
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${OUTPUT})

#zlib
set(ZLIB_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/src/main/c/share/zlib-1.2.8)

include(${ZLIB_SOURCE_DIR}/CMakeLists.txt)

# ZLIB_SRCS is defined in Zlib Cmake config
add_library(questdb SHARED ${SOURCE_FILES} ${ZLIB_SRCS})

set(DEBUG_OPTIONS "-Wall;-g;-O0")
set(RELEASE_OPTIONS "-O3")

if (AARCH64)

    add_compile_definitions(__aarch64__)

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
endif (AARCH64)

if (WIN32)
    target_link_libraries(questdb wsock32 ws2_32 secur32 shlwapi)
elseif (APPLE)
    target_link_libraries(questdb)
else ()
    target_link_libraries(questdb rt)
endif (WIN32)
