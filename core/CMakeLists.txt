cmake_minimum_required(VERSION 3.5)
project(questdb)

# deal with windows slashes in JAVA_HOME
if ($ENV{JAVA_HOME})
    FILE(TO_CMAKE_PATH $ENV{JAVA_HOME} JAVA_HOME)
endif ($ENV{JAVA_HOME})

set(
        VCL_FILES
        src/main/c/share/vect.cpp
#        src/main/c/share/vectkeysum.cpp
)

set(
        VCL_FILES_SSE2
        src/main/c/share/vect_vanilla.h
        src/main/c/share/vect_vanilla.cpp
        src/main/c/share/vect.cpp
        src/main/c/share/vcl/instrset_detect.cpp
#        src/main/c/share/vectkeysum.cpp
        src/main/c/share/vectkeysum_vanilla.h
        src/main/c/share/vectkeysum_vanilla.cpp
)

set(
        SOURCE_FILES
        src/main/c/share/files.h
        src/main/c/share/zip.h
        src/main/c/share/net.h
        src/main/c/share/zip.c
        src/main/c/share/os.h
        src/main/c/share/vect_vanilla.h
)

# JNI includes
include_directories($ENV{JAVA_HOME}/include/)

if (APPLE)
    include_directories($ENV{JAVA_HOME}/include/darwin/)
    set(OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/src/main/resources/io/questdb/bin/osx)
    set(
            SOURCE_FILES ${SOURCE_FILES}
            src/main/c/osx/kqueue.c
            src/main/c/share/net.c
            src/main/c/osx/affinity.c
            src/main/c/osx/accept.c
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
        )
    else (CMAKE_SYSTEM_NAME MATCHES "FreeBSD")
        MESSAGE("Building for GNU/Linux")
        include_directories($ENV{JAVA_HOME}/include/linux/)
        message(${CMAKE_SYSTEM_PROCESSOR})
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
    )
else ()
    set(
            SOURCE_FILES
            ${SOURCE_FILES}
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

if (AARCH64)

    ## on ARM64 we use vanilla arithmetic functions for now

    set(
            AARCH64_FILES
            src/main/c/aarch64/vect.cpp
            src/main/c/share/vect_vanilla.cpp
    )

    add_library(questdb-aarch64 OBJECT ${AARCH64_FILES})
    set_target_properties(
            questdb-aarch64 PROPERTIES
            COMPILE_FLAGS "-O3 -std=c++17"
    )

    target_link_libraries(
            questdb
            questdb-aarch64
    )

    set_target_properties(
            questdb PROPERTIES
            COMPILE_FLAGS "-O3"
    )

else ()

    ## We need to compile VCL four different times with different CXX options
    ## for different instruction sets. As vect.cpp compiles it will produce
    ## different function names for corresponding instruction sets.
    ## Results of these compilations are then combined together in a single library.
    ## This way same library will have a set of functions for each instruction sets.

    ## Java will then call a dispatcher, which will check instruction set for the runtime
    ## and fetch method pointer to the relevant implementation.
    # SSE 4.1 lib
    add_library(questdb-sse4 OBJECT ${VCL_FILES})
    set_target_properties(
            questdb-sse4 PROPERTIES
            COMPILE_FLAGS "-m64 -O3 -msse4.1 -fPIC -std=c++17"
    )

    # AVX2 lib
    add_library(questdb-avx2 OBJECT ${VCL_FILES} src/main/c/share/vectkeysum.cpp)
    set_target_properties(
            questdb-avx2 PROPERTIES
            COMPILE_FLAGS "-m64 -O3 -mavx2 -fPIC -mfma -std=c++17"
    )

    #AVX512 lib
    add_library(questdb-avx512 OBJECT ${VCL_FILES})
    set_target_properties(
            questdb-avx512 PROPERTIES
            COMPILE_FLAGS "-m64 -O3 -mavx512f -fPIC -mfma -mavx512vl -mavx512bw -mavx512dq -std=c++17"
    )

    #SSE2 lib
    add_library(questdb-sse2 OBJECT ${VCL_FILES_SSE2})
    target_link_libraries(
            questdb-sse2
            questdb-sse4
            questdb-avx2
            questdb-avx512
    )

    if (WIN32)
        set_target_properties(
                questdb-sse2 PROPERTIES
                COMPILE_FLAGS "-m64 -O3 -march=core2 -msse2 -std=c++17 -Wno-attributes"
        )
    else ()
        set_target_properties(
                questdb-sse2 PROPERTIES
                COMPILE_FLAGS "-m64 -O3 -march=core2 -msse2 -fPIC -std=c++17"
        )
    endif (WIN32)

    set_target_properties(
            questdb PROPERTIES
            COMPILE_FLAGS "-m64 -O3 "
    )

    include_directories(src/main/c/share/abseil-cpp)

    target_link_libraries(
            questdb
            questdb-sse2
            questdb-sse4
            questdb-avx2
            questdb-avx512
    )
endif (AARCH64)

if (WIN32)
    target_link_libraries(questdb wsock32 ws2_32 secur32 shlwapi)
endif (WIN32)
