cmake_minimum_required(VERSION 3.3)
project(questdb)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -m64 -Wimplicit-function-declaration")
#set(CMAKE_VERBOSE_MAKEFILE  on)

# deal with windows slashes in JAVA_HOME
if ($ENV{JAVA_HOME})
    FILE(TO_CMAKE_PATH $ENV{JAVA_HOME} JAVA_HOME)
endif ($ENV{JAVA_HOME})

set(
        SOURCE_FILES
        src/main/c/share/files.h
        src/main/c/share/zip.h
        src/main/c/share/net.h
        src/main/c/share/zip.c
        src/main/c/share/os.h
)

# JNI includes
include_directories($ENV{JAVA_HOME}/include/)

if (APPLE)
    include_directories($ENV{JAVA_HOME}/include/darwin/)
    set(OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/src/main/resources/binaries/osx)
    set(
            SOURCE_FILES ${SOURCE_FILES}
            src/main/c/osx/kqueue.c
            src/main/c/share/net.c
            src/main/c/osx/affinity.c
            src/main/c/osx/accept.c
    )
elseif (UNIX)
    include_directories($ENV{JAVA_HOME}/include/linux/)
    message(${CMAKE_SYSTEM_PROCESSOR})
    if (CMAKE_SYSTEM_PROCESSOR MATCHES "^aarch64")
        set(OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/src/main/resources/binaries/armlinux)
    else ()
        set(OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/src/main/resources/binaries/linux)
    endif ()
    set(
            SOURCE_FILES ${SOURCE_FILES}
            src/main/c/share/net.c
            src/main/c/linux/epoll.c
            src/main/c/linux/recvmmsg.c
            src/main/c/linux/affinity.c
            src/main/c/linux/accept.c)
endif (APPLE)

if (WIN32)
    include_directories($ENV{JAVA_HOME}/include/win32/)
    set(OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/src/main/resources/binaries/windows)
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

add_library(questdb SHARED ${SOURCE_FILES} ${ZLIB_SRCS})

if (WIN32)
    target_link_libraries(questdb wsock32 ws2_32 secur32 shlwapi)
endif (WIN32)
