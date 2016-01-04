cmake_minimum_required(VERSION 3.3)
project(nfsdb)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -m64")

# deal with windows slashes in JAVA_HOME
if ($ENV{JAVA_HOME})
    FILE(TO_CMAKE_PATH $ENV{JAVA_HOME} JAVA_HOME)
endif ($ENV{JAVA_HOME})

set(HEADERS
        src/main/c/share/files.h
        )

# JNI includes
include_directories(${JAVA_HOME}/include/)
if (APPLE)
    include_directories(${JAVA_HOME}/include/darwin/)
    set(OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/src/main/resources/binaries/osx)
    set(SOURCE_FILES ${HEADERS} src/main/c/share/files.c src/main/c/share/zip.c src/main/c/share/zip.h)
elseif (UNIX)
    include_directories(${JAVA_HOME}/include/linux/)
    set(OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/src/main/resources/binaries/linux)
    set(SOURCE_FILES ${HEADERS} src/main/c/share/files.c src/main/c/share/zip.c src/main/c/share/zip.h)
endif (APPLE)

if (WIN32)
    include_directories(${JAVA_HOME}/include/win32/)
    set(OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/src/main/resources/binaries/windows)
    set(SOURCE_FILES ${HEADERS} src/main/c/windows/files.c src/main/c/share/zip.c src/main/c/share/zip.h)
endif (WIN32)

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${OUTPUT})
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${OUTPUT})
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${OUTPUT})

#zlib
set(ZLIB_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/src/main/c/share/zlib-1.2.8)
include(${ZLIB_SOURCE_DIR}/CMakeLists.txt)

add_library(nfsdb SHARED ${SOURCE_FILES} ${ZLIB_SRCS} ${ZLIB_ASMS})