cmake_minimum_required(VERSION 3.3)
project(nfsdb)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -m64")

# deal with windows slashes in JAVA_HOME
FILE(TO_CMAKE_PATH $ENV{JAVA_HOME} JAVA_HOME)

set(HEADERS
        ${JAVA_HOME}/include/jni.h
        src/main/c/share/files.h
        )
# JNI includes
include_directories(${JAVA_HOME}/include/)
if (APPLE)
    include_directories(${JAVA_HOME}/include/darwin/)
    set(OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/src/main/resources/binaries/osx)
    set(SOURCE_FILES ${HEADERS} src/main/c/share/files.c)
endif (APPLE)

if (WIN32)
    include_directories(${JAVA_HOME}/include/win32/)
    set(OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/src/main/resources/binaries/windows)
    set(SOURCE_FILES ${HEADERS} src/main/c/windows/files.c)
endif (WIN32)

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${OUTPUT})
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${OUTPUT})
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${OUTPUT})

add_library(nfsdb SHARED ${SOURCE_FILES})