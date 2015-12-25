cmake_minimum_required(VERSION 3.3)
project(nfsdb)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
set(JAVA_HOME $ENV{JAVA_HOME})

# JNI includes
include_directories(${JAVA_HOME}/include/)
if (APPLE)
    include_directories(${JAVA_HOME}/include/darwin/)
    set(OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/src/main/resources/binaries/osx)
endif (APPLE)

if (WIN32)
    include_directories(${JAVA_HOME}/include/win32/)
    set(OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/src/main/resources/binaries/windows)
endif(WIN32)


set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${OUTPUT})
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${OUTPUT})
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${OUTPUT})

set(SOURCE_FILES ${JAVA_HOME}/include/jni.h src/main/c/share/files.h src/main/c/share/files.c)
add_library(nfsdb SHARED ${SOURCE_FILES})