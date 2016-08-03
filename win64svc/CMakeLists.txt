cmake_minimum_required(VERSION 3.5)
project(win64svc)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -m64")

# deal with windows slashes in JAVA_HOME
if ($ENV{JAVA_HOME})
    FILE(TO_CMAKE_PATH $ENV{JAVA_HOME} JAVA_HOME)
endif ($ENV{JAVA_HOME})


# JNI includes
include_directories(${JAVA_HOME}/include/)
include_directories(${JAVA_HOME}/include/win32/)

set(SOURCE_FILES main.c)
add_executable(questdb ${SOURCE_FILES})
