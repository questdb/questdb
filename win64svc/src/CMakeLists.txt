set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -m64")
set(SOURCE_FILES common.h main.c service.c questdb.c)
add_executable(questdb ${SOURCE_FILES})
