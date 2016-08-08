set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -m64")
set(SOURCE_FILES common.h main.c svcctrl.c questdb.c service.c service.c)
set(OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/../../core/src/main/bin)

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${OUTPUT})
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${OUTPUT})
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${OUTPUT})


add_executable(questdb ${SOURCE_FILES})
