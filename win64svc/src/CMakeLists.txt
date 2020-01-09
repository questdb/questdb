set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(SOURCE_FILES common.h getopt.h main.c svcctrl.c questdb.c service.c service.c)
# Use a generator expression so that the specified folder is used directly, without any
# configuration-dependent suffix.
#
# See https://cmake.org/cmake/help/v3.8/prop_tgt/LIBRARY_OUTPUT_DIRECTORY.html
set(OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/../../core/src/main/bin/$<$<CONFIG:Debug>:>)

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${OUTPUT})
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${OUTPUT})
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${OUTPUT})


add_executable(questdb ${SOURCE_FILES})
