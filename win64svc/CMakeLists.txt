cmake_minimum_required(VERSION 3.5)
project(win64svc)

# Use a generator expression so that the specified folder is used directly, without any
# configuration-dependent suffix.
#
# See https://cmake.org/cmake/help/v3.8/prop_tgt/LIBRARY_OUTPUT_DIRECTORY.html
set(OUTPUT ${CMAKE_CURRENT_LIST_DIR}/../core/src/main/bin/$<$<CONFIG:Debug>:>)
add_subdirectory(src)
