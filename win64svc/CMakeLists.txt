cmake_minimum_required(VERSION 3.5)
project(win64svc)


set(OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/../../core/src/main/bin/$<$<CONFIG:Debug>:>)
add_subdirectory(src)
