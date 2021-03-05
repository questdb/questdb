cmake_minimum_required(VERSION 3.16)
project(asm C ASM)
#set(CMAKE_VERBOSE_MAKEFILE ON)

set ( ASM_SOURCES 
    ${ASMLIB_SOURCE_DIR}instrset64.asm
    ${ASMLIB_SOURCE_DIR}procname64.asm
#    ${ASMLIB_SOURCE_DIR}rdtsc64.asm
#    ${ASMLIB_SOURCE_DIR}round64.asm
#    ${ASMLIB_SOURCE_DIR}libad64.asm
#    ${ASMLIB_SOURCE_DIR}libad64.def
    ${ASMLIB_SOURCE_DIR}memcpy64.asm
    ${ASMLIB_SOURCE_DIR}cputype64.asm
#    ${ASMLIB_SOURCE_DIR}mother64.asm
#    ${ASMLIB_SOURCE_DIR}mersenne64.asm
#    ${ASMLIB_SOURCE_DIR}debugbreak64.asm
    ${ASMLIB_SOURCE_DIR}unalignedisfaster64.asm
    ${ASMLIB_SOURCE_DIR}cachesize64.asm
#    ${ASMLIB_SOURCE_DIR}empty.c
)

enable_language(ASM_NASM)
set(INCLUDES ${INCLUDES} ${ASMLIB_SOURCE_DIR})
set(CMAKE_ASM_NASM_FLAGS "${CMAKE_ASM_NASM_FLAGS} -dPOSITIONINDEPENDENT")

if (WIN32)
    set(CMAKE_ASM_NASM_FLAGS "${CMAKE_ASM_NASM_FLAGS} -dWINDOWS")
    set(CMAKE_ASM_NASM_OBJECT_FORMAT win64)
elseif(APPLE)
    set(CMAKE_ASM_NASM_FLAGS "${CMAKE_ASM_NASM_FLAGS} -dWINDOWS")
    set(CMAKE_ASM_NASM_OBJECT_FORMAT macho64)
else()
    set(CMAKE_ASM_NASM_FLAGS "${CMAKE_ASM_NASM_FLAGS} -dUNIX")
    set(CMAKE_ASM_NASM_OBJECT_FORMAT elf64)
endif()


set(INCLUDES_DIR ${ASMLIB_SOURCE_DIR})
set(INCLUDES_DIR ${CMAKE_CURRENT_LIST_DIR})

set(CMAKE_ASM_NASM_COMPILE_OBJECT "<CMAKE_ASM_NASM_COMPILER> <INCLUDES> \
    <FLAGS> -f ${CMAKE_ASM_NASM_OBJECT_FORMAT} -o <OBJECT> <SOURCE>")

enable_language(ASM)
add_library(asm STATIC ${ASM_SOURCES})

target_include_directories(asm PRIVATE ${CMAKE_CURRENT_LIST_DIR})

target_link_libraries(
    asm
)