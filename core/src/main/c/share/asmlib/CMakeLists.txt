cmake_minimum_required(VERSION 3.16)

set(
        ASM_SOURCES
        ${ASMLIB_SOURCE_DIR}instrset64.asm
        ${ASMLIB_SOURCE_DIR}procname64.asm
        ${ASMLIB_SOURCE_DIR}memcpy64.asm
        ${ASMLIB_SOURCE_DIR}memset64.asm
        ${ASMLIB_SOURCE_DIR}cputype64.asm
        ${ASMLIB_SOURCE_DIR}unalignedisfaster64.asm
        ${ASMLIB_SOURCE_DIR}cachesize64.asm
        )

enable_language(ASM_NASM)
if (WIN32)
    set(CMAKE_ASM_NASM_FLAGS "-dWINDOWS")
    set(CMAKE_ASM_NASM_OBJECT_FORMAT win64)
elseif (APPLE)
    set(CMAKE_ASM_NASM_FLAGS "-dWINDOWS")
    set(CMAKE_ASM_NASM_OBJECT_FORMAT macho64)
else ()
    set(CMAKE_SHARED_LINKER_FLAGS "-z noexecstack")
    set(CMAKE_ASM_NASM_FLAGS "-dUNIX")
    set(CMAKE_ASM_NASM_OBJECT_FORMAT elf64)
endif ()

set(CMAKE_ASM_NASM_FLAGS "${CMAKE_ASM_NASM_FLAGS} -dPOSITIONINDEPENDENT")

add_library(asm ${ASM_SOURCES})

target_include_directories(asm PRIVATE ${CMAKE_CURRENT_LIST_DIR})

target_link_libraries(
        asm
)