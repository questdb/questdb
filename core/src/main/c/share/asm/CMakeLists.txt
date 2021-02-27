cmake_minimum_required(VERSION 3.16)
project(asm C ASM)

set ( ASM_SOURCES 
    ${ASMLIB_SOURCE_DIR}/instrset64.asm 
    ${ASMLIB_SOURCE_DIR}/procname64.asm 
    ${ASMLIB_SOURCE_DIR}/rdtsc64.asm 
    ${ASMLIB_SOURCE_DIR}/round64.asm
    ${ASMLIB_SOURCE_DIR}/libad64.asm 
    ${ASMLIB_SOURCE_DIR}/libad64.def
    ${ASMLIB_SOURCE_DIR}/memcpy64.asm 
    # memmove64.asm 
    # memset64.asm 
    # memcmp64.asm
    # strlen64.asm
    # strcat64.asm 
    # strcpy64.asm 
# strcmp64.asm 
# stricmp64.asm 
# strtouplow64.asm 
# strstr64.asm 
# substring64.asm 
# strspn64.asm 
# strcountutf864.asm 
# strcountset64.asm 
# divfixedi64.asm 
# divfixedv64.asm 
# popcount64.asm 
# cpuid64.asm 
${ASMLIB_SOURCE_DIR}/cputype64.asm 
# physseed64.asm 
${ASMLIB_SOURCE_DIR}/mother64.asm 
${ASMLIB_SOURCE_DIR}/mersenne64.asm 
${ASMLIB_SOURCE_DIR}/randomah.asi 
# sfmt64.asm 
${ASMLIB_SOURCE_DIR}/debugbreak64.asm 
${ASMLIB_SOURCE_DIR}/unalignedisfaster64.asm 
${ASMLIB_SOURCE_DIR}/cachesize64.asm 
# dispatchpatch64.asm
${ASMLIB_SOURCE_DIR}/empty.c
)

enable_language(ASM_NASM)
set(CMAKE_ASM_NASM_OBJECT_FORMAT elf64)
set(INCLUDES ${INCLUDES} ${ASMLIB_SOURCE_DIR})
# add_compile_options(-I ${CMAKE_CURRENT_SOURCE_DIR}/ )
set(CMAKE_ASM_NASM_COMPILE_OBJECT "<CMAKE_ASM_NASM_COMPILER> <INCLUDES> \
    <FLAGS> -f ${CMAKE_ASM_NASM_OBJECT_FORMAT} -o <OBJECT> <SOURCE> -dPOSITIONINDEPENDENT -dUNIX -I ${ASMLIB_SOURCE_DIR}")

enable_language(ASM)
add_library(asm STATIC ${ASM_SOURCES})

target_link_libraries(
    asm
)