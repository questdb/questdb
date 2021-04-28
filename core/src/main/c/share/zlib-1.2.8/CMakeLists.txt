# This make file has been modified to compile with questdb sources
cmake_minimum_required(VERSION 3.5)
set(CMAKE_ALLOW_LOOSE_LOOP_CONSTRUCTS ON)

set(VERSION "1.2.8")

option(ASM686 "Enable building i686 assembly implementation")
option(AMD64 "Enable building amd64 assembly implementation")

set(INSTALL_BIN_DIR "${CMAKE_INSTALL_PREFIX}/bin" CACHE PATH "Installation directory for executables")
set(INSTALL_LIB_DIR "${CMAKE_INSTALL_PREFIX}/lib" CACHE PATH "Installation directory for libraries")
set(INSTALL_INC_DIR "${CMAKE_INSTALL_PREFIX}/include" CACHE PATH "Installation directory for headers")
set(INSTALL_MAN_DIR "${CMAKE_INSTALL_PREFIX}/share/man" CACHE PATH "Installation directory for manual pages")
set(INSTALL_PKGCONFIG_DIR "${CMAKE_INSTALL_PREFIX}/share/pkgconfig" CACHE PATH "Installation directory for pkgconfig (.pc) files")

include(CheckTypeSize)
include(CheckFunctionExists)
include(CheckIncludeFile)
include(CheckCSourceCompiles)
enable_testing()

check_include_file(sys/types.h HAVE_SYS_TYPES_H)
check_include_file(stdint.h HAVE_STDINT_H)
check_include_file(stddef.h HAVE_STDDEF_H)

#
# Check to see if we have large file support
#
set(CMAKE_REQUIRED_DEFINITIONS -D_LARGEFILE64_SOURCE=1)
# We add these other definitions here because CheckTypeSize.cmake
# in CMake 2.4.x does not automatically do so and we want
# compatibility with CMake 2.4.x.
if (HAVE_SYS_TYPES_H)
    list(APPEND CMAKE_REQUIRED_DEFINITIONS -DHAVE_SYS_TYPES_H)
endif ()
if (HAVE_STDINT_H)
    list(APPEND CMAKE_REQUIRED_DEFINITIONS -DHAVE_STDINT_H)
endif ()
if (HAVE_STDDEF_H)
    list(APPEND CMAKE_REQUIRED_DEFINITIONS -DHAVE_STDDEF_H)
endif ()
check_type_size(off64_t OFF64_T)
if (HAVE_OFF64_T)
    add_definitions(-D_LARGEFILE64_SOURCE=1)
endif ()
set(CMAKE_REQUIRED_DEFINITIONS) # clear variable

#
# Check for fseeko
#
check_function_exists(fseeko HAVE_FSEEKO)
if (NOT HAVE_FSEEKO)
    add_definitions(-DNO_FSEEKO)
endif ()

#
# Check for unistd.h
#
check_include_file(unistd.h Z_HAVE_UNISTD_H)

if (MSVC)
    set(CMAKE_DEBUG_POSTFIX "d")
    add_definitions(-D_CRT_SECURE_NO_DEPRECATE)
    add_definitions(-D_CRT_NONSTDC_NO_DEPRECATE)
    include_directories(${ZLIB_SOURCE_DIR})
endif ()

if (NOT ZLIB_SOURCE_DIR STREQUAL CMAKE_CURRENT_BINARY_DIR)
    # If we're doing an out of source build and the user has a zconf.h
    # in their source tree...
    if (EXISTS ${ZLIB_SOURCE_DIR}/zconf.h)
        message(STATUS "Renaming")
        message(STATUS "    ${ZLIB_SOURCE_DIR}/zconf.h")
        message(STATUS "to 'zconf.h.included' because this file is included with zlib")
        message(STATUS "but CMake generates it automatically in the build directory.")
        file(RENAME ${ZLIB_SOURCE_DIR}/zconf.h ${ZLIB_SOURCE_DIR}/zconf.h.included)
    endif ()
endif ()

set(ZLIB_PC ${CMAKE_CURRENT_BINARY_DIR}/zlib.pc)
configure_file(${ZLIB_SOURCE_DIR}/zlib.pc.cmakein
        ${ZLIB_PC} @ONLY)
configure_file(${ZLIB_SOURCE_DIR}/zconf.h.cmakein
        ${CMAKE_CURRENT_BINARY_DIR}/zconf.h @ONLY)
include_directories(${CMAKE_CURRENT_BINARY_DIR} ${CMAKE_SOURCE_DIR})


#============================================================================
# zlib
#============================================================================

set(ZLIB_PUBLIC_HDRS
        ${CMAKE_CURRENT_BINARY_DIR}/zconf.h
        zlib.h
        )
set(ZLIB_PRIVATE_HDRS
        ${ZLIB_SOURCE_DIR}/crc32.h
        ${ZLIB_SOURCE_DIR}/deflate.h
        ${ZLIB_SOURCE_DIR}/gzguts.h
        ${ZLIB_SOURCE_DIR}/inffast.h
        ${ZLIB_SOURCE_DIR}/inffixed.h
        ${ZLIB_SOURCE_DIR}/inflate.h
        ${ZLIB_SOURCE_DIR}/inftrees.h
        ${ZLIB_SOURCE_DIR}/trees.h
        ${ZLIB_SOURCE_DIR}/zutil.h
        )
set(ZLIB_SRCS
        ${ZLIB_SOURCE_DIR}/adler32.c
        ${ZLIB_SOURCE_DIR}/compress.c
        ${ZLIB_SOURCE_DIR}/crc32.c
        ${ZLIB_SOURCE_DIR}/deflate.c
        ${ZLIB_SOURCE_DIR}/gzclose.c
        ${ZLIB_SOURCE_DIR}/gzlib.c
        ${ZLIB_SOURCE_DIR}/gzread.c
        ${ZLIB_SOURCE_DIR}/gzwrite.c
        ${ZLIB_SOURCE_DIR}/inflate.c
        ${ZLIB_SOURCE_DIR}/infback.c
        ${ZLIB_SOURCE_DIR}/inftrees.c
        ${ZLIB_SOURCE_DIR}/inffast.c
        ${ZLIB_SOURCE_DIR}/trees.c
        ${ZLIB_SOURCE_DIR}/uncompr.c
        ${ZLIB_SOURCE_DIR}/zutil.c
        )

if (NOT MINGW)
    set(ZLIB_DLL_SRCS
            ${ZLIB_SOURCE_DIR}/win32/zlib1.rc # If present will override custom build rule below.
            )
endif ()

if (CMAKE_COMPILER_IS_GNUCC)
    if (ASM686)
        set(ZLIB_ASMS ${ZLIB_SOURCE_DIR}/contrib/asm686/match.S)
    elseif (AMD64)
        set(ZLIB_ASMS ${ZLIB_SOURCE_DIR}/contrib/amd64/amd64-match.S)
    endif ()

    if (ZLIB_ASMS)
        add_definitions(-DASMV)
        set_source_files_properties(${ZLIB_ASMS} PROPERTIES LANGUAGE C COMPILE_FLAGS -DNO_UNDERLINE)
    endif ()
endif ()

if (MSVC)
    if (ASM686)
        ENABLE_LANGUAGE(ASM_MASM)
        set(ZLIB_ASMS
                ${ZLIB_SOURCE_DIR}/contrib/masmx86/inffas32.asm
                ${ZLIB_SOURCE_DIR}/contrib/masmx86/match686.asm
                )
    elseif (AMD64)
        ENABLE_LANGUAGE(ASM_MASM)
        set(ZLIB_ASMS
                ${ZLIB_SOURCE_DIR}/contrib/masmx64/gvmat64.asm
                ${ZLIB_SOURCE_DIR}/contrib/masmx64/inffasx64.asm
                )
    endif ()

    if (ZLIB_ASMS)
        add_definitions(-DASMV -DASMINF)
    endif ()
endif ()

# parse the full version number from zlib.h and include in ZLIB_FULL_VERSION
file(READ ${ZLIB_SOURCE_DIR}/zlib.h _zlib_h_contents)
string(REGEX REPLACE ".*#define[ \t]+ZLIB_VERSION[ \t]+\"([-0-9A-Za-z.]+)\".*"
        "\\1" ZLIB_FULL_VERSION ${_zlib_h_contents})

if (MINGW)
    # This gets us DLL resource information when compiling on MinGW.
    if (NOT CMAKE_RC_COMPILER)
        set(CMAKE_RC_COMPILER windres.exe)
    endif ()

    add_custom_command(OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/zlib1rc.obj
            COMMAND ${CMAKE_RC_COMPILER}
            -D GCC_WINDRES
            -I ${ZLIB_SOURCE_DIR}
            -I ${CMAKE_CURRENT_BINARY_DIR}
            -o ${CMAKE_CURRENT_BINARY_DIR}/zlib1rc.obj
            -i ${ZLIB_SOURCE_DIR}/win32/zlib1.rc)
    set(ZLIB_DLL_SRCS ${CMAKE_CURRENT_BINARY_DIR}/zlib1rc.obj)
endif (MINGW)
