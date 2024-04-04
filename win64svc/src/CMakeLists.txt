set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(SOURCE_FILES common.h getopt.h main.c svcctrl.c questdb.c service.c service.c)

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${OUTPUT})
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${OUTPUT})
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${OUTPUT})

if(MSVC)
    add_definitions(-D_CRT_SECURE_NO_WARNINGS)
endif()

add_executable(questdb ${SOURCE_FILES})
if (QUESTDB_MAIN_CLASS)
    target_compile_definitions(questdb PRIVATE QUESTDB_MAIN_CLASS="${QUESTDB_MAIN_CLASS}")
else()
    target_compile_definitions(questdb PRIVATE QUESTDB_MAIN_CLASS="io.questdb/io.questdb.ServerMain")
endif()

