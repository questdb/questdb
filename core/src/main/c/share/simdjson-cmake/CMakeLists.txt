# Use absolute paths and avoid SYSTEM to ensure vendored simdjson takes priority
# over any system-installed version (e.g., from Homebrew on macOS).
# SYSTEM uses -isystem which is searched AFTER -I paths.
set(SIMDJSON_INCLUDE_DIR "${CMAKE_SOURCE_DIR}/src/main/c/share/simdjson/include")
set(SIMDJSON_SRC_DIR "${CMAKE_SOURCE_DIR}/src/main/c/share/simdjson/src")
set(SIMDJSON_SRC "${SIMDJSON_SRC_DIR}/simdjson.cpp")

add_library(_simdjson ${SIMDJSON_SRC})
target_include_directories(_simdjson PUBLIC "${SIMDJSON_INCLUDE_DIR}" PRIVATE "${SIMDJSON_SRC_DIR}")

# simdjson is using its own CPU dispatching and get confused if we enable AVX/AVX2 flags.
#if(ARCH_AMD64)
#    target_compile_options(_simdjson PRIVATE -mno-avx -mno-avx2)
#endif()
