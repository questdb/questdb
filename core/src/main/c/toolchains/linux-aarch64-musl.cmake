set(CMAKE_SYSTEM_NAME               Linux)
set(CMAKE_SYSTEM_PROCESSOR          aarch64)
set(ARM True)
set(AARCH64 True)
set(CMAKE_SYSTEM_LIBC musl)

set(CMAKE_C_FLAGS "-I/usr/lib/jvm/java-17-openjdk/include -I/usr/lib/jvm/java-17-openjdk/include/linux -std=gnu99 -D_GNU_SOURCE -ftls-model=global-dynamic" CACHE INTERNAL "")
set(CMAKE_CXX_FLAGS "-I/usr/lib/jvm/java-17-openjdk/include -I/usr/lib/jvm/java-17-openjdk/include/linux -fno-exceptions -ftls-model=global-dynamic" CACHE INTERNAL "")

set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -Wl,--no-relax")

set(CMAKE_C_FLAGS_DEBUG             "-Os -g" CACHE INTERNAL "")
set(CMAKE_C_FLAGS_RELEASE           "-Os -DNDEBUG" CACHE INTERNAL "")
set(CMAKE_CXX_FLAGS_DEBUG           "${CMAKE_C_FLAGS_DEBUG}" CACHE INTERNAL "")
set(CMAKE_CXX_FLAGS_RELEASE         "${CMAKE_C_FLAGS_RELEASE}" CACHE INTERNAL "")

set(CMAKE_FIND_ROOT_PATH_MODE_PROGRAM NEVER)
set(CMAKE_FIND_ROOT_PATH_MODE_LIBRARY ONLY)
set(CMAKE_FIND_ROOT_PATH_MODE_INCLUDE ONLY)