set(CMAKE_SYSTEM_NAME Linux)
set(CMAKE_SYSTEM_PROCESSOR x86_64)
set(CMAKE_SYSTEM_LIBC musl)
set(MUSL True)

set(CMAKE_C_FLAGS "-I/usr/lib/jvm/java-17-openjdk/include -I/usr/lib/jvm/java-17-openjdk/include/linux -std=gnu99 -D_GNU_SOURCE -ftls-model=global-dynamic -fno-gnu-tm -Wno-int-conversion" CACHE INTERNAL "")
set(CMAKE_CXX_FLAGS "-I/usr/lib/jvm/java-17-openjdk/include -I/usr/lib/jvm/java-17-openjdk/include/linux -fno-exceptions -ftls-model=global-dynamic -fno-gnu-tm -Wno-int-conversion" CACHE INTERNAL "")

set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -Wl,--no-relax")