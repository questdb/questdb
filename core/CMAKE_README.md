## Compiling C, C++ Code

Native code is platform specific and has to be complied on Linux, Mac, Windows
and FreeBSD.

To compile the code CMAKE v3.19 or higher is required, also gcc, g++ >=8.1 are
required.

To build in `./core` run:

```bash
cmake -B build/release -DCMAKE_BUILD_TYPE=Release .
cmake --build build/release --config Release
```

Then you must **rebuild the jar** as described in CONTRIBUTING.md in the root
of this repository. Otherwise your changes will not take effect.

Building the native code will update dynamic link libraries under
`core/src/main/resources/io/questdb/bin/**` which will in turn be packaged as
resources in QuestDB's jar. At runtime, QuestDB extracts the appropriate
library from the jar and loads it using Java JNI.

### Installing and compiling on FreeBSD

This is detailed example of how to install and build on FreeBSD 12 stock VM
image.

1. Install Open JDK (11 at the time of writing), gcc (10 in the below example),
   cmake:

```bash
pkg install openjdk11-11.0.8+10.1
pkg install gcc10-devel
pkg install cmake
```

2. Add JAVA_HOME and reload the profile:

```bash
echo "export JAVA_HOME=/usr/local/openjdk11" >> ~/.profile
```

3. Open new shell to apply JAVA_HOME export and run cmake:

```bash
cmake -DCMAKE_BUILD_TYPE=Release -B build/release -H.
cmake --build build/release --config Release
```

It is also possible to compile using g++ as C++ compiler instead of default
clang:

```bash
cmake -DCMAKE_BUILD_TYPE:STRING=Release -DCMAKE_C_COMPILER=/usr/local/bin/gcc10 -DCMAKE_CXX_COMPILER=/usr/local/bin/g++10 .
make
```
