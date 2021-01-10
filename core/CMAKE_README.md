## Compiling C, C++ Code

Native code is platform specific and has to be complied on Linux, Mac, Windows and FreeBSD.

To compile the code CMAKE v3.19 or higher is required, also gcc, g++ >=8.1 are required.

To build in ./core run

```bash
cmake .
make
```

### Installing and compiling on FreeBSD

This is detailed example of how to install and build on FreeBSD 12 stock VM image.

1. Install Open JDK (11 at the time of writing), gcc (10 in the below example), cmake

```bash
pkg install openjdk11-11.0.8+10.1
pkg install gcc10-devel
pkg install cmake
```

2. Add JAVA_HOME

```bash
echo "export JAVA_HOME=/usr/local/openjdk11" >> ~/.profile
```

reload the profile 

3. Open new shell to apply JAVA_HOME export and run cmake 

```bash
cmake .
make  
```

it is also possible to compile using g++ as c++ compiler instead of default clang 

```bash
cmake -DCMAKE_BUILD_TYPE:STRING=Release -DCMAKE_C_COMPILER=/usr/local/bin/gcc10 -DCMAKE_CXX_COMPILER=/usr/local/bin/g++10 .
make
```
