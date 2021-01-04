## Compiping C, C++ Code

Native code is platform specific and has to be complied on Linux, Mac, Windows and FreeBSD.

To compile the code CMAKE v3.19 or higher is required. Also gcc, g++ >=8.1 are required.

To build in ./core run

```
cmake .
make
```

### Builing on FreeBSD

1. Install Open JDK

```
pkg search ^openjdk
pkg install openjdk11-11.0.8+10.1
```

2. Add JAVA_HOME

```
echo "export JAVA_HOME=/usr/local/openjdk11" >> ~/.profile
```

3. Install gcc

```
pkg install gcc10-devel
```

4. Open new shell to apply JAVA_HOME export and run cmake pointing to gcc and g++

```
cmake -DCMAKE_BUILD_TYPE:STRING=Release -DCMAKE_C_COMPILER=/usr/local/bin/gcc10 -DCMAKE_CXX_COMPILER=/usr/local/bin/g++10 .
make  
```
