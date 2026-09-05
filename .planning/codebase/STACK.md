# Technology Stack

**Analysis Date:** 2026-04-13

## Languages

**Primary:**
- Java 17 - Core database engine and server, zero-GC design
- Rust (nightly) - Performance-critical operations, Parquet codec, JNI bindings
- C/C++ - SIMD operations, platform-specific I/O, vectorized aggregations

**Secondary:**
- Java ILP Client - Client library for Java applications

## Runtime

**Environment:**
- Java Runtime Environment (JRE) 17+ (64-bit)
- Rust nightly toolchain for library compilation
- C/C++ compiler support for Linux, macOS, Windows, FreeBSD

**Package Manager:**
- Maven 3.0+ - Java dependency and build management
- Cargo - Rust package management and compilation

## Frameworks

**Core:**
- No third-party Java frameworks - QuestDB implements everything from first principles
- Cairo Engine (`io.questdb.cairo.*`) - Column-oriented storage, transactions, partitioning
- Griffin SQL Engine (`io.questdb.griffin.*`) - SQL parsing, compilation, optimization, JIT

**Protocol Support:**
- PostgreSQL Wire Protocol (`io.questdb.cutlass.pgwire.*`) - PG-compatible connections
- InfluxDB Line Protocol (`io.questdb.cutlass.line.*`) - ILP TCP and UDP ingestion
- HTTP (`io.questdb.cutlass.http.*`) - REST API and web console
- Cutlass Network Layer (`io.questdb.cutlass.*`) - Unified network protocol handling

**Testing:**
- JUnit 4.13.2 - Test framework
- SQLLogicTest (Rust) - SQL dialect compatibility testing

**Build/Dev:**
- Maven plugins:
  - `maven-compiler-plugin` 3.11.0 - Java compilation (target: Java 17)
  - `maven-surefire-plugin` 3.5.3 - Test execution with parallel fork support
  - `rust-maven-plugin` 1.2.0 (org.questdb) - Rust library compilation integration
  - `buildnumber-maven-plugin` 1.4 - Git commit hash embedding
  - `jacoco-maven-plugin` 0.8.8 - Code coverage analysis
  - `maven-assembly-plugin` 3.0.0 - Binary packaging
- CMake 3.5+ - C/C++ native library compilation
- jlink - Custom JRE runtime generation (Java 17 modules)

## Key Dependencies

**Critical Test Dependencies:**
- `junit:junit` 4.13.2 - Unit testing
- `org.postgresql:postgresql` 42.7.7 - PostgreSQL JDBC for test compatibility
- `org.questdb:questdb-client` 1.0.1 - ILP client for test scenarios

**Rust Core Libraries:**
- `jni` 0.21.1 - Java Native Interface for Rust-Java interop (core, qdbr, sqllogictest)
- `parquet2` 0.17.2 - Parquet file format implementation (local crate)
- `rayon` 1.10.0 - Data parallelism in Rust
- `serde`/`serde_json` 1.0.210/1.0.145 - Serialization framework
- `tokio` 1.38.0 - Async runtime for SQLLogicTest engines
- `rapidhash` 4.4.1 - Fast hashing for columnar operations

**Rust Parquet Support:**
- `parquet-format-safe` 0.2.4 - Safe Parquet metadata handling
- `parquet2` vendored with compression codecs (snap, brotli, streaming-decompression)
- `arrow` 56.2.0 - Arrow data format support (dev dependencies)

**C/C++ Dependencies:**
- Platform-specific system APIs:
  - Linux: `epoll`, `io_uring`, `inotify`, `recvmmsg`
  - macOS/FreeBSD: `kqueue`
  - Windows: Win32 API, custom I/O completion
- CPU instruction detection and SIMD support via VCL (Vectorized Class Library)

## Configuration

**Environment:**
- Property-based configuration via `DefaultServerConfiguration.java` and factory providers
- Configuration properties loaded into memory configuration classes
- JVM arguments for GC control: `-XX:+UseParallelGC`, `-ea` (assertions enabled)

**Build:**
- Platform-specific profiles: `platform-linux-x86-64`, `platform-osx-aarch64`, `platform-linux-aarch64`, `platform-windows-x86-64`, `platform-freebsd-x86-64`
- Feature profiles: `build-web-console`, `build-binaries`, `build-rust-library`, `qdbr-release`, `qdbr-coverage`
- Rust compilation flags configurable via `qdbr.rustflags` (default: `-D warnings`)
- Web console packaging: v1.2.0 downloaded from npmjs registry

## Platform Requirements

**Development:**
- Java 11+ (build requires Java 17+)
- Maven 3.0+
- Rust nightly toolchain
- CMake 3.5+
- C/C++ compiler (gcc/clang on Unix, MSVC on Windows)
- JAVA_HOME environment variable required for JNI headers

**Production:**
- Java 17 JRE or higher
- Linux x86-64, Linux aarch64, macOS x86-64, macOS aarch64, Windows x86-64, or FreeBSD x86-64
- No external JVM dependencies - zero-GC production deployments possible
- Native libraries (libquestdbr.so/dylib/dll) bundled in JAR or provided via source control

---

*Stack analysis: 2026-04-13*
