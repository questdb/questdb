# External Integrations

**Analysis Date:** 2026-04-13

## APIs & External Services

**PostgreSQL Compatibility:**
- PostgreSQL Wire Protocol support via `io.questdb.cutlass.pgwire.*`
  - SDK/Client: PostgreSQL JDBC driver 42.7.7 (for testing only)
  - Auth: Public key and password-based authentication
  - Location: `core/src/main/java/io/questdb/cutlass/pgwire/`

**InfluxDB Line Protocol (ILP):**
- TCP ingestion via `io.questdb.cutlass.line.tcp.*`
- UDP ingestion via `io.questdb.cutlass.line.udp.*`
- Auth: Challenge-response authenticator for authentication
- Location: `core/src/main/java/io/questdb/cutlass/line/`

**REST API:**
- HTTP protocol via `io.questdb.cutlass.http.*`
- REST endpoints for SQL queries and data import
- Authentication: Token and session-based auth via cookies
- Location: `core/src/main/java/io/questdb/cutlass/http/`

**QuestDB Java ILP Client:**
- Explicit client library: `org.questdb:questdb-client` 1.0.1
- Used for ILP-based data ingestion from Java applications
- Location: `java-questdb-client/`

## Data Storage

**Databases:**
- Primary: QuestDB itself (columnstore with SIMD acceleration)
- Cairo Engine: Custom zero-GC column-oriented storage (`io.questdb.cairo.*`)
  - Connection: File-based local tables or in-process via Java API
  - Client: TableWriter (writes) and TableReader (reads) in `io.questdb.cairo.*`

**File Storage:**
- Local filesystem only - no cloud object storage integration (S3, GCS, Azure)
- Custom columnar binary format with compression support
- Parquet export/import via Rust library (`core/rust/qdbr/parquet2/`)

**Caching:**
- In-memory query result caching via Cairo engine
- No external caching service (Redis, Memcached)
- Query plan caching in Griffin SQL compiler (`io.questdb.griffin.*`)

## Authentication & Identity

**Auth Provider:**
- Custom implementations in `io.questdb.cutlass.auth.*`
  - `AnonymousAuthenticator` - No-auth mode
  - `EllipticCurveAuthenticatorFactory` - Public key cryptography (ECDSA)
  - `ChallengeResponseMatcher` - Challenge-response for ILP
  - `UsernamePasswordMatcher` - Basic user/password auth

**Implementation:**
- Token-based authentication for HTTP and ILP protocols
- Session management via cookies in HTTP
- Per-user authentication via `io.questdb.cutlass.auth.SocketAuthenticator`
- Public key repository for authentication (`PublicKeyRepo.java`)

## Monitoring & Observability

**Error Tracking:**
- None detected - errors logged locally via JVM logging

**Logs:**
- Java logging framework (via `io.questdb.log.*`)
- Console and file output supported
- No external log aggregation (Splunk, ELK, Datadog)

**Metrics:**
- Internal metrics collection via `io.questdb.metrics.*`
  - `DefaultMetricsConfiguration` - Metrics configuration
  - `MetricsConfiguration` - Metrics interface
- Location: `core/src/main/java/io/questdb/metrics/`

**Telemetry:**
- Optional telemetry collection via `TelemetryConfiguration.java`
- Configuration-driven, can be disabled

## CI/CD & Deployment

**Hosting:**
- Self-hosted deployments (standalone JRE or Docker)
- Binary distributions available for: Linux (x86-64, aarch64), macOS (x86-64, aarch64), Windows (x86-64), FreeBSD (x86-64)

**CI Pipeline:**
- Maven-based build system
- GitHub Actions (configured in `.github/workflows/`)
- Automated release process via maven-release-plugin 3.1.1
- Central publishing via sonatype-central-publishing-maven-plugin 0.8.0

**Docker Support:**
- No Docker-specific configuration detected in core pom.xml
- Binary packaging supports standalone distribution with embedded JRE

## Environment Configuration

**Required env vars:**
- `JAVA_HOME` - Java runtime location (required for native compilation)
- `QDB_CLIENT_CONF` - ILP client configuration string (test environments)
- `LLVM_PROFILE_FILE` - Rust code coverage output (test environments with qdbr-coverage profile)

**Secrets location:**
- No detected external secrets management
- Configuration via property files or environment variables
- Authentication tokens stored in QuestDB configuration

## Webhooks & Callbacks

**Incoming:**
- HTTP REST API endpoints for queries and data import
- ILP TCP and UDP listeners for time-series data ingestion
- PostgreSQL Wire Protocol listener for standard SQL clients

**Outgoing:**
- None detected - QuestDB is a data sink, not a data source triggering external webhooks

## External Data Formats

**Parquet Support:**
- Rust implementation via `parquet2` 0.17.2 crate
- Read/write capabilities for columnar data interchange
- Compression: snap, brotli, streaming-decompression

**CSV Import:**
- Text import processor via `io.questdb.cutlass.http.processors.TextImportRequestHeaderProcessor`
- HTTP multipart form support

## No External Dependencies

**Zero third-party Java libraries:**
- All data structures implemented in-house: custom collections in `io.questdb.std.*`
- No ORM, no HTTP client libraries, no serialization frameworks
- Algorithm implementations from first principles
- Native code integration only via JNI (Rust/C)

---

*Integration audit: 2026-04-13*
