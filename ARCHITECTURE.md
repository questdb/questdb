# Architecture

This document is a code map for contributors: where major subsystems live, how
they relate, and which packages to open first when changing behavior. It is not
a product overview; see the [README](README.md) and
[CONTRIBUTING](CONTRIBUTING.md) for setup and contribution expectations.

## High-level layout

```text
                    +------------------+
   clients          |     cutlass      |  wire protocols (HTTP, PGWire, ILP, ...)
                    +--------+---------+
                             |
                             v
                    +------------------+
   SQL              |     griffin      |  parse, plan, execute SQL
                    +--------+---------+
                             |
                             v
                    +------------------+
   storage          |      cairo       |  tables, partitions, WAL, columns
                    +--------+---------+
                             ^
          +------------------+------------------+
          |                                     |
   +------+------+                       +------+------+
   |  network/mp |  I/O + worker pools   | std / jit   |  utils + filters
   +-------------+                       +-------------+
```

Runtime entry points:

| Piece | Path | Role |
|-------|------|------|
| Process bootstrap | `core/src/main/java/io/questdb/Bootstrap.java` | Load config, logging, native libs, then start the server |
| Server wiring | `core/src/main/java/io/questdb/ServerMain.java` | Construct engine, protocol servers, background jobs, lifecycle |
| Configuration | `io.questdb.PropServerConfiguration` and related `*Configuration` types | Map `server.conf` / env into typed settings |
| Lifecycle | `io.questdb.lifecycle.*` | Ordered component start/stop |

Most production code lives under `core/src/main/java/io/questdb/`. Tests mirror
that tree in `core/src/test/java/`. Native helpers are in `core/src/main/c/`
(and some Rust under `core/rust/`). The web console UI is a separate repository
([questdb/ui](https://github.com/questdb/ui)).

## Package map ("where's the thing that does X?")

### `cairo` — storage engine

Column-oriented table storage, readers/writers, partitions, indexes, WAL apply,
materialized/live views, and memory mapping.

| Area | Location (under `io.questdb.cairo`) | Start here when you need to... |
|------|-------------------------------------|--------------------------------|
| Engine facade | `CairoEngine`, table registry types | Open/create tables, resolve table tokens |
| WAL | `wal/` | Write-ahead log, apply-to-table path |
| Column / page memory | `vm/`, `file/` | Mapped files, column drivers |
| Frames / cursors | `frm/`, record cursor types | Scan partitions and produce rows |
| Indexes | `idx/` | Symbol and other index structures |
| Materialized views | `mv/` | Incremental mat view refresh |
| Live views | `lv/` | Live view maintenance |
| SQL-facing storage hooks | `sql/` | Storage support used from the SQL layer |
| Security | `security/` | Access control hooks at the engine |

### `griffin` — SQL compiler and execution

Turns SQL text into plans and runs them against Cairo.

| Area | Location | Start here when you need to... |
|------|----------|--------------------------------|
| Compiler front door | `io.questdb.griffin` (`SqlCompiler` and related) | Parse/compile a query |
| Execution engine | `griffin/engine/` | Operators, joins, window/group by, functions |
| Logical model | `griffin/model/` | Query model structures after parse |

Time-series SQL extensions (SAMPLE BY, LATEST ON, ASOF JOIN, and friends) are
implemented in the griffin engine packages, not as a separate top-level module.

### `cutlass` — network protocols and text/import services

Everything that speaks to the outside world on a socket (plus bulk import/export
helpers started from those paths).

| Area | Location (`io.questdb.cutlass`) | Protocol / role |
|------|---------------------------------|-----------------|
| HTTP + web console API | `http/` | REST, health, console backend |
| PostgreSQL wire | `pgwire/` | PG-compatible clients |
| Influx line protocol | `line/` | High-throughput ILP ingest (TCP/UDP) |
| QuestDB wire protocol | `qwp/` | Native/QWP paths |
| Auth shared pieces | `auth/` | Protocol authentication helpers |
| Text copy import | `text/` | COPY FROM-style ingestion jobs |
| Parquet export hooks | `parquet/` | Export-related jobs |

`cutlass.Services` is the factory used from `ServerMain` to construct protocol
servers given configuration and the shared `CairoEngine`.

### `network` and `mp` — I/O and concurrency

| Package | Role |
|---------|------|
| `io.questdb.network` | OS I/O dispatch (epoll/kqueue-style facades), non-blocking sockets |
| `io.questdb.mp` | Multiprocessing primitives: sequences, worker pools, jobs, wait strategies |

Background work (WAL apply, mat view refresh, telemetry, purge, copy import, ...)
runs as `mp.Job` implementations registered on worker pools in `ServerMain`.

### Supporting packages

| Package | Role |
|---------|------|
| `io.questdb.std` | Zero-GC oriented primitives: strings, bytes, numbers, hash maps, clocks |
| `io.questdb.jit` | JIT-compiled filter paths for eligible queries |
| `io.questdb.log` | Async logging |
| `io.questdb.metrics` | Metrics and query tracing hooks |
| `io.questdb.tasks` | Small task payloads exchanged with worker jobs |
| `io.questdb.preferences` | Server preference storage |

### Other repository roots

| Path | Role |
|------|------|
| `core/` | Main Maven module: Java server, native code, tests |
| `benchmarks/` | JMH / microbenchmarks |
| `compat/` | Compatibility-related modules |
| `java-questdb-client/` | Java client (note: Java 11 target; server is Java 17) |
| `pkg/` | Packaging bits |
| `win64svc/` | Windows service wrapper |
| `ci/`, `.github/` | CI pipelines and automation |
| `docs/` | In-repo developer notes (product docs are published separately) |

## Boundaries between layers

1. **Protocols (`cutlass`) must not own table file formats.** They authenticate,
   decode requests, and call into griffin (SQL) or cairo (direct ingest writers
   such as ILP).
2. **SQL (`griffin`) depends on cairo for storage**, not the reverse. Execution
   operators read/write through Cairo engines, writers, and cursors.
3. **Cairo is the source of truth for on-disk state** (including WAL). Jobs that
   mutate storage (WAL apply, purge, mat views) live next to cairo or are wired
   from `ServerMain` but still call cairo APIs.
4. **`std` / `mp` / `network` are shared infrastructure.** Prefer existing
   primitives (`ObjList`, Cairo mem facilities, worker jobs) over introducing
   parallel utility stacks.
5. **Configuration is read at bootstrap** into configuration objects passed
   downward. Avoid ad-hoc global static config in new code.

## Important types (quick index)

| Type | Package | Why it matters |
|------|---------|----------------|
| `ServerMain` | `io.questdb` | Process composition root |
| `CairoEngine` | `io.questdb.cairo` | Table/engine API used across the server |
| `TableToken` | `io.questdb.cairo` | Stable identity for a table |
| `SqlExecutionContext` | `io.questdb.griffin` | Per-execution SQL context |
| `WorkerPool` | `io.questdb.mp` | Runs jobs on shared threads |
| `HttpServer` / `PGServer` | `io.questdb.cutlass.http` / `pgwire` | Major client entry servers |
| `LineTcpReceiver` | `io.questdb.cutlass.line.tcp` | ILP TCP ingest |
| `PropServerConfiguration` | `io.questdb` | Primary config implementation |

## Where to make common changes

| Goal | First places to look |
|------|----------------------|
| New SQL function or opcode behavior | `griffin/engine/functions/`, then compiler binding sites in `griffin` |
| Query plan or optimizer behavior | `griffin` compiler + `griffin/engine` |
| Ingest path / ILP | `cutlass/line/`, cairo writers / WAL |
| PG protocol / JDBC issues | `cutlass/pgwire/` |
| REST or console API | `cutlass/http/` |
| On-disk layout, partitions, symbols | `cairo/` (and `wal/` for WAL tables) |
| Background maintenance | Jobs registered in `ServerMain`, task types in `tasks/` |
| Server config property | `PropertyKey` / `PropServerConfiguration` |
| Logging format | `log/` |

## Related docs

- [CONTRIBUTING.md](CONTRIBUTING.md) — environment, quality bar, repo navigation
- [README.md](README.md) — product features and getting started
- In-tree notes under `docs/` (for example Parquet metadata notes)

When this map drifts, prefer small PRs that update the relevant section rather
than rewriting the whole file.