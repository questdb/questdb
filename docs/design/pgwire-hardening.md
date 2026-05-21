# Hardening QuestDB's pgwire implementation

Status: in progress. Phase 0 direct parser harness, smoke tests, JUnit corpus
replay, starter `.pghex` corpus, response-frame oracle, pipeline-pool oracle,
Layer B in-process session fuzz target, and Layer B corpus replay are
implemented. Test-only startup/auth handoff fuzzing through
`handleClientOperation()` and production cleartext-auth fuzzing are implemented.
Standalone Jazzer wiring is implemented via `ci/run-pgwire-fuzz.sh`. Layer C
loopback `ServerMain` fuzzing with a starter corpus is implemented.
Differential oracle target and CI workflow wiring are implemented. Pcap-derived
corpus seeds, live long-soak evidence, richer differential classification, and
upstream PostgreSQL failure triage remain open.
Owner: TBD.
Last updated: 2026-05-21.

> **Investigation note (see §12 Q6):** the initial integer-overflow concern in
> `PGConnectionContext.parseMessage` was overstated. The current completeness
> guard makes the `msgLen + 1` overflow case unreachable, and `msgLimit`
> arithmetic is promoted through the `long` address operand. The arithmetic is
> still worth making explicit with a `long frameLen = (long) msgLen + 1L`, but
> treat that as defensive hardening/readability rather than a confirmed
> out-of-bounds bug.

## 1. Problem statement

QuestDB's PostgreSQL wire protocol server (`io.questdb.cutlass.pgwire`) is
exercised today through high-level client libraries: pgjdbc in
`core/src/test/java/io/questdb/test/cutlass/pgwire/`, and the cross-driver
suite in `compat/`. These tests cover the happy path and a respectable set of
edge cases that real drivers actually emit. They do not cover the input
distribution that matters most for hardening:

- Malformed frames (lying length fields, truncated bodies, oversized
  declared lengths).
- Out-of-order extended-query state transitions (Bind without Parse,
  Execute on a closed portal, Sync arriving inside a half-sent message).
- Wire-protocol-level fuzz: random byte sequences resembling Postgres
  messages, with biased mutation against parser code paths.
- Adversarial parameter encoding: text-format claims with binary-format
  bytes, multidimensional array headers with bogus dim counts, OID/length
  mismatches.
- Authentication corner cases: SCRAM nonces, repeated AuthenticationOk,
  startup messages with garbage version words.
- Concurrent lifecycle anomalies: CancelRequest racing query execution,
  slowloris-style partial writes, pipelining beyond declared buffer sizes.

A real driver cannot send most of these because the driver itself rejects
them before they hit the wire. We need a harness that operates *below* the
driver layer.

Goal: zero segfaults, zero `InternalError`/SQLSTATE `XX000`, zero native
memory leaks on any sequence of bytes a malicious or buggy peer could send.

Non-goal (for now): protecting against amplification-style abuse where the
peer is *authenticated* and authorized. Those concerns are separate.

## 2. Goals and non-goals

### Goals

- Coverage-guided fuzzing of the pgwire message parser, runnable from the
  existing Maven build.
- Stateful fuzzing of the extended-query protocol state machine.
- A regression corpus checked into the repo; every crash found becomes a
  permanent test case.
- Differential testing against PostgreSQL (oracle: "if PG doesn't crash
  on input X, QuestDB must not crash either").
- CI integration: corpus replay on every commit, short fuzz on every PR,
  long fuzz nightly.

### Non-goals

- Replacing existing pgjdbc/compat tests. They cover semantic correctness;
  this work covers robustness.
- Fuzzing SQL semantics. SQL fuzz is a separate effort with different
  tooling (e.g. SQLancer).
- Fuzzing the io_uring / epoll / kqueue I/O layer. Those have their own
  C-level test surface.

## 3. Background

### 3.1 Protocol entry point

The hot path is `PGConnectionContext.parseMessage(long address, int len)`
at `core/src/main/java/io/questdb/cutlass/pgwire/PGConnectionContext.java:1193`.
It is called from `handleClientOperation()` near line 449, after the
authenticator phase has consumed the StartupMessage.

`parseMessage` reads a 5-byte header: one type byte plus a big-endian
4-byte length (the length includes itself but excludes the type byte).
The header constant is `PREFIXED_MESSAGE_HEADER_LEN = 5` at line 144. After
the header it dispatches by type byte to a family of private `msg*`
methods (line 1249 onward):

| Type  | Method                   | Purpose                                             |
|-------|--------------------------|-----------------------------------------------------|
| `'P'` | `msgParse` (line 973)    | Parse: prepare a query                              |
| `'B'` | `msgBind` (line 694)     | Bind: assign parameter values to a parsed statement |
| `'D'` | `msgDescribe` (line 886) | Describe: ask for row description                   |
| `'E'` | `msgExecute` (line 913)  | Execute: run a bound portal                         |
| `'Q'` | `msgQuery` (line 1105)   | Simple Query: parse+bind+execute in one shot        |
| `'S'` | `msgSync`                | Sync: end of extended-query group                   |
| `'C'` | `msgClose` (line 840)    | Close: release a statement or portal                |
| `'X'` | `msgTerminate`           | Terminate connection                                |
| `'H'` | `msgFlush` (line 944)    | Flush: force response send                          |

Each `msg*` method receives a pair of native pointers `(lo, msgLimit)`
into the receive buffer. Reads are via `Unsafe.getByte/getInt/...`, and
strings are NUL-terminated UTF-8.

### 3.2 Per-connection state

Fields on `PGConnectionContext` worth knowing for the harness
(line numbers from the same file):

- `recvBuffer` (long, line 187): native pointer to the receive buffer.
- `sendBuffer` (long, line 192): native pointer to the send buffer.
- `responseUtf8Sink` (line 168): `Utf8Sink` that writes into `sendBuffer`.
- `pipelineCurrentEntry` (line 186): a `PGPipelineEntry` accumulating a
  Parse/Bind/Execute group, pooled.
- `namedStatements`, `namedPortals` (lines 163, 164): hash maps holding
  prepared statements and bound portals, also pooled.
- `pipeline`: a queue of completed `PGPipelineEntry` instances waiting
  for Sync.

Pool ownership: `PGPipelineEntry` instances move between
`pipelineCurrentEntry`, `pipeline`, `namedStatements`/`namedPortals`, and
a free pool. Any reset must respect that — see `clear()` at line 305 for
the canonical free-everything sequence.

### 3.3 Response sink

`PGResponseSink` (`core/src/main/java/io/questdb/cutlass/pgwire/PGResponseSink.java:33`)
extends `Utf8Sink`. The production implementation `ResponseUtf8Sink` is
an inner class of `PGConnectionContext` and is held in a final field. The
harness should keep the production sink and real PG send buffer; response
bytes are captured by the fake socket's `send(long, int)` implementation. This
keeps response-buffer behavior faithful while avoiding any production seam to
inject a separate sink.

### 3.4 Existing tests for orientation

- `core/src/test/java/io/questdb/test/cutlass/pgwire/` — pgjdbc-driven tests.
- `compat/` — cross-driver compatibility tests (Python, Node, etc.).
- `core/src/test/java/io/questdb/test/AbstractCairoTest.java` — the
  `assertMemoryLeak` base class. Fuzz harness will extend this for
  CairoEngine boot.

## 4. Approach

Three test layers, each at a different fidelity/throughput point.

### 4.1 Layer A — single-message decoder fuzz (~50k exec/s)

One iteration: write one synthetic PG frame into a native buffer, call
`parseMessage`, reset state. Catches parser bugs (off-by-ones, integer
overflow in length math, missing NUL terminator handling, oversized
declared lengths, UTF-8 validation).

```java
package io.questdb.test.cutlass.pgwire;

public final class PGParseMessageFuzz {
    private static final int BUF_LEN = 1 << 20;
    private static PGFuzzHarness harness;

    public static void fuzzerTestOneInput(byte[] input) throws Exception {
        if (input.length < 5 || input.length > BUF_LEN) {
            return;
        }

        PGFuzzHarness h = harness();
        long buf = h.inputBuffer();
        for (int i = 0; i < input.length; i++) {
            Unsafe.getUnsafe().putByte(buf + i, input[i]);
        }

        try {
            h.context().parseMessageForFuzz(buf, input.length);
        } catch (PGMessageProcessingException
                 | PeerDisconnectedException
                 | PeerIsSlowToReadException expected) {
            // protocol-level rejection is fine
        } finally {
            try {
                h.assertOutputFramesWellFormed();
            } finally {
                h.reset();
                h.assertPipelinePoolBalanced();
            }
        }
    }
}
```

Required test seams:

- The implemented direct parser fuzz target lives in the existing test package
  `io.questdb.test.cutlass.pgwire`, and the required `PGConnectionContext`
  hooks are `public @TestOnly`. A subpackage such as
  `io.questdb.test.cutlass.pgwire.fuzz` could not call package-private helpers.
- Construct `PGConnectionContext` with a fuzz `PGConfiguration` whose
  `FactoryProvider` returns a fake `SocketFactory`; the socket is created in
  the parent `IOContext` constructor, so it cannot be swapped after
  construction.
- Call `init()` so `doInit()` allocates the real PG receive/send buffers and
  initializes the authenticator. The fuzzer can still pass its own native input
  buffer to `parseMessageForFuzz`; it does not need to replace `recvBuffer`.
- `parseMessageForFuzz(long address, int len)` — thin wrapper exposing the
  existing private `parseMessage`.
- `assumeAuthenticatedForFuzz(SecurityContext securityContext)` — sets the SQL
  execution context to a permissive fuzz security context before direct parser
  calls. A fake authenticator alone is not enough if the harness bypasses
  `handleAuthentication()`.
- `resetForFuzz()` — releases `pipelineCurrentEntry` to its pool, drains the
  `pipeline` queue, calls `freePipelineEntriesFrom` over `namedStatements` and
  `namedPortals`, clears per-session insert cache if strict isolation is
  desired, resets `responseUtf8Sink`, and zeros offsets. It must model the
  state-cleaning part of `clear()` without `super.clear()`, without closing the
  socket/authenticator/circuit breaker, and without freeing PG buffers.

### 4.2 Layer B — stateful session fuzz (~1k exec/s)

One iteration: a sequence of synthetic frames decoded from the raw fuzzer
input. Catches state machine bugs that single-message fuzzing misses (Bind
without Parse, Execute on closed portal, Sync mid extended-query, pipelining +
Cancel).

The implemented `PGSessionFuzz` target uses an encoded raw-byte format: the
first byte chooses 1–32 frames, each frame has a biased type selector, a
two-byte body length capped to the remaining input and 4 KB, and then the body
bytes. The harness writes each selected frame as a complete PG message and calls
`parseMessageForFuzz(...)` repeatedly on the same `PGConnectionContext`, so
extended-query state survives across frames. It resets and checks the response
and pool oracles once at the end of the iteration.

`PGStartupSessionFuzz` covers the same connection-level entry point used by
`PGServer`: it feeds a full socket byte stream into the fake socket, drives
`PGConnectionContext.handleClientOperation(IOOperation.READ)`, and lets the
test authenticator publish leftover bytes by returning
`getRecvBufPseudoStart()`/`getRecvBufPos()` after a StartupMessage-length
prefix. This exercises the normal authentication handoff, receive-buffer
offsets, and post-auth parser loop.

`PGCleartextAuthFuzz` uses the same socket-stream entry point with the
production `PGCleartextPasswordAuthenticator` and
`DefaultPGCircuitBreakerRegistry`. It covers StartupMessage parsing,
SSL/GSS negotiation rejection, CancelRequest handling, cleartext
PasswordMessage length checks, credential verification, login-success
handoff, and post-auth parser entry.

```java
public static void fuzzerTestOneInput(byte[] input) throws Exception {
    FuzzInput data = new FuzzInput(input);
    PGFuzzHarness h = harness();
    h.context().assumeAuthenticatedForFuzz(AllowAllSecurityContext.INSTANCE);

    int steps = data.consumeInt(1, 32);
    while (steps-- > 0 && data.remainingBytes() > 5) {
        byte type = pickMessageType(data);   // biased toward valid set
        int bodyLen = data.consumeInt(0, 4096);
        byte[] body = data.consumeBytes(bodyLen);
        try {
            feedFrame(h.context(), type, body);
        } catch (PGMessageProcessingException
                 | PeerDisconnectedException
                 | PeerIsSlowToReadException ignored) {
            // expected
        }
    }
    h.context().resetForFuzz();
}

private static byte pickMessageType(FuzzInput data) {
    // ~80% valid, ~20% bogus — pure random spends almost all
    // its budget rejected at the type-byte check, wasted CPU.
    if (data.consumeProbabilityFloat() < 0.8f) {
        byte[] valid = {'P', 'B', 'D', 'E', 'S', 'Q', 'C', 'X', 'F', 'H'};
        return valid[data.consumeInt(0, valid.length - 1)];
    }
    return data.consumeByte();
}
```

### 4.3 Layer C — full ServerMain over loopback (~50 exec/s)

One iteration: a real `ServerMain` running in-process, real worker threads,
a raw `java.net.Socket` on loopback, real bytes pushed in. Catches
concurrency bugs the in-context layers cannot: CancelRequest racing query
execution, partial-write starvation, pipelining across the boundary of
buffer flushes.

Too slow per-iteration to be a primary fuzz driver. Use for nightly soak,
fed from the corpus collected by Layers A and B. Reuse `ServerMain`-based
test infrastructure already present in
`core/src/test/java/io/questdb/test/`.

## 5. Required production-code changes

All confined to `PGConnectionContext.java`; estimated 50–80 lines. Concrete
shape now known after investigation (§12).

### 5.1 Constructor and socket stubbing

`PGConnectionContext` constructor (lines 204–209) takes no `Socket`
directly:

```java
public PGConnectionContext(
        CairoEngine engine,
        PGConfiguration configuration,
        SqlExecutionContextImpl sqlExecutionContext,
        NetworkSqlExecutionCircuitBreaker circuitBreaker,
        AssociativeCache<TypesAndSelect> tasCache
)
```

The socket is created inside the parent `IOContext` constructor via the
`SocketFactory` obtained from
`configuration.getFactoryProvider().getPGWireSocketFactory()`. Buffer
allocation happens lazily in `doInit()` (line 1504), not in the
constructor — the harness must call `init()` before the first
`parseMessage`.

Socket I/O happens at:

- `doReceive()` line 1527 → `socket.recv()` line 1529.
- `sendBuffer()` line 1563 → `socket.send()` line 1564.
- `doSendWithRetries()` line 517 → `socket.send()` line 522.

Stubbing approach: a test-scoped `FuzzPGSocketFactory` returning a
subclass of `PlainSocket` that overrides `recv(long, int)` and
`send(long, int)` to copy from/to a test-controlled buffer. The existing
test code `FakeTlsSocket extends PlainSocket` in
`core/src/test/java/io/questdb/test/cutlass/pgwire/PGTlsCompatTest.java:195-252`
demonstrates the pattern.

### 5.2 Authentication bypass

The current authenticator is `PGCleartextPasswordAuthenticator`, the
only `SocketAuthenticator` implementation in
`core/src/main/java/io/questdb/cutlass/auth/`. There is no trust/no-auth
mode. `PGConnectionContext.handleAuthentication()` (line 581) gates
`parseMessage` on `authenticator.isAuthenticated()` returning true.

Direct parser fuzz bypasses `handleAuthentication()`, so it must explicitly
install a permissive fuzz security context via
`assumeAuthenticatedForFuzz(...)`. Session-level fuzz has two options:
explicitly call the same helper after `init()`, or use a fake authenticator
that starts unauthenticated, returns `OK` from `handleIO()`, supplies a stable
principal, and only reports authenticated after `loginOK()` so
`handleAuthentication()` installs the security context through the normal path.
Existing pattern: `PGErrorHandlingTest.java:124-160` mocks the authenticator
factory.

### 5.3 Pool ownership and reset

- `taiPool` (`WeakSelfReturningObjectPool<TypesAndInsert>`,
  `PGConnectionContext.java:173`): **per-context**, owned locally. Held
  `TypesAndInsert` instances are returned when pipeline entries and the
  per-session `taiCache` are closed/cleared; there is no direct pool-drain API.
- `entryPool` (`ObjectStackPool<PGPipelineEntry>`,
  `PGConnectionContext.java:158`): **per-context**, owned locally.
  Release via `releaseToPool()` (lines 552–561). Model `resetForFuzz`
  after `clear()` at line 305.
- `tasCache` (`AssociativeCache<TypesAndSelect>`,
  `PGConnectionContext.java:174`): **shared at `PGServer` level**,
  passed in via constructor (`PGServer.java:76-79, 187-192`).
  `resetForFuzz` must NOT touch it. The harness either accepts that
  cached statements leak across iterations (likely fine) or passes a
  fresh cache per iteration if it wants strict isolation.

### 5.4 Test-scoped seams to add

1. **Expose `parseMessage` to tests**: rename to package-private or add
   a wrapper `parseMessageForFuzz`.
2. **Fuzz harness factory**: creates the context with a configuration whose
   factory provider returns a fake socket factory, installs a fuzz
   authenticator, calls `init()`, and then calls
   `assumeAuthenticatedForFuzz(...)`.
3. **Fake socket**: captures `send(...)` output and feeds `recv(...)` only for
   harnesses that exercise `handleClientOperation()`. Direct `parseMessage`
   fuzzing can pass the native input buffer directly.
4. **`resetForFuzz`**: release `entryPool` objects via `releaseToPool`, clear
   `namedStatements` and `namedPortals` via `freePipelineEntriesFrom`, reset
   `responseUtf8Sink`, zero buffer offsets, and leave `tasCache` alone. Do not
   close/finalize `circuitBreaker`, `authenticator`, `bindVariableService`,
   socket, or PG buffers between iterations.
5. **`FuzzAuthenticator`** as a test-scoped helper under
   `core/src/test/java/io/questdb/test/cutlass/pgwire/`.

These are test-scoped seams. They do not change production behavior.

## 6. Tooling

### 6.1 Jazzer

[Jazzer](https://github.com/CodeIntelligenceTesting/jazzer) is a JVM
coverage-guided fuzzer built on libFuzzer. It instruments bytecode at
load time for edge coverage and mutates seed inputs to maximize new
coverage.

**JUnit version constraint**: Jazzer's `@FuzzTest` JUnit integration
(the `jazzer-junit` artifact) requires JUnit 5.9+. QuestDB is on
JUnit 4.13.2 (`core/pom.xml`), and `jazzer-junit` has never supported
JUnit 4 — see
[issue #865](https://github.com/CodeIntelligenceTesting/jazzer/issues/865),
still open. The latest `jazzer-junit` checked for this document (0.30.0,
February 2026) is JUnit 5
only.

This design therefore uses Jazzer's **standalone binary / main-class mode**
rather than the JUnit integration. For the raw-byte targets below, no
`jazzer-junit` or `jazzer-api` dependency is needed in `core/pom.xml`; the
target method can be called directly by a JUnit 4 replay test. Add
`jazzer-api` only if a later target deliberately uses `FuzzedDataProvider`.

The fuzz target is a plain class with a static `fuzzerTestOneInput` method,
not a `@FuzzTest`-annotated JUnit method:

```java
public final class PGParseMessageFuzz {
    public static void fuzzerTestOneInput(byte[] input) {
        // body as sketched in §4.1
    }
}
```

The Jazzer agent (the binary that drives mutation and coverage) is
distributed from
[GitHub releases](https://github.com/CodeIntelligenceTesting/jazzer/releases),
not Maven Central. A small `ci/` wrapper script resolves the correct
binary for the host OS.

Run modes:

- **Corpus-replay regression** (every-commit tier): a plain JUnit 4
  `@Test` method that walks
  `core/src/test/resources/pgwire-corpus/`, reads each file as `byte[]`, and
  calls `fuzzerTestOneInput(byte[])`. No Jazzer agent or JUnit 5 engine is
  required. Fast enough for `mvn test`. Asserts no uncaught throwable outside
  the expected set and no native-memory drift.
- **Actual fuzzing**:
  use
  `ci/run-pgwire-fuzz.sh --target parse|session|startup|cleartext|servermain|differential`.
  The wrapper runs Maven `compile` and `test-compile`, generates the test
  dependency classpath, passes the core Surefire-style JVM flags, sets
  `-XX:ErrorFile`, writes crash inputs and Java reproducers under the artifact
  directory, and points `questdb.pgwire.fuzz.root` at
  `core/target/pgwire-fuzz/root`. Before launching Jazzer, it materializes the
  selected corpus under `core/target/pgwire-fuzz/corpus/<target>` by decoding
  checked-in `.pghex` files to raw bytes and copying raw files as-is. It uses an
  existing `JAZZER=/path/to/jazzer` or `--jazzer`, or `--download-jazzer` to
  download the pinned Jazzer release into `core/target/jazzer/` with SHA-256
  verification.
  Examples:
  `ci/run-pgwire-fuzz.sh --download-jazzer --target parse --runs 1` and
  `ci/run-pgwire-fuzz.sh --target cleartext --time 60`.
  Invoked manually or from `.github/workflows/pgwire_fuzz.yml`, not from
  `mvn test`.
- **Long unattended runs**: same wrapper with
  `--keep-going N --time <seconds>`.

Alternative considered and rejected: adding JUnit 5 platform dependencies plus
`junit-vintage-engine` so JUnit 5 can run alongside JUnit 4 in the same Surefire
execution, enabling `@FuzzTest`. Rejected because it requires Surefire and
dependency reconfiguration across the whole project for marginal ergonomic
benefit over the standalone/raw-byte API.

### 6.2 PostgreSQL oracle

Run `postgres:16` in a container, expose port 5432, send the same byte
stream to both. Oracle definition:

| QuestDB outcome              | PostgreSQL outcome           | Verdict                   |
|------------------------------|------------------------------|---------------------------|
| Clean close or ErrorResponse | Anything                     | OK                        |
| SEGV / hang / leak           | Clean close or ErrorResponse | Bug in QuestDB            |
| SEGV / hang                  | SEGV / hang                  | Bug in PG (file upstream) |

Do not compare error codes or messages — they legitimately differ.

The implemented `PGDifferentialFuzz` target sends each accepted input through
`PGDifferentialFuzzSupport` to the real QuestDB `TestServerMain` loopback path
and, when configured, to a raw PostgreSQL socket oracle. PostgreSQL is
configured with
`questdb.pgwire.postgres.host`/`questdb.pgwire.postgres.port` or the matching
`QDB_PGWIRE_POSTGRES_HOST`/`QDB_PGWIRE_POSTGRES_PORT` environment variables.
When the PostgreSQL oracle is not configured, the differential smoke test and
standalone target still exercise the QuestDB `ServerMain` path; the sidecar
corpus replay is skipped.

`PGDifferentialFuzzSupport` now distinguishes PostgreSQL oracle availability
failures from QuestDB listener failures in the thrown exception text. It
verifies the PostgreSQL sidecar before fuzzing and after every sidecar input,
and it verifies that QuestDB's in-process pgwire listener still accepts a
loopback connection after the same input.

Open: classify richer per-input status than "listener still accepts
connections", especially if we later need to separate PostgreSQL upstream bugs
from CI sidecar failures automatically.

### 6.3 Corpus management

Layer A corpus directory: `core/src/test/resources/pgwire-corpus/`. Layer B
session corpus directory:
`core/src/test/resources/pgwire-session-corpus/`. Startup/auth handoff corpus
directory: `core/src/test/resources/pgwire-startup-corpus/`. Production
cleartext-auth corpus directory:
`core/src/test/resources/pgwire-cleartext-auth-corpus/`. Layer C ServerMain
corpus directory: `core/src/test/resources/pgwire-servermain-corpus/`.
Differential corpus directory:
`core/src/test/resources/pgwire-differential-corpus/`. One file per input.
These are custom replay corpora, not Jazzer's built-in package/class input
directories.
The checked-in starter corpora use reviewable `.pghex` files: ASCII hex with
optional whitespace and `#` comments, decoded by the JUnit 4 replay tests before
calling `fuzzerTestOneInput(byte[])`. The standalone wrapper performs the same
decoding step into `core/target/pgwire-fuzz/corpus/<target>` because Jazzer
itself treats every corpus file as raw input bytes. Minimized crash artifacts
from standalone runs can also be copied in as raw `.bin` files for permanent
replay.

`core/src/test/resources/.gitattributes` keeps `pgwire-corpus/**/*.bin` binary
and `pgwire-corpus/**/*.pghex` as LF-normalized text, with the same policy for
`pgwire-session-corpus/`, `pgwire-startup-corpus/`,
`pgwire-cleartext-auth-corpus/`, `pgwire-servermain-corpus/`, and
`pgwire-differential-corpus/`.

Seed sources:

- Hand-crafted `.pghex` edge cases: minimum-length frames of every type,
  truncated frames, frames with declared length 0, INT32_MAX, INT32_MIN.
- pcap captures of real pgjdbc sessions. Use `tshark` to extract the
  TCP payload, then chop on PG frame boundaries (length-prefixed).
- Jazzer-discovered crashes: `crash-<sha>` inputs and Java reproducers emitted
  under `core/target/pgwire-fuzz/artifacts/` by the wrapper. Minimize and check
  crash inputs in as raw `.bin` files unless a `.pghex` rendering is more useful
  for review.

## 7. Oracles

Jazzer catches for free: uncaught throwables that the target does not classify
as expected protocol rejection, SIGSEGV, SIGABRT, OOM, and hangs past the
configured deadline.

Add explicitly inside each iteration:

1. **Native memory accounting**. For JUnit replay, wrap the whole corpus in
   `assertMemoryLeak`. For long Jazzer runs, snapshot
   `Unsafe.getMemUsedByTag(MemoryTag.NATIVE_PGW_CONN)` and the global
   tag-sum before and after the run. Per-iteration assertion is too expensive;
   a leak shows up as monotonic growth in the JVM heap or the tag total at
   end-of-run.
2. **Response well-formedness**. After each iteration, walk the bytes captured
   by the fake socket's `send(...)` implementation and verify they decompose
   into legal `(type, length, body)` frames. Malformed output is a bug
   regardless of what input produced it.
3. **Round-trip decode** (Layer A). For complete Bind/Parse/Execute frames that
   `parseMessage` accepts, `PGMessageRoundTripOracle` decodes the protocol
   fields, re-serializes via a minimal encoder, and requires byte-identical
   output. This catches accepted frames whose field structure is not stable
   enough to round-trip.
4. **Pool balance**. At end-of-iteration, assert that
   `pipelineCurrentEntry == null` and that the pool population equals its
   initial value. The current Layer A harness checks
   `entryPool` outie count after `resetForFuzz`; pool drift indicates a leak
   path the reset missed.

## 8. Implementation plan

Ordered by what unblocks the next step.

### Phase 0 — scaffolding (implemented)

- Fuzz target and harness classes are under
  `core/src/test/java/io/questdb/test/cutlass/pgwire/`; production hooks are
  public `@TestOnly`.
- Fake socket/factory and fuzz authenticator are implemented for test use.
- Standalone target bootstrap is implemented by `PGStandaloneFuzzSupport`,
  with `PGStandaloneFuzzBootstrapSmokeTest` covering the no-injected-harness
  path used by the Jazzer launcher.
- `ci/run-pgwire-fuzz.sh` builds the fuzz classpath, downloads/verifies Jazzer
  when requested, and runs the standalone targets.
- `parseMessageForFuzz`, `assumeAuthenticatedForFuzz`, `resetForFuzz`, and an
  `entryPool` outie-count probe are implemented on `PGConnectionContext`.
- Smoke tests cover valid Parse, simple Query, invalid small length,
  `INT_MAX` declared length, and Terminate.

### Phase 1 — Layer A, single-message fuzz (partially implemented)

- `PGParseMessageFuzz` is implemented and the starter corpus currently covers
  `'P'`, `'B'`, `'D'`, `'E'`, `'Q'`, `'S'`, `'C'`, `'X'`, `'F'`, `'H'`, plus
  invalid lengths, truncated frames, server-to-client type bytes, CopyData, and
  malformed strings.
- `PGMessageRoundTripOracle` is wired into `PGParseMessageFuzz` for accepted
  complete Parse, Bind, and Execute frames, with unit coverage for all three
  message types.
- A hand-crafted `.pghex` starter corpus is implemented. A pcap-derived corpus
  from existing pgjdbc tests remains open.
- Standalone Jazzer one-shot execution has been validated for this target.
- Run for 1 h locally. Triage any crashes into `pgwire-corpus/` and
  fix the underlying bugs.

### Phase 2 — Layer B, stateful session fuzz (partially implemented)

- `PGSessionFuzz` with biased message-type selection is implemented for
  authenticated in-process parser sessions.
- `PGStartupSessionFuzz` plus `acceptStartupForFuzz` are implemented for the
  fake-authenticator StartupMessage handoff path through `handleClientOperation`.
- `PGCleartextAuthFuzz` is implemented for the production
  `PGCleartextPasswordAuthenticator` path, with starter corpus coverage for
  startup, SSL/GSS negotiation, cancel, password length, missing NUL, bad
  password, valid login, and login followed by a simple query.
- Pool-balance and response-well-formedness oracles are wired.
- Layer B corpus replay test, startup corpus replay test, production-auth
  corpus replay test, and starter corpora are implemented.
- Standalone Jazzer command wiring exists for all Layer B targets; the
  bootstrap smoke test verifies they can start without an injected JUnit
  harness.
- Run for 4 h locally. Triage.

### Phase 3 — Layer C, full ServerMain fuzz (partially implemented)

- `PGServerMainFuzz` is implemented using a real `TestServerMain` and a raw
  socket loopback client.
- `PGServerMainFuzzSupport` writes an isolated `server.conf`, disables non-PG
  listeners, binds pgwire to `127.0.0.1:0`, starts one pgwire worker, and sends
  raw byte streams over `java.net.Socket` with bounded connect/read timeouts.
- `PGServerMainFuzzSmokeTest`, `PGServerMainFuzzCorpusTest`, and the starter
  `pgwire-servermain-corpus/` are implemented.
- Standalone Jazzer one-shot execution has been validated for this target; the
  target permits only loopback connections through Jazzer's SSRF sanitizer.
- Reuse and expand the corpus from Layers A and B as seed.
- Run long local/nightly soaks and focus on concurrency-triggered bugs.

### Phase 4 — differential oracle (implemented; classification can deepen)

- `PGDifferentialFuzz` is implemented and fans the same input to QuestDB's real
  `TestServerMain` loopback path and an optional raw PostgreSQL socket oracle.
- `PGDifferentialFuzzSupport` is configured by
  `questdb.pgwire.postgres.host`/`questdb.pgwire.postgres.port` or
  `QDB_PGWIRE_POSTGRES_HOST`/`QDB_PGWIRE_POSTGRES_PORT`.
- `PGDifferentialFuzzCorpusTest` replays `pgwire-differential-corpus/` against
  the PostgreSQL oracle when configured. Without a PostgreSQL oracle, the
  sidecar corpus test is skipped; `PGDifferentialFuzzSmokeTest` still covers
  the no-sidecar QuestDB path.
- `ci/run-pgwire-fuzz.sh --target differential` is implemented and forwards the
  PostgreSQL oracle settings into the Jazzer JVM.
- PostgreSQL oracle unavailability and QuestDB listener unavailability are
  reported with distinct exception messages, so generated Jazzer reproducers and
  workflow logs identify which side failed.
- Open: preserve richer both-side status than listener liveness and classify
  PostgreSQL upstream crashes separately from CI sidecar failures.

### Phase 5 — CI integration (implemented; tuning remains)

- `.github/workflows/pgwire_fuzz.yml` runs JUnit 4 corpus replay for checked-in
  pgwire fuzz corpora on pgwire-related PRs, scheduled runs, and manual
  dispatch. The differential sidecar corpus test requires PostgreSQL oracle
  configuration and skips when it is absent.
- The same workflow runs short PR-time Jazzer fuzzing for `parse`, `session`,
  `startup`, `cleartext`, and `servermain`.
- Scheduled/manual long runs cover the same standalone targets plus
  `differential`; the differential job uses a `postgres:16` service.
- Failed fuzz jobs upload `core/target/pgwire-fuzz/artifacts/`. Scheduled
  failures create or update a GitHub issue for the affected target, listing all
  crash artifacts and inlining small crash/reproducer artifacts directly into
  the issue body via `ci/pgwire-fuzz-issue-body.sh`.
- Open: execute and tune the workflow under real CI load, deepen differential
  crash classification, add explicit minimization before filing when practical,
  and decide where the weekly 24 h differential soak should run if
  GitHub-hosted runner limits are too low.

## 9. Bug classes to systematically cover

These are the categories. Each becomes a section of the corpus and a
parameterized hand-written test, so fuzzing has a baseline of coverage
even on a cold start.

### Frame layer

- Declared length = 0, 1, 2, 3, 4 (anything less than self-size).
- Declared length negative.
- Declared length = `INT32_MAX`.
- Declared length = `INT32_MAX - 1` (largest non-overflowing boundary under
  the current completeness guard; see §12 Q6).
- Declared length larger than buffer.
- Declared length one byte short of body.
- Declared length one byte longer than body.
- Type byte 0x00, 0xFF, non-ASCII, valid-but-wrong (e.g. `'Z'` from
  server to client).

### String layer

- NUL-terminated string with no NUL inside frame.
- Empty string (single NUL).
- Embedded NUL.
- Invalid UTF-8: bare continuation bytes, overlong encodings, surrogate
  pairs encoded as UTF-8, codepoints beyond U+10FFFF.
- Strings up to 64 KB; cap above that to avoid OOM in the harness.

### Extended-query state machine

- Bind without Parse.
- Bind referencing nonexistent prepared statement.
- Execute on unbound portal.
- Execute on Closed portal.
- Sync inside a partial message stream.
- Describe before Bind.
- Close referencing nonexistent statement/portal.
- Close with `type` byte != `'S'` && != `'P'` (see `msgClose` at line 840).
- Pipelining: 10/100/1000 Parse-Bind-Execute groups before one Sync.

### Parameter binding

- Parameter count mismatch (claim N, supply M).
- OID = 0 (PG's "unknown" — explicit test).
- OID claims text format, body is binary.
- OID claims binary format, body is text.
- Binary array with mismatched dim count vs body.
- Binary multidimensional array (e.g. `DOUBLE[][]`) with bogus per-dim
  length. *Particularly relevant given QuestDB's multidim array support.
  Entry points: `PGNonNullBinaryArrayView`, `PGNonNullVarcharArrayView`.*
- Per-parameter length = -1 (PG's NULL encoding) on a non-nullable
  column.

### Authentication

- StartupMessage with protocol version != 196608.
- StartupMessage truncated at every byte offset.
- StartupMessage with garbage key/value pairs, missing user, duplicate
  user.
- CancelRequest at first message (magic int 80877102).
- SSLRequest twice in a row.
- Cleartext password: empty, very long, with embedded NULs.
- *SCRAM is not currently implemented (only `PGCleartextPasswordAuthenticator`
  exists). Skip SCRAM corpus. Revisit if SCRAM support is added.*

### Lifecycle / concurrency (Layer C only)

- CancelRequest racing in-flight query.
- *CopyData (`'d'`) is not implemented; see §12 Q4. Sending it should
  fall through to the default-case `msgKaput` path — worth a single
  corpus entry to confirm, but no dedicated harness.*
- Slowloris StartupMessage: one byte per second.
- Pipelining beyond `recvBuffer` size: triggers buffer-shift code path
  near line 1217–1223.
- 1000 simultaneous connections, each sending one byte then closing.

## 10. Known gotchas specific to QuestDB

- **JNI crashes kill the JVM**. Jazzer's libFuzzer parent process
  catches this and saves the input, but the Java side has no stack —
  rely on `hs_err_pid*.log`. Enable
  `-XX:ErrorFile=core/target/fuzz-crashes/hs_err_%p.log` for fuzz runs.
- **`assertMemoryLeak` does not compose with 50k iterations**. Per-call
  cost is dominated by the global counter snapshot. Snapshot once at
  `@BeforeAll`, assert once at `@AfterAll`. Leaks manifest as monotonic
  growth visible across runs.
- **Object pools must be drained per iteration**. `PGPipelineEntry`,
  `TypesAndInsert`, `TypesAndSelect` are pooled. A `resetForFuzz` that
  fails to return them will OOM the harness in under a minute.
- **Direct buffers**. PG protocol allows `INT32_MAX` parameter sizes.
  Honoring that literally means trying to allocate 2 GB per iteration,
  which crashes the harness rather than the parser. Cap parameter
  sizes in the harness to ~1 MB and rely on a hand-written test for
  the "huge claimed size" case.
- **Zero-GC discipline**. Avoid per-iteration allocation in the harness
  itself — reuse the raw input scratch buffer and fake-socket output buffers
  where practical, or accept that the harness's own allocation noise is small
  enough to not matter for >1k exec/s.
- **`java-questdb-client/` is a separate git repo** (submodule). No
  pgwire code lives there; do not commit fuzz code into it. See
  `CLAUDE.md`.

## 11. CI cadence

| Trigger      | Workload                                             | Budget |
|--------------|------------------------------------------------------|--------|
| Every commit | Corpus replay across all checked-in fuzz targets     | <30 s  |
| Every PR     | 5 min per standalone target                          | 5 min each |
| Nightly      | 4 h per standalone target, including differential    | 4 h each |
| Weekly       | 24 h soak on Layer C with differential oracle        | open   |

Crashes from any tier:

1. Auto-saved as `crash-<sha>` in the artifacts of the failing job.
2. Auto-filed as a GitHub issue on scheduled workflow failures, with links to
   the workflow artifacts and logs. Small crash/reproducer artifacts are also
   inlined into the issue body.
3. Manually minimized and checked in as `.pghex`/`.bin` corpus entries for
   permanent regression coverage.

## 12. Resolved questions (investigation findings)

Each subsection records the question that was open during initial design
and the answer found by reading the code. File:line citations are
included so the conclusion can be re-verified as the code evolves.

### Q1 — Booting `PGConnectionContext` without a real socket

**Resolved: feasible with a stub `SocketFactory`.**

- Constructor signature at `PGConnectionContext.java:204-209` takes no
  socket directly; the socket is created via
  `configuration.getFactoryProvider().getPGWireSocketFactory()` inside
  the parent `IOContext` constructor.
- Buffer allocation is deferred to `doInit()` (line 1504) — the harness
  must call `init()` before the first `parseMessage`.
- Socket I/O is contained to `doReceive()` (line 1527 → `socket.recv()`
  line 1529), `sendBuffer()` (line 1563 → `socket.send()` line 1564),
  and `doSendWithRetries()` (lines 517, 522).
- Pattern to follow: `FakeTlsSocket extends PlainSocket` in
  `core/src/test/java/io/questdb/test/cutlass/pgwire/PGTlsCompatTest.java:195-252`
  — subclass `PlainSocket`, override `recv`/`send`. Reference
  bootstrap: `BasePGTest.java:105-141`.

Action: see §5.1.

### Q2 — Authentication bypass

**Resolved: use explicit fuzz security-context installation for direct parser
fuzzing, or a test-only authenticator that drives the normal auth path.**

- `SocketAuthenticator` is the interface in
  `core/src/main/java/io/questdb/cutlass/auth/SocketAuthenticator.java:30-55`.
  `isAuthenticated()` (line 50) is the gate.
- Only implementation today is `PGCleartextPasswordAuthenticator`
  (`PGCleartextPasswordAuthenticator.java:244-245, 534-542`). No trust
  mode, no `noAuth` config flag.
- Gate is enforced at `PGConnectionContext.handleAuthentication()`
  (line 581) before `parseMessage` runs.
- Existing test pattern: `PGErrorHandlingTest.java:124-160` mocks the
  authenticator via factory injection.

Action: see §5.2. Direct parser fuzz uses `assumeAuthenticatedForFuzz(...)`.
Session fuzz either explicitly installs the fuzz security context or drives
`handleAuthentication()` through an initially unauthenticated fake
authenticator.

### Q3 — Pool ownership

**Resolved: `taiPool` and `entryPool` are per-context; `tasCache` is
shared at `PGServer` level.**

- `taiPool` (`WeakSelfReturningObjectPool<TypesAndInsert>`):
  `PGConnectionContext.java:173`, sized by
  `insertBlockCount * insertRowCount` at line 249. Per-context.
  `TypesAndInsert` extends `AbstractSelfReturningObject`, returns to
  pool via `close()` → `parentPool.push()`
  (`AbstractSelfReturningObject.java:37`).
- `entryPool` (`ObjectStackPool<PGPipelineEntry>`):
  `PGConnectionContext.java:158`, sized at line 244 by
  `configuration.getPipelineCapacity()`. Per-context. Released via
  `releaseToPool()` (lines 552–561).
- `tasCache` (`AssociativeCache<TypesAndSelect>`):
  `PGConnectionContext.java:174`, but **owned by `PGServer`**
  (`PGServer.java:76-79`, passed in to each context at lines 187–192).
  Shared across all connections of one server.

Action: see §5.3. `resetForFuzz` releases live `PGPipelineEntry` instances,
clears `namedStatements`/`namedPortals`, clears the per-session `taiCache` when
strict iteration isolation is required, and leaves shared `tasCache` alone.

### Q4 — PG COPY subprotocol support

**Resolved: not supported.**

- `PGConnectionContext.parseMessage` (lines 1249–1280) handles only
  `'P'`, `'B'`, `'D'`, `'E'`, `'Q'`, `'S'`, `'H'`, `'C'`, `'X'`. No
  cases for `'d'` (CopyData), `'c'` (CopyDone), `'f'` (CopyFail).
- No `CopyInResponse`/`CopyOutResponse`/`CopyBothResponse` emission
  anywhere in the pgwire package.
- SQL-level `COPY ... FROM/TO` is parsed by the compiler
  (`SqlCompilerImpl.java:3985-3988`) but operates on the filesystem
  (`CopyImportFactory`, `CopyExportFactory` at lines 2752, 2777). It
  never engages pgwire COPY messages.

Action: no dedicated COPY harness. A single corpus seed sending `'d'`
suffices to confirm the default-case `msgKaput` path. See §9 lifecycle
bullets.

### Q5 — SCRAM-SHA-256 support

**Resolved: not supported.**

- Only `PGCleartextPasswordAuthenticator` exists; no SASL/SCRAM/MD5
  implementations.
- `DefaultPGAuthenticatorFactory.java:69-77` hardcodes the cleartext
  authenticator.
- `PGCleartextPasswordAuthenticator.prepareLoginResponse()` (line 333)
  sends `AuthenticationCleartextPassword` (auth type 3). No code path
  emits `AuthenticationSASL` (type 10) or `AuthenticationSASLContinue`
  (type 11).
- Zero repo-wide references to SCRAM/SASL outside this design doc.

Action: auth-fuzz stops at cleartext. See §9 Authentication.

### Q6 — `parseMessage` integer overflow

**Resolved: no confirmed overflow/OOB bug in the current code; keep a small
defensive cleanup.**

Trace through `PGConnectionContext.parseMessage` lines 1193–1228:

- Line 1204: `msgLen = getIntUnsafe(address + 1)` — signed `int`.
- Line 1206: `msgLen < 1` guard catches negatives (including
  high-bit-set wire values). Safe.
- Line 1217: `msgLen > len - 1` guard. Because line 1198 enforces
  `len >= PREFIXED_MESSAGE_HEADER_LEN == 5`, the `len - 1`
  subtraction cannot underflow. Safe.
- Line 1217 also makes the only overflowing `msgLen + 1` case unreachable.
  `msgLen == Integer.MAX_VALUE` would require `len - 1 >= Integer.MAX_VALUE`,
  impossible for a positive `int len`. The largest complete frame that can pass
  the guard is `msgLen == Integer.MAX_VALUE - 1`, where `msgLen + 1` is exactly
  `Integer.MAX_VALUE` and does not overflow.
- Line 1228 does not have the same int-overflow shape as line 1227:
  `address` is `long`, so `address + msgLen + 1` is evaluated as long
  arithmetic.

**Why it still matters**:

1. The current expression is easy to misread and will keep attracting false
   positives in review/static analysis.
2. The fuzzer should keep the boundary seeds so any future parser rewrite or
   change to `len` typing preserves the guard.
3. The defensive cleanup is cheap and makes the intended frame length explicit.

**Recommended cleanup** (separate from fuzzing work): widen the arithmetic
explicitly once, use that value for the completeness guard, then reuse it:

```java
final long frameLen = (long) msgLen + 1L;
if (frameLen > len) {
    return;
}
recvBufferReadOffset += frameLen;
final long msgLimit = address + frameLen;
```

**Fuzzer action**: add corpus seeds with declared lengths
`Integer.MAX_VALUE`, `Integer.MAX_VALUE - 1`, and `0x7FFFFFFE`.
Already incorporated into §9 Frame-layer bullets. The `Integer.MAX_VALUE`
seed should remain an incomplete-message case under the current guard; the
`Integer.MAX_VALUE - 1` seed exercises the largest non-overflowing boundary.

## 13. References

### Code locations

- `core/src/main/java/io/questdb/cutlass/pgwire/PGConnectionContext.java`
  — main entry point and message dispatcher.
- `core/src/main/java/io/questdb/cutlass/pgwire/PGPipelineEntry.java`
  — extended-query state per pipeline group.
- `core/src/main/java/io/questdb/cutlass/pgwire/PGResponseSink.java`
  — response sink interface.
- `core/src/main/java/io/questdb/cutlass/pgwire/PGNonNullBinaryArrayView.java`
  — binary array decoding; high-value fuzz target given multidim arrays.
- `core/src/main/java/io/questdb/cutlass/pgwire/PGNonNullVarcharArrayView.java`
  — varchar array decoding.
- `core/src/main/java/io/questdb/cutlass/pgwire/PGCleartextPasswordAuthenticator.java`
  — cleartext auth handler.
- `core/src/main/java/io/questdb/cutlass/pgwire/PGAuthenticatorFactory.java`
  — auth handler selection.
- `core/src/test/java/io/questdb/test/AbstractCairoTest.java`
  — base class providing `engine`, `configuration`, `setUpStatic`.

### External references

- [PostgreSQL Frontend/Backend Protocol](https://www.postgresql.org/docs/current/protocol.html)
  — authoritative protocol spec. Section 55.7 lists every message format.
- [Jazzer documentation](https://github.com/CodeIntelligenceTesting/jazzer)
  — standalone target signatures, corpus layout, run modes.
- [libFuzzer corpus format](https://llvm.org/docs/LibFuzzer.html#corpus)
  — Jazzer inherits this exactly.

### Glossary

- **Frame** — one PG protocol message: type byte + length-prefixed body.
- **Extended-query protocol** — Parse + Bind + Execute + Sync sequence,
  distinct from simple Query (`'Q'`).
- **Pipeline** — multiple extended-query groups sent before a Sync.
- **Portal** — a bound prepared statement, ready to execute.
- **Corpus** — a directory of inputs that a coverage-guided fuzzer
  treats as starting points for mutation.
- **Oracle** — a check that decides whether a given execution found a
  bug, separate from the fuzzer's built-in crash detection.
