# Immutable hash join build boundary

Implements [RFC 130](https://github.com/questdb/rfc/discussions/130) task 2 on top of
[the phase 0 capability contract](parallel-hash-join-group-by.md). This component
is not yet connected to SQL execution. Default plans remain unchanged.

## Representation decision

Use dedicated `IntHashJoinBuild` storage behind `FrozenHashJoinBuild`. The latter
exposes key/row/byte counts and creates independent `Probe` objects. A probe finds
an INT key, iterates every payload match, exposes a typed payload `Record`, and
can reposition that record using an opaque handle without moving its iterator.
A later radix implementation can route probes to shards behind this interface.

| Option | Benefits | Work or costs |
| --- | --- | --- |
| Independent read-only views over `Unordered4Map` | Reuses the 12-byte INT-key/two-INT-value layout used by the light join, existing hashing and allocation machinery. | Current `withKey()` and value flyweights are mutable shared state. `LongChain.getCursor()` also returns one mutable iterator. Independent views, freezing/publication, cancellable rehashing, and copied-payload/symbol ownership would all need new contracts. The map's batch-offset limits and zero-key slot would become part of that contract. |
| Dedicated frozen lookup (selected) | Keeps the existing map API untouched; naturally separates mutable building from immutable backing and private probe state. Uses logical copied payloads after source closure and supports full INT equality, tracked growth peaks, and cancellation throughout building. | Larger hash slots and copied payloads consume more memory. Copying and symbol interning increase build cost. Measurements below quantify these costs against the existing row-ID representation. |

`DirectSymbolMap` was also inspected: it allocates eagerly without a query
`MemoryTracker`, uses 32-bit offsets, and has uncancellable growth/rehash loops.
Its existing caller-supplied string view is useful precedent, but using it here
would first require those ownership, tracking and cancellation changes. This
implementation instead owns a small dictionary specialized for the frozen phase.

The selected layout is a correctness-qualified starting point. It makes no
end-to-end speedup claim; the RFC's keyed pipeline and performance gate remain
pending. Representation tuning can follow measurements without changing callers.

## Layout and bounds

- A power-of-two linear-probing table uses 16-byte slots: INT key at offset 0,
  padding, LONG head handle at offset 8. Zero head means empty. A populated head
  is the row's byte offset plus one. Maximum occupied load is one half. Collisions
  wrap around; hashing uses `Hash.hashInt64` as in `Unordered4Map`.
- Every row starts with an eight-byte previous-match handle, then naturally aligned
  fixed-width fields; the whole row is rounded to eight bytes. Payload indexes
  follow the constructor's type/index lists, which are copied. Source indexes are
  compiled-record indexes, not candidate base-table indexes. A SYMBOL/DOUBLE
  payload occupies 24 bytes per row, including its duplicate link and padding.
- All matches, including the first, use the same row layout. Duplicates are linked
  in reverse input order, matching the light join's `LongChain`. There is no
  uniqueness assumption or joined-pair ordering promise for aggregates.
- Zero, negative values, `Integer.MAX_VALUE` and `Numbers.INT_NULL` are ordinary
  INT keys; null matches null under existing QuestDB hash-join equality. No key
  value doubles as the empty-slot sentinel. A SQL baseline test verifies null-key
  matching rather than importing generic SQL null semantics.
- BOOLEAN/BYTE/SHORT/CHAR/INT/LONG/DATE/TIMESTAMP/FLOAT/DOUBLE use their logical
  source getters, preserving null sentinels and FLOAT/DOUBLE NaNs. Timestamp
  nanoseconds retain all 64 bits. Unsupported payload types are rejected before
  native allocation. Empty payloads still retain every duplicate.
- Each distinct non-null SYMBOL text receives a dense nonnegative ID in one
  dictionary shared across all payload columns and probes. Null is
  `SymbolTable.VALUE_IS_NULL`, with no dictionary entry. Each dictionary entry
  stores a LONG character offset and INT UTF-16 length in 16 bytes; characters
  occupy a separate native buffer. A temporary 16-byte-slot text hash table is
  discarded at freeze. Hash collisions compare full UTF-16 text. Empty strings,
  Unicode, nulls and repeated values across source columns are covered by tests.
- Hash tables are bounded at `2^30` slots / `2^29` distinct entries (join keys or
  dictionary texts). Each data buffer is bounded at `2^48` bytes. Row size is a
  positive eight-byte multiple below `Integer.MAX_VALUE`; row count is bounded by
  buffer bytes divided by row size. Payload and character offsets are longs,
  without compressed-offset sign ambiguity. Representational overflow raises a
  Cairo capacity error; query memory limits normally bind far earlier.

## Publication, ownership and failure

Construction allocates only the fixed Java schema/skeleton. `open(tracker, breaker)`
binds the current query tracker before allocating the initial hash table. Rows and
symbols allocate lazily. `append(key, record)` copies a pruned payload;
`build(cursor, keyColumn)` consumes a **borrowed** cursor once and freezes. The
caller still closes that source cursor. Neither path stores its record, source
symbol IDs, transient text views, or source symbol-table readers.

`freeze()` permanently ends mutation for that execution and returns a frozen
snapshot of addresses, counts and capacities. It does not dispatch work or supply
a publication barrier itself. The future owner must publish the result through
the page-frame task mechanism after freezing. Tests use executor submission and
a latch to establish the same required happens-before relationship.

Each acquired logical execution slot creates its own probe, duplicate iterator,
payload record, and per-column A/B symbol flyweights. `newSymbolTable` creates
additional independent flyweights when functions need them. All views share only
immutable native backing. Getters and handles are valid only until execution
cleanup; `recordAt` must receive a handle from this build. `find` clears the payload
position and replaces the duplicate iterator even on a miss. Null extension is
still a task 3 joined-record responsibility. An empty build already provides a
symbol source that resolves the null key without a real dictionary allocation.

The factory/execution owner must stop and drain every reader before `close`, and
retain the build's dictionary through aggregate output and parent sorting. Probe
cancellation leaves backing alive for other slots. Closing frees all execution
allocations, invalidates snapshots/handles/views, and permits `open` with a fresh
tracker and new dictionary. Old views must never be reused in that execution.
Mutation after freeze, reopening a live build, and creating a probe from an expired
snapshot are rejected. Compile-and-close allocates no native build backing.

All dynamic storage uses `MemoryTag.NATIVE_JOIN_MAP` and the bound tracker.
Growth allocates a separate destination, copies or rehashes while the source stays
charged, then frees the source. Byte counts include spare capacity. This deliberately
charges the complete growth peak rather than only a realloc delta. Fixed Java
schema and probe skeletons follow the existing native-memory tracking convention;
there is no unbounded Java payload or dictionary collection.

Build appends, freeze, hash collision/rehash loops, UTF-16 hashing/comparison/copy,
and buffer clearing/copying consult the supplied throttled circuit breaker. Large
buffer operations run in at most 1 MiB chunks; text operations check every 1024
characters. Probe lookups and **each duplicate advance** also check their slot's
breaker. Exceptions while opening, appending, consuming or freezing close the
partial build, including an in-progress growth destination. There is no fallback
or consumed-input replay. The owner remains responsible for cancelling/draining
workers before closing published storage.

## Validation

```bash
mvn -pl core test \
  -Dtest=IntHashJoinBuildTest,HashJoinGroupByCandidateTest,HashJoinTest,LongChainTest,Unordered4MapTest,JoinMemoryTrackerTest
mvn -pl benchmarks -am package -DskipTests -Dmaven.test.skip=true
```

98 tests passed: build boundary 16, candidate 12, hash join 11, long chain 5,
unordered INT map 20, and join memory tracking 34. New coverage includes all
supported payload types and nulls, pruned/reordered columns, source closure and
mutable-string overwrite, same-hash SYMBOL collisions, zero/negative/null keys,
randomized multimap comparison, four concurrent readers with interleaved duplicate
iterators and symbol views, and empty/unique/duplicate builds.

Failure tests sweep memory limits across all allocations of a growing build,
cancel at every circuit-breaker check in construction, cancel during a large
payload copy and within a 100,000-match duplicate chain, and throw from a source
getter after symbol allocation. They verify zero leaked query bytes and successful
reuse. Exact-limit tests reject hash/payload growth when the final backing would
fit but old plus new backing exceeds the limit. Native leak checks cover the suite.

## Storage benchmark

`org.questdb.HashJoinBuildBenchmark` compares this representation with the actual
`MapFactory` INT map and `LongChain` configuration/layout used by
`HashJoinLightRecordCursorFactory`. Both build from the same native table; the
row-ID arm retains its cursor and uses real `recordAt` payload access, while the
copied arm closes its source before probing. Both consume country text and capacity
for every match and verify identical matched-pair counts and checksums on every run.

Arguments are distinct key count, duplicate fanout, probe count, and revision:

```bash
java --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED \
  --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow \
  -cp benchmarks/target/benchmarks.jar org.questdb.HashJoinBuildBenchmark \
  10000 1 2000000 "$(git rev-parse HEAD)"
```

The generator is printed in full. It stores contiguous duplicate keys, two SYMBOL
countries, and binary-fraction DOUBLE capacities. Seed 130/131 generates two million
random probes over ten times the key range (approximately 10% matches). Every
execution builds fresh storage. Each of two rounds warms both arms three times,
then records ten runs per arm in alternating order. No query worker pool is started;
this is one-thread component work. Build timing includes initial native allocation,
source iteration, copying/interning and freezing; source cursor acquisition and
closure, fixture generation and probe-array generation are outside timing. Probe
timing includes lookup, duplicate enumeration, typed payload and SYMBOL access.

Allocated bytes are exact **storage-only retained native allocations** from a
separate query tracker after build/freeze. They exclude source mapped files, source
reader/engine state, Java heap, the probe-key array and transient growth peaks. Tests
qualify peak enforcement separately. These measurements do not replace the RFC's
four-worker end-to-end aggregation gate or predict its speedup.

Measured 2026-09-12 on AMD Ryzen 9 7900 (12 cores / 24 threads), 61 GiB RAM,
Linux x86_64, OpenJDK 25.0.4. Source is base `2a0fe6f6b3` plus task 2 in the same
commit as this report. No tests or builds ran during the final measurements.

Both rounds' median build/probe times are milliseconds; bytes include spare capacity:

| Keys × fanout | Arm | Build ms (rounds 1 / 2) | Probe ms (rounds 1 / 2) | Native bytes |
| --- | --- | --- | --- | --- |
| 10,000 × 1 | row-id | 0.294 / 0.317 | 24.101 / 24.644 | 327,692 |
| 10,000 × 1 | copied | 0.551 / 0.562 | 18.167 / 19.319 | 786,560 |
| 100,000 × 1 | row-id | 3.411 / 3.416 | 21.806 / 21.856 | 5,242,892 |
| 100,000 × 1 | copied | 4.822 / 4.771 | 27.179 / 27.096 | 8,388,736 |
| 10,000 × 10 | row-id | 1.092 / 1.085 | 42.061 / 41.096 | 2,293,772 |
| 10,000 × 10 | copied | 2.845 / 2.796 | 23.121 / 22.342 | 4,718,720 |
| 1,000,000 × 1 | row-id | 50.320 / 48.966 | 80.526 / 78.211 | 41,943,052 |
| 1,000,000 × 1 | copied | 126.282 / 124.823 | 78.263 / 75.455 | 67,108,992 |

The copied build uses roughly 1.6–2.4 times the retained native storage in these
cases and takes longer to construct. The 10,000-key unique and duplicate cases
probe faster. The 100,000-key unique case regresses in both rounds, and the
1,000,000-key probe medians are close relative to their spread while copied build
time is much higher. These regressions are retained in the report; V1 still has
no build-size cutoff or runtime fallback. The future keyed pipeline must pass
its full end-to-end gate before Phase 2 or default enablement.

[Individual measurements](parallel-hash-join-group-by-build-results.csv) preserve
all 160 measured samples, including run-to-run spread and result checksums.
The initial 10,000-key smoke invocation overlapped a test build and was discarded;
the table and CSV contain only the subsequent final runs.
