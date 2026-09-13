# Fused hash join: execution allocations (RFC task 9c)

This change implements [RFC 130](https://github.com/questdb/rfc/discussions/130)
task 9c. The experimental flag remains false. Tasks 9d (circuit breakers), 9e
(the expanded semantic/storage/negative matrix), and the repeated task 10
performance/rollout gate remain pending. Allocation measurements do not replace
that latency gate or justify default enablement.

## Execution allocation audit

| State or phase | Implementation and lifecycle |
| --- | --- |
| Native build growth and rehash | One closed scratch `Buffer` belongs to `IntHashJoinBuild`. Growth uses it for the simultaneously tracked destination, transfers ownership, and closes it in `finally`. No wrapper is constructed per resize. Source and destination allocations remain charged together. |
| Build publication and probe slots | The fused atom opts into a reusable frozen snapshot. Every slot retains its own probe, payload record and SYMBOL flyweights. Binding refreshes addresses/counts and the execution generation after build completion, before task publication. The ordinary build API still creates execution-specific snapshots. |
| Handle validity | Each execution reserves a disjoint range of opaque payload handles. Rebinding resets duplicate/record state; old handles remain invalid even after explicit probe rebinding. Other slots and independently borrowed SYMBOL views do not become valid when one slot rebinds. Checks retain the build API's assertion-based access contract. |
| Scalar state | Each slot constructs a closed `SimpleMapValue` shell once. `reopen(tracker)` binds the active query before native allocation; close frees through that tracker and clears it. Empty initialization and intermediate-state merging reuse existing updaters. |
| Source SYMBOL views | `SymbolMapReaderImpl` lends independent uncached views from a synchronized pool of at most 256 closed views per dictionary. Each view retains three string flyweights and a reusable index cursor, without caching symbol text. Clone acquisition/return is outside row evaluation. Repeated execution, cursor cloning and dictionary updates retain native source lookup semantics. |
| Copied SYMBOL views | The snapshot's probes own per-column views and reuse closed independent clones for compiled expressions. Their bound is the fixed expression/slot shape, independent of symbol cardinality. Generation checks protect expired dictionaries. Build copying still owns the native dictionary and frees source views in `finally`. |
| Frame and decoder initialization | `PageFrameMemoryPool` prepares one native-closed buffer shell and partition-decoder shell during bounded construction. This includes the owner's local filter task, which may first encounter Parquet under queue pressure. Cursor close frees native backing and retains already-closed buffer shells within the existing 256-shell cap. Active monotonic/scattered cache caps remain 4/256. Reopen binds the new tracker before allocation. |
| Functions, filters and frame scans | Grouping sinks, updater instances, mutable filters and logical records retain their fixed schema/slot ownership. Source and copied SYMBOL views are independent. Frame descriptors and sparse indices remain tracked native vectors; growth does not allocate Java arrays. Native, mixed and Parquet scans are measured. |
| Probing, updates, merges and output | No per-row/match/group allocation is introduced. Duplicate traversal and outer null extension use the slot record; owner/sharded maps and scalar values remain native. Existing map cursors, merge destinations, output records and symbol getters are reused. The result consumer hashes all values directly into a fixed three-long result buffer. |
| Close, failures and factory reuse | Probe/merge work drains before functions, slots and build backing close. Retaining closed control objects does not retain charged native buffers. Failure regressions and the separately measured build cancellation check release every query charge and reexecute the same factory successfully. |

The changes to shared source views and decoder pools also serve ordinary scans.
Their controls are bounded by schema, simultaneous view leases, fixed queue cells
and cache caps; they do not store one Java object per row, symbol or group. The
retained-heap rerun below checks increasing cardinality with the changed layout.
The preallocation changes bounded setup/retention costs; task 10 must measure
end-to-end costs, including the existing uncached-predicate and decoder-cache
tradeoffs from task 9b.

## Measurement method

The standalone sources in `benchmarks/allocation` use JDK 25's per-thread
allocation counters and an ASM 9.9.1 javaagent. Both passes instrument the same code; they have distinct jobs:

- **Byte counters:** allocation-site hooks increment fixed primitive counters;
  stack capture is disabled. The hooks do not allocate. The agent independently
  measures allocated-byte deltas inside existing shared bookkeeping:
  `FanOut.and/remove`, `ConcurrentLongHashMap.putVal`, and `Unsafe.recordMemAlloc`.
  These scopes account for subscription retries, registry entries and the JDK
  `LongAdder` cells used by global native-memory counters. They contain no fused
  row processing. Fixed per-thread primitive arrays store the measurements.
  Every owner and all four workers are sampled from before cursor acquisition
  through build/probe/merge, output consumption and cursor close. Total bytes
  must equal the independently counted shared bytes; any residual fails the run.
- **Allocation census and stacks:** a separate pass instruments allocation
  bytecodes, allocating string concatenation and capturing-lambda sites in
  application classes, enabling stack capture for this pass. It records exact site hit counts and the first stack for
  every observed site, plus reducer participation per thread. Successful measured
  executions reject all recurring sites except the audited shared subscription
  and registry objects. Array/list sites require their shared subscription
  ancestry. This census is paired with the byte counters, since bytecodes alone
  cannot attribute allocations inside JDK methods or JVM compilation effects.

No assertion rests on GC collection counts or absence of sampled events. The
repeatable gate pins synchronous C1 compilation (`-Xbatch`,
`-XX:TieredStopAtLevel=1`), disables escape analysis (`-XX:-DoEscapeAnalysis`) and
thread-local allocation buffers (`-XX:-UseTLAB`). This prevents allocation
elimination from concealing sites and removes TLAB bookkeeping from the byte
windows. A C2 diagnostic recorded 160 bytes of String/byte-array allocation at
the allocation-free `PerWorkerFunctionList.isOwned()` type check during close;
the [exact diagnostic stacks](parallel-hash-join-group-by-allocation/compiler-diagnostic.txt.gz)
are retained. C1 separates this JVM behavior from the source-level execution
allocation gate. Default-JVM regression, retained-heap and smoke checks are
reported separately; these allocation runs make no latency claim. The harness
warms growth and merge code in a disposable factory. That factory closes and all source
readers are released before the measured factory is compiled. Its execution
maps/build are native and freed; the shared queue and decoder control pools have
fixed caps. This does not warm a Java pool proportional to the measured data.

Compiler allocation is reported separately. The measured factory first runs
with a bind range admitting 1,022 rows per input (`setup1024`), followed by 64
small setup executions and 32 executions at the first key range's full native
occupancy. The latter cover scheduling-dependent local tasks and peak simultaneous
SYMBOL view leases when two owners interchange source readers. Their heap cost is
reported separately as `BOUNDED_SETUP_BYTES`.

This setup warms only the audited control pools: views are bounded by expression/
slot leases, queue cells and decoder shells have fixed caps, and no symbol text is
cached. Every native dictionary, build and aggregation map closes on each setup
execution. The retained-heap rerun independently checks increasing cardinalities;
the direct build regression measures new symbols/growth after only 512-row warmups.
The full-occupancy setup is not a data-sized Java pool and must not become one in
future changes.

The bind then selects a **disjoint range of 8,192 or 65,536 previously uncopied
symbols** on each side for three fresh builds. Native build/dictionary/map growth
is forced anew on every execution; group-by presizing is disabled for this
experiment. Bind changes do not invalidate or recompile the factory. Every cursor
close is checked against the actual query tracker retained before unbinding, not a null execution-context
tracker. Numerical inputs are exact integer-valued doubles.

With two owners, a persistent peer thread uses its own factory, context, bind
service and result buffer while sharing the four query workers. Both cursors
must finish before the final thread counters are read. The census requires all
four workers to participate in every case across its measured executions.
The reader hashes result values and SYMBOL characters directly; formatting,
checksums copied for comparison, profiling stacks, CSV output and compiler work
are outside the byte measurement window. Agent counter arrays are fixed harness
storage, separate from the execution heap audit.

A deliberate breaker failure interrupts a live build after 100 checks. Its bytes
and allocation stacks are reported under `cancellation`, separately from the
successful-execution gate. The same factory then executes successfully and matches
the pre-cancellation result. Existing tests cover other cancellation/failure
phases, task drain, peer isolation and native-limit failures.

## Workload and artifacts

The 24 cases use a fixed four-worker query shape, 64-row frames/Parquet row groups,
two scanned partitions and an excluded active sentinel partition. Each input
contains `2 * rows + 1` source rows; binds select exactly `rows` rows from a
disjoint second key range. Mixed storage scans both formats (including rejected
Parquet rows); the all-Parquet cases decode accepted rows as well. The base
matrix covers native/mixed/Parquet storage and owner/sharded/scalar merging with
8,192 rows per input and one owner, then 65,536 rows and two owners. Owner cases
use INNER, sharded cases LEFT, and scalar cases normalized RIGHT. Six additional
mixed/Parquet RIGHT cases cover all merges with two owners and LONG-to-DOUBLE
logical conversion. Both inputs have duplicate keys, distinct SYMBOL values,
and grouping/output expressions on both sides. The measured query exercises
SUM/AVG DOUBLE, count(*), and COUNT INT/LONG/DOUBLE/SYMBOL on both inputs, plus
`year`, `month`, LIKE/ILIKE, input filtering and bind rebinding.

All **24 cases, 234 measured candidate executions and 405 owner/worker byte
windows passed**, with **zero unexplained successful-execution bytes** and all
four workers participating in every census case. The 48 byte/census processes
also passed their cancellation cleanup, post-failure reuse and ordinary-result
checks.

Keyed output grows to 131,072 groups. Each measured execution is checked for
reuse consistency; concurrent peers must agree, and the last successful result
and post-cancellation reuse are compared with ordinary execution. The byte and
census passes must agree. Plans, compiler/setup/failure bytes, every thread/phase
sample, allocation counts and stacks are retained in compressed case logs.

Artifacts live in [the allocation results directory](parallel-hash-join-group-by-allocation/):
[case matrix](parallel-hash-join-group-by-allocation/cases.csv),
[per-thread attribution](parallel-hash-join-group-by-allocation/summary.csv),
and [source, jar and environment hashes](parallel-hash-join-group-by-allocation/environment.txt).
The [retained-heap rerun](parallel-hash-join-group-by-allocation/heap/summary.csv)
uses the task 9b graph boundary, increasing rows/keys/symbols/groups/fanout,
fresh/reused execution, and live/output/closed snapshots. Its per-class totals,
plans, native balances and separate environment are retained beside the summary.

## Reproduction and validation

Use JDK 25, Maven 3 and the pinned Rust toolchain. ASM is a harness dependency;
it is not added to the engine or benchmark application dependencies. Do not run
Maven builds/tests concurrently in the checkout. Runners may use the completed
jar while Java tests execute.

```bash
mvn dependency:get -Dartifact=org.ow2.asm:asm:9.9.1
mvn -pl benchmarks -am package -P build-rust-library,qdbr-release -DskipTests -Dmaven.test.skip=true
bash benchmarks/parallel-hash-join-group-by-allocation.sh
bash benchmarks/parallel-hash-join-group-by-heap.sh docs/parallel-hash-join-group-by-allocation/heap
```

`validate.py` rejects missing cases, thread stages or participating workers,
result differences, missing positive/normalized plans, and any unexplained
successful-execution bytes. The small direct build regression additionally
measures zero heap bytes for two fresh 65,536-symbol builds with forced growth,
after warming only 512-row builds. Other new tests check expired handles after
probe rebinding, independent slot generations, cloned SYMBOL reuse, source view
independence, idempotent close and uncached lookup.

The final Java run passed **1,985 tests across 55 suites**, with two existing
conditional skips and zero failures/errors (1,987 total). The benchmark package
passed. The retained-heap rerun passed all **864 snapshots and 288 candidate
executions against 144 ordinary references**. Every closed query tracker was
zero; maximum fixed-case heap growth was **1,256 bytes**, with a largest array of
**16,400 bytes**. The default-JVM smoke passed all **43 ordered result checks**,
including 40 measured comparisons.

The exact [per-suite totals](parallel-hash-join-group-by-allocation/regressions.csv)
and [smoke output](parallel-hash-join-group-by-allocation/smoke.log.gz) are retained.
Reproduce the Java run with:

```bash
allocation_test_suites=$(python3 - <<'SUITES'
import csv
with open('docs/parallel-hash-join-group-by-allocation/regressions.csv') as source:
    print(','.join(row['suite'].rsplit('.', 1)[-1] for row in csv.DictReader(source)))
SUITES
)
mvn -pl core test -P build-rust-library,qdbr-release -Dtest="$allocation_test_suites"
```

The smoke command is the 100,000-row/four-worker invocation in the
[heap guide](parallel-hash-join-group-by-heap.md), with `--revision=task9c-working-tree`.
Rust source is unchanged; its task 9a tracked decoder library remains required.
