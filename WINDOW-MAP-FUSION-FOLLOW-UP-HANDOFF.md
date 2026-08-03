# In-memory window Map fusion follow-up

Date: 2026-08-03

Status: design and implementation handoff. No production code described below
has been written unless it is explicitly listed under "What has already
landed".

Source baseline: `17a7e4ed49` on `puzpuzpuz_live_view`. The disk-format handoff
at
`/home/puzpuzpuz/projects/hdfc-debug/LIVE-VIEW-FUSED-STATE-DISK-FORMAT-HANDOFF.md`
records the implementation through `738c66a209`; the baseline here also
includes the subsequent partition-key-count sharing commit.

## Executive conclusion

The proposed optimization is **partially, and for the original live-view
target almost completely, implemented already**.

For a compatible anchored live-view window, `LiveViewWindow` now owns one Map
whose value contains the anchor bookkeeping and all admitted accumulator
components. It loads the key once, resets and updates every component against
that one `MapValue`, then materializes all output projections. The participating
functions keep their private Maps closed and their ordinary hot-path methods are
no-ops. `sum(x)`, `avg(x)` and `count(x)` already use one
`(sum, nonNullCount)` component when their contribution semantics match.

The latest commit extends the same model to:

```text
count(*) / row_number() / count(partition_key)
```

where the last projection reads the row-count component with a NULL-key guard.

What is not implemented is a runtime-neutral facility for:

1. ordinary SQL window execution, where every partitioned function still
   constructs and probes a Map of its own;
2. live-view residual functions which do not join
   `LiveViewWindowStatePlan`, including bounded/ring-backed families.

The recommended follow-up is therefore not another special case in
`LiveViewWindow`. It is a generic **window Map-state group**:

```text
one exact window specification
└── one partition Map
    ├── component A slots
    ├── component B slots
    └── ...
```

Each stateful component registers its value-column types once, in the same
spirit as `GroupByFunction.initValueTypes`. Each output function receives a
binding to the component and field slots it reads. A derived/read-only output
registers no value columns and contributes no update.

The first production slice should be deliberately narrow:

- non-live-view `WindowRecordCursorFactory`, the streaming path;
- partitioned `UNBOUNDED PRECEDING ... CURRENT ROW` functions;
- direct-column `PARTITION BY` and accumulator arguments;
- the accumulator families already proved by the live-view work:
  DOUBLE sum/avg/count, row count/row number, and Welford dispersion;
- no disk-format change and no change to cached/two-pass execution yet.

That slice gives the intended win on ordinary cumulative window queries while
reusing implementations and semantic proofs that already have end-to-end
coverage. Cached windows and ring-backed state follow once their distinct
lifecycle issues are handled explicitly.

## What has already landed

### One live-view Map

Commit `35395924b7` moved compatible anchored state into
`LiveViewWindow`:

- `LiveViewWindow.fusedMapValueTypes` builds one value schema from the compiled
  component plan;
- `LiveViewWindow.processRow` performs the one key lookup;
- `resetWindowStateComponents` initializes or resets all component slices;
- `updateWindowState` first invokes one contributor per component and only
  afterwards invokes every projection;
- `BasePartitionedWindowFunction.reopen` leaves a bound function's private Map
  closed;
- `computeNext`, `resetPartition`, `markPartitionAlive`,
  `retainPartitions`, checkpoint freeze/restore and repair capture all avoid
  per-function state for bound functions.

For the original live-view target:

```sql
sum(amt_txn) OVER w,
count(cod_acct_no) OVER w
```

the arguments differ, so the states cannot share a counter. They nevertheless
already occupy one Map entry:

```text
key = cod_acct_no
value:
  anchor/bookkeeping
  sum(amt_txn).sum
  sum(amt_txn).nonNullCount
  count(cod_acct_no)
```

There is no remaining "fuse these two live-view Maps" optimization for that
shape.

### Read-only projections

The current interfaces already contain most of the needed runtime protocol:

- `WindowFunction.accumulateWindowState`
- `WindowFunction.projectWindowState`
- `WindowFunction.bindWindowStateSlots`
- `WindowFunction.isWindowStateOwned`

The current component/projection model already proves:

- identical `sum(x)` and `avg(x)` accumulator state;
- `count(x)` as a contained counter inside DOUBLE sum/avg;
- `count(x)` as a contained counter inside a Welford accumulator;
- `count(*)` and partitioned `row_number()` as one row counter;
- `count(k)` over its own SYMBOL/VARCHAR partition key as
  `k IS NULL ? 0 : rowCount`, but only when an unguarded row-count host exists.

A read-only projection is not completely inert: it still has to materialize its
current output into the function's scalar result field. What it does not do is
own a Map, probe a key, evaluate the accumulator argument, or mutate state.

### The scope is live-view-only

The generic cursor does not consume this plan:

- `SqlCodeGenerator.generateSelectWindow` calls
  `LiveViewCheckpointFunctionCompiler.windowStatePlan` only when
  `executionContext.isLiveViewCompile()` is true;
- the plan is adopted by `LiveViewWindow`, outside the ordinary window cursor;
- `WindowRecordCursorFactory.WindowRecordCursor.hasNext` still calls
  `computeNext(record)` independently on every window function;
- sum, avg and count factories still call
  `MapFactory.createUnorderedMap` independently while each function is parsed.

Consequently, an ordinary streaming query such as:

```sql
SELECT
  sum(x)   OVER w,
  avg(x)   OVER w,
  count(x) OVER w
FROM t
WINDOW w AS (
  PARTITION BY k
  ORDER BY ts
  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
);
```

still has three Maps, three key probes per row, three copies of the partition
key, two copies of `(sum, count)`, one separate counter, and repeated evaluation
of `x`.

## Two independent optimizations

The implementation should keep physical co-location and logical accumulator
sharing separate.

### Physical co-location

Functions over one exact window can use one key domain even when their
mathematics differ:

```sql
sum(x) OVER w, count(y) OVER w
```

The value contains two components, but the partition key and hash-table
structure exist once and the row performs one lookup.

This optimization requires:

- an exact window-group identity;
- a combined Map value schema;
- one driver-owned key projection and Map;
- a per-component slot base;
- explicit initialization of every component on a new entry.

It does not require two functions to share an accumulator.

### Logical sharing

Functions may also read the same component when identity or proved containment
allows it:

```text
sum(x) + avg(x) + count(x)
    -> one DOUBLE_SUM_COUNT component
    -> [sum: DOUBLE, nonNullCount: LONG]
```

This removes value slots and updates in addition to Maps and probes.

The sharing decision must remain a compiled plan decision. It must never be
inferred from equal SQL text, equal widths, or a convenient field position.

## Proposed runtime model

### Runtime-neutral accumulator classes

The accumulator mathematics currently live in classes named for live-view
durability:

- `LiveViewAccumulatorDescriptor`
- `LiveViewAccumulatorProjection`
- the component-building part of `LiveViewWindowStatePlan.Builder`
- direct-column and contribution-predicate logic in
  `LiveViewCheckpointFunctionCompiler`

Those concepts are no longer live-view-specific. Extract their runtime part to
the window package:

```text
WindowAccumulatorDescriptor
WindowAccumulatorProjection
WindowAccumulatorPlan
WindowAccumulatorPlanBuilder
```

`WindowAccumulatorDescriptor` owns only runtime facts:

- family;
- argument key;
- contribution predicate;
- runtime slot types and field-to-slot mapping;
- identity/reset logic;
- proved containment relationships.

The live-view layer retains the durable facts:

- component codec version;
- byte encoding and state length;
- manifest ordering and persisted offsets;
- the 256-byte inline-leaf budget;
- freeze/restore codecs.

The live-view descriptor may wrap the runtime descriptor, or the generic
descriptor may carry a durable adapter. Do not fork two independent family,
predicate or containment tables: they will drift.

The existing persisted encoding must stay byte-for-byte unchanged during this
extraction. Add a golden manifest/descriptor encoding test before moving the
classes.

### Value-type registration

The useful `GroupByFunction` precedent is:

1. compilation appends the function's value types to one shared schema;
2. the function remembers the first slot;
3. runtime accesses fields relative to that slot.

Window state should use the same shape at the **component** level:

```java
int appendValueTypes(ArrayColumnTypes valueTypes);
void resetState(MapValue value, int slotBase);
```

The plan calls `appendValueTypes` once per surviving component and records the
returned base. Projections receive a binding:

```java
final class WindowAccumulatorBinding {
    WindowAccumulatorDescriptor component;
    int componentIndex;
    int componentSlotBase;
    int projectionKind;
    int functionStateSlotBase; // host base + contained-slice offset
}
```

This differs intentionally from blindly calling `initValueTypes` on every
SELECT-list function. If `count(x)` reads the counter in `sum(x)`, calling it
would append a redundant LONG. The contributor/component registers the state;
the read-only projection is initialized from the contributor's index, like a
shared GroupBy reader initialized from its primary.

Use Map **column indexes**, not durable byte offsets, on the runtime API.

### Window Map group

Add an owner in `io.questdb.griffin.engine.window`, tentatively:

```text
WindowMapState
```

It owns:

- one `Map`;
- the key `VirtualRecord` and `RecordSink` it uses;
- copied key column types;
- one `WindowAccumulatorPlan`;
- contributors and projections in deterministic order;
- lifecycle state and test-only structural counters.

The streaming row loop is:

```java
void computeNext(Record record) {
    keyRecord.of(record);
    MapKey key = map.withKey();
    key.put(keyRecord, keySink);
    MapValue value = key.createValue();

    if (value.isNew()) {
        plan.resetAllComponents(value);
    }

    plan.accumulateAll(record, value);
    plan.projectAll(record, value);
}
```

The reset is mandatory. QuestDB Maps do not promise that a new value is
zero-filled; cleared or reallocated backing may contain stale bytes. Every
component must initialize its whole slice before the first read.

`accumulateAll` and `projectAll` must remain separate loops. Otherwise output
values depend on SELECT-list order when one projection reads state another
projection's contributor has not updated yet.

### Function protocol

Rename or generalize the existing live-view-shaped methods:

```java
@Nullable Function windowAccumulatorArgument();
int windowAccumulatorFamily();
int windowAccumulatorProjection();

void bindWindowAccumulator(@Nullable WindowAccumulatorBinding binding);
void accumulateWindowState(Record record, MapValue value);
void projectWindowState(Record record, MapValue value);
boolean isWindowMapStateBound();
```

The old checkpoint-named accessors can delegate during the migration, but there
should be one final source of truth.

A bound function:

- keeps ownership of its argument and its compiled partition expressions;
- owns no live Map backing;
- does not mutate state from `computeNext`;
- materializes its scalar output only from `projectWindowState`;
- still reports its real dependency to live-view repair;
- is **not** checkpoint-stateless.

The current sum/avg/count/Welford/row-number implementations already satisfy
most of this protocol.

### Private Map handling

Factories currently allocate a Map before the compiler has seen the other
window functions. Rewriting every factory before proving the execution design
would make the first patch unnecessarily broad.

For the first slice:

1. let eligible functions construct their current lazy/closed private Map;
2. compile and bind the shared group after all functions are known;
3. leave bound private Maps closed for the factory lifetime;
4. let the existing function `close()` free the dormant Map object.

This preserves ownership and cleanup while avoiding its native backing at
runtime. Once the group design is stable, factories can return schema-only
candidates and stop constructing the unused Map object.

Do not set `map = null` in the first implementation. Live-view validation and
checkpoint adapters currently use `getPartitionMap()` nullness and openness for
shape/lifecycle checks.

## Proving that functions use the same window

The cached compiler's existing `groupedWindow` key is only the ORDER BY index
list. It is a sort-sharing group, not a state-sharing proof. It may contain
different partition keys, frames or exclusions and must not be reused as the
Map-group identity.

Compile an immutable `WindowMapSpec` from the normalized window context while
the function is parsed. Initially it should include:

- direct-column partition indexes, in order;
- the actual Map key column types;
- effective order indexes and directions;
- whether the order was dismissed against the base cursor;
- normalized framing mode;
- normalized low and high bounds after timestamp-unit conversion;
- exclusion after its effective high-bound adjustment;
- pass kind and pass-1 scan direction;
- designated timestamp index/type where relevant.

Function-specific `IGNORE NULLS` belongs in the accumulator/projection
semantics, not in the physical key group, unless an implementation proves it
changes the group traversal.

For the first slice, decline a group if any PARTITION BY term is not a direct
compiled column of the metadata's own type. This is conservative and covers the
important case. A later canonical, type-resolved expression fingerprint can
admit expression keys.

Two separately written or separately named windows may share when their
normalized `WindowMapSpec`s are equal. Names and SQL rendering are not semantic
identity.

## Compiling components and read-only projections

For every exact `WindowMapSpec` bucket:

1. collect functions declaring an accumulator family and projection;
2. resolve a direct-column argument key where the family takes an argument;
3. include the contribution predicate and argument type in component identity;
4. merge identical components;
5. fold a component only through a pinned containment relation;
6. choose one non-derived, non-guarded contributor deterministically;
7. sort components by identity and assign slot bases;
8. bind every projection to its final component fields;
9. create a runtime group only when at least two function-owned Maps or updates
   are eliminated.

The final activation rule prevents a single function from being moved through a
new abstraction for no benefit. A one-function plan remains useful in unit tests
and for live-view composition, but ordinary runtime binding should require a
real structural reduction.

Contributor choice should preserve the current rules:

- a derived projection cannot contribute because it owns a narrower standalone
  image;
- a guarded partition-key count cannot contribute because its standalone
  counter is not a row count on the NULL-key partition;
- among otherwise equal candidates, lowest output position wins;
- contributor choice is not persisted.

### Example: identical argument

```text
sum(x), avg(x), count(x) over w

components:
  DOUBLE_SUM_COUNT(x, FINITE_DOUBLE)

value slots:
  0 DOUBLE sum
  1 LONG   nonNullCount

contributor:
  one of sum/avg, chosen deterministically

projections:
  sum   -> slot 0, NULL when slot 1 == 0
  avg   -> slot 0 / slot 1, NULL when slot 1 == 0
  count -> slot 1
```

The ordinary runtime goes from three Maps and five value slots to one Map and
two slots. It evaluates `x` and updates the accumulator once.

### Example: different arguments

```text
sum(x), count(y) over w

components:
  DOUBLE_SUM_COUNT(x, FINITE_DOUBLE)
  NON_NULL_COUNT(y, predicate-for-y)

value slots:
  sum, xCount, yCount
```

The counters do not merge. The key and Map lookup still do.

## Streaming cursor integration

Extend `WindowRecordCursorFactory` with:

```text
ObjList<WindowMapState> windowMapStates
ObjList<WindowFunction> residualWindowFunctions
```

On each row:

1. process every Map group once;
2. invoke existing `computeNext` dispatch;
3. bound functions no-op in step 2; residuals behave unchanged.

Keeping the existing second dispatch in the first implementation minimizes
surface area and mirrors the landed live-view sequence. It can later be narrowed
to residuals after tests prove no caller depends on it.

The cursor factory owns group Maps. Lifecycle must be symmetric:

- first `of`: bind the query `MemoryTracker`, then reopen groups;
- `toTop`: clear group Maps as well as resetting functions;
- ordinary close/reset: free group backing and reset scalar projection fields;
- reopen after close: reopen group Maps under the tracker;
- failed partial open: free every opened group and preserve the existing
  transactional retry behavior;
- factory close: free groups before or independently of functions without
  taking ownership of function arguments/key expressions twice.

The first implementation must be disabled for live-view compiles. A compatible
live-view function is already owned by `LiveViewWindow`; binding it to a second
generic owner would create two sources of truth. Unifying those owners is a
later refactor, not an implicit side effect.

## Cached-window follow-up

`CachedWindowRecordCursorFactory` and
`CachedWindowLightRecordCursorFactory` need a separate implementation step.
Both call `pass1` and optionally `pass2` function by function.

Two cases differ:

### Cumulative functions executed after a sort

ZERO/ONE-pass cumulative functions can use the same group sequence as the
streaming cursor:

1. lookup once;
2. initialize if new;
3. accumulate all;
4. project all;
5. write every projected scalar to the row's `WindowSPI` output address.

### Whole-partition two-pass functions

Current `AvgOverPartitionFunction.preparePass2` destructively replaces the sum
slot with the average. That is incompatible with a shared component because a
`sum` projection still needs the sum.

The shared implementation must instead:

- pass 1: accumulate immutable `(sum, count)` state;
- prepare pass 2: no destructive finalization;
- pass 2: lookup the group once and have sum/avg/count projections compute and
  write their own result from the same raw component.

The cached compiler must make subgroups by exact `WindowMapSpec` inside its
ORDER BY sort groups. Sharing a sort does not imply sharing a Map.

Implement the full and light factories together or keep the feature disabled
for both. They have intentionally parallel pass loops; supporting only one
would make the chosen physical plan change whether state fusion occurs.

## Live-view integration after the generic path

The current live-view runtime is the reference implementation and should remain
green throughout.

Once the generic plan is stable:

1. make `LiveViewWindowStatePlan` compose a generic accumulator plan;
2. add the anchor/bookkeeping slot prefix when it builds the Map schema;
3. keep durable offsets, manifest bytes and inline-budget truncation in the
   live-view wrapper;
4. retain adoption/decline state migration, which ordinary query factories do
   not need because they bind before processing any row;
5. retain the legacy checkpoint-root adapter.

Do not apply the live-view 256-byte leaf budget to an ordinary runtime Map. It
is a B-tree leaf-format limit, not a Map-state limit.

### Residual live-view functions

Do not put residual checkpoint functions into the generic Map in the first
follow-up. The checkpoint writer currently obtains a function's full key domain
and state through `WindowFunction.getPartitionMap()` and decoders assume the
function's state starts at its private slot zero. Moving a residual slice into a
shared Map would require:

- checkpoint iteration over a group-owned key domain;
- per-function slot bases for freeze/restore;
- dirty/removal tracking at the group level;
- frontier compaction of the group;
- native arena ownership for ring-backed functions.

That work can still retain separate on-disk function roots; runtime Map fusion
does not force a disk-format change. It is nevertheless a distinct lifecycle
change and should follow the ordinary scalar implementation.

## Concrete implementation plan

Each step should leave the tree green and have its own PR/commit section.

### Step 1: extract the runtime accumulator contract

Files centered on:

- `WindowFunction`
- `LiveViewAccumulatorDescriptor`
- `LiveViewAccumulatorProjection`
- `LiveViewWindowStatePlan`
- `LiveViewCheckpointFunctionCompiler`
- the five already participating window implementations

Changes:

1. introduce runtime-neutral accumulator descriptor/projection/binding classes;
2. move family, contribution, slot and containment logic to them;
3. keep codec/manifest logic in `io.questdb.cairo.lv`;
4. rename the checkpoint-prefixed function declarations to runtime-neutral
   names, with temporary delegating methods if needed;
5. make slot bases relative to the accumulator plan; let a runtime owner supply
   its prefix (zero ordinarily, anchor slots for live views);
6. add golden tests proving persisted live-view identities and manifests are
   unchanged.

No runtime behavior changes in this step.

### Step 2: compile exact runtime groups in shadow mode

Files centered on:

- `SqlCodeGenerator.generateSelectWindow`
- `WindowContextImpl`
- new `WindowMapSpec`
- new `WindowAccumulatorPlanBuilder`

Changes:

1. snapshot normalized window semantics while each function's context is live;
2. group only direct-column partition specs;
3. build components, containment and contributor/projection bindings;
4. expose test-only plan structure from the factory;
5. do not allocate or bind a shared Map yet;
6. compare live-view and generic accumulator plans for the already-supported
   anchored shapes in tests.

This step should prove that SELECT-list reordering, named versus inline windows,
and unrelated windows produce deterministic groups.

### Step 3: one Map for streaming physical co-location

Files centered on:

- new `WindowMapState`
- `WindowRecordCursorFactory`
- `BasePartitionedWindowFunction`
- `RowNumberFunctionFactory.RowNumberFunction`

Changes:

1. construct combined value types from every component in a group;
2. allocate one lazy Map with the group's key types;
3. bind the query `MemoryTracker` before reopen;
4. initialize every component slice on a new key;
5. update each component against the loaded `MapValue`;
6. project every output only after all updates;
7. keep bound private Maps closed;
8. add group reset/reopen/toTop/close and failed-open cleanup;
9. exclude live-view compiles;
10. initially keep one separate slice per function, even for equal
    accumulators, so this commit proves physical co-location independently.

After this step, `sum(x) + count(y)` over one window has one Map and one lookup
while retaining two independent states.

### Step 4: no-op/read-only projections

Enable the already-proved merge and containment relations:

- sum + avg over the same argument;
- count of that argument reading sum/avg's counter;
- the four Welford projections sharing one component;
- count of the same argument reading Welford's counter;
- count(*) + row_number();
- guarded count(partition key), only under the current host/type rules.

Add a direct assertion that:

```text
sum(x) + avg(x) + count(x)
```

has one Map, one component, two slots, one lookup and one argument evaluation
per row.

### Step 5: make the live-view plan consume the generic plan

Changes:

1. remove duplicate runtime family/slot/fold logic from the live-view builder;
2. retain its manifest, byte codec, anchor prefix and leaf-budget truncation;
3. retain reversible state adoption and legacy restore;
4. run the full live-view checkpoint/lifecycle suite;
5. prove the HDFC and sum/avg/count persisted manifests are byte-identical to
   step 1's golden values.

No new disk format and no forced checkpoint conversion should result.

### Step 6: cached and cached-light execution

Changes:

1. build exact-spec Map subgroups inside sort groups;
2. group cumulative pass-1 updates and output writes;
3. replace destructive whole-partition finalization with projection-time
   arithmetic;
4. group pass-2 lookups and writes;
5. keep full and light cursor behavior aligned;
6. cover forward and backward pass-1 directions.

### Step 7: widen physical co-location

Add families one state shape at a time:

- min/max and other fixed scalar cumulative state;
- DECIMAL state with native slot types;
- bounded ROWS state;
- bounded RANGE state;
- first/last/nth and ranking families;
- expression partition keys once a canonical compiled expression identity
  exists.

Ring-backed families may co-locate their Map slots while retaining separate
arenas. Sharing the ring itself is a stronger optimization and needs an
explicit owner/refcount/reset design.

### Step 8: live-view residual groups

Only after generic scalar and ring lifecycle is stable:

1. make checkpoint freeze/restore address a group Map plus a function slice;
2. move dirty/removal tracking to the group;
3. compact the shared key domain once;
4. keep residual on-disk roots unless a separate disk-format measurement
   justifies changing them;
5. migrate state reversibly when a recompile changes group eligibility.

## Correctness tests

### Plan identity

Add focused tests for:

- same named window joining one group;
- equivalent inline and named resolved specifications joining;
- different partition column/order/frame/exclusion not joining;
- different pass direction not joining;
- expression partition key declining in the first release;
- SELECT-list reorder producing the same component order;
- a component never being left with a derived or guarded contributor;
- unsupported functions remaining residual.

### Runtime values

For `sum(x)`, `avg(x)`, `count(x)`:

- ordinary finite values;
- interspersed NULL/NaN values;
- all-null partition;
- positive and negative infinity;
- repeated and new partition keys;
- NULL partition key;
- Map resize and key rehash;
- `toTop`, cursor close/reopen and a second cursor execution.

Negative controls:

- `sum(x)` with `count(y)` shares a Map but not a counter;
- different windows share neither;
- `count(*)` never aliases `count(x)`;
- `count(k)` guard on the NULL-key partition;
- an expression argument stays residual until expression identity exists.

For row-count and Welford families:

- count(*) and row_number stay equal;
- all four dispersion projections match independent execution;
- single-row sample/population semantics;
- all-null partition;
- count of the same argument reads Welford's counter;
- sum/avg do not read Welford's mean as a sum.

### Structural assertions

Expose package-private or `@TestOnly` figures:

```text
Map group count
component count
projection count
value slot count/types
lookup count
contributor invocation count
projection invocation count
Map implementation class
```

Do not infer lookup reduction only from elapsed time.

### Lifecycle and memory

Extend `WindowMemoryTrackerTest` and the window cursor tests for:

- allocation charged after tracker binding;
- close returns tracked Map memory;
- failed reopen cleans a partially opened group;
- retry after a memory-limit failure;
- `toTop` clears shared state exactly once;
- dormant private Maps never reopen;
- factory cleanup after a compile failure during group construction;
- no double-free of partition expressions or argument functions.

Run the existing live-view lifecycle/checkpoint suites unchanged after every
step touching the shared contracts.

### Cached paths

When step 6 lands, run every shape through:

- `CachedWindowRecordCursorFactory`;
- `CachedWindowLightRecordCursorFactory`;
- ordered and natural-order groups;
- zero/one-pass cumulative functions;
- two-pass whole-partition functions;
- random-access result reads;
- forward and backward pass directions.

The key regression case is `sum(x) + avg(x) + count(x)` over a whole partition:
avg must not overwrite the sum before sum projects.

## Benchmark and acceptance plan

Add a focused ordinary-window benchmark. Report:

```text
rows/s or ns/row
peak tracked native bytes
retained tracked native bytes
Map count and implementation
Map key lookup count
component update count
argument evaluation count
```

Run at least:

1. single sum control;
2. sum + avg + count over one argument;
3. sum(x) + count(y);
4. four dispersion projections + count(x);
5. count(*) + row_number() + guarded count(key);
6. two identical partition domains with different frames;
7. low-cardinality repeated keys;
8. near-unique keys;
9. INT, SYMBOL and STRING partition keys;
10. cached/full and cached/light variants once step 6 lands.

The value widening can change `MapFactory`'s selected implementation, exactly
as it did for the live-view benchmark. Always report the class; do not assume
one wider Map is faster than several narrow Maps.

Structural acceptance for streaming `sum(x) + avg(x) + count(x)`:

| metric | current | target |
|---|---:|---:|
| partition Maps | 3 | 1 |
| Map lookups per row | 3 | 1 |
| accumulator components | 3 | 1 |
| Map value slots | 5 | 2 |
| accumulator updates per row | 3 | 1 |
| argument evaluations per row | 3 | 1 |

Structural acceptance for `sum(x) + count(y)`:

| metric | current | target |
|---|---:|---:|
| partition Maps | 2 | 1 |
| Map lookups per row | 2 | 1 |
| accumulator components | 2 | 2 |
| Map value slots | 3 | 3 |

Performance acceptance:

- no regression beyond benchmark noise for a single-function control;
- lower peak native Map memory for multi-function groups;
- lower per-row time for repeated-key and high-cardinality multi-function
  groups;
- exact output equality across all cursor paths.

## Risks and mitigations

### Mistaking sort groups for state groups

Risk: functions with equal ORDER BY keys but different partition/frame semantics
share state.

Mitigation: compile `WindowMapSpec`; never use `groupedWindow` alone as the
sharing key.

### Reading uninitialized component bytes

Risk: only the first component sees `MapValue.isNew()` and later components
interpret stale backing as existing state.

Mitigation: the group initializes every component slice before any contributor
runs.

### SELECT-list-order dependency

Risk: avg/count projects before sum updates their shared component.

Mitigation: contributors loop first, projections loop second.

### Destructive cached finalization

Risk: avg replaces shared sum state with its output, corrupting sum and count.

Mitigation: keep raw accumulator state immutable across pass preparation and
perform final arithmetic per projection in pass 2.

### Two runtime owners in a live view

Risk: ordinary generic binding and `LiveViewWindow` both update the same logical
component.

Mitigation: exclude live-view compiles initially; later make the live-view owner
consume the generic plan rather than constructing a second group.

### Wider Map values selecting a slower implementation

Risk: co-location moves a key/value shape from `Unordered4Map` or
`Unordered8Map` to `OrderedMap`.

Mitigation: report Map class and retain a single-function control. The prior
live-view measurement found the lookup reduction outweighed this transition,
but ordinary windows need their own measurement.

### Conflating read-only with stateless

Risk: repair or lifecycle code assumes a projection has no history dependency.

Mitigation: read-only means "does not own/update state", not
`isCheckpointStateless`; keep the dependency descriptor.

### Auxiliary arena ownership

Risk: co-locating ring pointer slots causes double-free, leaks, or reset of one
function's arena through another.

Mitigation: keep ring families out initially; later make the Map group own only
the key/value Map while each component explicitly owns its arena lifecycle.

### Durable identity drift during extraction

Risk: moving the runtime descriptor changes live-view manifest bytes and forces
conversion or misreads a predecessor.

Mitigation: golden encoded-identity/manifest tests and byte-equal comparisons
before and after the refactor.

## Final expected shapes

Ordinary cumulative query over one argument:

```text
WindowMapState(w)
└── Map
    └── key = partition tuple
        value:
          DOUBLE sum
          LONG   nonNullCount
        contributor:
          sum or avg
        read-only projections:
          sum
          avg
          count
```

Ordinary cumulative query over different arguments:

```text
WindowMapState(w)
└── Map
    └── key = partition tuple
        value:
          DOUBLE sum(x)
          LONG   finiteCount(x)
          LONG   nonNullCount(y)
        contributors:
          DOUBLE_SUM_COUNT(x)
          NON_NULL_COUNT(y)
        projections:
          sum(x)
          count(y)
```

The existing live-view target remains:

```text
LiveViewWindow
└── one Map
    └── key = partition tuple
        value:
          anchor/bookkeeping
          runtime accumulator plan slots
```

That is the design to converge on: one exact window traversal, one key lookup,
one physical Map entry, one update per mathematical accumulator, and any number
of read-only output projections.
