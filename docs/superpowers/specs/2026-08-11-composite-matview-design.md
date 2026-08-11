# Composite Partitioning — Materialized Views (Sub-project 7) — Design

**Status:** drafted 2026-08-11, awaiting review. Sub-project 7 of 8.

## 1. Scope

Materialized views whose **base table** is composite-partitioned.

This sub-project is unlike the other six: there is no gate to remove, because there was never a gate.
`cairo/mv/` contains no composite awareness and no composite mat-view test exists, so the
combination has simply never been exercised. Sub-project 8 lands a temporary loud gate so the surface
is safe while this work is pending; this sub-project's outcome is to remove it.

## 2. Position: verify and support

**Decision: support it, and prove it — do not gate it permanently.**

The evidence says the combination is probably already close to working:

- Refresh reads the base table through ordinary SQL, and the composite read path is twin-correct
  across every shape (that is the premise sub-project 8's fuzz asserts).
- Incremental refresh is driven by WAL transaction ranges and time intervals, not by partition
  identity, so cell routing is invisible to it.
- The only composite-sensitive code found in `MatViewRefreshJob` is **estimation**:
  `estimateBucketsForRows` uses `baseTableReader.getPartitionCount()` and
  `approxPartitionDuration(getPartitionedBy())` to size refresh work.

That last point is the actual defect, and it is a performance bug rather than a correctness one: on a
composite table `getPartitionCount()` returns the number of `(day, cell)` records, not days, so a
table with 50 cells per day looks like it has 50× more partitions than it does. The estimator then
mis-sizes refresh batches — potentially by orders of magnitude.

**D1 — Estimation reasons in days, not partition records.** Where the refresh job needs "how many
partitions of duration D does this table have", it must use the count of distinct time partitions.
For a plain table that is unchanged; for composite it is the distinct-day count.

**D2 — Nothing else in the refresh path becomes cell-aware.** If the differential tests show refresh
results equal to a plain twin, the read path is already doing the work, and adding cell logic to the
mat-view layer would be inventing coupling that is not needed.

**D3 — A materialized view is never itself composite.** A view's storage table is created by the
mat-view machinery from the view definition, which has no partition-spec grammar. This mirrors the
measured behaviour of live views, where a composite base produced `dimensionCount=1` while the view's
own table reported `0`. If per-cell mat-view storage is ever wanted, it is a separate feature.

## 3. What must be proven, not assumed

The whole sub-project is an experiment with a fix attached. The claims to test:

1. Full refresh over a composite base produces the same view contents as over the plain twin.
2. Incremental refresh does too, after arbitrary interleaved multi-cell commits.
3. Refresh is correct when the base's cells change shape mid-life — a new cell appearing (new
   dimension value routed for the first time), and an existing cell extended out of order.
4. Refresh interacts correctly with sub-project 1: after `DROP PARTITION` removes some cells of a
   day, the view refreshes to match the plain twin that had the equivalent rows removed.
5. Estimation (D1) produces sane batch sizes — asserted via a counter, not by timing.

Claims 3 and 4 are where a latent bug is most plausible, because they are where the base's partition
set changes underneath a view that tracks it by time range.

## 4. Implementation surfaces

| File | Change |
|---|---|
| `cairo/mv/MatViewRefreshJob.java` `estimateBucketsForRows` (~`:326`) | count distinct time partitions, not `(day, cell)` records (D1) |
| `cairo/mv/MatViewRefreshJob.java` other `getPartitionCount()` uses | audit each for the same day-vs-record confusion |
| `griffin/SqlCompilerImpl.java` base-table validation (~`:4576–4588`) | **remove** the temporary composite gate landed by sub-project 8 |
| `cairo/TableReader.java` | may need a distinct-day count accessor if none exists |

## 5. Testing

- **Differential twin for refresh**, full and incremental, via the sub-project 8 harness extended
  with a mat view over both subject and reference.
- **New-cell-appears** and **out-of-order extend** cases (claims 3), asserting view equality after
  each commit rather than only at the end.
- **Refresh after partition removal** (claim 4), which couples this sub-project to sub-project 1 and
  should run once that lands.
- **Estimation sanity** (claim 5): a composite table with many cells per day must produce the same
  batch sizing as its plain twin with the same number of days — asserted on a counter.
- **The gate removal is itself tested**: `CompositeMatViewGateTest` from sub-project 8 inverts from
  "rejected" to "accepted and correct".
- **Plain byte-identity**: mat views over plain bases behave identically to before.

## 6. Risk

The honest risk statement is that this sub-project's scope is **unknown until the tests exist**. If
claims 1–4 pass with only D1 changed, it is a day's work. If incremental refresh turns out to track
base partitions by identity anywhere, it could be as large as sub-project 1. The first task is
therefore the differential test, not the fix — the test tells us which of those two projects we are
in.

That is also why sub-project 8 lands the gate first: it makes "unknown" safe rather than silent.

## 7. Out of scope

- Symbol-aware incremental refresh that recomputes only touched cells — the Phase 3 optimisation from
  the original design (`2026-07-15-…-design.md` §3). It is a performance feature that presupposes
  correctness, so it belongs after this.
- Composite-partitioned mat-view *storage* (D3).
- Live views over composite bases, which are already correct by construction — the view's own storage
  table is plain, as measured during the merge audit.
