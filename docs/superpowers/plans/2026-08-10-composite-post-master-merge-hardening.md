# Composite Partitioning — Post-Master-Merge Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close every integration gap left by merging `origin/master` (34 commits, 9.4.4-SNAPSHOT → 10.0.1-SNAPSHOT) into `feat/composite-partitioning`, bring Enterprise into line, and get the branch to a pushable, CI-green, PR-ready state.

**Architecture:** Three independent workstreams. **A** audits the merge surface for the three known hazard classes and locks each finding with a regression test. **B** brings Enterprise up to the new OSS core and audits its (small) composite-relevant surface. **C** is pre-PR hygiene and the first-ever CI run. A must finish before C2 (formatting must be the last code-touching step); B can run in parallel with A but its CI leg depends on C3.

**Tech Stack:** Java 25 (`/usr/lib/jvm/java-25-openjdk-amd64`), Maven (offline), JUnit 4, QuestDB core + questdb-ent, IntelliJ IDEA 2026.1.4 formatter (CI-pinned).

## Global Constraints

- Worktree: `/home/nick/claude/wt/oss/composite-partitioning`, branch `feat/composite-partitioning`.
- Pre-merge anchor tag: `pre-master-merge-2026-08-10` → `0de2fa4ef2`. Never delete it until the PR is merged.
- Merge base for all three-way analysis: `git merge-base 0de2fa4ef2 origin/master` = `cdb0ea073b`.
- Build/test env, every command: `export JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64` and `export QDB_TEST_TMPDIR=/dev/shm`. Use `mvn -o` (offline).
- **Never** `git checkout`/`git stash` in this worktree — it holds unpushed work (157+ commits). Use a separate worktree for any control run.
- A local QuestDB holds port **9003** (pid may change). `ExpParquetExportTest#testParquetExportReadOnlyHttp` and `#testParquetExportDisabledReadOnlyInstance` will ERROR with `could not bind socket`. That is environmental — **do not kill that process**; record those two as known-blocked.
- Invariants that must hold at every commit: plain (`dimCount == 0`) tables byte-identical in `_txn`/`_meta`; composite either behaves as the plain twin or fails LOUDLY; no silent write/read/DDL/maintenance path.
- Flag defaults stay as committed: `cairo.wal.composite.fastappend.enabled=true`, `cairo.wal.composite.fastappend.max.open.cells=64`.
- Formatter is pinned to **IDEA 2026.1.4** (`ci/templates/java-lint.yml`). Do not use a different version — a newer formatter reformats the whole repo.
- Do not spawn subagents/workflows unless the operator explicitly asks.

## The Three Hazard Classes

Every audit task below classifies master-introduced code against these. They are the lesson of this merge: two real defects were found, one per class (b) and (c).

- **(a) Stride arithmetic** — any index computation over the attached-partition array using the hardcoded `LONGS_PER_TX_ATTACHED_PARTITION` (4) instead of the dynamic `longsPerAttachedPartition` / `getLongsPerAttachedPartition()`. Composite persists stride 8. *Status: swept and believed complete; Task A2 re-verifies mechanically.*
- **(b) One-partition-per-day assumptions** — resolving a partition by timestamp alone (cellKey-blind), looping `getPartitionCount()` assuming one dir per day, or building a partition path with the 5-arg `setPathForNativePartition` (no cell segment). Found: master's `tryFastAppendInOrderBlock` (fixed, `0d04c2cd3c`).
- **(c) Capability flags upstream cannot know about** — this branch added `supportsConcurrentTimeFrameCursor()` and `supportsPageFrameCursorForUnorderedAggregation()` to `RecordCursorFactory`. `supportsConcurrentTimeFrameCursor()` **defaults to `supportsTimeFrameCursor()`**, so any wrapper factory that delegates the latter but not the former lies about its capability. Found: `LiveViewRecordCursorFactory` (fixed, `d5564f6d7b`).

## Audit Surface (measured, not estimated)

| Bucket | Count | Where |
|---|---|---|
| Files changed by **both** sides | 41 | `core/src`, list regenerated in Task A1 |
| Files **added by master** | 160 | of which **4** carry partition/frame hazard patterns |
| Enterprise hazard sites | 4 | all `setPathForNativePartition`; zero `_txn`/stride/frame-capability usage |

Tier assignment for the 41 both-sides files (by composite-symbol density × churn):

- **Tier 1 (deep read, 9):** `TableWriter`, `TableReader`, `TxWriter`, `TxReader`, `SqlCodeGenerator`, `TableSnapshotRestore`, `TableUtils`, `O3PartitionJob`, `O3PartitionPurgeJob`
- **Tier 2 (targeted pattern audit, 15):** `AbstractIntervalPartitionFrameCursor`, `IntervalFwd/BwdPartitionFrameCursor`, `IntervalPartitionFrameCursorFactory`, `FullFwd/BwdPartitionFrameCursor`, `FullPartitionFrameCursorFactory`, `PartitionFrameCursorFactory`, `RecordCursorFactory`, `ShowPartitionsRecordCursorFactory`, `ShowCreateTableRecordCursorFactory`, `O3PartitionTask`, `TableReaderMetadata`, `TableWriterMetadata`, `TableStructure`
- **Tier 3 (confirm benign, 17):** `PropServerConfiguration`, `PropertyKey`, `CairoConfiguration`, `CairoConfigurationWrapper`, `DefaultCairoConfiguration`, `SqlParser`, `SqlKeywords`, `SqlCompiler`, `SqlCompilerImpl`, `SqlCompilerPool`, `UpdateOperatorImpl`, `QueryProgress`, `SelectedRecordCursorFactory`, `ExtraNullColumnCursorFactory`, `LatestByRecordCursorFactory`, `LatestByLightRecordCursorFactory`, `ShowCreateTableTest`, plus any remainder the Task A1 script prints

---

## Workstream A — Merge Integration Audit

### Task A1: Reproducible hazard detector + baseline report

**Files:**
- Create: `.superpowers/sdd/merge-audit/detect.sh` (gitignored scratch — the method itself is recorded in this plan so nothing is lost)
- Create: `.superpowers/sdd/merge-audit/baseline-report.txt`

**Interfaces:**
- Produces: `detect.sh` prints one line per hit as `CLASS|file|line|text`. Tasks A3–A6 consume this report as their worklist.

- [ ] **Step 1: Write the detector script**

```bash
mkdir -p .superpowers/sdd/merge-audit
cat > .superpowers/sdd/merge-audit/detect.sh <<'SH'
#!/usr/bin/env bash
# Composite merge hazard detector. Run from the worktree root.
# Usage: ./.superpowers/sdd/merge-audit/detect.sh <baseRef> <theirRef>
set -euo pipefail
BASE="${1:?base ref}"
THEIRS="${2:?their ref}"

echo "### CLASS A: hardcoded stride arithmetic (must be dynamic for composite)"
grep -rn "LONGS_PER_TX_ATTACHED_PARTITION" core/src/main --include=*.java \
  | grep -vE "TableUtils\.java|/mig/" \
  | grep -vE "LONGS_PER_TX_ATTACHED_PARTITION_(COMPOSITE|MSB|COMPOSITE_MSB)" \
  | grep -vE "getLongsPerAttachedPartition\(\) >" \
  | sed 's/^/A|/' || true

echo "### CLASS B: one-partition-per-day assumptions in code master touched or added"
for f in $(git diff --name-only "$BASE" "$THEIRS" -- core/src/main); do
  [ -f "$f" ] || continue
  grep -nE "findAttachedPartitionRawIndexByLoTimestamp\(|getPartitionIndexByTimestamp\(|setPathForNativePartition\([^)]*\)|partitionRemoveCandidates\.add\([^,]*,[^,)]*\)|safeDeletePartitionDir\([^,]*,[^,)]*\)" "$f" \
    | sed "s|^|B|$f:|" || true
done

echo "### CLASS C: wrappers delegating supportsTimeFrameCursor but not our added flags"
for f in $(grep -rl "supportsTimeFrameCursor\|supportsPageFrameCursor" core/src/main --include=*.java); do
  if grep -q "public boolean supportsTimeFrameCursor" "$f" && ! grep -q "supportsConcurrentTimeFrameCursor" "$f"; then
    echo "C|$f|MISSING supportsConcurrentTimeFrameCursor delegation"
  fi
  if grep -q "public boolean supportsPageFrameCursor" "$f" && grep -q "return base.supportsPageFrameCursor" "$f" \
     && ! grep -q "supportsPageFrameCursorForUnorderedAggregation" "$f"; then
    echo "C|$f|delegates supportsPageFrameCursor; CHECK supportsPageFrameCursorForUnorderedAggregation"
  fi
done
SH
chmod +x .superpowers/sdd/merge-audit/detect.sh
```

- [ ] **Step 2: Run it and capture the baseline**

```bash
export JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64
B=$(git merge-base 0de2fa4ef2 origin/master)
./.superpowers/sdd/merge-audit/detect.sh "$B" origin/master \
  | tee .superpowers/sdd/merge-audit/baseline-report.txt
```

Expected: CLASS A section empty (the sweep is complete). CLASS B prints a worklist of by-timestamp/day-path call sites. CLASS C prints `PageFrameRecordCursorFactory` (benign — it computes the flag for the plain scan path, it does not delegate) and nothing else, because `LiveViewRecordCursorFactory` was fixed in `d5564f6d7b`.

- [ ] **Step 3: Triage every CLASS B line into the findings table**

Append to `.superpowers/sdd/merge-audit/baseline-report.txt`, one row per CLASS B hit, using exactly this format:

```
FILE:LINE | SITE | REACHABLE-FOR-COMPOSITE? (yes/no + why) | VERDICT (ok-gated / ok-plain-only / NEEDS-FIX) | EVIDENCE
```

A verdict of `ok-gated` requires naming the gate (`isRoutedComposite()`, `dimCount > 0`, or an enclosing method that is itself gated). A verdict of `NEEDS-FIX` becomes a task in A3/A4.

- [ ] **Step 4: Commit**

```bash
git add docs/superpowers/plans/2026-08-10-composite-post-master-merge-hardening.md
git commit -m "docs(composite): post-master-merge hardening plan + hazard detector method"
```

---

### Task A2: Class (a) + (c) mechanical sweeps to zero

**Files:**
- Modify: any factory the detector flags (expected: none beyond the already-fixed `LiveViewRecordCursorFactory`)
- Create: `core/src/test/java/io/questdb/test/cairo/CompositeCapabilityDelegationTest.java`

**Interfaces:**
- Consumes: `detect.sh` CLASS A/C output from A1.
- Produces: `CompositeCapabilityDelegationTest` — a permanent guard so a future merge cannot reintroduce a lying wrapper.

- [ ] **Step 1: Write the failing test**

```java
package io.questdb.test.cairo;

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.lv.LiveViewRecordCursorFactory;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;

/**
 * Guards hazard class (c): this branch added supportsConcurrentTimeFrameCursor() to
 * RecordCursorFactory, and it DEFAULTS to supportsTimeFrameCursor(). Any wrapper that delegates
 * supportsTimeFrameCursor() to a base but inherits the default concurrent flag will advertise a
 * concurrent cursor its base cannot produce. Upstream cannot know this flag exists, so every
 * master merge can reintroduce the bug.
 */
public class CompositeCapabilityDelegationTest {

    @Test
    public void testDelegatingWrappersAlsoDelegateConcurrentFlag() throws Exception {
        assertDelegates(LiveViewRecordCursorFactory.class);
    }

    private static void assertDelegates(Class<? extends RecordCursorFactory> cls) throws Exception {
        Method tf = cls.getDeclaredMethod("supportsTimeFrameCursor");
        Assert.assertNotNull("precondition: " + cls.getSimpleName() + " overrides supportsTimeFrameCursor", tf);
        Method concurrent;
        try {
            concurrent = cls.getDeclaredMethod("supportsConcurrentTimeFrameCursor");
        } catch (NoSuchMethodException e) {
            concurrent = null;
        }
        Assert.assertNotNull(
                cls.getSimpleName() + " delegates supportsTimeFrameCursor() but NOT"
                        + " supportsConcurrentTimeFrameCursor(); the default would make it lie about"
                        + " a concurrent cursor its base cannot produce",
                concurrent
        );
    }
}
```

- [ ] **Step 2: Run it — it must PASS now (the fix already landed) and FAIL if the fix is reverted**

```bash
export JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 QDB_TEST_TMPDIR=/dev/shm
mvn -o -pl core test-compile -DskipTests -q
mvn -o -pl core surefire:test -Dtest='CompositeCapabilityDelegationTest' -DfailIfNoTests=false
```

Expected: `Tests run: 1, Failures: 0`.

- [ ] **Step 3: Prove the guard is not vacuous (negative control)**

Temporarily comment out the `supportsConcurrentTimeFrameCursor()` override in
`core/src/main/java/io/questdb/griffin/engine/lv/LiveViewRecordCursorFactory.java`, re-run Step 2,
and confirm it FAILS with the "delegates supportsTimeFrameCursor() but NOT" message. Then restore
the override and re-run to green. **Do not commit the commented-out state.**

- [ ] **Step 4: Extend the guard to any other wrapper the detector flags**

For each additional class the CLASS C detector reports as `MISSING`, add one `assertDelegates(X.class);`
line to the test and add the 4-line override to that class:

```java
    @Override
    public boolean supportsConcurrentTimeFrameCursor() {
        return base.supportsConcurrentTimeFrameCursor();
    }
```

- [ ] **Step 5: Commit**

```bash
git add core/src/test/java/io/questdb/test/cairo/CompositeCapabilityDelegationTest.java core/src/main/java/io/questdb/griffin/engine/lv/
git commit -m "test(composite): guard capability-flag delegation against future master merges"
```

---

### Task A3: Tier-1 deep audit (9 files)

**Files:**
- Read (three-way): `TableWriter.java`, `TableReader.java`, `TxWriter.java`, `TxReader.java`, `SqlCodeGenerator.java`, `TableSnapshotRestore.java`, `TableUtils.java`, `O3PartitionJob.java`, `O3PartitionPurgeJob.java`
- Create: `.superpowers/sdd/merge-audit/tier1-findings.md`

**Interfaces:**
- Consumes: A1's CLASS B worklist.
- Produces: `tier1-findings.md` with one verdict row per master-introduced hunk; every `NEEDS-FIX` gets a fix + regression test in this task.

- [ ] **Step 1: For each file, produce the master-only change set**

```bash
B=$(git merge-base 0de2fa4ef2 origin/master)
for f in core/src/main/java/io/questdb/cairo/TableWriter.java \
         core/src/main/java/io/questdb/cairo/TableReader.java \
         core/src/main/java/io/questdb/cairo/TxWriter.java \
         core/src/main/java/io/questdb/cairo/TxReader.java \
         core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java \
         core/src/main/java/io/questdb/cairo/TableSnapshotRestore.java \
         core/src/main/java/io/questdb/cairo/TableUtils.java \
         core/src/main/java/io/questdb/cairo/O3PartitionJob.java \
         core/src/main/java/io/questdb/cairo/O3PartitionPurgeJob.java; do
  echo "######## $f"
  git diff "$B" origin/master -- "$f" | grep -E "^\+" | grep -vE "^\+\+\+"
done > .superpowers/sdd/merge-audit/tier1-master-additions.txt
wc -l .superpowers/sdd/merge-audit/tier1-master-additions.txt
```

- [ ] **Step 2: Classify every added hunk**

For each hunk, answer in `tier1-findings.md`:
1. Does it touch the attached-partition array, a partition path, a partition-by-day loop, or a capability flag? If no → `benign` (one line, no further work).
2. If yes → is it reachable with `dimCount > 0`? Answer with evidence: an existing gate, or a probe (Step 3).
3. Verdict: `ok-gated` (name the gate) / `ok-plain-only` (name why composite cannot reach) / `NEEDS-FIX`.

Known answers to record without re-deriving (already established):
- `TableWriter#tryFastAppendInOrderBlock` → NEEDS-FIX → **fixed** `0d04c2cd3c`, locked by `CompositeBlockFastAppendGateTest`.
- `TableWriter:11054` seqTxn stamp → resolved cell-aware via `findAttachedPartitionRawIndexBy(ts, cellKey)`.
- `O3PartitionPurgeJob:223` → uses the stride as a composite *detection* comparison, not arithmetic → `benign`.
- `TableUtils` → constant declarations + `partitionStrideMarker` → `benign`.

- [ ] **Step 3: For any hunk whose reachability is unclear, probe it — do not reason**

Insert a temporary throw at the top of the suspect branch:

```java
        if (metadata.getPartitionSpec().getDimensionCount() > 0) {
            throw CairoException.critical(0).put("PROBE-REACHED-<sitename>");
        }
```

Then run the composite suites:

```bash
export JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 QDB_TEST_TMPDIR=/dev/shm
mvn -o -pl core test-compile -DskipTests -q
mvn -o -pl core surefire:test -Dtest='Composite*' -DfailIfNoTests=false 2>&1 | grep -E "Tests run:|PROBE-REACHED"
```

**A green run with the probe in place proves nothing on its own.** If nothing fires, you must also
show the shape is exercised at all — add a positive control that drives the same path on a *plain*
table and assert it is reached (see `CompositeBlockFastAppendGateTest#testPlainTableStillUsesBlockFastAppend`
for the pattern). Remove every probe before committing: `grep -rn "PROBE-" core/src/main` must print nothing.

- [ ] **Step 4: Fix each NEEDS-FIX with a gate + a regression-lock test**

Gate pattern (loud where a wrong answer is possible, silent fallback only where the O3 path is the correct alternative):

```java
        // Composite-partitioning guard (master-merge 2026-08-10): <why this site is cell-blind>.
        // Regression-locked by <TestName>.
        if (metadata.getPartitionSpec().getDimensionCount() > 0) {
            return <the safe fallback>;   // or: throw CairoException.critical(0).put("...")
        }
```

Every gate gets a test asserting composite does not take the path **plus** a plain positive control.

- [ ] **Step 5: Run the composite + covering suites**

```bash
mvn -o -pl core surefire:test -Dtest='Composite*,CoveringIndexBlockApplySealTest' -DfailIfNoTests=false 2>&1 | tail -5
```

Expected: `Tests run: 332+, Failures: 0, Errors: 0` (330 before this task, plus the new guard tests).

- [ ] **Step 6: Commit**

```bash
git add core/src
git commit -m "fix(composite): tier-1 master-merge audit findings + regression locks"
```

---

### Task A4: Tier-2 targeted audit (15 files — frame cursors, factories, metadata)

**Files:**
- Read (three-way): the 15 Tier-2 files listed in "Audit Surface"
- Create: `.superpowers/sdd/merge-audit/tier2-findings.md`

**Interfaces:**
- Consumes: A1 CLASS B/C output.
- Produces: `tier2-findings.md`, same verdict format as Tier 1.

- [ ] **Step 1: Diff each Tier-2 file's master-side additions**

```bash
B=$(git merge-base 0de2fa4ef2 origin/master)
for f in core/src/main/java/io/questdb/cairo/AbstractIntervalPartitionFrameCursor.java \
         core/src/main/java/io/questdb/cairo/IntervalFwdPartitionFrameCursor.java \
         core/src/main/java/io/questdb/cairo/IntervalBwdPartitionFrameCursor.java \
         core/src/main/java/io/questdb/cairo/IntervalPartitionFrameCursorFactory.java \
         core/src/main/java/io/questdb/cairo/FullFwdPartitionFrameCursor.java \
         core/src/main/java/io/questdb/cairo/FullBwdPartitionFrameCursor.java \
         core/src/main/java/io/questdb/cairo/FullPartitionFrameCursorFactory.java \
         core/src/main/java/io/questdb/cairo/sql/PartitionFrameCursorFactory.java \
         core/src/main/java/io/questdb/cairo/sql/RecordCursorFactory.java \
         core/src/main/java/io/questdb/griffin/engine/table/ShowPartitionsRecordCursorFactory.java \
         core/src/main/java/io/questdb/griffin/engine/table/ShowCreateTableRecordCursorFactory.java \
         core/src/main/java/io/questdb/tasks/O3PartitionTask.java \
         core/src/main/java/io/questdb/cairo/TableReaderMetadata.java \
         core/src/main/java/io/questdb/cairo/TableWriterMetadata.java \
         core/src/main/java/io/questdb/cairo/TableStructure.java; do
  echo "######## $f"; git diff "$B" origin/master -- "$f" | grep -E "^\+" | grep -vE "^\+\+\+"
done > .superpowers/sdd/merge-audit/tier2-master-additions.txt
```

- [ ] **Step 2: Answer the two Tier-2-specific questions per file**

1. **Frame cursors:** does any master-added line iterate partitions or compute a frame index without honouring `allowedCellKeys`? Our branch added cell-pruning to these cursors; a new master loop that ignores it would over-scan (wrong results, not corruption). Grep each file for `allowedCellKeys` and confirm every partition-advancing loop consults it.
2. **Factories/metadata:** does any master-added override change `supportsPageFrameCursor()`, `getScanDirection()`, or `supportsRandomAccess()` in a way that contradicts `CompositePageFrameRecordCursorFactory`'s contract (`supportsPageFrameCursor=false`, `getScanDirection` truthful, `supportsConcurrentTimeFrameCursor=false`)?

- [ ] **Step 3: Verify the frame-exposure invariant still holds**

```bash
mvn -o -pl core surefire:test -Dtest='CompositeFrameExposureSafetyTest,CompositeVectorizedAggregationTest' -DfailIfNoTests=false 2>&1 | grep -E "Tests run:"
```

Expected: `Failures: 0, Errors: 0`. This is the test that guards the deliberately inverted invariant (real unordered frames behind a `false` flag).

- [ ] **Step 4: Fix findings, then re-run the read-shape suites**

```bash
mvn -o -pl core surefire:test -Dtest='CompositeReadShapesTest,CompositeEndToEndTest,Composite*' -DfailIfNoTests=false 2>&1 | tail -4
```

- [ ] **Step 5: Commit**

```bash
git add core/src
git commit -m "fix(composite): tier-2 master-merge audit findings (frame cursors, factories, metadata)"
```

---

### Task A5: Tier-3 confirm-benign (17 files)

**Files:**
- Read: the 17 Tier-3 files listed in "Audit Surface"
- Create: `.superpowers/sdd/merge-audit/tier3-findings.md`

- [ ] **Step 1: Confirm each file is config/grammar/plumbing only**

```bash
B=$(git merge-base 0de2fa4ef2 origin/master)
comm -12 <(git diff --name-only "$B" origin/master -- core/src | sort) \
         <(git diff --name-only "$B" 0de2fa4ef2 -- core/src | sort) > /tmp/bothsides.txt
# subtract the 9 tier-1 + 15 tier-2 files; whatever remains is tier 3
```

For each remaining file, record one line: `file | master's change in <=10 words | why it cannot affect composite semantics`.

A file qualifies as benign **only** if its master-side additions contain none of: `attachedPartitions`, `PartitionRawIndex`, `setPathForNativePartition`, `getPartitionCount()`, `supportsTimeFrameCursor`, `supportsPageFrameCursor`, `getDimensionCount`. Verify mechanically:

```bash
while read f; do
  hits=$(git diff "$B" origin/master -- "$f" | grep -E "^\+" \
    | grep -cE "attachedPartitions|PartitionRawIndex|setPathForNativePartition|getPartitionCount\(\)|supportsTimeFrameCursor|supportsPageFrameCursor|getDimensionCount")
  [ "$hits" -gt 0 ] && echo "NOT-BENIGN($hits): $f"
done < /tmp/bothsides.txt
```

Expected: prints only Tier-1/Tier-2 files. Any Tier-3 file that appears must be promoted to Tier 2 and audited under Task A4's method.

- [ ] **Step 2: Commit the findings**

```bash
git add docs/superpowers/plans/
git commit -m "docs(composite): tier-3 merge audit — confirmed benign"
```

---

### Task A6: Audit master-added files (160 added, 4 with hazard patterns)

**Files:**
- Read: `core/src/main/java/io/questdb/cairo/lv/LiveViewCheckpointScanCost.java`, `core/src/main/java/io/questdb/cairo/lv/LiveViewRefreshJob.java`, `core/src/main/java/io/questdb/griffin/engine/lv/LiveViewRecordCursorFactory.java`, `core/src/main/java/io/questdb/griffin/engine/table/RuntimeConstGateRecordCursorFactory.java`
- Create: `.superpowers/sdd/merge-audit/added-files-findings.md`

**Interfaces:**
- Consumes: nothing.
- Produces: verdicts; `LiveViewRecordCursorFactory` already has one fix (`d5564f6d7b`) — this task audits the *rest* of that class and the other three.

- [ ] **Step 1: Re-derive the hazard list (do not trust the number)**

```bash
B=$(git merge-base 0de2fa4ef2 origin/master)
git diff --diff-filter=A --name-only "$B" origin/master -- core/src/main | while read f; do
  [ -f "$f" ] && grep -qE "attachedPartitions|setPathForNativePartition|getPartitionIndexByTimestamp|supportsTimeFrameCursor|supportsPageFrameCursor|getPartitionCount\(\)" "$f" && echo "$f"
done
```

Expected: the 4 files above. If more appear, audit them too.

- [ ] **Step 2: `RuntimeConstGateRecordCursorFactory` — class (c) check**

This is a new wrapper. Confirm which capability methods it overrides and whether it delegates both of this branch's added flags:

```bash
grep -n "supportsPageFrameCursor\|supportsTimeFrameCursor\|supportsConcurrentTimeFrameCursor\|supportsPageFrameCursorForUnorderedAggregation\|getScanDirection" \
  core/src/main/java/io/questdb/griffin/engine/table/RuntimeConstGateRecordCursorFactory.java
```

If it delegates `supportsPageFrameCursor()` to a base but not `supportsPageFrameCursorForUnorderedAggregation()`, the consequence is a **lost optimisation, not a correctness bug** (that flag defaults to `false`) — record it as `ok-perf-only` unless it also delegates `supportsTimeFrameCursor`, in which case apply the Task A2 fix and add it to `CompositeCapabilityDelegationTest`.

- [ ] **Step 3: `LiveViewRefreshJob` — class (b) check**

A live view over a composite table is permitted (verified). Determine whether this job resolves partitions by day or builds partition paths:

```bash
grep -n "setPathForNativePartition\|getPartitionCount()\|getPartitionTimestampByIndex\|getPartitionIndexByTimestamp" \
  core/src/main/java/io/questdb/cairo/lv/LiveViewRefreshJob.java core/src/main/java/io/questdb/cairo/lv/LiveViewCheckpointScanCost.java
```

For each hit, decide whether the path is reachable when the live view's **base table** is composite. If reachable and cell-blind → NEEDS-FIX (gate the live view over a composite base, loudly, at CREATE time — that is the smallest correct fix and matches the branch's loud-gate discipline).

- [ ] **Step 4: If a gate is needed, add it with a test**

```java
    @Test
    public void testLiveViewOverCompositeIsRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE c (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) PARTITION BY DAY, exch WAL");
            execute("INSERT INTO c VALUES ('2024-01-01T00:00:00Z', 'BTC', 1.0)");
            drainWalQueue();
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS "
                        + "SELECT exch, ts, count(*) OVER (PARTITION BY exch ORDER BY ts"
                        + " ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS rn FROM c");
                Assert.fail("expected composite live-view rejection");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "composite");
            }
        });
    }
```

Note the live-view grammar (learned the hard way): no wildcard select, must contain a window function, and an unbounded window needs `ANCHOR` on a **named** `WINDOW` clause.

- [ ] **Step 5: Commit**

```bash
git add core/src
git commit -m "fix(composite): audit master-added files (live view, runtime-const gate)"
```

---

### Task A7: Empirical backstop — differential + full regression

**Files:**
- Run only; no source changes expected.

- [ ] **Step 1: Run the composite differential capstones**

```bash
export JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 QDB_TEST_TMPDIR=/dev/shm
mvn -o -pl core surefire:test -Dtest='CompositeEndToEndTest,CompositePartitionDdlTest,CompositeReadShapesTest,CompositeRoutingTest,CompositeUnsupportedOpsTest' -DfailIfNoTests=false 2>&1 | grep -E "Tests run:|<<< "
```

Expected: `Failures: 0, Errors: 0`. These are the composite-vs-plain-twin proofs; they are the strongest available evidence that the merge did not change composite semantics.

- [ ] **Step 2: Run the crash/power-loss suites**

```bash
mvn -o -pl core surefire:test -Dtest='CompositeFastAppendCrashTest,CompositeMultiCellFastAppendCrashTest' -DfailIfNoTests=false 2>&1 | grep -E "Tests run:"
```

Expected: `Tests run: 8, Failures: 0` (4 + 4).

- [ ] **Step 3: Full regression**

```bash
mvn -o -pl core surefire:test \
  -Dtest='Composite*,*Parquet*,O3*,Wal*,Commit*,TxReaderTest,TxWriterTest,ShowPartitionsTest,CoveringIndexBlockApplySealTest,LiveView*' \
  -DfailIfNoTests=false > .superpowers/sdd/merge-audit/regression-final.log 2>&1
grep -E "Tests run:.*(Failures: [1-9]|Errors: [1-9])" .superpowers/sdd/merge-audit/regression-final.log
```

Expected: exactly one line, `Errors: 2`, both `ExpParquetExportTest` port-bind cases. **Any other failure blocks the workstream.**

- [ ] **Step 4: Commit the audit records**

```bash
git add docs/superpowers/plans/
git commit -m "docs(composite): merge audit complete — findings and evidence"
```

---

## Workstream B — Enterprise

### Task B1: Refresh Enterprise and create the companion branch

**Files:**
- Create: worktree `~/claude/wt/ent/composite-partitioning`

**Interfaces:**
- Produces: an ent branch named **exactly** `feat/composite-partitioning` (the OSS pipeline triggers `enterprise-ci` against the same-named ent branch).

- [ ] **Step 1: Fetch — the local ent hub is a month stale (`e87f5971d`, 2026-07-17)**

```bash
cd /home/nick/claude/hub/questdb-enterprise
git fetch origin --prune
git log -1 --format='%h %ci %s' origin/main
```

- [ ] **Step 2: Create the companion worktree off ent main**

```bash
cd /home/nick/claude/hub/questdb-enterprise
git worktree add -b feat/composite-partitioning ~/claude/wt/ent/composite-partitioning origin/main
```

Beware the upstream footgun: creating a branch with `-b NEW origin/OTHER` sets upstream to OTHER. Verify and retarget:

```bash
cd ~/claude/wt/ent/composite-partitioning
git branch -vv | head -2
```

- [ ] **Step 3: Attach the OSS submodule at our composite commit**

```bash
cd ~/claude/wt/ent/composite-partitioning
git --git-dir=/home/nick/claude/hub/questdb-enterprise/.git/modules/questdb \
    worktree add ~/claude/wt/ent/composite-partitioning/questdb feat/composite-partitioning
git -C questdb log -1 --oneline
```

Expected: the head of our OSS composite branch.

- [ ] **Step 4: Commit the submodule pointer**

```bash
git add questdb
git commit -m "chore(ent): point questdb submodule at composite-partitioning"
```

---

### Task B2: Bump `ossversion` 9.4.4 → 10.0.1-SNAPSHOT and compile

**Files:**
- Modify: `questdb-ent/pom.xml` (`<ossversion>`)

**Interfaces:**
- Consumes: OSS core version `10.0.1-SNAPSHOT` (from `core/pom.xml` after the master merge).

- [ ] **Step 1: Install the OSS core our branch builds into the local m2**

```bash
export JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64
cd ~/claude/wt/ent/composite-partitioning
mvn -o -pl questdb/core -am install -DskipTests
```

Expected: `BUILD SUCCESS`, installing `questdb:10.0.1-SNAPSHOT`.

- [ ] **Step 2: Bump the pinned OSS version**

In `questdb-ent/pom.xml`, change:

```xml
        <ossversion>9.4.4-SNAPSHOT</ossversion>
```

to:

```xml
        <ossversion>10.0.1-SNAPSHOT</ossversion>
```

- [ ] **Step 3: Compile Enterprise against it**

```bash
mvn -o -pl questdb-ent -am test-compile -DskipTests 2>&1 | grep -E "ERROR|BUILD"
```

Expected: `BUILD SUCCESS`. A major-version jump (9 → 10) makes `cannot find symbol` likely; each one is a real API change in master that Enterprise must follow. Fix them in this task, one commit per coherent group.

- [ ] **Step 4: Commit**

```bash
git add questdb-ent/pom.xml questdb-ent/src
git commit -m "chore(ent): track OSS 10.0.1-SNAPSHOT for composite partitioning"
```

---

### Task B3: Enterprise composite audit (narrow — measured surface)

**Files:**
- Read: the 4 `setPathForNativePartition` sites in `questdb-ent/src/main`
- Read: Enterprise backup/restore and replication partition handling

**Interfaces:**
- Consumes: hazard classes (a)/(b)/(c).

- [ ] **Step 1: Confirm the surface is still what was measured**

```bash
cd ~/claude/wt/ent/composite-partitioning
for p in LONGS_PER_TX_ATTACHED_PARTITION attachedPartitions setPathForNativePartition \
         findAttachedPartitionRawIndex getPartitionIndexByTimestamp supportsTimeFrameCursor; do
  echo "$p -> $(grep -rn "$p" --include=*.java questdb-ent/src/main | wc -l)"
done
```

Expected: `setPathForNativePartition -> 4`, everything else `0`. If `LONGS_PER_TX_ATTACHED_PARTITION` or `attachedPartitions` is now non-zero, Enterprise reads `_txn` directly and needs the full class-(a) sweep — escalate and expand this task.

- [ ] **Step 2: Audit each of the 4 day-path sites**

```bash
grep -rn "setPathForNativePartition" --include=*.java questdb-ent/src/main
```

For each: is it reachable for a composite table? Composite is Enterprise-visible only once the OSS feature ships, so the honest verdict for most will be "reachable in principle". The correct minimal outcome is a **loud** rejection of composite tables in that Enterprise feature, not silent day-path construction.

- [ ] **Step 3: Resolve the two carried backlog items**

- **`isMetaFormatUpToDate` / LATEST 2 → 3:** OSS now sets `META_FORMAT_MINOR_VERSION_LATEST = 3` (`META_FORMAT_MINOR_VERSION_COMPOSITE_PARTITIONING = 3`). Confirm Enterprise has no independent `_meta` writer or format assertion that must learn about 3:

```bash
grep -rn "META_FORMAT_MINOR_VERSION\|isMetaFormatAtLeast\|META_OFFSET_META_FORMAT" --include=*.java questdb-ent/src
```

Expected: only `EntShowCreateTableTest` (a test that forces the legacy path by writing 0). If so, record "no ent change required" **with this command's output as the evidence**.

- **`BackupRestoreAgent` interner audit (P2 ticket I3-ent):** composite tables carry dedicated dimension dictionaries and a `_cell` registry as extra `.k/.v/.o` files. OSS `TableSnapshotRestore.rebuildCompositeInternerFiles` handles these. Find Enterprise's equivalent restore path and confirm it either rebuilds them or rejects composite tables:

```bash
grep -rln "rebuildSymbolFiles\|BackupRestore\|restore" --include=*.java questdb-ent/src/main | head -20
```

- [ ] **Step 4: Run the Enterprise suites most likely to be affected**

```bash
mvn -o -pl questdb/core,questdb-ent test -Dtest='*Backup*,*Restore*,*Snapshot*,*ShowCreateTable*' -Dsurefire.failIfNoSpecifiedTests=false 2>&1 | grep -E "Tests run:|BUILD"
```

Note: ACL tests (`AbstractAccessControlTest` subclasses) throw `FileSystemNotFoundException` when run against an installed core **test-jar** — that is a known harness artifact, not a failure. Keeping `questdb/core` in the reactor (as above) avoids it.

- [ ] **Step 5: Commit**

```bash
git add questdb-ent
git commit -m "fix(ent): composite-partitioning audit — gates and restore handling"
```

---

## Workstream C — Pre-PR

### Task C1: Remove the 4 dangling scratch references

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/TableWriter.java:5639`, `:5752`
- Modify: `core/src/test/java/io/questdb/test/cairo/CompositeFastAppendEligibilityTest.java:37`
- Modify: `core/src/test/java/io/questdb/test/cairo/CompositeMultiCellFastAppendEligibilityTest.java:37`

**Interfaces:**
- Produces: shipping source with zero references to the gitignored `.superpowers/sdd/` scratch tree.

- [ ] **Step 1: See exactly what each reference says**

```bash
grep -rn "superpowers/sdd" core/src --include=*.java
```

Expected: exactly the 4 lines above.

- [ ] **Step 2: Inline the fact or drop the pointer**

These all cite `.superpowers/sdd/task-1-brief.md`, which will not exist in the repo. Keep the surviving `docs/superpowers/specs/...` citation (that path **is** committed) and delete only the scratch pointer. Example, `TableWriter.java:5639`:

```java
     * fast-append-design.md} and {@code .superpowers/sdd/task-1-brief.md}). The per-cell analog of
```

becomes:

```java
     * fast-append-design.md}). The per-cell analog of
```

- [ ] **Step 3: Verify none remain and it still compiles**

```bash
grep -rn "superpowers/sdd" core/src --include=*.java; echo "exit=$?  (1 = none found, which is what we want)"
export JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64
mvn -o -q -pl core test-compile -DskipTests 2>&1 | grep -E "error:" | head -3
```

- [ ] **Step 4: Commit**

```bash
git add core/src
git commit -m "docs(composite): drop gitignored scratch-report refs from shipping source"
```

---

### Task C2: Run the CI formatter locally (must be after ALL code changes)

**Files:**
- Modify: whatever the formatter rewrites.

**Interfaces:**
- Consumes: the exact recipe from `ci/templates/java-lint.yml`. IntelliJ is **not** currently installed locally (`idea` not on PATH), so Step 1 downloads the pinned build.

- [ ] **Step 1: Fetch the pinned IntelliJ 2026.1.4**

```bash
IDEA_ROOT=/tmp/claude-1000/-tmp/a097d053-d87c-44a8-997d-c7a9e568b19e/scratchpad/intellij
mkdir -p "$IDEA_ROOT" && cd "$IDEA_ROOT"
wget -q "https://download.jetbrains.com/idea/idea-2026.1.4.tar.gz" -O intellij.tar.gz
tar xzf intellij.tar.gz -C "$IDEA_ROOT" && rm intellij.tar.gz
ln -sfn "$IDEA_ROOT"/idea-* "$IDEA_ROOT/idea"
ls "$IDEA_ROOT/idea/bin/idea.sh"
```

Do **not** substitute a different IntelliJ version; the pin exists because formatter behaviour changes between releases and would reformat the whole repo.

- [ ] **Step 2: Run the unterminated-log check (the lint stage's first step)**

```bash
cd /home/nick/claude/wt/oss/composite-partitioning
python3 find_unterminated_logs.py core/src --exclude=LogParanoiaTest.java
```

Expected: no output / exit 0.

- [ ] **Step 3: Apply the formatter exactly as CI does**

```bash
export JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64
IDEA_ROOT=/tmp/claude-1000/-tmp/a097d053-d87c-44a8-997d-c7a9e568b19e/scratchpad/intellij
"$IDEA_ROOT/idea/bin/idea.sh" format -s .idea/codeStyles/Project.xml -m "*.java" -r .
```

- [ ] **Step 4: Inspect what it changed, then confirm the tree is clean**

```bash
git status -s | head -40
git diff --stat | tail -3
```

Review the diff before committing — it should be pure whitespace/wrapping (commonly: single-line `/** … */` javadocs expanding to 3-line form, over-long `switch` arms wrapping). If it rewrites logic, stop: wrong formatter version.

- [ ] **Step 5: Recompile and re-run the composite suites after reformatting**

```bash
export QDB_TEST_TMPDIR=/dev/shm
mvn -o -q -pl core test-compile -DskipTests 2>&1 | grep -E "error:" | head -3
mvn -o -pl core surefire:test -Dtest='Composite*' -DfailIfNoTests=false 2>&1 | grep -E "Tests run:" | tail -2
```

- [ ] **Step 6: Commit**

```bash
git add -A core benchmarks
git commit -m "style: apply IntelliJ formatter (CI java-lint parity)"
```

---

### Task C3: Push and drive CI to green

**Files:**
- None (branch operations only).

**Interfaces:**
- Consumes: a fully audited, formatted branch. **Do not start this task until A7 and C2 are green.**

- [ ] **Step 1: Final local state check**

```bash
cd /home/nick/claude/wt/oss/composite-partitioning
git status --short           # expect empty
git log --oneline origin/master..HEAD | wc -l
grep -rn "PROBE-" core/src --include=*.java   # expect nothing
```

- [ ] **Step 2: Confirm the push target with the operator before pushing**

This branch has never left the machine and carries 160+ commits. Pushing is outward-facing and triggers CI (and `enterprise-ci`). Confirm the remote branch name, then:

```bash
git push -u origin feat/composite-partitioning
```

- [ ] **Step 3: Push the Enterprise companion branch (same name — the trigger matches by name)**

```bash
cd ~/claude/wt/ent/composite-partitioning
git push -u origin feat/composite-partitioning
```

- [ ] **Step 4: Watch the two pipelines**

Expect first-run failures to cluster in: the java-lint stage (should be pre-empted by C2), the shared build stage (which also compiles `benchmarks` — GitHub Actions does not, so Azure can fail where Actions passes), and `enterprise-ci` version coupling (B2).

- [ ] **Step 5: Triage each failure against the known-flake list before touching code**

Known environmental/flaky, per prior sessions: `maven.internal` resolution flake, `testSleepCancelledByConnectionDrop` (OSS pgwire), and ent's `promoteRebuiltStoreIsHydratedFromDisk` / `testFuzzReplication2TableWithRename` / `testQwpUdpIngestionFreezesAcrossRoleSwitch`. Re-runs require the operator — the PAT is read-only.

- [ ] **Step 6: Open the PR only once both pipelines are green**

---

## Execution Order and Dependencies

```
A1 ─→ A2 ─→ A3 ─→ A4 ─→ A5 ─→ A6 ─→ A7 ─┐
                                          ├─→ C2 ─→ C3
C1 ───────────────────────────────────────┘         ↑
B1 ─→ B2 ─→ B3 ──────────────────────────────────────┘ (ent push in C3 Step 3)
```

- **A must precede C2.** The formatter must run over final code; any audit fix afterwards invalidates it.
- **C1 is independent** and can be done at any point before C2.
- **B is independent of A** up to B3, and its CI leg lands in C3.
- **Nothing is pushed until A7 and C2 are green.**

## Definition of Done

- [ ] `detect.sh` reports zero CLASS A hits, zero unresolved CLASS B hits, zero CLASS C `MISSING` hits.
- [ ] Every audit finding is either fixed with a regression-lock test, or recorded with a named gate and evidence.
- [ ] Every new guard test has a positive control proving it is not vacuous.
- [ ] `grep -rn "PROBE-" core/src` is empty.
- [ ] Full regression: 0 failures; only the 2 known `:9003` port-bind errors.
- [ ] Enterprise compiles against OSS `10.0.1-SNAPSHOT` and its suites pass.
- [ ] `grep -rn "superpowers/sdd" core/src` is empty.
- [ ] Formatter run produces no diff on a second pass.
- [ ] OSS and ent pipelines green; PR open.
