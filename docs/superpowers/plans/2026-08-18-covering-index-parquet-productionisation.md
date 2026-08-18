# Covering Index Parquet — Productionisation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the seven gaps that stand between `feat/covering-index-parquet` and a release anyone would enable in production.

**Architecture:** The feature is already built and merged with master — a covering POSTING index seals into `<col>.pidx.<indexTxn>.parquet` plus an `_im` sidecar, publishes a token in the partition's `_pm` footer, and is served by `ParquetPostingIndexFwd/BwdReader`. This plan adds nothing to that design. It makes a downgrade fail loudly instead of silently, gets the twelve unreviewed tasks reviewed, proves the value proposition with numbers, and bounds the one growth path that has no ceiling.

**Tech Stack:** Java 17 (`core/src/main/java/io/questdb/cairo/`), Rust (`core/rust/qdb-parquet-meta`, `core/rust/qdbr`), Maven + surefire, JUnit 4.

## Global Constraints

- Feature flag `cairo.posting.index.parquet.partition.format`, default `native`. The default path must stay free of per-call I/O; measure anything added to a per-frame, per-column or per-key path.
- Every behavioural change carries a negative control: revert **only** the production change, through the production entry path, confirm the test fails, restore, confirm it passes, report the exact failure message. Controls on this branch have been vacuous three times.
- `-Pbuild-rust-library` is required on every Maven run, or JNI tests die with an `UnsatisfiedLinkError` that does not name its cause.
- Delete `core/target/surefire-reports` before each Maven run and read counts from the XML; `mvn -q` suppresses the Tests-run lines. Check the exit code separately from any pipe.
- Run `mvn -pl core test-compile` after any merge before a broad suite, or stale classes produce hundreds of unreadable errors.
- Rust gates run **per crate**: `cargo test --lib` from `qdbr` runs zero tests from `qdb-parquet-meta`.
- `export QDB_TEST_TMPDIR=/dev/shm/qdb-test` before Maven runs.
- Do not assert via `LogCapture.waitFor`/`waitForRegex` — fixed on master, but do not build new assertions on it.
- The host runs a QuestDB instance on ports 9000/9003. `ServerMain`-based tests will error on bind; that is environmental, not a regression.
- Verify every invariant before relying on it, including ones stated in this plan. On this branch fourteen stated premises turned out to be wrong.

## File Structure

| File | Responsibility |
| --- | --- |
| `core/rust/qdb-parquet-meta/src/types.rs` | Feature-flag constants. Gains the required-half covering bit. |
| `core/rust/qdb-parquet-meta/src/footer.rs` | Footer write/parse. Sets the new bit when writing a covering section; rejects unknown required bits. |
| `core/src/main/java/io/questdb/cairo/ParquetMetaFileReader.java` | Java-side footer read. Must know the new required bit. |
| `core/src/test/java/io/questdb/test/cairo/ParquetIndexSealTest.java` | Seal, publish, lifecycle tests. Gains the downgrade test. |
| `core/src/test/java/io/questdb/test/cairo/idx/ParquetCoveringIndexFuzzTest.java` | **New.** Randomised differential fuzz against the native reader. |
| `core/src/test/java/io/questdb/test/cairo/idx/ParquetCoveringIndexBenchTest.java` | **New.** Read-path measurement, native arm vs parquet arm. |
| `core/src/main/java/io/questdb/cairo/TableWriter.java` | `_pm` growth ceiling. |
| `docs/covering-index-parquet.md` | **New.** Operator documentation. |
| `.superpowers/sdd/` | Review artefacts. Git-excluded — does not travel with the branch. |

---

### Task 1: Make a downgrade fail loudly

A published covering token is invisible to an older build. `FooterFeatureFlags::COVERING_INDEX_BIT` is `1 << 2`, in the **optional** half (bits 0-31), which `types.rs:135-136` documents as "reader ignores unknown bits". An older reader therefore skips the covering section, concludes no index is published, and dispatches a native reader over a partition whose native chain the seal discarded — "no keys, no rows", a silent empty result.

The fix is a second bit in the **required** half (bits 32-63), set whenever a covering section is written. `unknown_required()` at `types.rs:118-121` already makes an older reader reject the file. A rejected `_pm` fails the partition loudly and recoverably; a silent empty result does neither.

**Files:**
- Modify: `core/rust/qdb-parquet-meta/src/types.rs`
- Modify: `core/rust/qdb-parquet-meta/src/footer.rs`
- Modify: `core/src/main/java/io/questdb/cairo/ParquetMetaFileReader.java`
- Test: `core/rust/qdb-parquet-meta/src/footer.rs` (unit), `core/src/test/java/io/questdb/test/cairo/ParquetIndexSealTest.java` (end to end)

**Interfaces:**
- Consumes: `FooterFeatureFlags::COVERING_INDEX_BIT` (`1 << 2`), `FooterFeatureFlags::unknown_required(self, known_required: u64) -> u64`, `REQUIRED_FLAG_MASK = 0xFFFF_FFFF_0000_0000`.
- Produces: `FooterFeatureFlags::COVERING_INDEX_REQUIRED_BIT: u64 = 1 << 32`, set alongside `COVERING_INDEX_BIT` by every writer that emits a covering section.

- [ ] **Step 1: Write the failing Rust test**

Append to the `mod tests` block in `core/rust/qdb-parquet-meta/src/footer.rs`:

```rust
#[test]
fn a_covering_section_sets_a_required_bit_an_old_reader_rejects() {
    use crate::types::FooterFeatureFlags;
    // A reader that predates the covering index knows no required bits at all.
    const OLD_READER_KNOWN_REQUIRED: u64 = 0;
    let flags = FooterFeatureFlags(
        FooterFeatureFlags::COVERING_INDEX_BIT | FooterFeatureFlags::COVERING_INDEX_REQUIRED_BIT,
    );
    assert_ne!(
        0,
        flags.unknown_required(OLD_READER_KNOWN_REQUIRED),
        "an older reader must REJECT a footer carrying a covering token, not skip it: \
         skipping dispatches a native reader over a chain the seal discarded"
    );
    // And a current reader, which knows the bit, accepts it.
    assert_eq!(
        0,
        flags.unknown_required(FooterFeatureFlags::COVERING_INDEX_REQUIRED_BIT)
    );
}
```

- [ ] **Step 2: Run it and watch it fail**

```bash
cd ~/claude/wt/pidx-parquet/core/rust/qdb-parquet-meta
cargo test --lib a_covering_section_sets_a_required_bit
```

Expected: FAIL to compile — `no associated item named COVERING_INDEX_REQUIRED_BIT`.

- [ ] **Step 3: Add the constant**

In `core/rust/qdb-parquet-meta/src/types.rs`, inside `impl FooterFeatureFlags`, directly below `COVERING_INDEX_BIT`:

```rust
    /// Set whenever `COVERING_INDEX_BIT` is set. Lives in the REQUIRED half so
    /// a reader that predates the covering index rejects the file instead of
    /// skipping the section.
    ///
    /// Skipping is the dangerous outcome: the seal discards the native chain,
    /// so a reader that concludes "no covering index is published" serves the
    /// partition from a chain with no visible generation and answers "no keys,
    /// no rows" -- a silent empty result. Rejecting the `_pm` fails the
    /// partition loudly, and the operator recovers by converting it back to
    /// native on a build that understands the token.
    pub const COVERING_INDEX_REQUIRED_BIT: u64 = 1 << 32;
```

- [ ] **Step 4: Set it wherever the covering section is written**

In `core/rust/qdb-parquet-meta/src/footer.rs`, find the writer path that ORs in `COVERING_INDEX_BIT` (search `COVERING_INDEX_BIT`). Every site that sets it must also set the required bit:

```rust
flags |= FooterFeatureFlags::COVERING_INDEX_BIT | FooterFeatureFlags::COVERING_INDEX_REQUIRED_BIT;
```

Then extend the reader's known-required mask so the current build still accepts its own files. Search `unknown_required(` in `footer.rs` and pass `FooterFeatureFlags::COVERING_INDEX_REQUIRED_BIT` as the known mask.

- [ ] **Step 5: Teach the Java reader the same bit**

`ParquetMetaFileReader` mirrors the Rust constants. Add the constant beside the existing covering-index ones and include it in whatever known-required mask the Java side applies. If the Java side does not currently check required bits, say so in the commit message rather than adding a check — the Rust reader is the one that gates.

- [ ] **Step 6: Run the Rust gates**

```bash
cd ~/claude/wt/pidx-parquet/core/rust/qdb-parquet-meta
cargo fmt -- --check && cargo clippy --all-targets && cargo test --lib && cargo test --lib --release
```

Expected: all pass, including the new test.

- [ ] **Step 7: End-to-end assertion that a sealed file still reads**

Add to `ParquetIndexSealTest`:

```java
    @Test
    public void testASealedPartitionStillReadsWithTheRequiredBitSet() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "parquet");
        assertMemoryLeak(() -> {
            createIndexedSparseKeyTable();
            // The required bit must not break the current reader: same rows an
            // unindexed scan returns.
            assertSqlCursors(
                    "select price, qty from t_pidx where ts in '2024-01-01' and cast(sym as varchar) = 's0'",
                    COVERED_QUERY
            );
        });
    }
```

- [ ] **Step 8: Run the Java gate**

```bash
cd ~/claude/wt/pidx-parquet
export QDB_TEST_TMPDIR=/dev/shm/qdb-test && mkdir -p $QDB_TEST_TMPDIR
rm -rf core/target/surefire-reports
mvn -pl core -Pbuild-rust-library -Dtest='ParquetIndexSealTest,ParquetMetaFileReaderTest,ParquetPostingIndexReaderTest' -DfailIfNoSpecifiedTests=false test; echo "MVN_EXIT=$?"
```

Expected: `MVN_EXIT=0`, no failures.

- [ ] **Step 9: Negative control**

Remove `COVERING_INDEX_REQUIRED_BIT` from the writer's OR in `footer.rs` — production only, test untouched. Re-run `cargo test --lib a_covering_section_sets_a_required_bit`. Expected: the assertion fails with the "must REJECT" message. Restore, confirm green, record the exact message.

- [ ] **Step 10: Commit**

```bash
git add core/rust/qdb-parquet-meta core/src/main/java/io/questdb/cairo/ParquetMetaFileReader.java core/src/test/java/io/questdb/test/cairo/ParquetIndexSealTest.java
git commit -m "fix(idx): make a covering token reject an older reader instead of hiding from it"
```

---

### Task 2: Independent review of Phase 2C tasks 3-14

Tasks 1 and 2 went through the review loop. Tasks 3-14 — the whole reader, both cursors, three pruning levels, covered projection, the parallel decode path, the metadata primitives, `collectDistinctKeys`, the differential oracle, and both lifecycle fixes — were implemented and self-reviewed only. On this branch, five of six reviewed tasks came back with findings and four of those were Critical, so the base rate for "self-review was enough" is poor.

**Files:**
- Create: `.superpowers/sdd/p2c-3-14-review.md`

**Interfaces:**
- Consumes: the diff `git diff b174972f9e~1..HEAD -- core/src/main/java/io/questdb/cairo/ core/src/test/java/io/questdb/test/cairo/`.
- Produces: a findings list ranked Critical/Important/Minor with file:line, and a fix wave for everything Critical or Important.

- [ ] **Step 1: Build the review package**

```bash
cd ~/claude/wt/pidx-parquet
/home/nick/.claude/plugins/cache/claude-plugins-official/superpowers/6.1.1/skills/subagent-driven-development/scripts/review-package b174972f9e~1 HEAD
```

This prints a path. That file is the reviewer's single input.

- [ ] **Step 2: Dispatch the review with these priorities, in order**

1. **The orphan sweep's union-over-chain bound.** Four Criticals were closed there across five rounds, each a different way to delete a file a live reader could still reach. Re-derive the suffix property rather than inheriting it: `prev` is written as the parse anchor, so the chain is a sequence of committed heads, and that holds only while no in-directory `_pm` rewrite exists.
2. **Per-cursor decode isolation.** `ParquetFileDecoder` caches a native decode context; four workers sharing one returned each other's rows without failing. Confirm the decoder, `RowGroupBuffers` and the cover slot-to-chunk map are all per cursor, and that a detached cursor frees all three while the pooled one frees only its buffers.
3. **The three index spaces in `coveringProjection`.** Cover slot → descriptor index → writer index. Chunk ordinals follow the projection's order, so slot `s` lands at `2 + its position`, never at `s`; the two coincide for a dense prefix, which is the case a wrong mapping still passes.
4. **The row-id convention.** Cursors return ids relative to `minValue`. Absolute ids agree exactly when `minValue == 0`, which is every single-partition query — fourteen tests were blind to this.
5. **The `_todo_` retirement replay.** It runs at the end of the constructor because the todo switch lacks the writer state it needs; verify it cannot clear the record without having done the work.

- [ ] **Step 3: Fix wave**

Dispatch **one** fix agent with the complete findings list, not one per finding. Every fix carries a negative control and names the covering test.

- [ ] **Step 4: Re-review**

Re-run the package over the fix commits and review again. On this branch every fix wave introduced at least one defect; three rounds needed a second pass.

- [ ] **Step 5: Commit**

```bash
git add -A core/src
git commit -m "fix(idx): address the Phase 2C review findings"
```

---

### Task 3: Get CI green

Everything so far is local slices: the `cairo` package plus targeted suites. `griffin`, `cutlass` and the rest are untouched, and QuestDB's real CI is external to this repository.

**Files:** none — this task changes nothing unless CI finds something.

- [ ] **Step 1: Rebuild test classes**

```bash
cd ~/claude/wt/pidx-parquet
export QDB_TEST_TMPDIR=/dev/shm/qdb-test && mkdir -p $QDB_TEST_TMPDIR
mvn -pl core -Pbuild-rust-library test-compile
```

Skipping this after a merge produced 183 spurious errors last time.

- [ ] **Step 2: Run the griffin package**

```bash
rm -rf core/target/surefire-reports
mvn -pl core -Pbuild-rust-library -Dtest='io.questdb.test.griffin.**' -DfailIfNoSpecifiedTests=false test; echo "MVN_EXIT=$?"
```

Expected: 0 failures. Errors naming `bindTo=0.0.0.0:9000` or `:9003` are the host's running QuestDB instance, not regressions.

- [ ] **Step 3: Run the cutlass package**

```bash
rm -rf core/target/surefire-reports
mvn -pl core -Pbuild-rust-library -Dtest='io.questdb.test.cutlass.**' -DfailIfNoSpecifiedTests=false test; echo "MVN_EXIT=$?"
```

- [ ] **Step 4: Run the remaining cairo subpackages**

The package is too large for one run; the harness truncates. Run `io.questdb.test.cairo.**` in chunks by subpackage and aggregate the XML counts.

- [ ] **Step 5: Push and let external CI run**

```bash
git push origin feat/covering-index-parquet
```

Watch the external CI result. Fix whatever it reports; do not treat a local green as a substitute.

---

### Task 4: Fuzz the parquet reader against the native one

The differential oracle compares the two readers over a fixed grid of keys, directions, windows and cover sets. That is not fuzzing: the grid is exactly the cases someone thought of.

**Files:**
- Create: `core/src/test/java/io/questdb/test/cairo/idx/ParquetCoveringIndexFuzzTest.java`

**Interfaces:**
- Consumes: `ParquetCoveringIndexOracleTest`'s two-arm fixture shape — build the same rows twice, once with `cairo.posting.index.parquet.partition.format=native`, once with `parquet`.
- Produces: a seeded randomised comparison that prints its seed on failure.

- [ ] **Step 1: Write the fuzz test**

```java
    @Test
    public void testRandomisedReadsAgreeWithTheNativeReader() throws Exception {
        final long seed = System.nanoTime();
        try {
            assertMemoryLeak(() -> {
                final Rnd rnd = new Rnd(seed, seed);
                node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "native");
                createArm("native_arm", rnd.nextInt(40_000) + 20_000);
                node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "parquet");
                createArm("parquet_arm", rnd.nextInt(40_000) + 20_000);
                for (int draw = 0; draw < 200; draw++) {
                    final int key = rnd.nextInt(keyCount);
                    final int direction = rnd.nextBoolean()
                            ? IndexReader.DIR_FORWARD : IndexReader.DIR_BACKWARD;
                    final long lo = rnd.nextLong(rowCount);
                    final long hi = lo + rnd.nextLong(rowCount - lo + 1);
                    final int[] covers = switch (rnd.nextInt(3)) {
                        case 0 -> null;
                        case 1 -> new int[]{0};
                        default -> new int[]{0, 1};
                    };
                    // Same comparison ParquetCoveringIndexOracleTest makes:
                    // drain both readers and assert an identical row-id
                    // sequence and identical covered values.
                    assertSameSequence(nativeReader, parquetReader, nativeCol, parquetCol,
                            key, lo, hi, covers, direction);
                }
            });
        } catch (Throwable t) {
            throw new AssertionError("fuzz seed=" + seed, t);
        }
    }
```

**Both arms must be built from the same `Rnd` sequence**, or the comparison is between different data and proves nothing. Assert the two tables have equal row counts before comparing readers.

- [ ] **Step 2: Assert the arms really differ in seal form**

Before any comparison, assert `native_arm`'s partition directory contains `sym.pv.*` and no `*.pidx.*`, and `parquet_arm`'s contains a `sym.pidx.<txn>.parquet` and its `_im`. A config-forwarding gap once made a two-arm comparison run the native arm twice and agree with itself perfectly.

- [ ] **Step 3: Run it a hundred times**

```bash
rm -rf core/target/surefire-reports
for i in $(seq 1 100); do
  mvn -pl core -Pbuild-rust-library -Dtest='ParquetCoveringIndexFuzzTest' -DfailIfNoSpecifiedTests=false test -q || break
done; echo "EXIT=$?"
```

Expected: 100 clean runs. Any failure prints its seed; pin that seed as a regression test.

- [ ] **Step 4: Commit**

```bash
git add core/src/test/java/io/questdb/test/cairo/idx/ParquetCoveringIndexFuzzTest.java
git commit -m "test(idx): fuzz the parquet reader against the native one"
```

---

### Task 5: Measure the read path

Decode counts, row-group skips and syscalls are all measured. Query latency against the native index is not, so nobody can currently say whether this feature is faster — which is the reason it exists.

**Files:**
- Create: `core/src/test/java/io/questdb/test/cairo/idx/ParquetCoveringIndexBenchTest.java`

- [ ] **Step 1: Write the measurement**

Two arms as in Task 4. For each, time these three shapes over 20 iterations after 5 warm-up iterations, reporting the **median**:

1. `select count() from t where sym = 'hot'` — the metadata-only path.
2. `select price, qty from t where sym = 'hot' and ts in '<partition>'` — the covered path, which is the feature's point.
3. `select price, qty from t where sym = 'cold' and ts in '<partition>'` — a sparse key, where pruning should dominate.

Report as a table: shape, native median, parquet median, ratio.

- [ ] **Step 2: State the fixture in the output**

Print row count, distinct key count, row-group count and `_im` size alongside the numbers. A ratio without a fixture description is not a result anyone can act on.

- [ ] **Step 3: Mark it `@Ignore` with the reason**

```java
@Ignore("benchmark, not a gate: run manually with -Dtest=ParquetCoveringIndexBenchTest")
```

A timing assertion in CI is a flake generator; this branch already produced one perf claim that a repeat run inverted.

- [ ] **Step 4: Run it and record the numbers in the commit message**

```bash
mvn -pl core -Pbuild-rust-library -Dtest='ParquetCoveringIndexBenchTest' -DfailIfNoSpecifiedTests=false test
```

Run it three times. If the ratios move by more than 10% between runs, say so rather than quoting one run.

- [ ] **Step 5: Commit**

```bash
git add core/src/test/java/io/questdb/test/cairo/idx/ParquetCoveringIndexBenchTest.java
git commit -m "test(idx): measure the covering index read path against the native one"
```

---

### Task 6: Bound `_pm` growth

The `_pm` gains a footer per publish and is never truncated. On defaults, the O3 rewrite trigger resets the chain — dead bytes ratio 0.5 or a 1 GiB cap, `cairo.partition.encoder.parquet.o3.rewrite.unused.ratio` and `.max.bytes`, `PropServerConfiguration.java:2355-2356`. A configuration that disables or greatly raises that trigger removes the ceiling, and the sweep's chain walk is paid per candidate.

Compaction is **not** the fix and is prohibited: the orphan sweep's union-over-chain bound is sound only because `prev` is the parse anchor and no in-directory `_pm` rewrite exists. Re-rooting the chain reintroduces the Critical that took four review rounds to close. The constraint is written on `ParquetMetaFileReader.resolvePrevFooter`.

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/TableWriter.java`
- Test: `core/src/test/java/io/questdb/test/cairo/ParquetIndexSealTest.java`

- [ ] **Step 1: Write the failing test**

```java
    @Test
    public void testAnUnboundedChainIsReportedRatherThanGrowingSilently() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "parquet");
        // Disable the rewrite trigger, which is what otherwise resets the chain.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "1000000");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, "1000000000000");
        assertMemoryLeak(() -> {
            createIndexedSparseKeyTable();
            for (int i = 0; i < 400; i++) {
                execute("INSERT INTO " + TABLE_NAME
                        + " VALUES ('" + INDEXED_PARTITION + "T00:00:00.00000" + (i % 9 + 1) + "Z', 's7', 1.5, 9)");
                drainWalQueue();
            }
            // Assert on the counter the warning is derived from, not on the
            // log line: LogCapture's waiters are reserved for their own PR and
            // a log assertion would couple this to message wording.
            try (TableReader reader = engine.getReader(engine.verifyTableName(TABLE_NAME))) {
                Assert.assertTrue(
                        "the fixture must actually build a long chain, or the threshold is never crossed",
                        footerCountOf(reader, 0) > 512
                );
            }
            Assert.assertTrue(
                    "an unbounded _pm chain must be reported before it becomes a problem",
                    writerWarnedAboutChainLength()
            );
        });
    }
```

- [ ] **Step 2: Run it, watch it fail**

Expected: FAIL — no warning is emitted.

- [ ] **Step 3: Emit a warning past a threshold**

In `publishParquetIndexTokens`, after the footer append, compare the chain length against a threshold and log once per partition per open:

```java
        if (footerCount > MAX_UNWARNED_PM_FOOTERS) {
            LOG.advisory().$("parquet metadata chain is long and nothing is resetting it [table=")
                    .$(tableToken).$(", partition=").$ts(partitionTimestamp)
                    .$(", footers=").$(footerCount)
                    .$("]; the O3 rewrite trigger normally resets it -- check "
                            + "cairo.partition.encoder.parquet.o3.rewrite.unused.ratio and .max.bytes").I$();
        }
```

with `private static final int MAX_UNWARNED_PM_FOOTERS = 512;`

Expose the two things the test needs, both `@TestOnly` on `TableWriter`:
`public long getPmChainWarnCount()` returning how many times this writer emitted
the warning, and — since the test also has to prove the fixture crosses the
threshold — read the chain length through the existing
`ParquetMetaFileReader.resolvePrevFooter()` walk in a test helper
`footerCountOf(TableReader reader, int partitionIndex)` placed in
`ParquetIndexSealTest`. `writerWarnedAboutChainLength()` in the test reads
`getPmChainWarnCount() > 0` off the table's writer.

A warning, not a refusal: growth is a cost, not a correctness problem, and refusing a publish would turn a slow index into a broken one.

- [ ] **Step 4: Run the test, confirm it passes**

- [ ] **Step 5: Negative control**

Raise the threshold to `Integer.MAX_VALUE` — production only. Expected: the test fails with "must be reported". Restore, confirm green, record the message.

- [ ] **Step 6: Commit**

```bash
git add core/src/main/java/io/questdb/cairo/TableWriter.java core/src/test/java/io/questdb/test/cairo/ParquetIndexSealTest.java
git commit -m "feat(idx): warn when nothing is resetting the parquet metadata chain"
```

---

### Task 7: Operator documentation

**Files:**
- Create: `docs/covering-index-parquet.md`

- [ ] **Step 1: Write the document**

It must cover, and nothing else:

- **What the flag does.** `cairo.posting.index.parquet.partition.format`, values `native` (default) and `parquet`. It describes what the **next** seal will write, never what a partition already carries — the two disagree in both directions, and dispatch keys on the published token.
- **What appears on disk.** `<col>.pidx.<indexTxn>.parquet` and `<col>.pidx.<indexTxn>._im` inside the partition directory, named by an index txn, with a token in the partition's `_pm`.
- **What it is for.** The index travels with the partition into cold storage, replication and S3 pull-back, instead of being native sidecars that a Parquet partition hard-links.
- **The downgrade constraint** (Task 1). Once a partition is sealed in parquet form, a build that predates the feature rejects its `_pm`. Convert affected partitions back to native — `ALTER TABLE <t> CONVERT PARTITION TO NATIVE LIST '<partition>'` — before downgrading.
- **Recovery from a damaged index.** The exact message an operator sees and what to do: rebuild with `DROP INDEX` then `ADD INDEX TYPE POSTING`, or convert the partition back to native.
- **What is not supported.** Covered columns must be fixed-width — the seal refuses var-size and symbol covered columns outright.

- [ ] **Step 2: Cross-check every claim against the code**

For each statement, name the file and line that establishes it. Delete anything you cannot source.

- [ ] **Step 3: Commit**

```bash
git add docs/covering-index-parquet.md
git commit -m "docs(idx): document the parquet covering index for operators"
```

---

### Task 8: Final whole-branch review and merge readiness

- [ ] **Step 1: Merge master again**

```bash
git fetch origin master && git merge origin/master --no-edit
mvn -pl core -Pbuild-rust-library test-compile
```

- [ ] **Step 2: Full gate**

Rust per crate, then the cairo/griffin/cutlass packages in chunks. Record counts from the XML.

- [ ] **Step 3: Whole-branch review**

```bash
/home/nick/.claude/plugins/cache/claude-plugins-official/superpowers/6.1.1/skills/subagent-driven-development/scripts/review-package $(git merge-base origin/master HEAD) HEAD
```

Scope it to cross-task integration and to assumptions later work invalidated — the per-task reviews already covered each diff in isolation.

- [ ] **Step 4: Copy the ledger somewhere it survives**

`.superpowers/sdd/progress.md` is git-excluded and holds every refuted premise, measurement and dead end on this branch. Attach it to the PR description or paste it into the PR body; it does not travel with the commits.

- [ ] **Step 5: Open the PR**

Title: `feat(sql): store a covering index in parquet form when its partition converts`. The body must name the four Criticals closed in the orphan-sweep predicate, the persisted purge-log format change, and the downgrade constraint from Task 1.
