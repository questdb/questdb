/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.test.cairo;

import io.questdb.cairo.CompositeDimensionTransform;
import io.questdb.cairo.MapWriter;
import io.questdb.cairo.O3PartitionPurgeJob;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.griffin.SqlException;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

/**
 * Plan 4a (composite partitioning write routing), Task 1: the per-row {@code cellKey} resolver
 * for IDENTITY dimensions, with per-commit memoization. This is deliberately NOT wired into
 * {@code processO3Block}/the WAL-apply path (that is Task 4) -- these tests drive
 * {@link TableWriter#resolveCellKey(int[])} directly as a self-contained hook, exactly mirroring
 * how the Plan 2/3 tests in this package (e.g. {@link CompositeDictPersistenceTest},
 * {@link CompositeTxCellTest}) reach {@code getCompositeDictionaries()}/{@code
 * internDimensionValue()} directly rather than through a real O3/WAL-driven {@code INSERT}.
 * <p>
 * For IDENTITY, the resolver's ordinal input is the source SYMBOL column's own resolved global
 * symbol key (Task 4 will read this straight off the column buffer at O3 time); here it is
 * obtained the same way {@link TableWriter#internDimensionValue(int, CharSequence)}'s IDENTITY
 * branch does -- {@code getSymbolMapWriter(colIndex).put(value)} -- since no rows have been
 * appended yet to derive a real key from a column buffer.
 */
public class CompositeRoutingTest extends AbstractCairoTest {

    @Test
    public void testResolveCellKeyIdentityMemoizedAndPersists() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exch symbol, x double) " +
                    "timestamp(ts) partition by day, exch wal");

            try (TableWriter w = getWriter("c")) {
                // exch is column index 1; a fresh symbol map assigns dense keys 0, 1, ... in
                // first-seen order (same guarantee CellRegistry.internCell itself documents).
                int keyA = w.getSymbolMapWriter(1).put("A");
                int keyB = w.getSymbolMapWriter(1).put("B");
                Assert.assertEquals(0, keyA);
                Assert.assertEquals(1, keyB);

                // Drive resolveCellKey for the ordinal sequence {0, 1, 0}, reusing one
                // allocation-light scratch array across calls (the shape Task 4's per-row O3
                // loop will use).
                int[] scratch = new int[1];
                int[] cellKeys = new int[3];
                int[] ordinalSequence = {keyA, keyB, keyA};
                for (int i = 0; i < ordinalSequence.length; i++) {
                    scratch[0] = ordinalSequence[i];
                    cellKeys[i] = w.resolveCellKey(scratch);
                }

                // Stable per tuple: the 3rd call (ordinal 0 again) reuses the 1st's cellKey.
                Assert.assertArrayEquals(new int[]{0, 1, 0}, cellKeys);

                // Memoized: exactly 2 distinct tuples were ever interned (0 and 1), not one memo
                // entry per call -- the 3rd call must have been a memo hit, not a fresh intern.
                Assert.assertEquals(2, w.getCellKeyMemoSize());
                Assert.assertEquals(2, w.getCompositeDictionaries().cellRegistry().size());

                // A real row, so commit() is non-empty (inTransaction() must be true for the
                // registry's symbol count to actually persist -- see
                // CompositeDictPersistenceTest's javadoc for why an isolated intern call alone
                // would never be durable).
                TableWriter.Row row = w.newRow(0);
                row.putSym(1, "A");
                row.append();
                w.commit();

                Assert.assertEquals(2, w.getCompositeDictionaries().cellRegistry().size());
                // Commit is a memo boundary: the per-commit memo is empty again afterwards.
                Assert.assertEquals(0, w.getCellKeyMemoSize());
            }

            // Reader-side confirmation the interned cells are durable across a cold reopen.
            engine.releaseInactive();
            try (TableReader r = getReader("c")) {
                Assert.assertEquals(2, r.getCompositeDictionaries().cellRegistry().size());
            }
        });
    }

    /**
     * A rollback can truncate the {@code _cell} registry back past a memoized tuple's slot (see
     * {@code CompositeDictionariesTest#testRollbackDiscardsInterns}, the pre-existing plain
     * registry-truncation behavior this test builds on). The per-commit memo must be cleared at
     * that same boundary, or a later call could hand back a cellKey the registry no longer
     * agrees with. Mirrors {@code testRollbackDiscardsInterns}'s exact idiom: a real row is
     * appended alongside the intern so {@code inTransaction()} is true and {@code rollback()}'s
     * real body (not a no-op) actually engages.
     */
    @Test
    public void testCellKeyMemoResetOnRollback() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exch symbol, x double) " +
                    "timestamp(ts) partition by day, exch wal");

            try (TableWriter w = getWriter("c")) {
                int keyA = w.getSymbolMapWriter(1).put("A");
                Assert.assertEquals(0, w.resolveCellKey(new int[]{keyA}));
                Assert.assertEquals(1, w.getCellKeyMemoSize());

                TableWriter.Row row = w.newRow(0);
                row.putSym(1, "A");
                row.append();
                w.rollback();

                Assert.assertEquals("memo must be cleared on rollback", 0, w.getCellKeyMemoSize());
                Assert.assertEquals("registry must be truncated back by rollback",
                        0, w.getCompositeDictionaries().cellRegistry().size());
            }
        });
    }

    /**
     * Gating requirement: {@code resolveCellKey} must never be reachable on a plain (non-composite)
     * table. Nothing calls it from production code yet (Task 4 wires the real per-row call), so
     * this directly exercises the defensive guard that will make any accidental future misuse on
     * a plain table fail loudly instead of NPE-ing through a null {@code getCompositeDictionaries()}.
     */
    @Test
    public void testResolveCellKeyRejectedOnPlainTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table p (ts timestamp, x double) timestamp(ts) partition by day");
            try (TableWriter w = getWriter("p")) {
                try {
                    w.resolveCellKey(new int[]{0});
                    Assert.fail("expected resolveCellKey to reject a plain (non-composite) table");
                } catch (UnsupportedOperationException e) {
                    Assert.assertTrue(e.getMessage().contains("non-composite"));
                }
            }
        });
    }

    /**
     * Arity must not be hardcoded to 1: a 2-dimension spec is a real, already-supported shape
     * (see {@code io.questdb.test.griffin.CompositePartitionParseTest#testParseTwoDimsAndOrderBy}),
     * and {@code resolveCellKey}'s packed-{@code long} memo key takes a different (2-int) path for
     * it. Reuses the exact 2-dimension table shape {@link CompositeDictionariesTest} and
     * {@link CompositeDictPersistenceTest} already exercise (identity(exchange) + truncate(symbol,3))
     * rather than inventing a new one, so this table shape is independently known to be valid.
     */
    @Test
    public void testResolveCellKeyArityTwoPacking() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, exchange symbol, symbol symbol) " +
                    "timestamp(ts) partition by day, exchange, truncate(symbol, 3) wal");

            try (TableWriter w = getWriter("t")) {
                // dim0 = identity(exchange); dim1 = truncate(symbol, 3).
                int tupleA0 = w.getSymbolMapWriter(1).put("NYSE");
                int tupleA1 = w.internDimensionValue(1, "BTCUSDT"); // truncated prefix "BTC"
                int tupleB0 = w.getSymbolMapWriter(1).put("NASDAQ");
                int tupleB1 = w.internDimensionValue(1, "ETHUSDT"); // truncated prefix "ETH"

                int cellA = w.resolveCellKey(new int[]{tupleA0, tupleA1});
                int cellB = w.resolveCellKey(new int[]{tupleB0, tupleB1});
                int cellARepeat = w.resolveCellKey(new int[]{tupleA0, tupleA1});

                Assert.assertEquals(0, cellA);
                Assert.assertEquals(1, cellB);
                Assert.assertEquals("repeated 2-dim tuple must reuse the first cellKey", cellA, cellARepeat);
                Assert.assertEquals(2, w.getCellKeyMemoSize());
                Assert.assertEquals(2, w.getCompositeDictionaries().cellRegistry().size());
            }
        });
    }

    /**
     * Task 2 (Plan 4a): {@link TableWriter#resolveDimensionOrdinal(int, int, CharSequence)} for a
     * {@code HASH} dimension -- the counterpart to Task 1's {@code resolveCellKey}, this resolves
     * ONE dimension's ordinal from a provided {@code (sourceSymbolKey, value)} pair (Task 4 will
     * source both from the WAL-segment local symbol map at O3 time; this test drives the resolver
     * directly, exactly mirroring how {@link #testResolveCellKeyIdentityMemoizedAndPersists()}
     * drives {@code resolveCellKey} directly).
     * <p>
     * Buckets are found programmatically via {@link CompositeDimensionTransform#hashBucket} itself
     * rather than hand-picked magic strings, so this test keeps discriminating even if the
     * underlying hash function ever changes.
     */
    @Test
    public void testResolveDimensionOrdinalHashDistinctAndColliding() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table h (ts timestamp, exch symbol, x double) " +
                    "timestamp(ts) partition by day, hash(exch, 4) wal");

            try (TableWriter w = getWriter("h")) {
                // dim0 = hash(exch, 4); exch is column index 1.
                final int buckets = 4;
                final String anchor = "SYM0";
                final int anchorBucket = CompositeDimensionTransform.hashBucket(anchor, buckets);
                String differentBucketValue = null;
                String sameBucketValue = null;
                for (int i = 1; i < 1000 && (differentBucketValue == null || sameBucketValue == null); i++) {
                    String candidate = "SYM" + i;
                    int bucket = CompositeDimensionTransform.hashBucket(candidate, buckets);
                    if (bucket != anchorBucket && differentBucketValue == null) {
                        differentBucketValue = candidate;
                    } else if (bucket == anchorBucket && sameBucketValue == null) {
                        sameBucketValue = candidate;
                    }
                }
                Assert.assertNotNull("expected a SYM<i> with a different hash(.,4) bucket than SYM0 within 1000 tries", differentBucketValue);
                Assert.assertNotNull("expected a SYM<i> with the same hash(.,4) bucket as SYM0 within 1000 tries", sameBucketValue);

                int keyAnchor = w.getSymbolMapWriter(1).put(anchor);
                int keyDifferent = w.getSymbolMapWriter(1).put(differentBucketValue);
                int keySame = w.getSymbolMapWriter(1).put(sameBucketValue);

                int ordinalAnchor = w.resolveDimensionOrdinal(0, keyAnchor, anchor);
                int ordinalDifferent = w.resolveDimensionOrdinal(0, keyDifferent, differentBucketValue);
                int ordinalSame = w.resolveDimensionOrdinal(0, keySame, sameBucketValue);

                Assert.assertEquals(anchorBucket, ordinalAnchor);
                Assert.assertNotEquals("differing hash(.,4) buckets must produce distinct ordinals",
                        ordinalAnchor, ordinalDifferent);
                Assert.assertEquals("colliding hash(.,4) buckets must produce the same ordinal",
                        ordinalAnchor, ordinalSame);

                // Memo proof: repeat keyAnchor but pass a DIFFERENT string (differentBucketValue,
                // engineered above to hash to a DIFFERENT bucket) -- a real memo hit must ignore
                // the new string entirely and return the first call's ordinal; if the transform
                // were actually re-invoked, this would instead return ordinalDifferent's bucket.
                int ordinalAnchorRepeat = w.resolveDimensionOrdinal(0, keyAnchor, differentBucketValue);
                Assert.assertEquals("a repeated sourceSymbolKey must return the memoized ordinal " +
                                "without recomputing the transform on the (different, ignored) string",
                        ordinalAnchor, ordinalAnchorRepeat);

                // Exactly 3 distinct (dimIndex, sourceSymbolKey) pairs were ever resolved -- the
                // repeat call must not have added a 4th memo entry.
                Assert.assertEquals(3, w.getDimensionOrdinalMemoSize());
            }
        });
    }

    /**
     * Task 2 (Plan 4a): {@link TableWriter#resolveDimensionOrdinal(int, int, CharSequence)} for a
     * {@code TRUNCATE} dimension. {@code "ABCDEF"}/{@code "ABCXYZ"} share the 3-char prefix
     * {@code "ABC"} so must resolve to the same ordinal; {@code "XYZ"} must resolve to a distinct
     * one. The memo proof uses a brand-new never-before-seen prefix ({@code "ZZZ"}) as the
     * "poisoned" repeat value: if the memo did not short-circuit, this call would freshly intern
     * "ZZZ" (growing the dedicated dictionary and returning a brand-new ordinal) instead of
     * returning the first call's memoized ordinal untouched.
     */
    @Test
    public void testResolveDimensionOrdinalTruncateSharedPrefixAndMemo() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tc (ts timestamp, sku symbol, x double) " +
                    "timestamp(ts) partition by day, truncate(sku, 3) wal");

            try (TableWriter w = getWriter("tc")) {
                // dim0 = truncate(sku, 3); sku is column index 1.
                int keyAbcdef = w.getSymbolMapWriter(1).put("ABCDEF");
                int keyAbcxyz = w.getSymbolMapWriter(1).put("ABCXYZ");
                int keyXyz = w.getSymbolMapWriter(1).put("XYZ");

                int ordinalAbcdef = w.resolveDimensionOrdinal(0, keyAbcdef, "ABCDEF");
                int ordinalAbcxyz = w.resolveDimensionOrdinal(0, keyAbcxyz, "ABCXYZ");
                Assert.assertEquals("ABCDEF/ABCXYZ share the 3-char prefix ABC -> same ordinal",
                        ordinalAbcdef, ordinalAbcxyz);

                int ordinalXyz = w.resolveDimensionOrdinal(0, keyXyz, "XYZ");
                Assert.assertNotEquals("XYZ's prefix differs from ABC -> distinct ordinal",
                        ordinalAbcdef, ordinalXyz);

                MapWriter dedicatedDict = w.getCompositeDictionaries().dedicatedDictFor(0);
                int dictSizeBeforeRepeat = dedicatedDict.getSymbolCount();
                Assert.assertEquals("exactly 2 distinct prefixes (ABC, XYZ) interned so far",
                        2, dictSizeBeforeRepeat);
                Assert.assertEquals(3, w.getDimensionOrdinalMemoSize());

                // Memo proof: repeat keyAbcdef's sourceSymbolKey with "ZZZ" -- a brand-new,
                // never-before-seen prefix. A real memo hit ignores it and returns ordinalAbcdef
                // untouched; a recompute would freshly intern "ZZZ", growing the dict to 3 and
                // returning a new ordinal.
                int ordinalAbcdefRepeat = w.resolveDimensionOrdinal(0, keyAbcdef, "ZZZ");
                Assert.assertEquals("a repeated sourceSymbolKey must return the memoized ordinal, " +
                                "ignoring a different (here, brand-new) string",
                        ordinalAbcdef, ordinalAbcdefRepeat);
                Assert.assertEquals("dedicated dictionary must not grow on a memoized repeat " +
                                "even though the ignored string (\"ZZZ\") is a brand-new prefix",
                        dictSizeBeforeRepeat, dedicatedDict.getSymbolCount());
                Assert.assertEquals("repeat must not add a 4th memo entry",
                        3, w.getDimensionOrdinalMemoSize());
            }
        });
    }

    /**
     * Task 4 (Plan 4a, the CRUX task): the ACCEPTANCE TEST. Ends dormancy -- rows for two exchanges
     * across two days, all in ONE commit, must physically route into 4 distinct on-disk cell
     * directories (2 exchanges x 2 days), not the single dormant cellKey-0 directory Plan 1-3b left
     * every row in. Compares a composite table {@code c} (partition by day, exch) against an
     * identically-populated plain twin {@code p} (partition by day) throughout, mirroring {@link
     * CompositeEndToEndTest}'s own twin-comparison idiom.
     * <p>
     * Day 2 (the chronologically-last day in this one commit) is where a naive implementation would
     * be most tempted to use the writer's append fast path for both exchanges' rows -- exactly the
     * scenario {@code processO3BlockComposite}'s own docs identify as unsafe to do naively (the
     * writer's single shared open-column-file-handle set can only point at one cell's files at a
     * time), so this test exercises that exact case, not just an easier all-archival scenario.
     */
    @Test
    public void testMultiCellCommitRoutesToFourCellDirectoriesAndMatchesPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            // Explicit WAL: a same-order VALUES insert on a BYPASS WAL table (the harness default,
            // confirmed empirically -- CompositeEndToEndTest's own SHOW CREATE TABLE output shows
            // "BYPASS WAL" for a bare CREATE TABLE) never reaches processO3Block at all -- strictly
            // increasing timestamps take TableWriter#newRow's direct ROW_ACTION_SWITCH_PARTITION path
            // (switchPartition/openPartition), never o3Commit. A WAL table's apply, by contrast, always
            // funnels through processWalCommitFinishApply -> processO3Block regardless of row order
            // (see processO3BlockComposite's own docs) -- the actual mechanism Task 4 rewrites.
            execute("create table c (ts timestamp, exch symbol, px double) timestamp(ts) partition by day, exch wal");
            execute("create table p (ts timestamp, exch symbol, px double) timestamp(ts) partition by day wal");

            // Two exchanges, two days, deliberately INTERLEAVED (A, B, A, B) within one commit so the
            // O3 sorted-by-timestamp range for EACH day genuinely spans both cellKeys -- the multi-cell
            // regrouping path, not just the single-cellKey fast path.
            final String rows = " values " +
                    "('2020-01-01T00:00:00.000000Z','A',1.0), ('2020-01-01T12:00:00.000000Z','B',1.5), " +
                    "('2020-01-02T00:00:00.000000Z','A',2.0), ('2020-01-02T12:00:00.000000Z','B',2.5)";
            execute("insert into c" + rows);
            execute("insert into p" + rows);
            drainWalQueue();

            engine.releaseInactive(); // cold reopen -- no pooled reader/writer may mask a fresh self-detect

            // 1. PHYSICAL: 4 cell directories on disk -- exch=A and exch=B under EACH of the 2 days,
            // not 2 bare day directories and not everything collapsed into one dormant cell.
            TableToken tableToken = engine.verifyTableName("c");
            FilesFacade ff = configuration.getFilesFacade();
            Assert.assertEquals(
                    "day 2020-01-01 must contain exactly the two cell directories, no dormant leftover",
                    setOf("exch=A", "exch=B"),
                    listCellDirNames(ff, tableToken, "2020-01-01"));
            Assert.assertEquals(
                    "day 2020-01-02 must contain exactly the two cell directories, no dormant leftover",
                    setOf("exch=A", "exch=B"),
                    listCellDirNames(ff, tableToken, "2020-01-02"));

            // 2. LOGICAL: c must match p row-for-row -- full scan, count, and a per-exchange filter.
            assertSqlCursors("select ts, exch, px from p order by ts, exch", "select ts, exch, px from c order by ts, exch");
            assertQuery("select count() from c").noLeakCheck().noRandomAccess().expectSize().returns("count\n4\n");
            assertSqlCursors("select count() from p", "select count() from c");
            assertSqlCursors(
                    "select ts, exch, px from p where exch = 'A' order by ts",
                    "select ts, exch, px from c where exch = 'A' order by ts");
            assertQuery("select count() from c where exch = 'A'").noLeakCheck().noRandomAccess().expectSize().returns("count\n2\n");

            // 3. CATALOGUE: table_partitions() row count reflects the CELL count (4), not the day
            // count (2) -- TxReader.getPartitionCount() is a bare attachedPartitions.size() /
            // longsPerAttachedPartition, already stride/cellKey-aware since Plan 3; this is the direct
            // proof real routing now populates 4 distinct (ts, cellKey) attached-partition records.
            assertQuery("select count() from table_partitions('c')").noLeakCheck().noRandomAccess().expectSize().returns("count\n4\n");
        });
    }

    /**
     * Plan 4a Task 5 (per-cell frontiers). This used to be the first of two dedicated regression tests
     * for Task 4's loud second-commit guard ({@code guardCompositeSecondCommitNotYetSupported}, removed
     * by this task) and was originally written expecting a second commit landing on the table's CURRENT
     * last day to route correctly. It does NOT: this specific commit revisits (extends) day2's ALREADY-
     * populated cellA from commit 1, and that shape reproduces a genuine native heap corruption (glibc
     * "malloc(): invalid size (unsorted)"), not just a bookkeeping nit -- see this task's own report.
     * Per the project's safety rule, a new, NARROWER guard ({@code dispatchCompositeCellRange}'s own
     * {@code srcDataMax > 0} check) now blocks exactly this shape, loudly, while every OTHER
     * repeated-commit shape this task proved safe (new day, new cell on an existing or single-cell day,
     * out-of-order backfill into a brand-new earlier day -- see the other tests in this class) is
     * unaffected. This test now proves the guard, not success.
     */
    @Test
    public void testSecondCommitExtendingExistingCellThrowsInsteadOfSilentlyMisrouting() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exch symbol, px double) timestamp(ts) partition by day, exch wal");
            execute("create table p (ts timestamp, exch symbol, px double) timestamp(ts) partition by day wal");

            // Commit 1: multi-day, multi-cell -- spans a partition boundary, defeating the WAL-LAG
            // fast path and forcing real per-cell routing (mirrors the acceptance test's own shape).
            final String rows1 = " values " +
                    "('2020-01-01T00:00:00.000000Z','A',1.0), ('2020-01-01T12:00:00.000000Z','B',1.5), " +
                    "('2020-01-02T00:00:00.000000Z','A',2.0), ('2020-01-02T12:00:00.000000Z','B',2.5)";
            execute("insert into c" + rows1);
            execute("insert into p" + rows1);
            drainWalQueue();
            assertWalTableNotSuspended("c");

            // Commit 2: ONE small, in-order row landing on day2 (the CURRENT last day), which ALREADY
            // has two cells (A, B) from commit 1 -- revisits (extends) the existing cellA partition
            // rather than creating a new one -- the guarded shape.
            execute("insert into c values ('2020-01-02T18:00:00.000000Z','A',3.0)");
            execute("insert into p values ('2020-01-02T18:00:00.000000Z','A',3.0)");
            drainWalQueue();

            // c must be suspended with the new, narrower guard's clear message -- NOT silently wrong,
            // NOT a native crash. p (plain) is completely unaffected.
            assertWalTableSuspendedWithMessage("c", "does not yet support a commit that extends an already-populated cell");
            assertWalTableNotSuspended("p");
            engine.releaseInactive();
            assertQuery("select count() from p").noLeakCheck().noRandomAccess().expectSize().returns("count\n5\n");
        });
    }

    /**
     * Same shape as {@link #testSecondCommitSameLastDayRoutesCorrectly()}, but the second commit's row
     * lands on a BRAND NEW day (never touched by commit 1 at all) -- proves per-cell frontier tracking
     * is correct for a genuinely new day too, not just a revisit of an existing cell.
     */
    @Test
    public void testSecondCommitNewDayRoutesCorrectly() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exch symbol, px double) timestamp(ts) partition by day, exch wal");
            execute("create table p (ts timestamp, exch symbol, px double) timestamp(ts) partition by day wal");

            final String rows1 = " values " +
                    "('2020-01-01T00:00:00.000000Z','A',1.0), ('2020-01-01T12:00:00.000000Z','B',1.5), " +
                    "('2020-01-02T00:00:00.000000Z','A',2.0), ('2020-01-02T12:00:00.000000Z','B',2.5)";
            execute("insert into c" + rows1);
            execute("insert into p" + rows1);
            drainWalQueue();
            assertWalTableNotSuspended("c");

            // Commit 2: a single row on day3 -- a day neither cell has ever touched before.
            execute("insert into c values ('2020-01-03T00:00:00.000000Z','A',3.0)");
            execute("insert into p values ('2020-01-03T00:00:00.000000Z','A',3.0)");
            drainWalQueue();

            assertWalTableNotSuspended("c");
            assertWalTableNotSuspended("p");
            engine.releaseInactive();
            assertTablesMatch("c", "p");
        });
    }

    /**
     * Plan 4a Task 5's ORIGINAL acceptance-test shape, as the dispatch specified it verbatim: commit 1
     * populates day1 with both cells; commit 2 -- a SEPARATE {@code insert}/{@code drainWalQueue} --
     * adds MORE rows to day1's two EXISTING cells AND rows for a brand-new day2. This does NOT route
     * correctly: "more rows for day1's two EXISTING cells" is precisely the "extends an already-
     * populated cell" shape {@link #testSecondCommitExtendingExistingCellThrowsInsteadOfSilentlyMisrouting()}
     * documents -- a real native heap corruption, not a bookkeeping nit -- so per the project's safety
     * rule this whole commit is now blocked loudly rather than left to silently corrupt (the guard
     * fires on day1's cellA block before day2's brand-new cells are ever reached, so the "new day2 cells"
     * half of this scenario is separately proven safe by {@link #testMultiCommitAddsSecondCellToSingleCellDayMatchesPlainTwin()}
     * and {@link #testMultiCommitOutOfOrderEarlierDayMatchesPlainTwin()}, just not combined with a
     * same-commit cell-extension the way the dispatch's own example combined them). This test now
     * documents that gap explicitly rather than silently passing or silently corrupting.
     */
    @Test
    public void testMultiCommitExtendingExistingCellsThrowsInsteadOfSilentlyMisrouting() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exch symbol, px double) timestamp(ts) partition by day, exch wal");
            execute("create table p (ts timestamp, exch symbol, px double) timestamp(ts) partition by day wal");

            final String rows1 = " values " +
                    "('2020-01-01T00:00:00.000000Z','A',1.0), ('2020-01-01T12:00:00.000000Z','B',1.5)";
            execute("insert into c" + rows1);
            execute("insert into p" + rows1);
            drainWalQueue();

            // Commit 2: a SEPARATE insert + drainWalQueue (not a continuation of commit 1's WAL
            // segment) -- more rows for day1's two EXISTING cells (A, B) AND rows for a brand-new
            // day2, itself with two brand-new cells (A, B). The "day1" half of this is the guarded
            // shape.
            final String rows2 = " values " +
                    "('2020-01-01T06:00:00.000000Z','A',1.1), ('2020-01-01T18:00:00.000000Z','B',1.6), " +
                    "('2020-01-02T00:00:00.000000Z','A',2.0), ('2020-01-02T12:00:00.000000Z','B',2.5)";
            execute("insert into c" + rows2);
            execute("insert into p" + rows2);
            drainWalQueue();

            assertWalTableSuspendedWithMessage("c", "does not yet support a commit that extends an already-populated cell");
            assertWalTableNotSuspended("p");
            engine.releaseInactive();
            // p: 2 rows from commit 1 + 4 rows from commit 2 = 6, fully committed and unaffected.
            assertQuery("select count() from p").noLeakCheck().noRandomAccess().expectSize().returns("count\n6\n");
        });
    }

    /**
     * The out-of-order variant the dispatch specifically asked for: commit 2 targets a day EARLIER
     * than every day commit 1 touched (a backfill), rather than the current/new tail. Exercises the
     * {@code partitionTimestamp < trackedTailPartitionTimestamp} branch of the per-cell frontier fix
     * with a REAL second commit (that branch was never actually unsafe pre-fix, but this proves the
     * table's {@code lastPartitionTimestamp}/{@code partitionTimestampHi} bookkeeping -- which commit 1
     * leaves correct only once the split/cell conflation in {@code TxWriter#getNextPartitionTimestamp}
     * is fixed -- stays correct across an out-of-order commit too, not just a chronologically-advancing
     * one).
     */
    @Test
    public void testMultiCommitOutOfOrderEarlierDayMatchesPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exch symbol, px double) timestamp(ts) partition by day, exch wal");
            execute("create table p (ts timestamp, exch symbol, px double) timestamp(ts) partition by day wal");

            // Commit 1: day2 (the LATER day) first, both cells.
            final String rows1 = " values " +
                    "('2020-01-02T00:00:00.000000Z','A',1.0), ('2020-01-02T12:00:00.000000Z','B',1.5)";
            execute("insert into c" + rows1);
            execute("insert into p" + rows1);
            drainWalQueue();

            // Commit 2: day1 -- EARLIER than commit 1's day2 -- a backfill, both cells.
            final String rows2 = " values " +
                    "('2020-01-01T00:00:00.000000Z','A',2.0), ('2020-01-01T12:00:00.000000Z','B',2.5)";
            execute("insert into c" + rows2);
            execute("insert into p" + rows2);
            drainWalQueue();

            assertWalTableNotSuspended("c");
            assertWalTableNotSuspended("p");
            engine.releaseInactive();

            assertPerDayExchCountsMatch("2020-01-01", "2020-01-02");
            assertTablesMatch("c", "p");
        });
    }

    /**
     * A third shape: day1 starts with only ONE cell (A) from commit 1; commit 2 adds cellB to that SAME
     * existing day (a brand-new cell on a day that previously had only a single cell) AND a brand-new
     * day2. Distinguishes "day already had 2+ cells" (the other multi-commit tests above) from "day had
     * exactly 1 cell and gains its 2nd" -- both must accumulate correctly rather than overwrite.
     */
    @Test
    public void testMultiCommitAddsSecondCellToSingleCellDayMatchesPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exch symbol, px double) timestamp(ts) partition by day, exch wal");
            execute("create table p (ts timestamp, exch symbol, px double) timestamp(ts) partition by day wal");

            execute("insert into c values ('2020-01-01T00:00:00.000000Z','A',1.0)");
            execute("insert into p values ('2020-01-01T00:00:00.000000Z','A',1.0)");
            drainWalQueue();

            final String rows2 = " values " +
                    "('2020-01-01T12:00:00.000000Z','B',1.5), " +
                    "('2020-01-02T00:00:00.000000Z','A',2.0), ('2020-01-02T12:00:00.000000Z','B',2.5)";
            execute("insert into c" + rows2);
            execute("insert into p" + rows2);
            drainWalQueue();

            assertWalTableNotSuspended("c");
            assertWalTableNotSuspended("p");
            engine.releaseInactive();

            assertPerDayExchCountsMatch("2020-01-01", "2020-01-02");
            assertTablesMatch("c", "p");
        });
    }

    /**
     * Plan 4b Task 1: distinguishes the {@code canAppendOnly} sub-shape of the guarded "extend an
     * already-populated cell" case from the genuine-merge sub-shape below. Commit 2 lands a single row
     * on day2's already-populated cellA, IN ORDER (strictly after cellA's one existing row) -- the
     * {@code O3PartitionJob#processPartition}'s {@code srcDataMax >= 1} branch would take the
     * {@code canAppendOnly}/{@code OPEN_MID_PARTITION_FOR_APPEND} path (append after existing data, no
     * merge, no directory rewrite) if it ran. Plan 4b Task 1's investigation root-caused and fixed two
     * independent bugs that made exactly this shape corrupt the native heap (see
     * {@code TableWriter#o3ConsumePartitionUpdateSink}'s and {@code TxWriter#beginPartitionSizeUpdate}'s
     * own updated docs) -- this specific sub-shape (pure append, no directory rewrite) is now provably
     * safe end to end (verified directly with the guard temporarily removed: no crash, correct data,
     * byte-for-byte match with the plain twin, across repeated fresh-JVM runs -- see the task's own
     * report). It still throws here because the guard fires uniformly on {@code srcDataMax > 0} before
     * dispatch can know whether the shape will end up append-only or a genuine merge -- see the
     * out-of-order test below for why the guard cannot yet be narrowed to "only genuine merges."
     */
    @Test
    public void testSecondCommitExtendingExistingCellInOrderAppendThrowsInsteadOfSilentlyMisrouting() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exch symbol, px double) timestamp(ts) partition by day, exch wal");
            execute("create table p (ts timestamp, exch symbol, px double) timestamp(ts) partition by day wal");

            final String rows1 = " values " +
                    "('2020-01-01T00:00:00.000000Z','A',1.0), ('2020-01-01T12:00:00.000000Z','B',1.5), " +
                    "('2020-01-02T00:00:00.000000Z','A',2.0), ('2020-01-02T12:00:00.000000Z','B',2.5)";
            execute("insert into c" + rows1);
            execute("insert into p" + rows1);
            drainWalQueue();
            assertWalTableNotSuspended("c");

            // Commit 2: ONE row, in-order (18:00 > cellA's existing 00:00), on day2's already-populated
            // cellA -- would be a pure append after existing data (canAppendOnly), no merge, if the
            // guard did not block it first.
            execute("insert into c values ('2020-01-02T18:00:00.000000Z','A',3.0)");
            execute("insert into p values ('2020-01-02T18:00:00.000000Z','A',3.0)");
            drainWalQueue();

            assertWalTableSuspendedWithMessage("c", "does not yet support a commit that extends an already-populated cell");
            assertWalTableNotSuspended("p");
            engine.releaseInactive();
            assertQuery("select count() from p").noLeakCheck().noRandomAccess().expectSize().returns("count\n5\n");
        });
    }

    /**
     * Plan 4b Task 1: the genuine-merge sub-shape. Day2's cellA gets TWO rows in commit 1 (00:00,
     * 12:00); commit 2 lands a single row at 06:00 -- strictly BETWEEN cellA's two existing rows --
     * which would force {@code O3PartitionJob#processPartition}'s {@code srcDataMax >= 1} branch to
     * take a genuine {@code O3_BLOCK_MERGE} (not {@code canAppendOnly}), i.e.
     * {@code OPEN_MID_PARTITION_FOR_MERGE} -- a directory-version rewrite, queuing the old cellA
     * directory version for removal. This is still guarded (unlike the pure-append sub-shape above):
     * Plan 4b Task 1's investigation found and fixed the two bugs that were the guard's own documented
     * proximate cause (both also cover this sub-shape -- verified directly, no crash, correct merged
     * row content, with the guard temporarily removed), but surfaced a THIRD, independent, and more
     * severe bug specifically in the post-merge directory-purge step: {@code
     * TableWriter#processPartitionRemoveCandidates0} resolves the physical path of the OLD (now
     * superseded) directory version via the cell-BLIND 5-arg {@code setPathForNativePartition} overload
     * (no {@code cellSegment}), which for a composite table can resolve to the bare, multi-cell DAY
     * directory instead of the one cell's own subdirectory -- risking deleting sibling cells' still-live
     * data, not just corrupting bookkeeping. That bug is broader than this task's own scope (the same
     * cell-blind {@code partitionRemoveCandidates} queue also feeds TTL eviction, TRUNCATE, writer-open/
     * rollback cleanup, and automatic split-squash housekeeping -- none of those are guarded today
     * either) and needs its own dedicated fix, not a rushed one here. Per the project's safety rule, the
     * guard therefore stays for this sub-shape too, loudly, rather than risk shipping a merge path that
     * can delete a sibling cell's directory. See the task's own report for the full root-cause chain and
     * evidence.
     */
    @Test
    public void testSecondCommitExtendingExistingCellOutOfOrderMergeThrowsInsteadOfSilentlyMisrouting() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exch symbol, px double) timestamp(ts) partition by day, exch wal");
            execute("create table p (ts timestamp, exch symbol, px double) timestamp(ts) partition by day wal");

            final String rows1 = " values " +
                    "('2020-01-01T00:00:00.000000Z','A',1.0), ('2020-01-01T12:00:00.000000Z','B',1.5), " +
                    "('2020-01-02T00:00:00.000000Z','A',2.0), ('2020-01-02T12:00:00.000000Z','A',2.2), " +
                    "('2020-01-02T18:00:00.000000Z','B',2.5)";
            execute("insert into c" + rows1);
            execute("insert into p" + rows1);
            drainWalQueue();
            assertWalTableNotSuspended("c");

            // Commit 2: ONE row at 06:00 -- strictly between cellA's existing 00:00 and 12:00 rows on
            // day2 -- would be a genuine out-of-order merge into the existing cell (directory rewrite),
            // not a pure append, if the guard did not block it first.
            execute("insert into c values ('2020-01-02T06:00:00.000000Z','A',2.1)");
            execute("insert into p values ('2020-01-02T06:00:00.000000Z','A',2.1)");
            drainWalQueue();

            assertWalTableSuspendedWithMessage("c", "does not yet support a commit that extends an already-populated cell");
            assertWalTableNotSuspended("p");
            engine.releaseInactive();
            assertQuery("select count() from p").noLeakCheck().noRandomAccess().expectSize().returns("count\n6\n");
        });
    }

    /**
     * Direct, targeted proof of the {@code TxReader#getNextPartitionTimestamp} split/cell-conflation
     * fix, isolated from {@code finishO3Commit}'s separate fix (which stopped calling that method with
     * an exact existing-day floor entirely, for a different reason -- see this task's report): commit 1
     * gives day1 TWO existing cells (A, B) so a THIRD, brand-new cell's row landing at EXACTLY day1's
     * floor timestamp in commit 2 forces {@code processO3BlockComposite}'s own outer range-finding call
     * ({@code getCurrentPartitionMaxTimestamp(o3Timestamp)}, used to bound {@code srcOooHi} -- a
     * DIRECT-ASSIGNMENT call this task's {@code finishO3Commit} fix does not touch, unlike the
     * Math.max-guarded end-of-method update) to search a day with 2 existing same-floor entries. Without
     * this task's fix that call returns day1's OWN floor (conflating cellB's sibling entry for a
     * genuine split), i.e. a ceiling BEFORE this row's own timestamp -- which starves the bounded binary
     * search into an empty range and (verified directly, before this fix existed) silently drops this
     * row from dispatch entirely: not a crash, not an exception, just a vanished row -- table {@code c}
     * commits "successfully" one row short of its plain twin. This is the sharpest, most concrete
     * evidence that this fix belongs in the composite gate, not just the plain-degenerate no-op the
     * rest of the composite dispatch's own tests happen to exercise.
     */
    @Test
    public void testNewCellAtExactDayFloorExercisesGetNextPartitionTimestampFix() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exch symbol, px double) timestamp(ts) partition by day, exch wal");
            execute("create table p (ts timestamp, exch symbol, px double) timestamp(ts) partition by day wal");

            // Commit 1: day1 gets TWO existing cells (A, B) -- the precondition the conflation bug
            // needs (a lone cell isn't enough: advancing past it lands exactly at the array's end,
            // which already falls through to the correct ceil path regardless of this fix).
            execute("insert into c values ('2020-01-01T00:00:00.000000Z','A',1.0), ('2020-01-01T12:00:00.000000Z','B',1.5)");
            execute("insert into p values ('2020-01-01T00:00:00.000000Z','A',1.0), ('2020-01-01T12:00:00.000000Z','B',1.5)");
            drainWalQueue();

            // Commit 2: ONE row, brand-new cellC, at EXACTLY day1's floor timestamp -- the same exact
            // instant as cellA's existing entry, with day1 already home to 2 cells.
            execute("insert into c values ('2020-01-01T00:00:00.000000Z','C',9.0)");
            execute("insert into p values ('2020-01-01T00:00:00.000000Z','C',9.0)");
            drainWalQueue();

            assertWalTableNotSuspended("c");
            assertWalTableNotSuspended("p");
            engine.releaseInactive();
            assertTablesMatch("c", "p");
        });
    }

    /**
     * Whole-branch review (Plan 4a) finding C1: {@code O3PartitionPurgeJob} walks a table's root BY
     * DAY ONLY, probing each day at cellKey 0 ({@code findAttachedPartitionRawIndexByLoTimestamp} is
     * the cellKey-0 delegate of {@code findAttachedPartitionRawIndexBy(ts, cellKey)}). For a REAL
     * composite table where a day's ONLY cell is NOT cellKey 0, that probe returns &lt;0 (not found)
     * even though the day IS attached (just under a different cellKey), so
     * {@code O3PartitionPurgeJob#processPartition} misclassifies the whole day directory as DETACHED
     * and recursively deletes it via {@code purgePartition}/{@code ff.unlinkOrRemove} -- silent,
     * permanent data loss for every row {@code _txn} still references there.
     * <p>
     * Day1 gets exch='A' in commit 1 (the first-ever interned dimension value -> cellKey 0). Day2 -- a
     * brand-new day commit 1 never touched -- gets exch='B' in commit 2 (interned second -> cellKey 1),
     * so day2's ONLY attached-partition entry is cellKey 1, with NO cellKey-0 entry at that day at all:
     * exactly the trigger shape. This is the brand-new-day/brand-new-cell commit shape Task 5 already
     * proved safe to ROUTE (see {@link #testSecondCommitNewDayRoutesCorrectly()}), so it reaches the
     * purge job cleanly, with no unrelated guard (e.g. the extend-existing-cell throw) in the way.
     * <p>
     * RED (pre-fix): day2's {@code exch=B} directory -- and its one row -- is deleted; {@code count()}
     * drops from 2 to 1. GREEN (post-fix): {@code O3PartitionPurgeJob} skips the whole table (gated
     * {@code txReader.getLongsPerAttachedPartition() > LONGS_PER_TX_ATTACHED_PARTITION}), day2 survives
     * untouched, both rows still read back correctly.
     */
    @Test
    public void testO3PartitionPurgeJobDoesNotDeleteNonCellZeroDay() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exch symbol, px double) timestamp(ts) partition by day, exch wal");

            execute("insert into c values ('2020-01-01T00:00:00.000000Z','A',1.0)");
            drainWalQueue();
            assertWalTableNotSuspended("c");

            // Brand-new day, brand-new (2nd-ever-interned) cell -- day2's ONLY entry ends up at
            // cellKey 1, never cellKey 0.
            execute("insert into c values ('2020-01-02T00:00:00.000000Z','B',2.0)");
            drainWalQueue();
            assertWalTableNotSuspended("c");

            assertQuery("select count() from c").noLeakCheck().noRandomAccess().expectSize().returns("count\n2\n");

            runPurgeJobDirectly("c");

            // day2's exch=B cell directory (its only cell, cellKey 1) must survive the purge -- the
            // bug recursively deletes the whole day, taking every cell (here, day2's only row) with it.
            TableToken tableToken = engine.verifyTableName("c");
            Assert.assertEquals(
                    "day2's exch=B cell directory must survive the purge (no cellKey-0 entry at that day)",
                    setOf("exch=B"),
                    listCellDirNames(configuration.getFilesFacade(), tableToken, "2020-01-02"));

            assertQuery("select ts, exch, px from c order by ts").timestamp("ts").noLeakCheck().expectSize().returns(
                    "ts\texch\tpx\n" +
                            "2020-01-01T00:00:00.000000Z\tA\t1.0\n" +
                            "2020-01-02T00:00:00.000000Z\tB\t2.0\n");
            Assert.assertEquals("0 partition purge errors expected", 0, engine.getPartitionOverwriteControl().getErrorCount());
        });
    }

    /**
     * Negative control for {@link #testO3PartitionPurgeJobDoesNotDeleteNonCellZeroDay()}: every day
     * here DOES have a cellKey-0 entry (the interned value "A" is the first, and only, dimension value
     * ever interned in this table, so it is always cellKey 0), so the PRE-FIX day-blind probe finds a
     * match at every day and never misclassifies anything as detached -- nothing is deleted, with or
     * without the fix. This proves the repro test's RED result is specific to the cellKey-0-absent
     * shape, not an artifact of this harness/test idiom always losing data regardless of setup.
     */
    @Test
    public void testO3PartitionPurgeJobKeepsDayWhenCellZeroPresent() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exch symbol, px double) timestamp(ts) partition by day, exch wal");

            execute("insert into c values ('2020-01-01T00:00:00.000000Z','A',1.0)");
            drainWalQueue();
            // Brand-new day, but the SAME (already cellKey-0) exch value -- day2's only entry is ALSO
            // cellKey 0, unlike the repro test above.
            execute("insert into c values ('2020-01-02T00:00:00.000000Z','A',2.0)");
            drainWalQueue();
            assertWalTableNotSuspended("c");

            assertQuery("select count() from c").noLeakCheck().noRandomAccess().expectSize().returns("count\n2\n");

            runPurgeJobDirectly("c");

            TableToken tableToken = engine.verifyTableName("c");
            Assert.assertEquals(
                    setOf("exch=A"),
                    listCellDirNames(configuration.getFilesFacade(), tableToken, "2020-01-02"));
            assertQuery("select ts, exch, px from c order by ts").timestamp("ts").noLeakCheck().expectSize().returns(
                    "ts\texch\tpx\n" +
                            "2020-01-01T00:00:00.000000Z\tA\t1.0\n" +
                            "2020-01-02T00:00:00.000000Z\tA\t2.0\n");
            Assert.assertEquals("0 partition purge errors expected", 0, engine.getPartitionOverwriteControl().getErrorCount());
        });
    }

    /**
     * Directly enqueues (bypassing the reader-release/scoreboard timing that would normally trigger
     * it -- irrelevant to what these tests probe) and fully drains an {@link O3PartitionPurgeJob} run
     * for {@code tableName}, mirroring how {@code TableWriter}/{@code TableReader}/
     * {@code DatabaseCheckpointAgent} themselves call {@link TableUtils#schedulePurgeO3Partitions}.
     */
    private void runPurgeJobDirectly(String tableName) throws Exception {
        TableToken tableToken = engine.verifyTableName(tableName);
        int timestampType;
        int partitionBy;
        try (TableReader r = getReader(tableName)) {
            timestampType = r.getMetadata().getTimestampType();
            partitionBy = r.getMetadata().getPartitionBy();
        }
        engine.releaseInactive();

        try (O3PartitionPurgeJob purgeJob = new O3PartitionPurgeJob(engine, 1)) {
            Assert.assertTrue(
                    "expected a purge task to be queued for " + tableName,
                    TableUtils.schedulePurgeO3Partitions(engine.getMessageBus(), tableToken, timestampType, partitionBy));
            purgeJob.drain(0);
        }
    }

    private void assertWalTableNotSuspended(String tableName) {
        Assert.assertFalse(
                tableName + " must not be suspended",
                engine.getTableSequencerAPI().isSuspended(engine.verifyTableName(tableName)));
    }

    private void assertWalTableSuspendedWithMessage(String tableName, String expectedMessageSubstring) throws Exception {
        Assert.assertTrue(
                tableName + " must be suspended after the not-yet-supported commit",
                engine.getTableSequencerAPI().isSuspended(engine.verifyTableName(tableName)));
        assertQuery("select suspended, errorMessage like '%" + expectedMessageSubstring + "%' clearMessage " +
                "from wal_tables() where name = '" + tableName + "'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("suspended\tclearMessage\ntrue\ttrue\n");
    }

    /**
     * Per-(day, exch) count parity between {@code c} and {@code p}, for every combination of the given
     * ISO day strings crossed with exchanges A and B -- the exact granularity the dispatch asked for,
     * finer than a table-wide {@code count()} (which could still coincidentally match even if two
     * cells' rows were swapped between each other).
     * <p>
     * Deliberately filters on {@code to_str(ts, 'yyyy-MM-dd')} rather than a {@code ts >= day and ts <
     * day+1} range: the latter is recognized by the SQL optimiser as a prunable interval and hits a
     * PRE-EXISTING, out-of-scope bug in composite-table interval/partition-frame scanning that silently
     * returns zero rows for a day whose cell(s) were not the table's most-recently-appended partition --
     * reproduced directly (this task's own diagnostic) even for a composite table's ORIGINAL, already-
     * green, single-commit acceptance-test data (day1 then day2 in one commit, no second commit involved
     * at all), so it predates this task and is unrelated to per-cell frontiers. {@code to_str(...)} is an
     * opaque per-row function the optimiser cannot fold into an interval, so it falls back to a plain
     * filtered scan -- which this task's own diagnostics confirm reads composite data correctly -- and
     * so measures exactly what this method is meant to measure without tripping over that separate gap.
     * A plain table (like {@code p}) is unaffected by that gap either way (also confirmed directly); the
     * same predicate shape is used on both sides here purely so the two queries stay textually parallel.
     */
    private void assertPerDayExchCountsMatch(String... isoDays) throws SqlException {
        for (String day : isoDays) {
            for (String exch : new String[]{"A", "B"}) {
                String predicate = " where to_str(ts, 'yyyy-MM-dd') = '" + day + "' and exch = '" + exch + "'";
                assertSqlCursors("select count() from p" + predicate, "select count() from c" + predicate);
            }
        }
    }

    /**
     * Full-table parity between {@code c} and {@code p}: ordered scan, table-wide count, per-exchange
     * count, and {@code LATEST ON} -- the exact assertions the dispatch's acceptance test named.
     */
    private void assertTablesMatch(String composite, String plain) throws SqlException {
        assertSqlCursors("select ts, exch, px from " + plain + " order by ts, exch", "select ts, exch, px from " + composite + " order by ts, exch");
        assertSqlCursors("select count() from " + plain, "select count() from " + composite);
        assertSqlCursors("select exch, count() from " + plain + " order by exch", "select exch, count() from " + composite + " order by exch");
        assertSqlCursors(
                "select ts, exch, px from " + plain + " latest on ts partition by exch order by exch",
                "select ts, exch, px from " + composite + " latest on ts partition by exch order by exch");
    }

    private static Set<String> setOf(String... values) {
        Set<String> set = new HashSet<>();
        for (String v : values) {
            set.add(v);
        }
        return set;
    }

    /**
     * Lists the immediate child directory names of {@code <dbRoot>/<tableToken>/<dayDirName>},
     * stripping each entry's trailing {@code .<nameTxn>} version suffix (e.g. {@code "exch=A.3"} ->
     * {@code "exch=A"}) so the result is comparable regardless of the exact nameTxn a real commit
     * happened to assign. Mirrors {@code ShowPartitionsRecordCursorFactory#scanDetachedAndAttachablePartitions}'s
     * own {@code ff.findFirst/findName/findType/findNext/findClose} idiom.
     */
    private static Set<String> listCellDirNames(FilesFacade ff, TableToken tableToken, String dayDirName) {
        Set<String> names = new HashSet<>();
        try (Path path = new Path()) {
            path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat(dayDirName).$();
            long pFind = ff.findFirst(path.$());
            Assert.assertTrue("expected day directory to exist: " + path, pFind > 0L);
            try {
                StringSink nameSink = new StringSink();
                do {
                    nameSink.clear();
                    long name = ff.findName(pFind);
                    Utf8s.utf8ToUtf16Z(name, nameSink);
                    int type = ff.findType(pFind);
                    if (type == Files.DT_DIR && !Chars.equals(nameSink, ".") && !Chars.equals(nameSink, "..")) {
                        String entry = nameSink.toString();
                        int dot = entry.lastIndexOf('.');
                        names.add(dot > -1 ? entry.substring(0, dot) : entry);
                    }
                } while (ff.findNext(pFind) > 0);
            } finally {
                ff.findClose(pFind);
            }
        }
        return names;
    }
}
