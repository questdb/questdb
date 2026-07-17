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
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
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
                    "timestamp(ts) partition by day, exch");

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
                    "timestamp(ts) partition by day, exch");

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
                    "timestamp(ts) partition by day, hash(exch, 4)");

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
                    "timestamp(ts) partition by day, truncate(sku, 3)");

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
