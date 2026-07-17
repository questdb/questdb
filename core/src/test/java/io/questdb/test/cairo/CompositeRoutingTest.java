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
import io.questdb.cairo.TableWriter;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

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
}
