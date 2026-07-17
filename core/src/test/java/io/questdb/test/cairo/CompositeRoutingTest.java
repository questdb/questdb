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
}
