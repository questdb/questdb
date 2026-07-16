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

import io.questdb.cairo.CompositeInternerLayout;
import io.questdb.cairo.PartitionDimension;
import io.questdb.cairo.PartitionSpec;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Task 3 (Plan 2): CREATE TABLE for a composite table must provision, at the table root, a
 * dedicated on-disk symbol-map dictionary for every dimension whose transform needs one ({@code
 * TRUNCATE}/{@code EXPRESSION}), plus the cell registry ({@code _cell}) symbol-map. These files are
 * dormant infrastructure in this plan -- later tasks open and populate them -- but must exist on
 * disk from the moment the table is created.
 * <p>
 * A plain table (no composite dimensions) must provision neither. The gate is
 * {@link CompositeInternerLayout#hasInterners()}, not {@link PartitionSpec#isComposite()}: a
 * cluster-only table (zero partition dimensions, {@code ORDER BY} clustering only) is composite but
 * has no dimension tuple, so it also provisions nothing (covered by the empty layout, not
 * re-asserted here -- see the plain-table case below, which is the {@code hasInterners()==false}
 * path shared by both).
 */
public class CompositeDictPersistenceTest extends AbstractCairoTest {

    @Test
    public void testCompositeCreatesDictAndRegistryFiles() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, exchange symbol, symbol symbol) " +
                    "timestamp(ts) partition by day, exchange, truncate(symbol, 3) wal");

            TableToken tt = engine.verifyTableName("t");
            CompositeInternerLayout layout;
            try (TableMetadata m = engine.getTableMetadata(tt)) {
                PartitionSpec spec = m.getPartitionSpec();
                layout = CompositeInternerLayout.of(spec);

                // locate the truncate(symbol, 3) dimension by kind rather than assuming its
                // ordinal position, so the test does not silently rot if dimension order changes.
                int truncateDimIdx = -1;
                for (int i = 0, n = spec.getDimensionCount(); i < n; i++) {
                    if (spec.getDimension(i).getKind() == PartitionDimension.KIND_TRUNCATE) {
                        truncateDimIdx = i;
                        break;
                    }
                }
                Assert.assertTrue("expected a truncate() dimension in the spec", truncateDimIdx >= 0);
                Assert.assertTrue(layout.hasInterners());
                Assert.assertTrue(layout.needsDedicatedDict(truncateDimIdx));

                FilesFacade ff = configuration.getFilesFacade();
                try (Path path = new Path()) {
                    path.of(configuration.getDbRoot()).concat(tt);
                    int plen = path.size();

                    // dedicated dict for truncate(symbol, 3): name/txn derived from the layout, not
                    // hardcoded, so a change to alias-naming or txn allocation doesn't break this test.
                    Assert.assertTrue(ff.exists(TableUtils.offsetFileName(
                            path.trimTo(plen),
                            layout.dictName(truncateDimIdx),
                            layout.dictColumnNameTxn(truncateDimIdx)
                    )));

                    // cell registry
                    Assert.assertTrue(ff.exists(TableUtils.offsetFileName(
                            path.trimTo(plen),
                            CompositeInternerLayout.REGISTRY_NAME,
                            CompositeInternerLayout.REGISTRY_TXN
                    )));
                }
            }
        });
    }

    @Test
    public void testPlainTableCreatesNoRegistryFile() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table p (ts timestamp, s symbol) timestamp(ts) partition by day wal");

            TableToken tt = engine.verifyTableName("p");
            FilesFacade ff = configuration.getFilesFacade();
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(tt);
                int plen = path.size();

                // a plain table's layout is EMPTY (hasInterners() == false): no registry provisioned.
                Assert.assertFalse(ff.exists(TableUtils.offsetFileName(
                        path.trimTo(plen),
                        CompositeInternerLayout.REGISTRY_NAME,
                        CompositeInternerLayout.REGISTRY_TXN
                )));
            }
        });
    }

    /**
     * Task 6 (Plan 2): the reader-side mirror of {@link CompositeDictionariesTest}'s writer-side
     * registration. The registry symbol count only becomes durable on {@code commit()}; forcing the
     * reader to reopen from disk (via {@code engine.releaseInactive()}) proves the reader opens its
     * own {@link io.questdb.cairo.SymbolMapReaderImpl} over the {@code _cell} file rather than reusing
     * writer state, and reverse-looks-up the interned ordinal back to the same tuple.
     * <p>
     * A row is appended alongside the {@code internCell} call: {@code TableWriter.commit()} is gated
     * on {@code inTransaction()} (row/O3/column-version activity) and is a no-op when the only change
     * is a raw registry {@code MapWriter.put} with zero rows appended, which would never persist any
     * symbol count -- not specific to the composite registry, and not this task's concern to change.
     * A real row makes the commit non-empty, so {@code storeSymbolCounts} flushes every dense symbol
     * writer's count, including the registry's.
     */
    @Test
    public void testReaderReadsRegistryAndDictAfterReopen() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, exchange symbol, symbol symbol) " +
                    "timestamp(ts) partition by day, exchange, truncate(symbol, 3) wal");
            int ord;
            try (TableWriter w = getWriter("t")) {
                ord = w.getCompositeDictionaries().cellRegistry().internCell(new int[]{3, 4}, 2);
                TableWriter.Row row = w.newRow(0);
                row.append();
                w.commit();                       // persist the registry symbol count into _txn
            }
            engine.releaseInactive();             // force reader to re-open from disk
            try (TableReader r = getReader("t")) {
                int[] out = new int[2];
                r.getCompositeDictionaries().cellRegistry().getTuple(ord, out);
                Assert.assertArrayEquals(new int[]{3, 4}, out);
            }
        });
    }

    /**
     * Companion to {@link #testReaderReadsRegistryAndDictAfterReopen()}: a plain (non-composite)
     * table's reader must not open any interner readers.
     */
    @Test
    public void testPlainTableReaderHasNoCompositeDictionaries() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table p (ts timestamp, s symbol) timestamp(ts) partition by day wal");
            try (TableReader r = getReader("p")) {
                Assert.assertNull(r.getCompositeDictionaries());
            }
        });
    }

    /**
     * Task 7 (Plan 2): read-side dimension value interning for a {@code TRUNCATE} dimension. Two
     * different values sharing the same {@code N}-char prefix must intern to the same dense key on
     * the write side, and that key must survive a writer-to-reader round trip after reopen -- both
     * looking a fresh value up by its prefix ({@code keyOfDimensionValue}) and reversing a key back
     * to its interned prefix ({@code valueOfDimensionKey}).
     * <p>
     * Mirrors {@link #testReaderReadsRegistryAndDictAfterReopen()}'s row-append-to-persist idiom: a
     * real row is appended so {@code commit()} is non-empty and actually flushes the dedicated dict's
     * symbol count (see that test's Javadoc for why an isolated intern alone would not persist).
     */
    @Test
    public void testTruncateDimInternsPrefixAndReaderKeyOf() throws Exception {     // truncate + reader round-trip
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, symbol symbol) " +
                    "timestamp(ts) partition by day, truncate(symbol, 3) wal");
            int key;
            try (TableWriter w = getWriter("t")) {
                key = w.internDimensionValue(0, "BTCUSDT");              // truncate dim0 -> prefix "BTC"
                Assert.assertEquals(key, w.internDimensionValue(0, "BTCETH")); // same prefix -> same key
                TableWriter.Row row = w.newRow(0);                       // a real row so commit() persists (see Task 6 nuance)
                row.putSym(1, "BTCUSDT");
                row.append();
                w.commit();
            }
            engine.releaseInactive();
            try (TableReader r = getReader("t")) {
                Assert.assertEquals(key, r.keyOfDimensionValue(0, "BTCZZZ")); // "BTC" prefix -> same key
                TestUtils.assertEquals("BTC", r.valueOfDimensionKey(0, key));
            }
        });
    }

    /**
     * Task 8 (Plan 2): the core "trust {@code _txn}, not files" crash-safety guarantee. An
     * {@code internCell} call alone (no row appended) never makes {@link TableWriter#inTransaction()}
     * true, so {@link TableWriter#commit()} -- called implicitly on close, and explicitly everywhere
     * else in this file -- takes its {@code !inTransaction()} short-circuit and never reaches
     * {@code storeSymbolCounts}. The registry's on-disk {@code .o}/{@code .c} files may already carry
     * the interned value, but the durable {@code _txn} symbol count for that slot is never bumped.
     * Forcing a reopen from disk (via {@code engine.releaseInactive()}) proves the reader trusts only
     * the persisted {@code _txn} count, not whatever the underlying symbol-map files happen to contain.
     */
    @Test
    public void testUncommittedInternsDiscardedOnReopen() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, exchange symbol, symbol symbol) " +
                    "timestamp(ts) partition by day, exchange, truncate(symbol, 3) wal");
            try (TableWriter w = getWriter("t")) {
                w.getCompositeDictionaries().cellRegistry().internCell(new int[]{1, 2}, 2);
            }                                                        // writer closed WITHOUT commit
            engine.releaseInactive();
            try (TableReader r = getReader("t")) {
                // _txn registry count was never advanced -> reopen sees zero (uncommitted intern gone)
                Assert.assertEquals(0, r.getCompositeDictionaries().cellRegistry().size());
            }
        });
    }

    /**
     * Companion to {@link #testUncommittedInternsDiscardedOnReopen()}: this time a real row is
     * appended alongside the {@code internCell} call, so {@link TableWriter#inTransaction()} is true
     * and {@link TableWriter#rollback()} actually engages (rather than being a no-op over an empty
     * transaction). {@code rollbackSymbolTables} truncates every dense symbol map writer -- including
     * composite interner slots, which it addresses purely by dense position, agnostic to whether a
     * slot backs a real column or an interner -- back to the count last recorded in {@code _txn} (zero,
     * since nothing was ever committed), discarding both the row and the interned cell in one motion.
     */
    @Test
    public void testRollbackDiscardsInterns() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, exchange symbol, symbol symbol) " +
                    "timestamp(ts) partition by day, exchange, truncate(symbol, 3) wal");
            try (TableWriter w = getWriter("t")) {
                w.getCompositeDictionaries().cellRegistry().internCell(new int[]{1, 2}, 2);
                TableWriter.Row row = w.newRow(0);                   // a real row -> inTransaction() true, so rollback engages
                row.putSym(1, "AAA");
                row.putSym(2, "BBB");
                row.append();
                w.rollback();                                        // discard the whole transaction (row + interns)
                Assert.assertEquals(0, w.getCompositeDictionaries().cellRegistry().size());  // registry truncated to _txn count 0
            }
        });
    }
}
