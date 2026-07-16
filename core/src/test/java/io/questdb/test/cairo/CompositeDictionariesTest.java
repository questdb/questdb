/*******************************************************************************
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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CompositeDictionaries;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Task 5 (Plan 2): the composite dedicated dictionaries and the {@code _cell} registry are
 * first-class {@code _txn} symbol maps. Two halves of one invariant:
 * <ul>
 *     <li><b>Part A</b> (create path): the initial {@code _txn} counts the dedicated dicts + the
 *     registry, so its symbol-count region reserves a zero-count slot for each.</li>
 *     <li><b>Part B</b> (writer open): the writer registers those interners into
 *     {@code denseSymbolMapWriters}, in layout order (dedicated dicts by dimension, then the
 *     registry), and exposes them through {@link CompositeDictionaries}.</li>
 * </ul>
 * A plain (non-composite) table has no interners: its {@code _txn} is unchanged and
 * {@code getCompositeDictionaries()} is null.
 */
public class CompositeDictionariesTest extends AbstractCairoTest {

    @Test
    public void testInitialTxnCountsInterners() throws Exception {          // Part A
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, exchange symbol, symbol symbol) " +
                    "timestamp(ts) partition by day, exchange, truncate(symbol, 3) wal");
            // 2 real SYMBOL cols (exchange, symbol) + 1 dedicated dict (truncate) + 1 registry = 4
            try (TableReader r = getReader("t")) {
                Assert.assertEquals(4, r.getTxFile().getSymbolColumnCount());
            }
        });
    }

    @Test
    public void testPlainTableRegistersNoInterners() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table p (ts timestamp, s symbol) timestamp(ts) partition by day wal");
            try (TableWriter w = getWriter("p")) {
                Assert.assertNull(w.getCompositeDictionaries());        // no interners for a plain table
                Assert.assertEquals(1, w.getDenseSymbolMapCount());     // exactly the 1 SYMBOL column
            }
            try (TableReader r = getReader("p")) {
                Assert.assertEquals(1, r.getTxFile().getSymbolColumnCount()); // _txn unchanged for plain
            }
        });
    }

    @Test
    public void testWriterRegistersDedicatedInternersInOrder() throws Exception {   // Part B
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, exchange symbol, symbol symbol) " +
                    "timestamp(ts) partition by day, exchange, truncate(symbol, 3) wal");
            try (TableWriter w = getWriter("t")) {
                // dim0 = identity(exchange) [reuses column dict, no dedicated]; dim1 = truncate(symbol,3) [dedicated]
                CompositeDictionaries d = w.getCompositeDictionaries();
                Assert.assertNotNull(d);
                Assert.assertNull(d.dedicatedDictFor(0));               // identity -> no dedicated dict
                Assert.assertNotNull(d.dedicatedDictFor(1));            // truncate -> dedicated dict
                Assert.assertNotNull(d.cellRegistry());
                Assert.assertEquals(2 + 2, w.getDenseSymbolMapCount()); // 2 real symbols + dict + registry
            }
        });
    }

    /**
     * Reviewer-mandated CRITICAL fix: appending a new per-column {@link io.questdb.cairo.SymbolMapWriter}
     * after the interners (as {@code ADD COLUMN ... SYMBOL} does today) desyncs the {@code _txn}
     * symbol-count slot order on the next writer reopen ({@code TableWriter.configureColumnMemory()}
     * always rebuilds as {@code [realSymbols..., x, dedicatedDicts..., registry]}), corrupting counts
     * silently. Until ordering is fixed (later plan), this must be rejected outright on
     * composite-partitioned tables.
     */
    @Test
    public void testAddSymbolColumnRejectedOnComposite() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, exchange symbol, symbol symbol) " +
                    "timestamp(ts) partition by day, exchange, truncate(symbol, 3) wal");
            try (TableWriter w = getWriter("t")) {
                try {
                    w.addColumn("x", ColumnType.SYMBOL, AllowAllSecurityContext.INSTANCE);
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "composite");
                }
            }
        });
    }

    /**
     * Companion to {@link #testAddSymbolColumnRejectedOnComposite()}: the guard must be narrow.
     * Non-symbol ADD COLUMN never touches {@code denseSymbolMapWriters} ordering, so it stays safe
     * and allowed on composite-partitioned tables.
     */
    @Test
    public void testAddNonSymbolColumnAllowedOnComposite() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, exchange symbol, symbol symbol) " +
                    "timestamp(ts) partition by day, exchange, truncate(symbol, 3) wal");
            try (TableWriter w = getWriter("t")) {
                w.addColumn("y", ColumnType.LONG, AllowAllSecurityContext.INSTANCE);
                Assert.assertTrue(w.getMetadata().getColumnIndexQuiet("y") >= 0);
            }
        });
    }

    /**
     * Task 7 (Plan 2): write-side dimension value interning. {@code IDENTITY} must reuse the source
     * column's own symbol map (same ordinal as calling {@code put} on the column directly, not a
     * separate dict), and {@code HASH} must produce a pure bucket in {@code [0, param)} with no
     * dictionary involved.
     */
    @Test
    public void testInternDimensionValueIdentityReuseAndHash() throws Exception {   // writer
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, exchange symbol, symbol symbol) " +
                    "timestamp(ts) partition by day, exchange, hash(symbol, 8) wal");
            try (TableWriter w = getWriter("t")) {
                int viaCol = w.getSymbolMapWriter(1).put("NYSE");         // exchange col idx 1
                int viaDim = w.internDimensionValue(0, "NYSE");           // identity(exchange)
                Assert.assertEquals(viaCol, viaDim);                     // identity reuses the column dict
                int h = w.internDimensionValue(1, "BTC");                // hash(symbol, 8)
                Assert.assertTrue(h >= 0 && h < 8);
            }
        });
    }

    /**
     * Regression for {@link io.questdb.cairo.CompositeDimensionTransform#hashBucket}: a bug caught in
     * review passed {@code buckets} straight through as {@link io.questdb.std.Hash#boundedHash}'s
     * bitmask argument instead of reducing into range with {@link Math#floorMod(int, int)}. That
     * bitmask bug is invisible for a power-of-two bucket count -- e.g. {@code hash(symbol, 8)} above,
     * where {@code 8} is {@code 0b1000} and the buggy AND can only ever produce {@code 0} or {@code 8}
     * (the latter already happening to be caught by nothing, since that test only checks one value) --
     * so this uses a non-power-of-two count instead: {@code 7} is {@code 0b111}, a full 3-bit mask, so
     * the buggy form spreads uniformly over {@code {0..7}} inclusive, and {@code 7} itself is out of
     * the required half-open {@code [0, 7)} range. Interning many distinct values makes hitting that
     * out-of-range bucket empirically certain under the buggy form (confirmed via negative control:
     * of "SYM0".."SYM63", 7 of the 64 land on bucket 7 under the bitmask bug).
     */
    @Test
    public void testInternDimensionValueHashNonPowerOfTwoBucketsInRange() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, symbol symbol) " +
                    "timestamp(ts) partition by day, hash(symbol, 7) wal");
            try (TableWriter w = getWriter("t")) {
                for (int i = 0; i < 64; i++) {
                    String v = "SYM" + i;
                    int h = w.internDimensionValue(0, v);                // hash(symbol, 7)
                    Assert.assertTrue("hash " + h + " out of range for " + v, h >= 0 && h < 7);
                }
            }
        });
    }

    /**
     * Task 8 (Plan 2): byte-identity for a plain (non-composite) table -- both writer and reader
     * sides. Companion to {@link #testPlainTableRegistersNoInterners()} (writer-only) and
     * {@link CompositeDictPersistenceTest#testPlainTableReaderHasNoCompositeDictionaries()}
     * (reader-only): this consolidates both into one round-trip so a regression that nulls one side
     * but not the other cannot slip through either existing test alone.
     */
    @Test
    public void testPlainTableNoInternersTxnByteIdentical() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table p (ts timestamp, a symbol, b symbol) timestamp(ts) partition by day wal");
            try (TableWriter w = getWriter("p")) {
                Assert.assertNull(w.getCompositeDictionaries());      // no interners for a plain table
                Assert.assertEquals(2, w.getDenseSymbolMapCount());   // exactly the 2 SYMBOL columns
            }
            try (TableReader r = getReader("p")) {
                Assert.assertNull(r.getCompositeDictionaries());
                Assert.assertEquals(2, r.getTxFile().getSymbolColumnCount()); // _txn symbol region unchanged vs pre-feature
            }
        });
    }

    /**
     * Task 8 (Plan 2): a composite dimension pins its source SYMBOL column by stable WRITER index
     * ({@link io.questdb.cairo.PartitionDimension#getColumnIndex()}). Dropping that column would leave
     * the dimension dangling, so {@code removeColumn} must reject it -- the DROP-side mirror of
     * {@link #testAddSymbolColumnRejectedOnComposite()}.
     * <p>
     * This drops a lower-index non-dimension column ({@code foo}) first before attempting the
     * dimension-source drops, to exercise the guard across a mixed drop sequence rather than in
     * isolation. <b>Verified empirically (negative control) that this ordering does NOT, in the
     * current codebase, make dense position diverge from writer index within a live
     * {@code TableWriter}</b>: {@code TableWriterMetadata.removeColumn} only tombstones a column in
     * place ({@code markDeleted()}), it never renumbers survivors, and {@code addColumn} always
     * assigns a fresh slot via {@code metadata.getColumnCount()} -- so a column's dense position and
     * its {@code getWriterIndex()} are the same value for the life of a writer instance regardless of
     * how many other columns are dropped first. Temporarily swapping the guard's writer-index lookup
     * for the plain dense {@code index} still passed this exact test (confirmed by re-running it under
     * that swap). The dense/writer divergence {@link io.questdb.cairo.PartitionDimension}'s javadoc
     * warns about is real, but it is a reader/metadata-cache-side phenomenon
     * ({@code TableReaderMetadata} compacts tombstoned columns out of its dense list on reload via
     * {@code buildColumnListFromMetadataFile}, while {@code writerIndex} is preserved) -- not something
     * reachable from {@code TableWriter.removeColumn} today. The guard still resolves via
     * {@code getWriterIndex()} rather than the raw dense index: that is the documented, self-explaining
     * contract for comparing against {@code PartitionDimension.getColumnIndex()}, matches the existing
     * {@code tombstoneCoveredColumnInOtherIndexes} call one line below, and remains correct by
     * construction rather than by this incidental writer-side invariant. This test therefore stands as
     * a solid behavioral regression test (reject dimension sources, allow everything else, across a
     * mixed-order drop sequence) rather than as a proven discriminator of a reachable dense-vs-writer
     * bug.
     */
    @Test
    public void testDropDimensionSourceColumnRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, foo double, exchange symbol, symbol symbol, price double) " +
                    "timestamp(ts) partition by day, exchange, truncate(symbol, 3) wal");
            try (TableWriter w = getWriter("t")) {
                // Drop a lower-index non-dimension column first to exercise a mixed drop sequence (see
                // class Javadoc above for why this does not, in fact, diverge dense from writer index).
                w.removeColumn("foo");                                // foo = double, writer idx 1, non-dim -> allowed
                try { w.removeColumn("symbol"); Assert.fail("dropping truncate-dim source must be rejected"); }
                catch (CairoException e) { TestUtils.assertContains(e.getFlyweightMessage(), "composite"); }
                try { w.removeColumn("exchange"); Assert.fail("dropping identity-dim source must be rejected"); }
                catch (CairoException e) { TestUtils.assertContains(e.getFlyweightMessage(), "composite"); }
                w.removeColumn("price");                              // non-dimension double -> allowed
                Assert.assertTrue(w.getMetadata().getColumnIndexQuiet("price") < 0);
            }
        });
    }

    /**
     * Task 8 (Plan 2): companion to {@link #testDropDimensionSourceColumnRejected()} for the other
     * half of a composite {@link io.questdb.cairo.PartitionSpec} -- cluster (ORDER BY) columns. A
     * cluster column is pinned by stable WRITER index ({@link io.questdb.cairo.PartitionSpec#getClusterColumn(int)}),
     * same as a dimension source; dropping it would leave the persisted partition spec dangling (SHOW
     * CREATE renders cluster columns by writer index), so {@code removeColumn} must reject it the same
     * way. This table has zero partition dimensions and one cluster column -- a cluster-only composite
     * table (see class Javadoc of {@link CompositeDictPersistenceTest}, which documents this shape as
     * composite via {@link io.questdb.cairo.PartitionSpec#isComposite()} even with no dimensions) -- to
     * prove the guard doesn't accidentally depend on a non-empty dimension list.
     */
    @Test
    public void testDropClusterOrderByColumnRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, exchange symbol, price double) " +
                    "timestamp(ts) partition by day order by exchange wal");
            try (TableWriter w = getWriter("t")) {
                try { w.removeColumn("exchange"); Assert.fail("dropping an ORDER BY/cluster column must be rejected"); }
                catch (CairoException e) { TestUtils.assertContains(e.getFlyweightMessage(), "composite"); }
                w.removeColumn("price");                                  // non-cluster column -> allowed
                Assert.assertTrue(w.getMetadata().getColumnIndexQuiet("price") < 0);
            }
        });
    }
}
