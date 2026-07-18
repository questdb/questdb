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
import io.questdb.cairo.TableToken;
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
     * Task 8 (Plan 2): originally, a composite dimension pinned its source SYMBOL column by stable
     * WRITER index ({@link io.questdb.cairo.PartitionDimension#getColumnIndex()}); dropping that column
     * would leave the dimension dangling, so this test proved {@code removeColumn} rejected ONLY the
     * dimension-source columns ({@code symbol}, {@code exchange}) while a mixed drop sequence still let
     * ordinary columns ({@code foo}, {@code price}) through -- the DROP-side mirror of
     * {@link #testAddSymbolColumnRejectedOnComposite()}.
     * <p>
     * <b>Superseded by Plan 4b's feature-gate sweep (see {@code TableWriter#removeColumn}'s own updated
     * comment, and commit {@code 0fe4ff70db}).</b> {@code removeColumnFiles}'s purge ({@code
     * PurgingOperator}/{@code ColumnPurgeOperator}) resolves a dropped column's per-cell physical files
     * via cellKey-0-only/bare-path lookups with zero composite awareness anywhere -- confirmed reachable:
     * DROP COLUMN silently leaked a routed cell's files. DROP COLUMN is therefore now rejected
     * UNCONDITIONALLY for any real (non-dormant) composite table (any {@code dimensionCount > 0} table
     * that is not a pre-existing, never-routed legacy one -- see {@code isDormantWithPreexistingData()}),
     * BEFORE the narrower dimension/cluster-column-reference guard below it is ever reached. That makes
     * the original "dimension-source columns only" distinction this test drew unreachable for a live
     * composite table: EVERY column drop is rejected now, {@code foo}/{@code price} included, not just
     * {@code symbol}/{@code exchange}. Confirmed this is the intended, correct behaviour (not a bug):
     * the gate is deliberate, evidenced by an empirical negative control in its own commit message, and
     * consistent with every other cell-blind "does not yet support X" gate added across Plan 4a/4b.
     * <p>
     * Updated to assert the blanket gate fires uniformly, dimension source or not, keeping the same
     * mixed-order drop sequence as before -- still a useful regression test that the gate applies without
     * exception, at the {@code TableWriter} API level this class otherwise exercises. The narrower
     * dimension/cluster-source-specific guard this test used to isolate remains independently reachable
     * and covered by {@link #testDropClusterOrderByColumnRejected()} (a zero-dimension, cluster-only
     * composite table, which the blanket gate's {@code dimensionCount > 0} check does not fire on); the
     * blanket gate itself is also covered end-to-end via SQL by {@code
     * CompositeUnsupportedOpsTest#testDropColumnGated}.
     */
    @Test
    public void testDropDimensionSourceColumnRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, foo double, exchange symbol, symbol symbol, price double) " +
                    "timestamp(ts) partition by day, exchange, truncate(symbol, 3) wal");
            try (TableWriter w = getWriter("t")) {
                // Every column drop is now rejected by the blanket Plan 4b gate, dimension source or
                // not -- unlike the pre-Plan-4b behaviour this test originally documented, where foo/price
                // (non-dimension columns) were allowed through (see class Javadoc above).
                for (String column : new String[]{"foo", "symbol", "exchange", "price"}) {
                    try {
                        w.removeColumn(column);
                        Assert.fail("DROP COLUMN " + column + " must be rejected on a real composite table");
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "composite partitioning does not yet support DROP COLUMN");
                    }
                }
                Assert.assertTrue("foo must still be present -- its drop was rejected", w.getMetadata().getColumnIndexQuiet("foo") >= 0);
                Assert.assertTrue("price must still be present -- its drop was rejected", w.getMetadata().getColumnIndexQuiet("price") >= 0);
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

    /**
     * Whole-branch review finding I1: composite DDL guards ({@link #testAddSymbolColumnRejectedOnComposite()}
     * et al.) live only on the apply-side {@code TableWriter}. {@code WalWriter} validates ALTER
     * statements synchronously too, via a separate {@code MetadataValidatorService} -- but it shares
     * {@code WalWriterMetadata metadata} with the apply-side {@code MetadataWriterService}, and {@code
     * WalWriterMetadata} (unlike {@code TableWriterMetadata}/{@code TableReaderMetadata}) implements
     * only {@code TableRecordMetadata}/{@code TableRecordMetadataSink} -- neither of which extends
     * {@code TableStructure}, the interface that declares {@code getPartitionSpec()} (default {@code
     * PartitionSpec.EMPTY}, overridden by {@code TableWriterMetadata}/{@code TableReaderMetadata} via
     * {@code TableMetadata extends TableRecordMetadata, TableStructure} to return the real composite
     * spec). {@code WalWriterMetadata} has no {@code getPartitionSpec()} at all, so neither of {@code
     * WalWriter}'s {@code MetadataServiceStub} implementations can evaluate the composite guard,
     * synchronously or otherwise.
     * <p>
     * Confirmed empirically (this test originally asserted the aspirational synchronous-rejection
     * behavior via try/catch/fail, mirroring {@link #testAddSymbolColumnRejectedOnComposite()}, and
     * that assertion FAILED -- the ALTER returned normally with no exception): {@code ALTER TABLE ...
     * ADD COLUMN x SYMBOL} against a composite WAL table is accepted synchronously (recorded into the
     * WAL) and only rejected later, when {@code ApplyWal2TableJob} applies the transaction through the
     * real {@code TableWriter} -- which suspends the table instead of the client ever seeing a
     * synchronous error. This is materially worse than the non-WAL behavior and is TICKETED rather
     * than fixed here: plumbing the composite spec into {@code WalWriterMetadata} (reading the same
     * additive {@code _meta} block {@code TableWriterMetadata}/{@code TableReaderMetadata} already
     * read) is a real structural change, not a guard one-liner, and is out of scope for this pass.
     * <p>
     * This test intentionally documents the CURRENT (undesirable) suspend behavior rather than the
     * aspirational synchronous-rejection one, so it fails loudly -- forcing a look at this Javadoc --
     * the day either {@code WalWriterMetadata} gains spec access (and should reject synchronously
     * instead) or the suspend mechanism itself changes.
     */
    @Test
    public void testWalCompositeAddSymbolSuspendsRatherThanRejectsSynchronously() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, exchange symbol, symbol symbol) " +
                    "timestamp(ts) partition by day, exchange, truncate(symbol, 3) wal");

            // No synchronous rejection today (I1): the ALTER "succeeds" from the client's perspective...
            execute("alter table t add column x symbol");

            // ...but the queued transaction fails at WAL-apply time through the real TableWriter guard,
            // suspending the table instead.
            drainWalQueue();
            Assert.assertTrue(
                    "expected table to be suspended after WAL-apply rejects the composite ADD COLUMN SYMBOL",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("t"))
            );
        });
    }

    /**
     * Companion to {@link #testWalCompositeAddSymbolSuspendsRatherThanRejectsSynchronously()} for DROP
     * COLUMN of a dimension source column -- same root cause (I1), same ticketed-not-fixed status: the
     * DROP is accepted synchronously and only the WAL-apply-side {@code TableWriter.removeColumn}
     * guard rejects it, suspending the table.
     */
    @Test
    public void testWalCompositeDropDimensionSourceSuspendsRatherThanRejectsSynchronously() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, exchange symbol, symbol symbol) " +
                    "timestamp(ts) partition by day, exchange, truncate(symbol, 3) wal");

            execute("alter table t drop column exchange");     // no synchronous rejection today (I1)

            drainWalQueue();
            Assert.assertTrue(
                    "expected table to be suspended after WAL-apply rejects the composite DROP COLUMN",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("t"))
            );
        });
    }
}
