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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.PartitionDimension;
import io.questdb.cairo.PartitionSpec;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Composite partitioning: round-trip coverage of the additive, minor-version-gated composite
 * {@code _meta} block. Task 5 wrote the block; Task 6 reads it back through the real metadata
 * readers ({@code TableReaderMetadata} / {@code TableWriterMetadata} / {@code MetadataCache}) via
 * {@link TableMetadata#getPartitionSpec()} and re-emits it on ALTER, so the assertions below drive
 * the production reader rather than parsing raw bytes.
 * <p>
 * The on-disk composite block, appended after the covering-index section only when the spec is
 * composite, is:
 * <pre>
 *   [i8  namingMode]
 *   [i32 dimensionCount]
 *     repeated dimensionCount times:
 *       [i8  kind]
 *       [i32 columnIndex]
 *       [i32 param]
 *       [str alias]     // 4-byte length prefix then len*2 UTF-16 bytes; len == -1 => null
 *       [str exprText]  // same; null (len == -1) for identity/hash/truncate
 *   [i32 clusterColumnCount]
 *     repeated clusterColumnCount times:
 *       [i32 columnIndex]
 * </pre>
 */
public class CompositeMetaFormatTest extends AbstractCairoTest {

    @Test
    public void testAlterPreservesCompositeSpec() throws Exception {
        // Proves the rewriteMetadata re-emit: a structural ALTER rewrites _meta, and the composite
        // block (dropped by the pre-Task-6 code path) must now survive so getPartitionSpec() still
        // reports the same dimensions after the table's metadata is re-read from disk.
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, exchange symbol) timestamp(ts) partition by day, exchange wal");
            execute("alter table t add column px double");
            drainWalQueue();
            engine.releaseInactive(); // force a fresh _meta read (the rewritten file)

            TableToken tableToken = engine.verifyTableName("t");
            try (TableMetadata m = engine.getTableMetadata(tableToken)) {
                Assert.assertEquals(3, m.getColumnCount()); // ts, exchange, px
                PartitionSpec spec = m.getPartitionSpec();
                Assert.assertTrue("composite spec must survive ADD COLUMN", spec.isComposite());
                Assert.assertEquals(PartitionBy.DAY, spec.getTimeUnit());
                Assert.assertEquals(1, spec.getDimensionCount());
                Assert.assertEquals(PartitionDimension.KIND_IDENTITY, spec.getDimension(0).getKind());
                Assert.assertEquals("exchange", spec.getDimension(0).getAlias());
                Assert.assertNull(spec.getDimension(0).getExprText());
            }

            // A second structural ALTER (DROP of the non-key column) rewrites _meta again; the block
            // must still survive, and the dimension's stable writer index (exchange=1) is unaffected.
            execute("alter table t drop column px");
            drainWalQueue();
            engine.releaseInactive();

            try (TableMetadata m = engine.getTableMetadata(tableToken)) {
                Assert.assertEquals(2, m.getColumnCount()); // ts, exchange
                PartitionSpec spec = m.getPartitionSpec();
                Assert.assertTrue("composite spec must survive DROP COLUMN", spec.isComposite());
                Assert.assertEquals(1, spec.getDimensionCount());
                Assert.assertEquals(1, spec.getDimension(0).getColumnIndex());
                Assert.assertEquals("exchange", spec.getDimension(0).getAlias());
            }

            // Regression: a plain-table ALTER must still work and stay non-composite.
            execute("create table p (ts timestamp, s symbol) timestamp(ts) partition by day wal");
            execute("alter table p add column px double");
            drainWalQueue();
            engine.releaseInactive();

            try (TableMetadata m = engine.getTableMetadata(engine.verifyTableName("p"))) {
                Assert.assertEquals(3, m.getColumnCount()); // ts, s, px
                Assert.assertFalse(m.getPartitionSpec().isComposite());
            }
        });
    }

    @Test
    public void testCompositeBlockRoundTripAfterCoveringSection() throws Exception {
        // A table that is BOTH composite AND has a covering index writes the covering-index section
        // and THEN the composite block into the same additive _meta tail; the reader must walk past
        // the covering section to find the block. Proves the two sections coexist byte-correctly.
        assertMemoryLeak(() -> {
            final PartitionSpec spec = new PartitionSpec();
            spec.setTimeUnit(PartitionBy.DAY);
            spec.setNamingMode(PartitionSpec.MODE_HIVE);
            spec.addDimension(new PartitionDimension(PartitionDimension.KIND_IDENTITY, 1, 0, "exchange", null));

            final IntList covering = new IntList();
            covering.add(0);
            covering.add(2);

            TableModel model = new TableModel(configuration, "d", PartitionBy.DAY) {
                @Override
                public IntList getCoveringColumnIndices(int columnIndex) {
                    return columnIndex == 1 ? covering : null;
                }

                @Override
                public PartitionSpec getPartitionSpec() {
                    return spec;
                }
            };
            model.timestamp("ts").col("exchange", ColumnType.SYMBOL).col("symbol", ColumnType.SYMBOL);

            try (MemoryCARW mem = Vm.getCARWInstance(4096, 8, MemoryTag.NATIVE_DEFAULT)) {
                TableUtils.writeMetadata(model, ColumnType.VERSION, 7, mem);

                PartitionSpec readBack = new PartitionSpec();
                TableUtils.readCompositePartitionSpec(mem, readBack);

                Assert.assertTrue(readBack.isComposite());
                Assert.assertEquals(PartitionBy.DAY, readBack.getTimeUnit());
                Assert.assertEquals(1, readBack.getDimensionCount());
                PartitionDimension d0 = readBack.getDimension(0);
                Assert.assertEquals(PartitionDimension.KIND_IDENTITY, d0.getKind());
                Assert.assertEquals(1, d0.getColumnIndex());
                Assert.assertEquals("exchange", d0.getAlias());
                Assert.assertNull(d0.getExprText());
                Assert.assertEquals(0, readBack.getClusterColumnCount());
            }
        });
    }

    @Test
    public void testCompositeMetaRaisesMinorAndReadsBackSpec() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, exchange symbol, symbol symbol) " +
                    "timestamp(ts) partition by day, exchange, hash(symbol, 16) wal");

            TableToken tableToken = engine.verifyTableName("t");
            try (Path path = new Path(); MemoryMR mem = openMeta(path, tableToken)) {
                // A composite table raises the minor version to the composite gate so that OLD
                // readers (which validate the checksum, unchanged, then compare >= their requested
                // version) will not attempt to parse the new trailing block.
                Assert.assertEquals(
                        TableUtils.META_FORMAT_MINOR_VERSION_COMPOSITE_PARTITIONING,
                        Numbers.decodeHighShort(mem.getInt(TableUtils.META_OFFSET_META_FORMAT_MINOR_VERSION))
                );
            }

            // The real reader (TableReaderMetadata via the metadata pool) now surfaces the block.
            try (TableMetadata md = engine.getTableMetadata(tableToken)) {
                Assert.assertEquals(3, md.getColumnCount());
                PartitionSpec spec = md.getPartitionSpec();
                Assert.assertTrue(spec.isComposite());
                Assert.assertEquals(PartitionSpec.MODE_HIVE, spec.getNamingMode());
                Assert.assertEquals(PartitionBy.DAY, spec.getTimeUnit());
                Assert.assertEquals(2, spec.getDimensionCount());

                // dim 0: identity(exchange) -> column 1, null exprText (null-string read path)
                PartitionDimension d0 = spec.getDimension(0);
                Assert.assertEquals(PartitionDimension.KIND_IDENTITY, d0.getKind());
                Assert.assertEquals(1, d0.getColumnIndex());
                Assert.assertEquals("exchange", d0.getAlias());
                Assert.assertNull(d0.getExprText());

                // dim 1: hash(symbol, 16) -> column 2, null exprText
                PartitionDimension d1 = spec.getDimension(1);
                Assert.assertEquals(PartitionDimension.KIND_HASH, d1.getKind());
                Assert.assertEquals(2, d1.getColumnIndex());
                Assert.assertEquals(16, d1.getParam());
                Assert.assertNull(d1.getExprText());

                Assert.assertEquals(0, spec.getClusterColumnCount());
            }
        });
    }

    @Test
    public void testEmptySpecIsImmutable() {
        // Hardening: PartitionSpec.EMPTY is a shared static returned by the TableStructure default;
        // any mutation must throw, otherwise a single corrupted EMPTY would make every plain table
        // write a bogus composite block. NOTE: run in isolation when demonstrating the pre-fix
        // failure -- an unguarded EMPTY.addDimension() would pollute EMPTY for the whole JVM.
        assertThrowsUnsupported(() -> PartitionSpec.EMPTY.addDimension(
                new PartitionDimension(PartitionDimension.KIND_IDENTITY, 0, 0, "x", null)));
        assertThrowsUnsupported(() -> PartitionSpec.EMPTY.addClusterColumn(0));
        assertThrowsUnsupported(() -> PartitionSpec.EMPTY.setNamingMode(PartitionSpec.MODE_PLAIN));
        assertThrowsUnsupported(() -> PartitionSpec.EMPTY.setTimeUnit(PartitionBy.DAY));
        assertThrowsUnsupported(PartitionSpec.EMPTY::clear);

        // Still pristine and non-composite after the rejected mutations.
        Assert.assertFalse(PartitionSpec.EMPTY.isComposite());
        Assert.assertEquals(0, PartitionSpec.EMPTY.getDimensionCount());
        Assert.assertEquals(0, PartitionSpec.EMPTY.getClusterColumnCount());
    }

    @Test
    public void testPlainTableMetaUnchanged() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table p (ts timestamp, s symbol) timestamp(ts) partition by day wal");

            TableToken tableToken = engine.verifyTableName("p");
            try (Path path = new Path(); MemoryMR mem = openMeta(path, tableToken)) {
                // A plain (non-composite) table must persist EXACTLY the pre-feature minor version.
                // The checksum (low short) is computed identically and does not depend on the
                // version value, and no composite block is appended, so the _meta bytes are
                // byte-identical to those a pre-Task-5 build would have written.
                Assert.assertEquals(
                        TableUtils.META_FORMAT_MINOR_VERSION_TABLE_FORMAT,
                        Numbers.decodeHighShort(mem.getInt(TableUtils.META_OFFSET_META_FORMAT_MINOR_VERSION))
                );
            }
            // The table reads back through the ordinary metadata path with an empty (non-composite)
            // spec -- never null.
            try (TableMetadata md = engine.getTableMetadata(tableToken)) {
                Assert.assertEquals(2, md.getColumnCount());
                Assert.assertNotNull(md.getPartitionSpec());
                Assert.assertFalse(md.getPartitionSpec().isComposite());
            }
        });
    }

    @Test
    public void testReopenAfterRestartKeepsSpec() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, exchange symbol) timestamp(ts) partition by day, exchange wal");
            engine.releaseInactive(); // force re-read of _meta from disk

            try (TableMetadata m = engine.getTableMetadata(engine.verifyTableName("t"))) {
                Assert.assertTrue(m.getPartitionSpec().isComposite());
                Assert.assertEquals("exchange", m.getPartitionSpec().getDimension(0).getAlias());
            }
        });
    }

    /**
     * Hermetic round-trip: write directly with {@link TableUtils#writeMetadata} then read back with
     * the production {@link TableUtils#readCompositePartitionSpec} reader, exercising BOTH the null
     * exprText path (hash dimension) and the non-null exprText path (expression dimension), plus
     * cluster columns and the non-default naming mode -- shapes the end-to-end DDL above does not
     * all reach today. This is the definitive proof the read is byte-exactly symmetric to the write.
     */
    @Test
    public void testWriteMetadataCompositeBlockRoundTrip() throws Exception {
        assertMemoryLeak(() -> {
            final PartitionSpec spec = new PartitionSpec();
            spec.setTimeUnit(PartitionBy.DAY);
            spec.setNamingMode(PartitionSpec.MODE_PLAIN);
            spec.addDimension(new PartitionDimension(PartitionDimension.KIND_HASH, 2, 16, "symbol_hash", null));
            spec.addDimension(new PartitionDimension(PartitionDimension.KIND_EXPRESSION, -1, 0, "asset_class", "s in ('BTC','ETH')"));
            spec.addClusterColumn(1);
            spec.addClusterColumn(2);

            TableModel model = new TableModel(configuration, "d", PartitionBy.DAY) {
                @Override
                public PartitionSpec getPartitionSpec() {
                    return spec;
                }
            };
            model.timestamp("ts").col("exchange", ColumnType.SYMBOL).col("symbol", ColumnType.SYMBOL);

            try (MemoryCARW mem = Vm.getCARWInstance(4096, 8, MemoryTag.NATIVE_DEFAULT)) {
                TableUtils.writeMetadata(model, ColumnType.VERSION, 42, mem);

                Assert.assertEquals(
                        TableUtils.META_FORMAT_MINOR_VERSION_COMPOSITE_PARTITIONING,
                        Numbers.decodeHighShort(mem.getInt(TableUtils.META_OFFSET_META_FORMAT_MINOR_VERSION))
                );

                PartitionSpec readBack = new PartitionSpec();
                TableUtils.readCompositePartitionSpec(mem, readBack);

                Assert.assertTrue(readBack.isComposite());
                Assert.assertEquals(PartitionSpec.MODE_PLAIN, readBack.getNamingMode());
                Assert.assertEquals(PartitionBy.DAY, readBack.getTimeUnit());
                Assert.assertEquals(2, readBack.getDimensionCount());

                PartitionDimension d0 = readBack.getDimension(0);
                Assert.assertEquals(PartitionDimension.KIND_HASH, d0.getKind());
                Assert.assertEquals(2, d0.getColumnIndex());
                Assert.assertEquals(16, d0.getParam());
                Assert.assertEquals("symbol_hash", d0.getAlias());
                Assert.assertNull(d0.getExprText()); // null exprText round-trips to null

                PartitionDimension d1 = readBack.getDimension(1);
                Assert.assertEquals(PartitionDimension.KIND_EXPRESSION, d1.getKind());
                Assert.assertEquals(-1, d1.getColumnIndex());
                Assert.assertEquals(0, d1.getParam());
                Assert.assertEquals("asset_class", d1.getAlias());
                Assert.assertEquals("s in ('BTC','ETH')", d1.getExprText()); // non-null exprText round-trips

                Assert.assertEquals(2, readBack.getClusterColumnCount());
                Assert.assertEquals(1, readBack.getClusterColumn(0));
                Assert.assertEquals(2, readBack.getClusterColumn(1));
            }
        });
    }

    private static void assertThrowsUnsupported(Runnable r) {
        try {
            r.run();
            Assert.fail("expected UnsupportedOperationException mutating PartitionSpec.EMPTY");
        } catch (UnsupportedOperationException expected) {
            // ok
        }
    }

    private static MemoryMR openMeta(Path path, TableToken tableToken) {
        FilesFacade ff = configuration.getFilesFacade();
        LPSZ name = path.of(root).concat(tableToken).concat(TableUtils.META_FILE_NAME).$();
        return Vm.getCMRInstance(ff, name, ff.length(name), MemoryTag.MMAP_DEFAULT);
    }
}
