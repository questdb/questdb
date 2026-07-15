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
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Task 5 of composite partitioning: verifies the WRITE side of the additive, minor-version-gated
 * composite {@code _meta} block. This test does not use the Task 6 reader (which is not yet
 * implemented); instead it parses the raw {@code _meta} bytes directly. The
 * {@link #readCompositeBlockOffset} / {@link Cursor} helpers below are a temporary stand-in that
 * Task 6 will replace with the real {@code TableMetadata.getPartitionSpec()} reader.
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

    // Mirror of the package-private TableUtils.META_FLAG_BIT_COVERING (1 << 6): this test lives in a
    // different package and cannot reference the constant directly. Temporary; removed with Task 6.
    private static final long META_FLAG_BIT_COVERING = 1L << 6;

    @Test
    public void testCompositeMetaRaisesMinorAndWritesBlock() throws Exception {
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

                Cursor c = new Cursor(mem, readCompositeBlockOffset(mem));
                Assert.assertEquals(PartitionSpec.MODE_HIVE, c.nextByte()); // namingMode
                Assert.assertEquals(2, c.nextInt());                        // dimensionCount

                // dim 0: identity(exchange) -> null exprText (null-string write path)
                Dim d0 = c.nextDim();
                Assert.assertEquals(PartitionDimension.KIND_IDENTITY, d0.kind);
                Assert.assertNull(d0.exprText);

                // dim 1: hash(symbol, 16)
                Dim d1 = c.nextDim();
                Assert.assertEquals(PartitionDimension.KIND_HASH, d1.kind);
                Assert.assertEquals(16, d1.param);
                Assert.assertNull(d1.exprText);

                Assert.assertEquals(0, c.nextInt()); // clusterColumnCount
            }

            // Backward-compat: an existing reader (which knows nothing of the composite block) must
            // still open a composite table's _meta without error -- the trailing block sits in the
            // same additive tail as the covering-index section that readers already tolerate.
            try (TableMetadata md = engine.getTableMetadata(tableToken)) {
                Assert.assertEquals(3, md.getColumnCount());
            }
        });
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
                // The table must still read back through the ordinary metadata path without error.
                try (TableMetadata md = engine.getTableMetadata(tableToken)) {
                    Assert.assertEquals(2, md.getColumnCount());
                }
            }
        });
    }

    /**
     * Hermetic round-trip directly against {@link TableUtils#writeMetadata}, exercising BOTH the
     * null exprText path (hash dimension) and the non-null exprText path (expression dimension),
     * plus cluster columns and the non-default naming mode -- shapes the end-to-end DDL above does
     * not all reach today. This is the definitive byte-format contract Task 6 must read back.
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

                Cursor c = new Cursor(mem, readCompositeBlockOffset(mem));
                Assert.assertEquals(PartitionSpec.MODE_PLAIN, c.nextByte());
                Assert.assertEquals(2, c.nextInt());

                Dim d0 = c.nextDim();
                Assert.assertEquals(PartitionDimension.KIND_HASH, d0.kind);
                Assert.assertEquals(2, d0.columnIndex);
                Assert.assertEquals(16, d0.param);
                Assert.assertEquals("symbol_hash", d0.alias);
                Assert.assertNull(d0.exprText); // null exprText round-trips to null

                Dim d1 = c.nextDim();
                Assert.assertEquals(PartitionDimension.KIND_EXPRESSION, d1.kind);
                Assert.assertEquals(-1, d1.columnIndex);
                Assert.assertEquals(0, d1.param);
                Assert.assertEquals("asset_class", d1.alias);
                Assert.assertEquals("s in ('BTC','ETH')", d1.exprText); // non-null exprText round-trips

                Assert.assertEquals(2, c.nextInt()); // clusterColumnCount
                Assert.assertEquals(1, c.nextInt());
                Assert.assertEquals(2, c.nextInt());
            }
        });
    }

    private static MemoryMR openMeta(Path path, TableToken tableToken) {
        FilesFacade ff = configuration.getFilesFacade();
        LPSZ name = path.of(root).concat(tableToken).concat(TableUtils.META_FILE_NAME).$();
        return Vm.getCMRInstance(ff, name, ff.length(name), MemoryTag.MMAP_DEFAULT);
    }

    /**
     * Replays the exact layout {@link TableUtils#writeMetadata} writes to find where the trailing
     * composite block begins: fixed header, then {@code count} fixed-size column entries, then the
     * variable-length column names, then the covering-index section (present per column only when
     * that column's entry has the covering flag set).
     */
    private static long readCompositeBlockOffset(MemoryR mem) {
        final int count = mem.getInt(TableUtils.META_OFFSET_COUNT);
        long pos = TableUtils.META_OFFSET_COLUMN_TYPES + (long) count * TableUtils.META_COLUMN_DATA_SIZE;
        for (int i = 0; i < count; i++) {
            pos += strStorageLen(mem, pos); // column names
        }
        for (int i = 0; i < count; i++) {
            long flags = mem.getLong(TableUtils.META_OFFSET_COLUMN_TYPES + (long) i * TableUtils.META_COLUMN_DATA_SIZE + Integer.BYTES);
            if ((flags & META_FLAG_BIT_COVERING) != 0) {
                pos += Integer.BYTES + (long) mem.getInt(pos) * Integer.BYTES;
            }
        }
        return pos;
    }

    private static long strStorageLen(MemoryR mem, long pos) {
        int len = mem.getInt(pos);
        return Integer.BYTES + (len < 0 ? 0L : (long) len * 2);
    }

    private static final class Cursor {
        private final MemoryR mem;
        private long pos;

        private Cursor(MemoryR mem, long pos) {
            this.mem = mem;
            this.pos = pos;
        }

        byte nextByte() {
            return mem.getByte(pos++);
        }

        Dim nextDim() {
            Dim d = new Dim();
            d.kind = nextByte();
            d.columnIndex = nextInt();
            d.param = nextInt();
            d.alias = nextStr();
            d.exprText = nextStr();
            return d;
        }

        int nextInt() {
            int v = mem.getInt(pos);
            pos += Integer.BYTES;
            return v;
        }

        String nextStr() {
            CharSequence cs = mem.getStrA(pos);
            pos += strStorageLen(mem, pos);
            return cs == null ? null : cs.toString();
        }
    }

    private static final class Dim {
        String alias;
        int columnIndex;
        String exprText;
        byte kind;
        int param;
    }
}
