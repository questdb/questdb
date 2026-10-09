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

package io.questdb.test.cairo.wal;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.SymbolMapReader;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.cairo.wal.WalColumnSink;
import io.questdb.cairo.wal.WalColumnarRowAppender;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Drives {@link WalColumnSink} from outside its package, the way a client other than the
 * QWP appender would: whole column blocks appended to the writer's segment memories.
 */
public class WalColumnSinkTest extends AbstractCairoTest {
    private static final long TS0 = 1_767_225_600_000_000L; // 2026-01-01T00:00:00.000000Z

    @Test
    public void testAppenderWritesThroughForeignSink() throws Exception {
        assertMemoryLeak(() -> {
            TableToken token = createTable(new TableModel(configuration, "sink_appender", PartitionBy.HOUR)
                    .col("a", ColumnType.LONG)
                    .timestamp("ts")
                    .wal()
            );
            final int rowCount = 3;
            final long valuesAddr = Unsafe.malloc(rowCount * 8L, MemoryTag.NATIVE_DEFAULT);
            final long tsAddr = Unsafe.malloc(rowCount * 8L, MemoryTag.NATIVE_DEFAULT);
            try (WalWriter walWriter = engine.getWalWriter(token)) {
                for (int i = 0; i < rowCount; i++) {
                    Unsafe.putLong(valuesAddr + i * 8L, 500L + i);
                    Unsafe.putLong(tsAddr + i * 8L, TS0 + i * 1_000_000L);
                }
                ForwardingSink sink = new ForwardingSink(walWriter);
                try (WalColumnarRowAppender appender = new WalColumnarRowAppender(sink)) {
                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedColumn(0, valuesAddr, rowCount, 8, 0, rowCount);
                    appender.putTimestampColumn(1, tsAddr, rowCount, 0, rowCount, Numbers.LONG_NULL);
                    appender.endColumnarWrite(TS0, TS0 + (rowCount - 1) * 1_000_000L, false);
                }
                walWriter.commit();
                Assert.assertEquals(1, sink.beginCount);
                Assert.assertEquals(1, sink.finishCount);
            } finally {
                Unsafe.free(valuesAddr, rowCount * 8L, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(tsAddr, rowCount * 8L, MemoryTag.NATIVE_DEFAULT);
            }
            drainWalQueue();
            assertQuery("SELECT a, ts FROM sink_appender")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            a\tts
                            500\t2026-01-01T00:00:00.000000Z
                            501\t2026-01-01T00:00:01.000000Z
                            502\t2026-01-01T00:00:02.000000Z
                            """);
        });
    }

    @Test
    public void testBeginRollFailureLatchesDistressed() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_ROW_COUNT, 1);
        final AtomicBoolean failSegmentDir = new AtomicBoolean();
        final String segmentOneSuffix = Files.SEPARATOR + "1" + Files.SEPARATOR;
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public int mkdirs(Path path, int mode) {
                if (failSegmentDir.get() && Utf8s.endsWithAscii(path, segmentOneSuffix)) {
                    return -1;
                }
                return super.mkdirs(path, mode);
            }
        };
        assertMemoryLeak(ff, () -> {
            TableToken token = createTable(new TableModel(configuration, "sink_roll_fail", PartitionBy.HOUR)
                    .col("a", ColumnType.LONG)
                    .timestamp("ts")
                    .wal()
            );
            try (WalWriter walWriter = engine.getWalWriter(token)) {
                TableWriter.Row row = walWriter.newRow(TS0);
                row.putLong(0, 1L);
                row.append();
                // one row reaches the rollover threshold, so the next block rolls the segment first
                walWriter.commit();

                failSegmentDir.set(true);
                assertRefused(walWriter::beginColumnarWrite, "Cannot create WAL segment directory");
                Assert.assertTrue(walWriter.isDistressed());

                failSegmentDir.set(false);
                assertRefused(walWriter::beginColumnarWrite, "WAL writer is distressed");
            }
        });
    }

    @Test
    public void testBeginRollsPendingSegmentAfterAddColumn() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_ROW_COUNT, 1);
        assertMemoryLeak(() -> {
            TableToken token = createTable(new TableModel(configuration, "sink_roll", PartitionBy.HOUR)
                    .col("a", ColumnType.LONG)
                    .timestamp("ts")
                    .wal()
            );
            try (WalWriter walWriter = engine.getWalWriter(token)) {
                TableWriter.Row row = walWriter.newRow(TS0);
                row.putLong(0, 100L);
                row.append();
                walWriter.commit();
                // addColumn() defers opening the new column's files to the pending segment roll
                walWriter.addColumn("b", ColumnType.LONG, AllowAllSecurityContext.INSTANCE);
                Assert.assertEquals(0, walWriter.getSegmentId());

                WalColumnSink sink = new ForwardingSink(walWriter);
                sink.beginColumnarWrite();
                Assert.assertEquals(1, walWriter.getSegmentId());
                long start = sink.getSegmentRowCount();
                Assert.assertEquals(0, start);
                // column indexes: a = 0, ts = 1, b = 2
                for (int i = 0; i < 2; i++) {
                    sink.getDataColumn(0).putLong(1000L + i);
                    sink.getDataColumn(1).putLong128(TS0 + (i + 1) * 1_000_000L, start + i);
                    sink.getDataColumn(2).putLong(2000L + i);
                }
                for (int col = 0; col < 3; col++) {
                    sink.setRowValueNotNullColumnar(col, start + 1);
                }
                sink.finishColumnarWrite(2, TS0 + 1_000_000L, TS0 + 2_000_000L, false);
                walWriter.commit();
            }
            drainWalQueue();
            assertQuery("SELECT a, b, ts FROM sink_roll ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            a\tb\tts
                            100\tnull\t2026-01-01T00:00:00.000000Z
                            1000\t2000\t2026-01-01T00:00:01.000000Z
                            1001\t2001\t2026-01-01T00:00:02.000000Z
                            """);
        });
    }

    @Test
    public void testFinishWithoutDesignatedTimestampIsRefused() throws Exception {
        assertMemoryLeak(() -> {
            TableToken token = createTable(new TableModel(configuration, "sink_no_ts", PartitionBy.HOUR)
                    .col("a", ColumnType.LONG)
                    .timestamp("ts")
                    .wal()
            );
            try (WalWriter walWriter = engine.getWalWriter(token)) {
                WalColumnSink sink = walWriter;
                sink.beginColumnarWrite();
                long start = sink.getSegmentRowCount();
                sink.getDataColumn(0).putLong(7L);
                sink.setRowValueNotNullColumnar(0, start);
                try {
                    sink.finishColumnarWrite(1, TS0, TS0, false);
                    Assert.fail("expected the block without the designated timestamp to be refused");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "columnar write did not write designated timestamp column");
                }
                sink.cancelColumnarWrite(start);
                Assert.assertEquals(start, sink.getSegmentRowCount());

                // the writer stays usable
                sink.beginColumnarWrite();
                sink.getDataColumn(0).putLong(8L);
                sink.getDataColumn(1).putLong128(TS0, start);
                sink.setRowValueNotNullColumnar(0, start);
                sink.setRowValueNotNullColumnar(1, start);
                sink.finishColumnarWrite(1, TS0, TS0, false);
                walWriter.commit();
            }
            drainWalQueue();
            assertQuery("SELECT a, ts FROM sink_no_ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            a\tts
                            8\t2026-01-01T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testForeignSinkAppendsBlock() throws Exception {
        assertMemoryLeak(() -> {
            TableToken token = createTable(new TableModel(configuration, "sink_block", PartitionBy.HOUR)
                    .col("v", ColumnType.LONG)
                    .col("s", ColumnType.SYMBOL)
                    .timestamp("ts")
                    .wal()
            );
            try (WalWriter walWriter = engine.getWalWriter(token)) {
                WalColumnSink sink = new ForwardingSink(walWriter);
                sink.beginColumnarWrite();
                long start = sink.getSegmentRowCount();
                SymbolMapReader symbols = sink.getSymbolMapReader(1);
                String[] values = {"a", "b", "a"};
                for (int i = 0; i < values.length; i++) {
                    sink.getDataColumn(0).putLong(10L + i);
                    sink.getDataColumn(1).putInt(sink.resolveSymbol(1, values[i], symbols));
                    sink.getDataColumn(2).putLong128(TS0 + i * 1_000_000L, start + i);
                }
                for (int col = 0; col < 3; col++) {
                    sink.setRowValueNotNullColumnar(col, start + values.length - 1);
                }
                sink.finishColumnarWrite(values.length, TS0, TS0 + 2_000_000L, false);
                walWriter.commit();
            }
            drainWalQueue();
            assertQuery("SELECT v, s, ts FROM sink_block")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            v\ts\tts
                            10\ta\t2026-01-01T00:00:00.000000Z
                            11\tb\t2026-01-01T00:00:01.000000Z
                            12\ta\t2026-01-01T00:00:02.000000Z
                            """);
        });
    }

    @Test
    public void testNullSymbolSetsTableNullFlag() throws Exception {
        assertMemoryLeak(() -> {
            TableToken token = createTable(new TableModel(configuration, "sink_null_sym", PartitionBy.HOUR)
                    .col("s", ColumnType.SYMBOL)
                    .timestamp("ts")
                    .wal()
            );
            try (WalWriter walWriter = engine.getWalWriter(token)) {
                WalColumnSink sink = walWriter;
                sink.beginColumnarWrite();
                long start = sink.getSegmentRowCount();
                SymbolMapReader symbols = sink.getSymbolMapReader(0);
                int keyA = sink.resolveSymbol(0, "a", symbols);
                int keyNull = sink.resolveSymbol(0, null, symbols);
                Assert.assertEquals(SymbolTable.VALUE_IS_NULL, keyNull);
                sink.getDataColumn(0).putInt(keyA);
                sink.getDataColumn(0).putInt(keyNull);
                sink.getDataColumn(1).putLong128(TS0, start);
                sink.getDataColumn(1).putLong128(TS0 + 1_000_000L, start + 1);
                sink.setRowValueNotNullColumnar(0, start + 1);
                sink.setRowValueNotNullColumnar(1, start + 1);
                sink.finishColumnarWrite(2, TS0, TS0 + 1_000_000L, false);
                walWriter.commit();
            }
            drainWalQueue();
            assertQuery("SELECT count() FROM sink_null_sym WHERE s IS NULL")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n1\n");
            try (TableReader reader = engine.getReader(token)) {
                Assert.assertTrue(reader.getSymbolMapReader(0).containsNullValue());
            }
        });
    }

    @Test
    public void testPoolReturnDuringSinkWriteDiscardsBlock() throws Exception {
        assertMemoryLeak(() -> {
            TableToken token = createTable(new TableModel(configuration, "sink_pool", PartitionBy.HOUR)
                    .col("a", ColumnType.LONG)
                    .timestamp("ts")
                    .wal()
            );
            try (WalWriter walWriter = engine.getWalWriter(token)) {
                Assert.assertEquals(1, walWriter.getWalId());
                WalColumnSink sink = walWriter;
                sink.beginColumnarWrite();
                long start = sink.getSegmentRowCount();
                for (int i = 0; i < 3; i++) {
                    sink.getDataColumn(0).putLong(900L + i);
                    sink.getDataColumn(1).putLong128(TS0 + i, start + i);
                }
                // close() returns the writer to the pool with the block still open
            }
            try (WalWriter walWriter = engine.getWalWriter(token)) {
                Assert.assertEquals(1, walWriter.getWalId());
                WalColumnSink sink = walWriter;
                sink.beginColumnarWrite();
                long start = sink.getSegmentRowCount();
                Assert.assertEquals(0, start);
                sink.getDataColumn(0).putLong(42L);
                sink.getDataColumn(1).putLong128(TS0, start);
                sink.setRowValueNotNullColumnar(0, start);
                sink.setRowValueNotNullColumnar(1, start);
                sink.finishColumnarWrite(1, TS0, TS0, false);
                walWriter.commit();
            }
            drainWalQueue();
            assertQuery("SELECT a, ts FROM sink_pool")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            a\tts
                            42\t2026-01-01T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testRowOperationsDuringSinkWriteAreRefused() throws Exception {
        assertMemoryLeak(() -> {
            TableToken token = createTable(new TableModel(configuration, "sink_guard", PartitionBy.HOUR)
                    .col("a", ColumnType.LONG)
                    .timestamp("ts")
                    .wal()
            );
            try (WalWriter walWriter = engine.getWalWriter(token)) {
                WalColumnSink sink = walWriter;
                sink.beginColumnarWrite();
                long start = sink.getSegmentRowCount();
                assertRefused(sink::beginColumnarWrite, "already in columnar write mode");
                assertRefused(walWriter::commit, "cannot commit during columnar write");
                assertRefused(walWriter::rollback, "cannot rollback during columnar write");
                assertRefused(() -> walWriter.newRow(TS0), "cannot use row-oriented newRow() during columnar write");
                sink.cancelColumnarWrite(start);

                // after the cancel, row writes work again
                TableWriter.Row row = walWriter.newRow(TS0);
                row.putLong(0, 1L);
                row.append();
                walWriter.commit();
            }
            drainWalQueue();
            assertQuery("SELECT a FROM sink_guard")
                    .noLeakCheck()
                    .expectSize()
                    .returns("a\n1\n");
        });
    }

    private static void assertRefused(Runnable operation, String expectedMessage) {
        try {
            operation.run();
            Assert.fail("expected a CairoException containing: " + expectedMessage);
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), expectedMessage);
        }
    }

    /**
     * A sink implemented outside the sink's package. It forwards every call and counts the
     * calls that open and close a block.
     */
    private static final class ForwardingSink implements WalColumnSink {
        private final WalColumnSink delegate;
        private int beginCount;
        private int finishCount;

        private ForwardingSink(WalColumnSink delegate) {
            this.delegate = delegate;
        }

        @Override
        public void beginColumnarWrite() {
            beginCount++;
            delegate.beginColumnarWrite();
        }

        @Override
        public void cancelColumnarWrite(long startRowId) {
            delegate.cancelColumnarWrite(startRowId);
        }

        @Override
        public void checkDistressed() {
            delegate.checkDistressed();
        }

        @Override
        public void finishColumnarWrite(int rowCount, long minTimestamp, long maxTimestamp, boolean outOfOrder) {
            finishCount++;
            delegate.finishColumnarWrite(rowCount, minTimestamp, maxTimestamp, outOfOrder);
        }

        @Override
        public MemoryMA getAuxColumn(int columnIndex) {
            return delegate.getAuxColumn(columnIndex);
        }

        @Override
        public MemoryMA getDataColumn(int columnIndex) {
            return delegate.getDataColumn(columnIndex);
        }

        @Override
        public TableRecordMetadata getMetadata() {
            return delegate.getMetadata();
        }

        @Override
        public long getSegmentRowCount() {
            return delegate.getSegmentRowCount();
        }

        @Override
        public SymbolMapReader getSymbolMapReader(int columnIndex) {
            return delegate.getSymbolMapReader(columnIndex);
        }

        @Override
        public void markSymbolMapNull(int columnIndex) {
            delegate.markSymbolMapNull(columnIndex);
        }

        @Override
        public int resolveSymbol(int columnIndex, CharSequence symbolValue, SymbolMapReader symbolMapReader) {
            return delegate.resolveSymbol(columnIndex, symbolValue, symbolMapReader);
        }

        @Override
        public void setRowValueNotNullColumnar(int columnIndex, long lastWrittenRow) {
            delegate.setRowValueNotNullColumnar(columnIndex, lastWrittenRow);
        }

        @Override
        public void validateDesignatedTimestampBounds(long timestamp) {
            delegate.validateDesignatedTimestampBounds(timestamp);
        }
    }
}
