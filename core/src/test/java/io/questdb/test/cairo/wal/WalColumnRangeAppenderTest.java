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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypeDriver;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.StringTypeDriver;
import io.questdb.cairo.SymbolMapReader;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.cairo.wal.WalColumnRangeAppender;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Drives {@link WalColumnRangeAppender} with ranges already in WAL layout, such as the rows of another table's
 * segment, appended as blocks of a stock writer.
 */
public class WalColumnRangeAppenderTest extends AbstractCairoTest {
    private static final long SECOND = 1_000_000L;
    private static final long TS0 = 1_767_225_600_000_000L; // 2026-01-01T00:00:00.000000Z

    @Test
    public void testAppendsFixedAndTimestampRanges() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = createTable(new TableModel(configuration, "ranges", PartitionBy.HOUR)
                    .col("a", ColumnType.LONG)
                    .timestamp("ts")
                    .wal()
            );
            final int rows = 3;
            final long values = Unsafe.malloc(rows * 8L, MemoryTag.NATIVE_DEFAULT);
            final long pairs = Unsafe.malloc(rows * 16L, MemoryTag.NATIVE_DEFAULT);
            try (WalWriter writer = engine.getWalWriter(token)) {
                for (int i = 0; i < rows; i++) {
                    Unsafe.putLong(values + i * 8L, 500L + i);
                    putPair(pairs, i, TS0 + i * SECOND, i);
                }
                try (WalColumnRangeAppender appender = new WalColumnRangeAppender(writer)) {
                    appender.begin(rows);
                    appender.putFixedRange(0, values, rows * 8L);
                    appender.putTimestampRange(1, pairs, 0);
                    appender.finish(TS0, TS0 + 2 * SECOND, false);
                }
                writer.commit();
            } finally {
                Unsafe.free(values, rows * 8L, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(pairs, rows * 16L, MemoryTag.NATIVE_DEFAULT);
            }
            drainWalQueue();
            assertQuery("SELECT a, ts FROM ranges")
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
    public void testAppendsStringRangesWithShiftedOffsets() throws Exception {
        // six STRING values in WAL layout (a leading aux entry, then one 8-byte offset per value); the first block
        // takes rows 0 to 2 at data offset 0, row 3 is left out, and the second block takes rows 4 and 5, so its data
        // offset in the source is ahead of the sink's append offset and its aux entries are shifted back
        assertMemoryLeak(() -> {
            final TableToken token = createTable(new TableModel(configuration, "strings", PartitionBy.HOUR)
                    .col("s", ColumnType.STRING)
                    .timestamp("ts")
                    .wal()
            );
            final ColumnTypeDriver driver = ColumnType.getDriver(ColumnType.STRING);
            final long pairs = Unsafe.malloc(3 * 16L, MemoryTag.NATIVE_DEFAULT);
            try (
                    MemoryCARW aux = Vm.getCARWInstance(1 << 20, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                    MemoryCARW data = Vm.getCARWInstance(1 << 20, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                    WalWriter writer = engine.getWalWriter(token);
                    WalColumnRangeAppender appender = new WalColumnRangeAppender(writer)
            ) {
                driver.configureAuxMemO3RSS(aux);
                StringTypeDriver.appendValue(aux, data, "first");
                StringTypeDriver.appendValue(aux, data, "second value");
                driver.appendNull(aux, data);
                StringTypeDriver.appendValue(aux, data, "left out of the range");
                StringTypeDriver.appendValue(aux, data, "fourth");
                StringTypeDriver.appendValue(aux, data, "v5");
                final long auxBase = aux.addressOf(0);

                appendVarSizeBlock(appender, driver, auxBase, data, pairs, 0, 3, TS0);
                appendVarSizeBlock(appender, driver, auxBase, data, pairs, 4, 6, TS0 + 3 * SECOND);
                writer.commit();
            } finally {
                Unsafe.free(pairs, 3 * 16L, MemoryTag.NATIVE_DEFAULT);
            }
            drainWalQueue();
            assertQuery("SELECT s, ts FROM strings")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            s\tts
                            first\t2026-01-01T00:00:00.000000Z
                            second value\t2026-01-01T00:00:01.000000Z
                            \t2026-01-01T00:00:02.000000Z
                            fourth\t2026-01-01T00:00:03.000000Z
                            v5\t2026-01-01T00:00:04.000000Z
                            """);
        });
    }

    @Test
    public void testAppendsVarcharRangesIncludingInlinedValues() throws Exception {
        // short VARCHAR values live in their 16-byte aux entry alone: the first block has no data bytes at all; the
        // second holds a long value, and a long value between the two is left out, so the second block's data lands
        // at a shifted offset
        assertMemoryLeak(() -> {
            final TableToken token = createTable(new TableModel(configuration, "varchars", PartitionBy.HOUR)
                    .col("v", ColumnType.VARCHAR)
                    .timestamp("ts")
                    .wal()
            );
            final ColumnTypeDriver driver = ColumnType.getDriver(ColumnType.VARCHAR);
            final long pairs = Unsafe.malloc(3 * 16L, MemoryTag.NATIVE_DEFAULT);
            try (
                    MemoryCARW aux = Vm.getCARWInstance(1 << 20, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                    MemoryCARW data = Vm.getCARWInstance(1 << 20, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                    WalWriter writer = engine.getWalWriter(token);
                    WalColumnRangeAppender appender = new WalColumnRangeAppender(writer)
            ) {
                driver.configureAuxMemO3RSS(aux);
                VarcharTypeDriver.appendValue(aux, data, new Utf8String("ab"));
                VarcharTypeDriver.appendValue(aux, data, null);
                VarcharTypeDriver.appendValue(aux, data, new Utf8String("cd"));
                VarcharTypeDriver.appendValue(aux, data, new Utf8String("left out of the range, longer than nine"));
                VarcharTypeDriver.appendValue(aux, data, new Utf8String("a value longer than nine bytes"));
                VarcharTypeDriver.appendValue(aux, data, new Utf8String("ef"));
                final long auxBase = aux.addressOf(0);
                Assert.assertEquals("the first three values are inlined", 0, driver.getDataVectorSize(auxBase, 0, 2));

                appendVarSizeBlock(appender, driver, auxBase, data, pairs, 0, 3, TS0);
                appendVarSizeBlock(appender, driver, auxBase, data, pairs, 4, 6, TS0 + 3 * SECOND);
                writer.commit();
            } finally {
                Unsafe.free(pairs, 3 * 16L, MemoryTag.NATIVE_DEFAULT);
            }
            drainWalQueue();
            assertQuery("SELECT v, ts FROM varchars")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            v\tts
                            ab\t2026-01-01T00:00:00.000000Z
                            \t2026-01-01T00:00:01.000000Z
                            cd\t2026-01-01T00:00:02.000000Z
                            a value longer than nine bytes\t2026-01-01T00:00:03.000000Z
                            ef\t2026-01-01T00:00:04.000000Z
                            """);
        });
    }

    @Test
    public void testFillsConstantSymbolKey() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = createTable(new TableModel(configuration, "filled", PartitionBy.HOUR)
                    .col("s", ColumnType.SYMBOL)
                    .timestamp("ts")
                    .wal()
            );
            final int rows = 3;
            final long pairs = Unsafe.malloc(rows * 16L, MemoryTag.NATIVE_DEFAULT);
            try (
                    WalWriter writer = engine.getWalWriter(token);
                    WalColumnRangeAppender appender = new WalColumnRangeAppender(writer)
            ) {
                for (int i = 0; i < rows; i++) {
                    putPair(pairs, i, TS0 + i * SECOND, i);
                }
                appender.begin(rows);
                final int key = appender.resolveSymbol(0, "x");
                appender.fillSymbolKey(0, key);
                try {
                    appender.fillSymbolKey(0, key);
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "column already written in this block [column=0]");
                }
                appender.putTimestampRange(1, pairs, 0);
                appender.finish(TS0, TS0 + 2 * SECOND, false);
                writer.commit();
            } finally {
                Unsafe.free(pairs, rows * 16L, MemoryTag.NATIVE_DEFAULT);
            }
            drainWalQueue();
            assertQuery("SELECT s, ts FROM filled")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            s\tts
                            x\t2026-01-01T00:00:00.000000Z
                            x\t2026-01-01T00:00:01.000000Z
                            x\t2026-01-01T00:00:02.000000Z
                            """);
        });
    }

    @Test
    public void testNullValueForAKeyMarksNull() throws Exception {
        // a key the lookup cannot resolve becomes a NULL symbol, and the column carries the null flag
        assertMemoryLeak(() -> {
            final TableToken token = createTable(new TableModel(configuration, "gap", PartitionBy.HOUR)
                    .col("s", ColumnType.SYMBOL)
                    .timestamp("ts")
                    .wal()
            );
            final int rows = 2;
            final long keys = Unsafe.malloc(rows * 4L, MemoryTag.NATIVE_DEFAULT);
            final long pairs = Unsafe.malloc(rows * 16L, MemoryTag.NATIVE_DEFAULT);
            try (
                    WalWriter writer = engine.getWalWriter(token);
                    WalColumnRangeAppender appender = new WalColumnRangeAppender(writer)
            ) {
                Unsafe.putInt(keys, 7);
                Unsafe.putInt(keys + 4, 2);
                for (int i = 0; i < rows; i++) {
                    putPair(pairs, i, TS0 + i * SECOND, i);
                }
                appender.begin(rows);
                appender.putSymbolRange(0, keys, key -> key == 2 ? "two" : null);
                try {
                    appender.putSymbolRange(0, keys, key -> null);
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "column already written in this block [column=0]");
                }
                appender.putTimestampRange(1, pairs, 0);
                appender.finish(TS0, TS0 + SECOND, false);
                writer.commit();
            } finally {
                Unsafe.free(keys, rows * 4L, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(pairs, rows * 16L, MemoryTag.NATIVE_DEFAULT);
            }
            drainWalQueue();
            assertQuery("SELECT s, ts FROM gap")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            s\tts
                            \t2026-01-01T00:00:00.000000Z
                            two\t2026-01-01T00:00:01.000000Z
                            """);
            try (TableReader reader = engine.getReader(token)) {
                Assert.assertTrue(reader.getSymbolMapReader(0).containsNullValue());
            }
        });
    }

    @Test
    public void testRebasesTimestampRowsOfALaterBlock() throws Exception {
        // the second block comes from a source segment that starts at row 0, but lands at segment row 3: its pairs
        // must carry rows 3 to 5, which the out-of-order apply reads to find each timestamp's value
        assertMemoryLeak(() -> {
            final TableToken token = createTable(new TableModel(configuration, "rebase", PartitionBy.HOUR)
                    .col("a", ColumnType.LONG)
                    .timestamp("ts")
                    .wal()
            );
            final int rows = 3;
            final long values = Unsafe.malloc(rows * 8L, MemoryTag.NATIVE_DEFAULT);
            final long pairs = Unsafe.malloc(rows * 16L, MemoryTag.NATIVE_DEFAULT);
            try (WalWriter writer = engine.getWalWriter(token)) {
                try (WalColumnRangeAppender appender = new WalColumnRangeAppender(writer)) {
                    for (int i = 0; i < rows; i++) {
                        Unsafe.putLong(values + i * 8L, 1L + i);
                        putPair(pairs, i, TS0 + i * SECOND, i);
                    }
                    appender.begin(rows);
                    appender.putFixedRange(0, values, rows * 8L);
                    appender.putTimestampRange(1, pairs, 0);
                    appender.finish(TS0, TS0 + 2 * SECOND, false);

                    final long[] later = {TS0 + 5 * SECOND, TS0 + 3 * SECOND, TS0 + 4 * SECOND};
                    for (int i = 0; i < rows; i++) {
                        Unsafe.putLong(values + i * 8L, 4L + i);
                        putPair(pairs, i, later[i], i);
                    }
                    appender.begin(rows);
                    appender.putFixedRange(0, values, rows * 8L);
                    appender.putTimestampRange(1, pairs, 0);
                    appender.finish(TS0 + 3 * SECOND, TS0 + 5 * SECOND, true);
                }
                writer.commit();
            } finally {
                Unsafe.free(values, rows * 8L, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(pairs, rows * 16L, MemoryTag.NATIVE_DEFAULT);
            }
            drainWalQueue();
            assertQuery("SELECT a, ts FROM rebase")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            a\tts
                            1\t2026-01-01T00:00:00.000000Z
                            2\t2026-01-01T00:00:01.000000Z
                            3\t2026-01-01T00:00:02.000000Z
                            5\t2026-01-01T00:00:03.000000Z
                            6\t2026-01-01T00:00:04.000000Z
                            4\t2026-01-01T00:00:05.000000Z
                            """);
        });
    }

    @Test
    public void testRefusesDroppedAndOutOfRangeColumns() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = createTable(new TableModel(configuration, "dropped", PartitionBy.HOUR)
                    .col("a", ColumnType.LONG)
                    .col("b", ColumnType.LONG)
                    .col("s", ColumnType.SYMBOL)
                    .timestamp("ts")
                    .wal()
            );
            execute("ALTER TABLE dropped DROP COLUMN b");
            final long values = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
            try (
                    WalWriter writer = engine.getWalWriter(token);
                    WalColumnRangeAppender appender = new WalColumnRangeAppender(writer)
            ) {
                // the dropped column keeps its index, 1, with a negative type; index 10 is outside the metadata
                appender.begin(1);
                assertRefused(() -> appender.putFixedRange(1, values, 8), "column is dropped [column=1]");
                assertRefused(() -> appender.putFixedRange(10, values, 8), "column index out of range [column=10");
                assertRefused(() -> appender.putFixedRange(-1, values, 8), "column index out of range [column=-1");
                assertRefused(() -> appender.putSymbolRange(1, values, key -> null), "column is dropped [column=1]");
                assertRefused(() -> appender.putTimestampRange(10, values, 0), "column index out of range [column=10");
                assertRefused(() -> appender.putVarSizeRange(1, values, values, 0, 0), "column is dropped [column=1]");
                assertRefused(() -> appender.fillSymbolKey(10, 0), "column index out of range [column=10");
                assertRefused(() -> appender.resolveSymbol(1, "x"), "column is dropped [column=1]");
                appender.cancel();
                Assert.assertEquals(0, writer.getSegmentRowCount());
            } finally {
                Unsafe.free(values, 8, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testRefusesPutOutsideBlock() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = createTable(new TableModel(configuration, "outside", PartitionBy.HOUR)
                    .col("a", ColumnType.LONG)
                    .timestamp("ts")
                    .wal()
            );
            try (
                    WalWriter writer = engine.getWalWriter(token);
                    WalColumnRangeAppender appender = new WalColumnRangeAppender(writer)
            ) {
                try {
                    appender.putFixedRange(0, 0, 0);
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "no open block");
                }
                try {
                    appender.finish(TS0, TS0, false);
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "no open block");
                }
                appender.cancel(); // without a block: nothing
                Assert.assertEquals(0, writer.getSegmentRowCount());
            }
        });
    }

    @Test
    public void testRefusesRangeOfWrongSize() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = createTable(new TableModel(configuration, "sizes", PartitionBy.HOUR)
                    .col("a", ColumnType.LONG)
                    .timestamp("ts")
                    .wal()
            );
            final int rows = 3;
            final long values = Unsafe.malloc(rows * 8L, MemoryTag.NATIVE_DEFAULT);
            final long pairs = Unsafe.malloc(rows * 16L, MemoryTag.NATIVE_DEFAULT);
            try (WalWriter writer = engine.getWalWriter(token)) {
                for (int i = 0; i < rows; i++) {
                    Unsafe.putLong(values + i * 8L, 7L + i);
                    putPair(pairs, i, TS0 + i * SECOND, i);
                }
                try (WalColumnRangeAppender appender = new WalColumnRangeAppender(writer)) {
                    appender.begin(rows);
                    try {
                        appender.putFixedRange(0, values, 16);
                        Assert.fail();
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "fixed-size range does not match the block [column=0, size=16, expected=24]");
                    }
                    appender.cancel();
                    Assert.assertEquals(0, writer.getSegmentRowCount());

                    appender.begin(rows);
                    appender.putFixedRange(0, values, rows * 8L);
                    appender.putTimestampRange(1, pairs, 0);
                    appender.finish(TS0, TS0 + 2 * SECOND, false);
                }
                writer.commit();
            } finally {
                Unsafe.free(values, rows * 8L, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(pairs, rows * 16L, MemoryTag.NATIVE_DEFAULT);
            }
            drainWalQueue();
            assertQuery("SELECT a FROM sizes")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a
                            7
                            8
                            9
                            """);
        });
    }

    @Test
    public void testRefusesSecondRangeForSameColumn() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = createTable(new TableModel(configuration, "twice", PartitionBy.HOUR)
                    .col("a", ColumnType.LONG)
                    .timestamp("ts")
                    .wal()
            );
            final int rows = 3;
            final long values = Unsafe.malloc(rows * 8L, MemoryTag.NATIVE_DEFAULT);
            final long pairs = Unsafe.malloc(rows * 16L, MemoryTag.NATIVE_DEFAULT);
            try (WalWriter writer = engine.getWalWriter(token)) {
                for (int i = 0; i < rows; i++) {
                    Unsafe.putLong(values + i * 8L, 20L + i);
                    putPair(pairs, i, TS0 + i * SECOND, i);
                }
                try (WalColumnRangeAppender appender = new WalColumnRangeAppender(writer)) {
                    appender.begin(rows);
                    appender.putFixedRange(0, values, rows * 8L);
                    try {
                        appender.putFixedRange(0, values, rows * 8L);
                        Assert.fail();
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "column already written in this block [column=0]");
                    }
                    appender.putTimestampRange(1, pairs, 0);
                    try {
                        appender.putTimestampRange(1, pairs, 0);
                        Assert.fail();
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "column already written in this block [column=1]");
                    }
                    appender.cancel();
                    Assert.assertEquals(0, writer.getSegmentRowCount());

                    appender.begin(rows);
                    appender.putFixedRange(0, values, rows * 8L);
                    appender.putTimestampRange(1, pairs, 0);
                    appender.finish(TS0, TS0 + 2 * SECOND, false);
                }
                writer.commit();
            } finally {
                Unsafe.free(values, rows * 8L, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(pairs, rows * 16L, MemoryTag.NATIVE_DEFAULT);
            }
            drainWalQueue();
            assertQuery("SELECT a FROM twice")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a
                            20
                            21
                            22
                            """);
        });
    }

    @Test
    public void testRefusesTimestampPairsNotStartingAtSourceRow() throws Exception {
        // the client claims the pairs start at source row 0, which is the segment's row, so they would be copied;
        // the pairs carry rows 5 to 7, which the copy would write as the segment's row indexes
        assertMemoryLeak(() -> {
            final TableToken token = createTable(new TableModel(configuration, "pairs", PartitionBy.HOUR)
                    .col("a", ColumnType.LONG)
                    .timestamp("ts")
                    .wal()
            );
            final int rows = 3;
            final long values = Unsafe.malloc(rows * 8L, MemoryTag.NATIVE_DEFAULT);
            final long pairs = Unsafe.malloc(rows * 16L, MemoryTag.NATIVE_DEFAULT);
            try (
                    WalWriter writer = engine.getWalWriter(token);
                    WalColumnRangeAppender appender = new WalColumnRangeAppender(writer)
            ) {
                for (int i = 0; i < rows; i++) {
                    Unsafe.putLong(values + i * 8L, 30L + i);
                    putPair(pairs, i, TS0 + i * SECOND, 5 + i);
                }
                appender.begin(rows);
                appender.putFixedRange(0, values, rows * 8L);
                try {
                    appender.putTimestampRange(1, pairs, 0);
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "timestamp pairs do not start at the source row [column=1, sourceFirstRow=0, firstPairRow=5]");
                }
                // the honest claim takes the rewrite branch and lands
                appender.putTimestampRange(1, pairs, 5);
                appender.finish(TS0, TS0 + 2 * SECOND, false);
                writer.commit();
            } finally {
                Unsafe.free(values, rows * 8L, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(pairs, rows * 16L, MemoryTag.NATIVE_DEFAULT);
            }
            drainWalQueue();
            assertQuery("SELECT a, ts FROM pairs")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            a\tts
                            30\t2026-01-01T00:00:00.000000Z
                            31\t2026-01-01T00:00:01.000000Z
                            32\t2026-01-01T00:00:02.000000Z
                            """);
        });
    }

    @Test
    public void testRefusesTimestampRangeOnOtherColumn() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = createTable(new TableModel(configuration, "other", PartitionBy.HOUR)
                    .col("a", ColumnType.LONG)
                    .timestamp("ts")
                    .wal()
            );
            try (
                    WalWriter writer = engine.getWalWriter(token);
                    WalColumnRangeAppender appender = new WalColumnRangeAppender(writer)
            ) {
                appender.begin(1);
                try {
                    appender.putTimestampRange(0, 0, 0);
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "not the designated timestamp [column=0]");
                }
                try {
                    appender.putFixedRange(1, 0, 16);
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "not a fixed-size column [column=1]");
                }
                appender.cancel();
            }
        });
    }

    @Test
    public void testRefusesVarSizeRangeOnFixedColumn() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = createTable(new TableModel(configuration, "notvar", PartitionBy.HOUR)
                    .col("a", ColumnType.LONG)
                    .timestamp("ts")
                    .wal()
            );
            try (
                    WalWriter writer = engine.getWalWriter(token);
                    WalColumnRangeAppender appender = new WalColumnRangeAppender(writer)
            ) {
                appender.begin(1);
                try {
                    appender.putVarSizeRange(0, 0, 0, 0, 0);
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "not a var-size column [column=0]");
                }
                appender.cancel();
            }
        });
    }

    @Test
    public void testRefusesVarSizeRangeWhoseDataSizeDisagrees() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = createTable(new TableModel(configuration, "mismatch", PartitionBy.HOUR)
                    .col("s", ColumnType.STRING)
                    .timestamp("ts")
                    .wal()
            );
            final ColumnTypeDriver driver = ColumnType.getDriver(ColumnType.STRING);
            final long pairs = Unsafe.malloc(3 * 16L, MemoryTag.NATIVE_DEFAULT);
            try (
                    MemoryCARW aux = Vm.getCARWInstance(1 << 20, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                    MemoryCARW data = Vm.getCARWInstance(1 << 20, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                    WalWriter writer = engine.getWalWriter(token);
                    WalColumnRangeAppender appender = new WalColumnRangeAppender(writer)
            ) {
                driver.configureAuxMemO3RSS(aux);
                StringTypeDriver.appendValue(aux, data, "one");
                StringTypeDriver.appendValue(aux, data, "two");
                StringTypeDriver.appendValue(aux, data, "three");
                final long auxBase = aux.addressOf(0);
                final long dataSize = driver.getDataVectorSize(auxBase, 0, 2);
                for (int i = 0; i < 3; i++) {
                    putPair(pairs, i, TS0 + i * SECOND, i);
                }
                appender.begin(3);
                try {
                    appender.putVarSizeRange(0, auxBase, data.addressOf(0), 0, dataSize - 2);
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "var-size range does not match its aux entries [column=0, dataEnd=" + dataSize + ", expected=" + (dataSize - 2) + "]");
                }
                appender.putVarSizeRange(0, auxBase, data.addressOf(0), 0, dataSize);
                appender.putTimestampRange(1, pairs, 0);
                appender.finish(TS0, TS0 + 2 * SECOND, false);
                writer.commit();
            } finally {
                Unsafe.free(pairs, 3 * 16L, MemoryTag.NATIVE_DEFAULT);
            }
            drainWalQueue();
            assertQuery("SELECT s FROM mismatch")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            s
                            one
                            two
                            three
                            """);
        });
    }

    @Test
    public void testResolvesSymbolKeysInFirstRowOrder() throws Exception {
        // source keys 1, 0, 1, -1, -1 with values b, a: "b" is resolved first and takes the table's key 0, as the row
        // API would give it; a negative source key, here twice, is NULL and is resolved without a lookup
        assertMemoryLeak(() -> {
            final TableToken token = createTable(new TableModel(configuration, "symbols", PartitionBy.HOUR)
                    .col("s", ColumnType.SYMBOL)
                    .timestamp("ts")
                    .wal()
            );
            final int rows = 5;
            final long keys = Unsafe.malloc(rows * 4L, MemoryTag.NATIVE_DEFAULT);
            final long pairs = Unsafe.malloc(rows * 16L, MemoryTag.NATIVE_DEFAULT);
            final AtomicInteger lookups = new AtomicInteger();
            try (
                    WalWriter writer = engine.getWalWriter(token);
                    WalColumnRangeAppender appender = new WalColumnRangeAppender(writer)
            ) {
                final int[] sourceKeys = {1, 0, 1, -1, -1};
                for (int i = 0; i < rows; i++) {
                    Unsafe.putInt(keys + i * 4L, sourceKeys[i]);
                    putPair(pairs, i, TS0 + i * SECOND, i);
                }
                appender.begin(rows);
                appender.putSymbolRange(0, keys, key -> {
                    lookups.incrementAndGet();
                    return key == 0 ? "a" : "b";
                });
                appender.putTimestampRange(1, pairs, 0);
                appender.finish(TS0, TS0 + 4 * SECOND, false);
                writer.commit();
            } finally {
                Unsafe.free(keys, rows * 4L, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(pairs, rows * 16L, MemoryTag.NATIVE_DEFAULT);
            }
            Assert.assertEquals("one lookup per distinct non-negative key", 2, lookups.get());
            drainWalQueue();
            assertQuery("SELECT s, ts FROM symbols")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            s\tts
                            b\t2026-01-01T00:00:00.000000Z
                            a\t2026-01-01T00:00:01.000000Z
                            b\t2026-01-01T00:00:02.000000Z
                            \t2026-01-01T00:00:03.000000Z
                            \t2026-01-01T00:00:04.000000Z
                            """);
            try (TableReader reader = engine.getReader(token)) {
                final SymbolMapReader symbols = reader.getSymbolMapReader(0);
                Assert.assertEquals("b", symbols.valueOf(0).toString());
                Assert.assertEquals("a", symbols.valueOf(1).toString());
                Assert.assertTrue(symbols.containsNullValue());
            }
        });
    }

    @Test
    public void testRewritesTimestampRowsOfASourceRangeStartingLater() throws Exception {
        // the source segment's rows 100 to 102 land at segment row 0: the pairs must carry rows 0 to 2, which the
        // out-of-order apply reads to find each timestamp's value
        assertMemoryLeak(() -> {
            final TableToken token = createTable(new TableModel(configuration, "later", PartitionBy.HOUR)
                    .col("a", ColumnType.LONG)
                    .timestamp("ts")
                    .wal()
            );
            final int rows = 3;
            final long values = Unsafe.malloc(rows * 8L, MemoryTag.NATIVE_DEFAULT);
            final long pairs = Unsafe.malloc(rows * 16L, MemoryTag.NATIVE_DEFAULT);
            try (WalWriter writer = engine.getWalWriter(token)) {
                final long[] timestamps = {TS0 + 2 * SECOND, TS0, TS0 + SECOND};
                for (int i = 0; i < rows; i++) {
                    Unsafe.putLong(values + i * 8L, 1L + i);
                    putPair(pairs, i, timestamps[i], 100 + i);
                }
                try (WalColumnRangeAppender appender = new WalColumnRangeAppender(writer)) {
                    appender.begin(rows);
                    appender.putFixedRange(0, values, rows * 8L);
                    appender.putTimestampRange(1, pairs, 100);
                    appender.finish(TS0, TS0 + 2 * SECOND, true);
                }
                writer.commit();
            } finally {
                Unsafe.free(values, rows * 8L, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(pairs, rows * 16L, MemoryTag.NATIVE_DEFAULT);
            }
            drainWalQueue();
            assertQuery("SELECT a, ts FROM later")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            a\tts
                            2\t2026-01-01T00:00:00.000000Z
                            3\t2026-01-01T00:00:01.000000Z
                            1\t2026-01-01T00:00:02.000000Z
                            """);
        });
    }

    @Test
    public void testSyncsWrittenColumnsInsideBlockOnly() throws Exception {
        final AtomicInteger msyncs = new AtomicInteger();
        ff = new TestFilesFacadeImpl() {
            @Override
            public void msync(long addr, long len, boolean async) {
                msyncs.incrementAndGet();
                super.msync(addr, len, async);
            }
        };
        try {
            assertMemoryLeak(() -> {
                final TableToken token = createTable(new TableModel(configuration, "synced", PartitionBy.HOUR)
                        .col("a", ColumnType.LONG)
                        .col("v", ColumnType.VARCHAR)
                        .timestamp("ts")
                        .wal()
                );
                final int rows = 2;
                final long values = Unsafe.malloc(rows * 8L, MemoryTag.NATIVE_DEFAULT);
                final long pairs = Unsafe.malloc(rows * 16L, MemoryTag.NATIVE_DEFAULT);
                try (
                        MemoryCARW aux = Vm.getCARWInstance(1 << 20, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                        MemoryCARW data = Vm.getCARWInstance(1 << 20, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                        WalWriter writer = engine.getWalWriter(token);
                        WalColumnRangeAppender appender = new WalColumnRangeAppender(writer)
                ) {
                    final ColumnTypeDriver driver = ColumnType.getDriver(ColumnType.VARCHAR);
                    driver.configureAuxMemO3RSS(aux);
                    // values longer than nine bytes, so that the column's data page is written and mapped
                    VarcharTypeDriver.appendValue(aux, data, new Utf8String("the first value, not inlined"));
                    VarcharTypeDriver.appendValue(aux, data, new Utf8String("the second value, not inlined"));
                    for (int i = 0; i < rows; i++) {
                        Unsafe.putLong(values + i * 8L, i);
                        putPair(pairs, i, TS0 + i * SECOND, i);
                    }
                    try {
                        appender.syncWrittenColumns();
                        Assert.fail();
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "no open block");
                    }
                    appender.begin(rows);
                    appender.putFixedRange(0, values, rows * 8L);
                    appender.putVarSizeRange(1, aux.addressOf(0), data.addressOf(0), 0, driver.getDataVectorSize(aux.addressOf(0), 0, 1));
                    try {
                        appender.putVarSizeRange(1, aux.addressOf(0), data.addressOf(0), 0, driver.getDataVectorSize(aux.addressOf(0), 0, 1));
                        Assert.fail();
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "column already written in this block [column=1]");
                    }
                    appender.putTimestampRange(2, pairs, 0);
                    msyncs.set(0);
                    appender.syncWrittenColumns();
                    // one msync per memory with a mapped page: the data memories of a, v and ts, and the aux memory of v
                    Assert.assertEquals(4, msyncs.get());
                    appender.finish(TS0, TS0 + SECOND, false);
                    writer.commit();
                } finally {
                    Unsafe.free(values, rows * 8L, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.free(pairs, rows * 16L, MemoryTag.NATIVE_DEFAULT);
                }
                drainWalQueue();
                assertQuery("SELECT a, v FROM synced")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                a\tv
                                0\tthe first value, not inlined
                                1\tthe second value, not inlined
                                """);
            });
        } finally {
            ff = null;
        }
    }

    static void putPair(long pairs, int i, long timestamp, long row) {
        Unsafe.putLong(pairs + i * 16L, timestamp);
        Unsafe.putLong(pairs + i * 16L + 8, row);
    }

    private static void assertRefused(Runnable call, CharSequence message) {
        try {
            call.run();
            Assert.fail("expected a CairoException containing: " + message);
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), message);
        }
    }

    // rows [rowLo, rowHi) of the source vectors as one block, with one timestamp per row from firstTimestamp
    private static void appendVarSizeBlock(
            WalColumnRangeAppender appender,
            ColumnTypeDriver driver,
            long auxBase,
            MemoryCARW data,
            long pairs,
            int rowLo,
            int rowHi,
            long firstTimestamp
    ) {
        final int rows = rowHi - rowLo;
        for (int i = 0; i < rows; i++) {
            putPair(pairs, i, firstTimestamp + i * SECOND, rowLo + i);
        }
        final long dataOffset = driver.getDataVectorOffset(auxBase, rowLo);
        final long dataSize = driver.getDataVectorSize(auxBase, rowLo, rowHi - 1);
        appender.begin(rows);
        appender.putVarSizeRange(0, auxBase + driver.getAuxVectorOffset(rowLo), data.addressOf(dataOffset), dataOffset, dataSize);
        appender.putTimestampRange(1, pairs, rowLo);
        appender.finish(firstTimestamp, firstTimestamp + (rows - 1) * SECOND, false);
    }
}
