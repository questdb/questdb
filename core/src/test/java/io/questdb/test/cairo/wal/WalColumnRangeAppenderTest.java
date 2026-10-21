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
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.WalColumnRangeAppender;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

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

    static void putPair(long pairs, int i, long timestamp, long row) {
        Unsafe.putLong(pairs + i * 16L, timestamp);
        Unsafe.putLong(pairs + i * 16L + 8, row);
    }
}
