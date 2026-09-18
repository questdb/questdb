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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.test.tools.TestUtils.assertContains;

/**
 * Direct-writer-API coverage for NOT NULL enforcement at row append: a rejected
 * append must leave the row storage untouched so that {@link TableWriter.Row#cancel()}
 * recovers cleanly and subsequent rows land in the correct slots. SQL INSERT does
 * not exercise this path: the compiler rejects NULL literals ahead of the writer,
 * so the writer-side rejection is only reachable through the writer API (ILP et al.).
 */
public class NotNullWriterRowCancelTest extends AbstractCairoTest {

    @Test
    public void testCancelRejectedFirstRowThenCommitNoTimestamp() throws Exception {
        // dirtyTransientRowCount == 1 ordering: rejection on the very first row of the table
        assertMemoryLeak(() -> {
            execute("create table t (a int, b int not null)");
            try (TableWriter writer = getWriter("t")) {
                TableWriter.Row rejected = writer.newRow();
                assertNotNullRejection(rejected, "b");
                rejected.cancel();

                TableWriter.Row row = writer.newRow();
                row.putInt(0, 1);
                row.putInt(1, 1);
                row.append();
                writer.commit();
            }
            assertQuery("select * from t")
                    .expectSize()
                    .returns("""
                            a\tb
                            1\t1
                            """);
        });
    }

    @Test
    public void testCancelRejectedRowMixedWrittenSubsets() throws Exception {
        // multiple NOT NULL columns; different written/unwritten subsets across rejections
        assertMemoryLeak(() -> {
            execute("create table t (a int, b int not null, c int not null, d int)");
            try (TableWriter writer = getWriter("t")) {
                TableWriter.Row row = writer.newRow();
                row.putInt(0, 1);
                row.putInt(1, 1);
                row.putInt(2, 1);
                row.putInt(3, 1);
                row.append();
                writer.commit();

                // nothing written: rejection names the first missing NOT NULL column
                TableWriter.Row rejected = writer.newRow();
                assertNotNullRejection(rejected, "b");
                rejected.cancel();

                // b written, c omitted: rejection moves to the next missing NOT NULL column
                rejected = writer.newRow();
                rejected.putInt(1, 42);
                assertNotNullRejection(rejected, "c");
                rejected.cancel();

                // only nullable columns written
                rejected = writer.newRow();
                rejected.putInt(0, 42);
                rejected.putInt(3, 42);
                assertNotNullRejection(rejected, "b");
                rejected.cancel();

                TableWriter.Row good = writer.newRow();
                good.putInt(0, 2);
                good.putInt(1, 2);
                good.putInt(2, 2);
                good.putInt(3, 2);
                good.append();
                writer.commit();
            }
            assertQuery("select * from t")
                    .expectSize()
                    .returns("""
                            a\tb\tc\td
                            1\t1\t1\t1
                            2\t2\t2\t2
                            """);
        });
    }

    @Test
    public void testCancelRejectedRowThenCommitDesignatedTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (a int, b int not null, ts timestamp) timestamp(ts) partition by day bypass wal");
            try (TableWriter writer = getWriter("t")) {
                TableWriter.Row row = writer.newRow(MicrosFormatUtils.parseTimestamp("2024-01-01T00:00:00.000000Z"));
                row.putInt(0, 1);
                row.putInt(1, 1);
                row.append();
                writer.commit();

                TableWriter.Row rejected = writer.newRow(MicrosFormatUtils.parseTimestamp("2024-01-01T01:00:00.000000Z"));
                assertNotNullRejection(rejected, "b");
                rejected.cancel();

                TableWriter.Row good = writer.newRow(MicrosFormatUtils.parseTimestamp("2024-01-01T02:00:00.000000Z"));
                good.putInt(0, 2);
                good.putInt(1, 2);
                good.append();
                writer.commit();
            }
            assertQuery("select * from t")
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            a\tb\tts
                            1\t1\t2024-01-01T00:00:00.000000Z
                            2\t2\t2024-01-01T02:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testCancelRejectedRowThenCommitNoTimestamp() throws Exception {
        // review probe replica: (1,1) committed, reject-append-nothing-written, cancel, (2,2) committed
        assertMemoryLeak(() -> {
            execute("create table t (a int, b int not null)");
            try (TableWriter writer = getWriter("t")) {
                TableWriter.Row row = writer.newRow();
                row.putInt(0, 1);
                row.putInt(1, 1);
                row.append();
                writer.commit();

                TableWriter.Row rejected = writer.newRow();
                assertNotNullRejection(rejected, "b");
                rejected.cancel();

                TableWriter.Row good = writer.newRow();
                good.putInt(0, 2);
                good.putInt(1, 2);
                good.append();
                writer.commit();
            }
            assertQuery("select * from t")
                    .expectSize()
                    .returns("""
                            a\tb
                            1\t1
                            2\t2
                            """);
        });
    }

    @Test
    public void testCancelRejectedRowThenCommitO3() throws Exception {
        // O3 ordering: rejection happens while the writer is in O3 mode
        assertMemoryLeak(() -> {
            execute("create table t (a int, b int not null, ts timestamp) timestamp(ts) partition by day bypass wal");
            try (TableWriter writer = getWriter("t")) {
                TableWriter.Row row = writer.newRow(MicrosFormatUtils.parseTimestamp("2024-01-01T02:00:00.000000Z"));
                row.putInt(0, 1);
                row.putInt(1, 1);
                row.append();

                // older timestamp flips the writer into O3 mode
                TableWriter.Row rejected = writer.newRow(MicrosFormatUtils.parseTimestamp("2024-01-01T01:00:00.000000Z"));
                assertNotNullRejection(rejected, "b");
                rejected.cancel();

                TableWriter.Row good = writer.newRow(MicrosFormatUtils.parseTimestamp("2024-01-01T01:00:00.000000Z"));
                good.putInt(0, 2);
                good.putInt(1, 2);
                good.append();
                writer.commit();
            }
            assertQuery("select * from t")
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            a\tb\tts
                            2\t2\t2024-01-01T01:00:00.000000Z
                            1\t1\t2024-01-01T02:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testCancelRejectedRowThenCommitWalWriter() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (a int, b int not null, ts timestamp) timestamp(ts) partition by day wal");
            try (WalWriter writer = getWalWriter("t")) {
                TableWriter.Row row = writer.newRow(MicrosFormatUtils.parseTimestamp("2024-01-01T00:00:00.000000Z"));
                row.putInt(0, 1);
                row.putInt(1, 1);
                row.append();
                writer.commit();

                TableWriter.Row rejected = writer.newRow(MicrosFormatUtils.parseTimestamp("2024-01-01T01:00:00.000000Z"));
                assertNotNullRejection(rejected, "b");
                rejected.cancel();

                TableWriter.Row good = writer.newRow(MicrosFormatUtils.parseTimestamp("2024-01-01T02:00:00.000000Z"));
                good.putInt(0, 2);
                good.putInt(1, 2);
                good.append();
                writer.commit();
            }
            drainWalQueue();
            assertQuery("select * from t")
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            a\tb\tts
                            1\t1\t2024-01-01T00:00:00.000000Z
                            2\t2\t2024-01-01T02:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testCancelRejectedRowThenMoreRowsBeforeCommitNoTimestamp() throws Exception {
        // cancel-then-more-rows: several uncommitted rows follow the cancelled rejection
        assertMemoryLeak(() -> {
            execute("create table t (a int, b int not null)");
            try (TableWriter writer = getWriter("t")) {
                TableWriter.Row row = writer.newRow();
                row.putInt(0, 1);
                row.putInt(1, 1);
                row.append();

                TableWriter.Row rejected = writer.newRow();
                assertNotNullRejection(rejected, "b");
                rejected.cancel();

                for (int i = 2; i < 5; i++) {
                    TableWriter.Row good = writer.newRow();
                    good.putInt(0, i);
                    good.putInt(1, i);
                    good.append();
                }
                writer.commit();
            }
            assertQuery("select * from t")
                    .expectSize()
                    .returns("""
                            a\tb
                            1\t1
                            2\t2
                            3\t3
                            4\t4
                            """);
        });
    }

    private static void assertNotNullRejection(TableWriter.Row row, String columnName) {
        try {
            row.append();
            Assert.fail("append should have rejected missing NOT NULL column: " + columnName);
        } catch (CairoException e) {
            assertContains(e.getFlyweightMessage(), "NOT NULL constraint violation, column is required [column=" + columnName + ']');
        }
    }
}
