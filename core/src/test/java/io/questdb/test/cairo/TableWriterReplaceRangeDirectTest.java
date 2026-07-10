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

import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Direct-invocation tests for {@link TableWriter#replaceRange(long, long, io.questdb.cairo.sql.RecordCursor,
 * io.questdb.griffin.RecordToRowCopier, int)} - the empty-range (survivorCursor == null) path, which empties
 * {@code [lo, hiExcl)} in place. Mirrors {@code WalWriterReplaceRangeTest}'s NOT-BETWEEN reference approach but
 * exercises {@code replaceRange} directly on a {@link TableWriter} rather than through a WAL replace commit.
 */
public class TableWriterReplaceRangeDirectTest extends AbstractCairoTest {

    @Test
    public void testReplaceRangeEmptyDeletesSubRange() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, x long) timestamp(ts) partition by DAY BYPASS WAL");
            execute("insert into src select (x*60*1000000L)::timestamp, x from long_sequence(200)");
            execute("create table ref as (select * from src where ts not between " +
                    "'1970-01-01T01:00:00.000000Z' and '1970-01-01T02:00:00.000000Z') " +
                    "timestamp(ts) partition by DAY BYPASS WAL");

            TableToken tt = engine.verifyTableName("src");
            long lo = MicrosTimestampDriver.floor("1970-01-01T01:00:00.000000Z");
            long hiExcl = MicrosTimestampDriver.floor("1970-01-01T02:00:00.000001Z");
            long deleted;
            try (TableWriter w = getWriter(tt)) {
                deleted = w.replaceRange(lo, hiExcl, null, null, w.getMetadata().getTimestampIndex());
            }

            // rows with ts in [01:00:00, 02:00:00] inclusive: minute rows 60..120 -> 61 rows
            Assert.assertEquals(61, deleted);
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "ref", "src", LOG);
        });
    }

    @Test
    public void testReplaceRangeEmptyDropsWholePartition() throws Exception {
        assertMemoryLeak(() -> {
            // 72 hourly rows spanning three daily partitions: 1970-01-01, -02, -03 (24 rows each).
            execute("create table src (ts timestamp, x long) timestamp(ts) partition by DAY BYPASS WAL");
            execute("insert into src select timestamp_sequence('1970-01-01T00:00:00.000000Z', 60*60*1000000L), x " +
                    "from long_sequence(72)");
            // Reference: everything except the whole middle partition (1970-01-02).
            execute("create table ref as (select * from src where ts < '1970-01-02T00:00:00.000000Z' " +
                    "or ts >= '1970-01-03T00:00:00.000000Z') timestamp(ts) partition by DAY BYPASS WAL");

            TableToken tt = engine.verifyTableName("src");
            long lo = MicrosTimestampDriver.floor("1970-01-02T00:00:00.000000Z");
            long hiExcl = MicrosTimestampDriver.floor("1970-01-03T00:00:00.000000Z");
            long deleted;
            try (TableWriter w = getWriter(tt)) {
                deleted = w.replaceRange(lo, hiExcl, null, null, w.getMetadata().getTimestampIndex());
            }

            // The entire 1970-01-02 partition (24 rows) is dropped.
            Assert.assertEquals(24, deleted);
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "ref", "src", LOG);
        });
    }

    @Test
    public void testReplaceRangeEmptySpansPartitionBoundary() throws Exception {
        assertMemoryLeak(() -> {
            // 72 hourly rows spanning three daily partitions.
            execute("create table src (ts timestamp, x long) timestamp(ts) partition by DAY BYPASS WAL");
            execute("insert into src select timestamp_sequence('1970-01-01T00:00:00.000000Z', 60*60*1000000L), x " +
                    "from long_sequence(72)");
            // Range [1970-01-01T18:00, 1970-01-02T06:00) trims the tail of the first partition and the head
            // of the second - a delete straddling a partition boundary. Reference keeps the rows outside it.
            execute("create table ref as (select * from src where ts < '1970-01-01T18:00:00.000000Z' " +
                    "or ts >= '1970-01-02T06:00:00.000000Z') timestamp(ts) partition by DAY BYPASS WAL");

            TableToken tt = engine.verifyTableName("src");
            long lo = MicrosTimestampDriver.floor("1970-01-01T18:00:00.000000Z");
            long hiExcl = MicrosTimestampDriver.floor("1970-01-02T06:00:00.000000Z");
            long deleted;
            try (TableWriter w = getWriter(tt)) {
                deleted = w.replaceRange(lo, hiExcl, null, null, w.getMetadata().getTimestampIndex());
            }

            // 6 rows from day-1 (hours 18..23) + 6 rows from day-2 (hours 0..5) = 12 rows.
            Assert.assertEquals(12, deleted);
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "ref", "src", LOG);
        });
    }

    @Test
    public void testReplaceRangeEmptyDeleteAllTruncates() throws Exception {
        assertMemoryLeak(() -> {
            // 100 rows spanning a single partition.
            execute("create table src (ts timestamp, x long) timestamp(ts) partition by DAY BYPASS WAL");
            execute("insert into src select (x*60*1000000L)::timestamp, x from long_sequence(100)");
            // Reference: empty table (no rows satisfy the NOT BETWEEN constraint that covers all).
            execute("create table ref (ts timestamp, x long) timestamp(ts) partition by DAY BYPASS WAL");

            TableToken tt = engine.verifyTableName("src");
            long lo = MicrosTimestampDriver.floor("1970-01-01T00:00:00.000000Z");
            long hiExcl = MicrosTimestampDriver.floor("1970-01-02T00:00:00.000000Z");
            long deleted;
            try (TableWriter w = getWriter(tt)) {
                deleted = w.replaceRange(lo, hiExcl, null, null, w.getMetadata().getTimestampIndex());
            }

            // All 100 rows should be deleted.
            Assert.assertEquals(100, deleted);
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "ref", "src", LOG);
        });
    }

    @Test
    public void testReplaceRangeEmptyRangeOutsideDataNoOp() throws Exception {
        assertMemoryLeak(() -> {
            // 48 rows on 1970-01-01 (hourly from 00:00 to 47:59, mapping to 2 days).
            execute("create table src (ts timestamp, x long) timestamp(ts) partition by DAY BYPASS WAL");
            execute("insert into src select timestamp_sequence('1970-01-01T00:00:00.000000Z', 60*60*1000000L), x " +
                    "from long_sequence(48)");
            // Reference: identical to src (no rows deleted).
            execute("create table ref as (select * from src) timestamp(ts) partition by DAY BYPASS WAL");

            TableToken tt = engine.verifyTableName("src");
            // Range entirely after the data: 1970-01-03 00:00 onwards.
            long lo = MicrosTimestampDriver.floor("1970-01-03T00:00:00.000000Z");
            long hiExcl = MicrosTimestampDriver.floor("1970-01-03T12:00:00.000000Z");
            long deleted;
            try (TableWriter w = getWriter(tt)) {
                deleted = w.replaceRange(lo, hiExcl, null, null, w.getMetadata().getTimestampIndex());
            }

            // No rows in the range, so nothing is deleted.
            Assert.assertEquals(0, deleted);
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "ref", "src", LOG);
        });
    }
}
