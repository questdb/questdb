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

import io.questdb.cairo.EntityColumnFilter;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.RecordToRowCopier;
import io.questdb.griffin.RecordToRowCopierUtils;
import io.questdb.griffin.SqlCompiler;
import io.questdb.std.BytecodeAssembler;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Direct-invocation tests for {@link TableWriter#replaceRange(long, long, io.questdb.cairo.sql.RecordCursor,
 * io.questdb.griffin.RecordToRowCopier, int, io.questdb.griffin.SqlExecutionContext)} - the empty-range
 * (survivorCursor == null) path, which empties {@code [lo, hiExcl)} in place. Mirrors
 * {@code WalWriterReplaceRangeTest}'s NOT-BETWEEN reference approach but exercises {@code replaceRange} directly
 * on a {@link TableWriter} rather than through a WAL replace commit.
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
                deleted = w.replaceRange(lo, hiExcl, null, null, w.getMetadata().getTimestampIndex(), sqlExecutionContext);
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
                deleted = w.replaceRange(lo, hiExcl, null, null, w.getMetadata().getTimestampIndex(), sqlExecutionContext);
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
                deleted = w.replaceRange(lo, hiExcl, null, null, w.getMetadata().getTimestampIndex(), sqlExecutionContext);
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
                deleted = w.replaceRange(lo, hiExcl, null, null, w.getMetadata().getTimestampIndex(), sqlExecutionContext);
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
                deleted = w.replaceRange(lo, hiExcl, null, null, w.getMetadata().getTimestampIndex(), sqlExecutionContext);
            }

            // No rows in the range, so nothing is deleted.
            Assert.assertEquals(0, deleted);
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "ref", "src", LOG);
        });
    }

    @Test
    public void testReplaceRangeSurvivorsRewritesPartition() throws Exception {
        assertMemoryLeak(() -> {
            // 300 minute-spaced rows, all on 1970-01-01 (x*60s for x in 1..300 -> up to 05:00:00).
            execute("create table src (ts timestamp, x long, s symbol) timestamp(ts) partition by DAY BYPASS WAL");
            execute("insert into src select (x*60*1000000L)::timestamp, x, rnd_symbol('a','b','c') from long_sequence(300)");
            // Survivors of "delete where x % 2 = 0" -> keep the odd-x rows. ref captures the real (non-random)
            // symbol values for the survivors, so it is a faithful post-delete reference including the symbol column.
            execute("create table ref as (select * from src where not (x % 2 = 0)) timestamp(ts) partition by DAY BYPASS WAL");

            TableToken tt = engine.verifyTableName("src");
            // Single-partition seed -> the whole day spans it; the executor (Task 1.10) invokes replaceRange
            // once per partition with a survivor cursor filtered to [partLo, partHi).
            long partLo = MicrosTimestampDriver.floor("1970-01-01T00:00:00.000000Z");
            long partHi = MicrosTimestampDriver.floor("1970-01-02T00:00:00.000000Z");

            long removed;
            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    RecordCursorFactory factory = compiler.compile(
                            "select * from src where not (x % 2 = 0) and ts >= " + partLo + " and ts < " + partHi,
                            sqlExecutionContext
                    ).getRecordCursorFactory();
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                final EntityColumnFilter columnFilter = new EntityColumnFilter();
                columnFilter.of(factory.getMetadata().getColumnCount());
                try (TableWriter w = getWriter(tt)) {
                    final RecordToRowCopier copier = RecordToRowCopierUtils.generateCopier(
                            new BytecodeAssembler(),
                            factory.getMetadata(),
                            w.getMetadata(),
                            columnFilter,
                            configuration
                    );
                    removed = w.replaceRange(partLo, partHi, cursor, copier, w.getMetadata().getTimestampIndex(), sqlExecutionContext);
                }
            }

            // 300 rows, even-x deleted -> 150 removed, 150 odd-x survivors remain.
            Assert.assertEquals(150, removed);
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "ref", "src", LOG);
        });
    }

    /**
     * {@code replaceRange}'s survivor path must not assume the incoming cursor already arrives in timestamp
     * order: it has an internal sort/dispatch/swap branch specifically to reorder a genuinely out-of-order
     * survivor cursor before committing. Every other direct test in this file - and every DELETE-driven
     * survivor cursor in production (see {@code OperationExecutor#executeDelete}) - sources its survivor rows
     * from a plain {@code SELECT * FROM <designated-ts table> WHERE ...}, which always scans in physical
     * (ascending-ts) order, a QuestDB storage invariant; the reorder branch is reachable in principle but
     * never actually exercised by those tests.
     * <p>
     * This test forces a genuinely descending-ts cursor via an explicit {@code ORDER BY x DESC} (x and ts are
     * monotonically related by construction, and QuestDB does not eliminate an explicit reverse sort just
     * because the underlying scan is already ascending) over the exact same predicate and data as
     * {@link #testReplaceRangeSurvivorsRewritesPartition} - row order is the ONLY difference from that
     * passing test - and asserts the post-replace partition is nonetheless fully and correctly ts-ordered
     * against the same NOT-predicate reference. A {@code replaceRange} that trusted cursor order blindly
     * would either corrupt the partition's physical ts order - caught here because a subsequent scan of a
     * designated-timestamp table always reads back in physical order, so it would then mismatch {@code ref}
     * (naturally ascending) in content and/or order - or throw outright.
     */
    @Test
    public void testReplaceRangeSurvivorsFromUnorderedCursorReordersCorrectly() throws Exception {
        assertMemoryLeak(() -> {
            // 300 minute-spaced rows, all on 1970-01-01 (x*60s for x in 1..300 -> up to 05:00:00).
            execute("create table src (ts timestamp, x long, s symbol) timestamp(ts) partition by DAY BYPASS WAL");
            execute("insert into src select (x*60*1000000L)::timestamp, x, rnd_symbol('a','b','c') from long_sequence(300)");
            // Survivors of "delete where x % 2 = 0" -> keep the odd-x rows (same reference shape as
            // testReplaceRangeSurvivorsRewritesPartition).
            execute("create table ref as (select * from src where not (x % 2 = 0)) timestamp(ts) partition by DAY BYPASS WAL");

            TableToken tt = engine.verifyTableName("src");
            long partLo = MicrosTimestampDriver.floor("1970-01-01T00:00:00.000000Z");
            long partHi = MicrosTimestampDriver.floor("1970-01-02T00:00:00.000000Z");

            long removed;
            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    // ORDER BY x DESC is the only difference from testReplaceRangeSurvivorsRewritesPartition's
                    // cursor query: x (and therefore ts) descends, so this cursor is genuinely, fully out of
                    // the ts order replaceRange must commit in.
                    RecordCursorFactory factory = compiler.compile(
                            "select * from src where not (x % 2 = 0) and ts >= " + partLo + " and ts < " + partHi +
                                    " order by x desc",
                            sqlExecutionContext
                    ).getRecordCursorFactory();
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                final EntityColumnFilter columnFilter = new EntityColumnFilter();
                columnFilter.of(factory.getMetadata().getColumnCount());
                try (TableWriter w = getWriter(tt)) {
                    final RecordToRowCopier copier = RecordToRowCopierUtils.generateCopier(
                            new BytecodeAssembler(),
                            factory.getMetadata(),
                            w.getMetadata(),
                            columnFilter,
                            configuration
                    );
                    removed = w.replaceRange(partLo, partHi, cursor, copier, w.getMetadata().getTimestampIndex(), sqlExecutionContext);
                }
            }

            // Same predicate/data as testReplaceRangeSurvivorsRewritesPartition -> same counts.
            Assert.assertEquals(150, removed);
            // Content AND order: a subsequent scan of src (designated-timestamp table) reads back in physical
            // order, so this only matches ref (naturally ascending) if replaceRange actually re-sorted the
            // descending input cursor before committing.
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "ref", "src", LOG);
        });
    }

    /**
     * Regression test for a latent NPE: {@code copier.copy(null, record, row)} used to pass a null
     * {@link io.questdb.griffin.SqlExecutionContext} on the survivor-cursor path. That is fine for
     * schema-identical copies of ordinary types, but {@code RecordToRowCopierUtils} has no same-type fast
     * path for DECIMAL8..DECIMAL256 destination columns - the generated copier unconditionally calls
     * {@code context.getDecimal256()}/{@code getDecimal128()} as scratch space whenever the destination
     * column is a DECIMAL, even when source and destination types match exactly. A table with a DECIMAL
     * column therefore NPE'd on every replaceRange survivor copy. This test fails with a
     * NullPointerException against the pre-fix code and passes once a real context is threaded through.
     */
    @Test
    public void testReplaceRangeSurvivorsWithDecimalColumn() throws Exception {
        assertMemoryLeak(() -> {
            // 300 minute-spaced rows, all on 1970-01-01, with a decimal(18,4) column (DECIMAL64 storage -
            // precision 18 falls in the 10-18 -> 8-byte bucket). cast(x as decimal(18,4)) mirrors
            // UpdateTest#testUpdateDecimalColumn's cast(x as decimal(..)) pattern for seeding decimals from x.
            execute("create table src (ts timestamp, x long, d decimal(18,4)) timestamp(ts) partition by DAY BYPASS WAL");
            execute("insert into src select (x*60*1000000L)::timestamp, x, cast(x as decimal(18,4)) from long_sequence(300)");
            // Survivors of "delete where x % 2 = 0" -> keep the odd-x rows.
            execute("create table ref as (select * from src where not (x % 2 = 0)) timestamp(ts) partition by DAY BYPASS WAL");

            TableToken tt = engine.verifyTableName("src");
            // Single-partition seed -> the whole day spans it.
            long partLo = MicrosTimestampDriver.floor("1970-01-01T00:00:00.000000Z");
            long partHi = MicrosTimestampDriver.floor("1970-01-02T00:00:00.000000Z");

            long removed;
            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    RecordCursorFactory factory = compiler.compile(
                            "select * from src where not (x % 2 = 0) and ts >= " + partLo + " and ts < " + partHi,
                            sqlExecutionContext
                    ).getRecordCursorFactory();
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                final EntityColumnFilter columnFilter = new EntityColumnFilter();
                columnFilter.of(factory.getMetadata().getColumnCount());
                try (TableWriter w = getWriter(tt)) {
                    final RecordToRowCopier copier = RecordToRowCopierUtils.generateCopier(
                            new BytecodeAssembler(),
                            factory.getMetadata(),
                            w.getMetadata(),
                            columnFilter,
                            configuration
                    );
                    removed = w.replaceRange(partLo, partHi, cursor, copier, w.getMetadata().getTimestampIndex(), sqlExecutionContext);
                }
            }

            // 300 rows, even-x deleted -> 150 removed, 150 odd-x survivors remain.
            Assert.assertEquals(150, removed);
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "ref", "src", LOG);
        });
    }
}
