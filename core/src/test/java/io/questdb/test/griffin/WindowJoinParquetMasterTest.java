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

package io.questdb.test.griffin;

import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.table.ExtraNullColumnCursorFactory;
import io.questdb.griffin.engine.table.TablePageFrameCursor;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.std.Files;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;

/**
 * A WINDOW JOIN whose ON filter is constant false compiles to an
 * {@code ExtraNullColumnCursorFactory} splice over the join MASTER: the slave never
 * contributes rows, so the master scan is padded with synthetic NULL columns for the
 * window aggregates. A parquet-backed master - {@code read_parquet()} with an explicit
 * {@code TIMESTAMP(ts)} declaration - hands the splice a plain {@link PageFrameCursor},
 * not a {@link TablePageFrameCursor}, so the splice must claim only the surface its base
 * actually provides. A table-backed master must preserve the table cursor surface for
 * consumers that use its reader and partition controls. Page-frame consumers above the
 * splice (parallel GROUP BY, top-K) drive the plain seam end-to-end.
 */
public class WindowJoinParquetMasterTest extends AbstractCairoTest {

    private static final String CONST_FALSE_TABLE_WINDOW_JOIN =
            "SELECT v, ts, sum(price) AS w " +
                    "FROM table_x " +
                    "WINDOW JOIN prices p ON (0 = 1) " +
                    "RANGE BETWEEN 1 MINUTE PRECEDING AND 1 MINUTE FOLLOWING";
    private static final String CONST_FALSE_WINDOW_JOIN =
            "SELECT v, ts, sum(price) AS w " +
                    "FROM ((SELECT * FROM read_parquet('x.parquet')) TIMESTAMP(ts)) " +
                    "WINDOW JOIN prices p ON (0 = 1) " +
                    "RANGE BETWEEN 1 MINUTE PRECEDING AND 1 MINUTE FOLLOWING";

    @Test
    public void testConstFalseWindowJoinOverParquetMasterPageFrameSurface() throws Exception {
        // Contract-level lock: the splice's page-frame cursor over a non-table base must be a
        // plain PageFrameCursor wrapper. Pre-fix ExtraNullColumnCursorFactory.getPageFrameCursor
        // cast the base cursor to TablePageFrameCursor unconditionally and threw
        // ClassCastException over the plain read-parquet page-frame cursor.
        assertMemoryLeak(() -> {
            createFixture();
            try (RecordCursorFactory factory = select(CONST_FALSE_WINDOW_JOIN)) {
                RecordCursorFactory splice = findFactory(factory, ExtraNullColumnCursorFactory.class);
                Assert.assertNotNull("const-false WINDOW JOIN must compile to the ExtraNullColumn splice", splice);
                Assert.assertTrue("the splice must keep the parquet base page-frame capability", splice.supportsPageFrameCursor());
                Assert.assertFalse("read_parquet() has no time-frame capability to forward", splice.supportsTimeFrameCursor());
                try (PageFrameCursor cursor = splice.getPageFrameCursor(sqlExecutionContext, ORDER_ASC)) {
                    Assert.assertFalse(
                            "the splice must not claim TablePageFrameCursor over a non-table base",
                            cursor instanceof TablePageFrameCursor
                    );
                    Assert.assertTrue("the parquet base is external", cursor.isExternal());
                    Assert.assertNotNull("column mapping must delegate to the parquet cursor", cursor.getColumnMapping());
                    long frameRows = 0;
                    PageFrame frame;
                    while ((frame = cursor.next()) != null) {
                        Assert.assertEquals("the splice pads the two master columns with one null column",
                                3, frame.getColumnCount());
                        frameRows += frame.getPartitionHi() - frame.getPartitionLo();
                    }
                    Assert.assertEquals("every parquet master row must flow through the splice", 10, frameRows);
                    cursor.toTop();
                    Assert.assertNotNull("the cursor must be reusable after toTop", cursor.next());
                }
            }
        });
    }

    @Test
    public void testConstFalseWindowJoinOverParquetMasterParallelGroupBy() throws Exception {
        // End-to-end page-frame consumer: the parallel GROUP BY drives the splice's page frames
        // through ExtraNullColumnCursorFactory.getPageFrameCursor - the exact seam that used to
        // throw ClassCastException over the plain read-parquet base cursor. Every master row
        // survives the const-false join, the window aggregate is NULL on each of them.
        assertMemoryLeak(() -> {
            createFixture();
            assertQuery("SELECT sum(v) s, count(ts) c, count(w) cw FROM (" + CONST_FALSE_WINDOW_JOIN + ")")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            s\tc\tcw
                            55\t10\t0
                            """);
        });
    }

    @Test
    public void testConstFalseWindowJoinOverParquetMasterTopK() throws Exception {
        // End-to-end top-K consumer: ORDER BY + LIMIT keeps the splice as the page-frame leaf
        // (canPeelForTopK does not peel it) and opens its page-frame cursor over the plain
        // read-parquet base - the second production consumer of the same seam.
        assertMemoryLeak(() -> {
            createFixture();
            assertQuery("SELECT * FROM (" + CONST_FALSE_WINDOW_JOIN + ") ORDER BY v DESC LIMIT 3")
                    .expectSize()
                    .returns("""
                            v\tts\tw
                            10\t1970-01-01T00:00:10.000000Z\tnull
                            9\t1970-01-01T00:00:09.000000Z\tnull
                            8\t1970-01-01T00:00:08.000000Z\tnull
                            """);
        });
    }

    @Test
    public void testConstFalseWindowJoinOverTableMasterPageFrameSurface() throws Exception {
        // A table base must keep the TablePageFrameCursor contract through the null-padding
        // splice. Window and horizon joins use this surface for table readers and partition
        // positioning when the splice appears as a slave of another join.
        assertMemoryLeak(() -> {
            createFixture();
            execute("""
                    CREATE TABLE table_x AS (
                        SELECT x::int v, (x * 1_000_000)::timestamp ts FROM long_sequence(10)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            try (RecordCursorFactory factory = select(CONST_FALSE_TABLE_WINDOW_JOIN)) {
                RecordCursorFactory splice = findFactory(factory, ExtraNullColumnCursorFactory.class);
                Assert.assertNotNull("const-false WINDOW JOIN must compile to the ExtraNullColumn splice", splice);
                Assert.assertTrue("the splice must keep the table base page-frame capability", splice.supportsPageFrameCursor());
                Assert.assertTrue("the splice must keep the table base time-frame capability", splice.supportsTimeFrameCursor());
                try (PageFrameCursor cursor = splice.getPageFrameCursor(sqlExecutionContext, ORDER_ASC)) {
                    Assert.assertTrue(
                            "the splice must preserve TablePageFrameCursor over a table base",
                            cursor instanceof TablePageFrameCursor
                    );
                    TablePageFrameCursor tableCursor = (TablePageFrameCursor) cursor;
                    Assert.assertNotNull("the table reader must remain available through the splice", tableCursor.getTableReader());
                    Assert.assertFalse("the unfiltered table scan has no interval filter", tableCursor.hasIntervalFilter());
                    Assert.assertFalse("the table cursor is not external", tableCursor.isExternal());
                    tableCursor.toPartition(0);
                    long frameRows = 0;
                    PageFrame frame;
                    while ((frame = tableCursor.next()) != null) {
                        Assert.assertEquals("the splice appends one synthetic column", 3, frame.getColumnCount());
                        Assert.assertEquals("the synthetic column has no data page", 0, frame.getPageAddress(2));
                        Assert.assertEquals("the synthetic column has no data bytes", 0, frame.getPageSize(2));
                        Assert.assertEquals("the synthetic column has no auxiliary page", 0, frame.getAuxPageAddress(2));
                        Assert.assertEquals("the synthetic column has no auxiliary bytes", 0, frame.getAuxPageSize(2));
                        frameRows += frame.getPartitionHi() - frame.getPartitionLo();
                    }
                    Assert.assertEquals("the selected partition must expose every master row", 10, frameRows);
                }
            }
            assertQuery(CONST_FALSE_TABLE_WINDOW_JOIN)
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            v\tts\tw
                            1\t1970-01-01T00:00:01.000000Z\tnull
                            2\t1970-01-01T00:00:02.000000Z\tnull
                            3\t1970-01-01T00:00:03.000000Z\tnull
                            4\t1970-01-01T00:00:04.000000Z\tnull
                            5\t1970-01-01T00:00:05.000000Z\tnull
                            6\t1970-01-01T00:00:06.000000Z\tnull
                            7\t1970-01-01T00:00:07.000000Z\tnull
                            8\t1970-01-01T00:00:08.000000Z\tnull
                            9\t1970-01-01T00:00:09.000000Z\tnull
                            10\t1970-01-01T00:00:10.000000Z\tnull
                            """);
        });
    }

    private static RecordCursorFactory findFactory(RecordCursorFactory f, Class<?> target) {
        while (f != null && !target.isInstance(f)) {
            f = f.getBaseFactory();
        }
        return f;
    }

    private void createFixture() throws Exception {
        execute("create table x as (select x::int v, (x * 1_000_000)::timestamp ts from long_sequence(10))");
        try (
                Path path = new Path();
                PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                TableReader reader = engine.getReader("x")
        ) {
            path.of(root).concat("x.parquet");
            PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
            PartitionEncoder.encode(partitionDescriptor, path);
            Assert.assertTrue(Files.exists(path.$()));
        }
        inputRoot = root;
        execute("CREATE TABLE prices (pts TIMESTAMP, price DOUBLE) TIMESTAMP(pts) PARTITION BY DAY");
        execute("INSERT INTO prices VALUES ('1970-01-01T00:00:00.500000Z', 1.0)");
    }
}
