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

package io.questdb.test.cairo.o3;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableToken;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * End-to-end tests for writing a partition as a COMPOSITE - several pieces over one set of column files,
 * with the incoming rows appended at the tail and the untouched pieces left exactly where they are.
 * <p>
 * FIXED-WIDTH columns only for now: the column-level merge is implemented for those, and var-size columns
 * throw until their aux vectors are handled.
 * <p>
 * The oracle in each case is a table built by plain UNION ALL, which never touches the composite machinery.
 * That is what makes these tests about the RESULT rather than about the geometry: whatever pieces the plan
 * decided on, the rows read back have to be the same rows in the same order.
 */
public class O3CompositePartitionTest extends AbstractCairoTest {

    @Test
    public void testBackdatedInsertMergesOnlyWhereItLands() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");

            // One day at 15s, so the partition holds 5760 rows of fixed-width columns only.
            final String base = "SELECT x::INT i, -x j, timestamp_sequence('2020-02-03', 15*1000000L) ts" +
                    " FROM long_sequence(5760)";
            // A later day, so 2020-02-03 is never the active partition and the write goes through the O3
            // path rather than an append to the open one.
            final String nextDay = "SELECT x::INT + 90000 i, -x - 90000L j," +
                    " timestamp_sequence('2020-02-06', 60*1000000L) ts FROM long_sequence(50)";
            execute("CREATE TABLE x AS (" + base + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x " + nextDay);
            drainWalQueue();

            final TableToken xt = engine.verifyTableName("x");

            // A narrow backdated batch landing inside the day. This is the shape the design exists for:
            // the rows either side of it should be KEPT, and only the stride it overlaps rewritten.
            final String backfill = "SELECT x::INT + 70000 i, -x - 70000L j," +
                    " timestamp_sequence('2020-02-03T04:00:07', 5*1000000L) ts FROM long_sequence(200)";
            execute("INSERT INTO x " + backfill);
            drainWalQueue();
            Assert.assertFalse("the composite write suspended the table", engine.getTableSequencerAPI().isSuspended(xt));

            // The oracle: the same rows, assembled without ever touching the composite machinery.
            execute("CREATE TABLE o AS (" + base + " UNION ALL " + nextDay + " UNION ALL " + backfill +
                    ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            assertSameRows();

            // ...and again with no reader or writer cached, so the read comes off _txn and _geometry as
            // they are on disk rather than out of anything still resident.
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            assertSameRows();
        });
    }

    @Test
    public void testChronologicalAppendRewritesNothing() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");

            final String base = "SELECT x::INT i, -x j, timestamp_sequence('2020-02-03', 15*1000000L) ts" +
                    " FROM long_sequence(5760)";
            final String nextDay = "SELECT x::INT + 90000 i, -x - 90000L j," +
                    " timestamp_sequence('2020-02-06', 60*1000000L) ts FROM long_sequence(50)";
            execute("CREATE TABLE x AS (" + base + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x " + nextDay);
            drainWalQueue();

            // Rows that sort ABOVE everything 2020-02-03 holds but still inside that day. Every existing
            // piece is KEPT and the batch becomes a piece of its own, so the commit writes only the rows it
            // brought - no amplification at all.
            final String tail = "SELECT x::INT + 80000 i, -x - 80000L j," +
                    " timestamp_sequence('2020-02-03T23:00:00', 1000000L) ts FROM long_sequence(100)";
            execute("INSERT INTO x " + tail);
            drainWalQueue();

            final TableToken xt = engine.verifyTableName("x");
            Assert.assertFalse("the composite write suspended the table", engine.getTableSequencerAPI().isSuspended(xt));

            execute("CREATE TABLE o AS (" + base + " UNION ALL " + nextDay + " UNION ALL " + tail +
                    ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            assertSameRows();
        });
    }

    private static void assertSameRows() throws Exception {
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "SELECT * FROM o ORDER BY ts, i",
                "SELECT * FROM x ORDER BY ts, i",
                LOG
        );
    }
}
