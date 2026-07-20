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

package io.questdb.test.griffin;

import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * Task 3 of the composite window/horizon-join-slave work: wires
 * {@link io.questdb.griffin.engine.table.CompositeTimeFrameRecordCursor} into
 * {@link io.questdb.griffin.engine.table.CompositePageFrameRecordCursorFactory} so a composite table
 * ({@code PARTITION BY DAY, <dimension>}) can be the SLAVE of a WINDOW / HORIZON join.
 * <p>
 * The merged time-frame cursor is single-threaded (there is no per-worker concurrent twin --
 * {@code newTimeFrameCursor()} stays null), so the factory reports
 * {@code supportsConcurrentTimeFrameCursor() == false} and the code generator must route such a slave
 * to the SERIAL join factory ({@code Window Join} / {@code Horizon Join}), never the async/parallel one
 * ({@code Async Window Join} / {@code Async Horizon Join}) whose atom would NPE on the null concurrent
 * cursor. These tests pin that routing (via EXPLAIN) AND cross-check the serial composite join result
 * row-for-row against a byte-identical PLAIN twin table, all with parallel joins explicitly ENABLED so
 * the guard -- not merely a disabled parallel knob -- is what forces the serial path.
 * <p>
 * Before Task 3 wiring these composite-slave queries did not compile at all: the slave gate threw
 * "right side of window join must be a table" / "right-hand side of HORIZON JOIN can only be a table".
 * <p>
 * Dataset: master {@code m} (plain, page-frame scan -- so the parallel path is a real candidate), a
 * composite slave {@code c} with multiple sibling cells per day (the cross-cell merge), and its plain
 * twin {@code p} holding the identical rows. Timestamps are minutes apart so the +/-2 minute
 * window/horizon range captures neighbours for a non-trivial result.
 */
public class CompositeWindowHorizonSlaveTest extends AbstractCairoTest {

    private static final String COMPOSITE_HORIZON = "select h.offset, sum(s.v) sv " +
            "from m t " +
            "horizon join c s " +
            "range from -2m to 2m step 1m as h " +
            "order by h.offset";
    private static final String COMPOSITE_WINDOW = "select t.ts, sum(s.v) sv " +
            "from m t " +
            "window join c s " +
            "range between 2 minute preceding and 2 minute following exclude prevailing " +
            "order by t.ts";
    private static final String TWIN_HORIZON = "select h.offset, sum(s.v) sv " +
            "from m t " +
            "horizon join p s " +
            "range from -2m to 2m step 1m as h " +
            "order by h.offset";
    private static final String TWIN_WINDOW = "select t.ts, sum(s.v) sv " +
            "from m t " +
            "window join p s " +
            "range between 2 minute preceding and 2 minute following exclude prevailing " +
            "order by t.ts";

    @Test
    public void testHorizonJoinCompositeSlaveTakesSerialPathAndMatchesTwin() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // Parallel horizon join is a real candidate; the concurrent-cursor guard -- not a disabled
            // knob -- is what must force the serial path for the composite slave.
            sqlExecutionContext.setParallelHorizonJoinEnabled(true);

            // Compiles + runs + correct vs the plain twin (RED before Task 3: the slave gate threw).
            assertSqlCursors(TWIN_HORIZON, COMPOSITE_HORIZON);

            // Routed to the SERIAL Horizon Join over the composite merge scan...
            assertQuery(COMPOSITE_HORIZON)
                    .noLeakCheck()
                    .assertsPlanContaining("Horizon Join", "Composite cross-cell merge scan");
            // ...and provably NOT the async path (so the null concurrent cursor is never dereferenced).
            assertQuery(COMPOSITE_HORIZON)
                    .noLeakCheck()
                    .assertsPlanNotContaining("Async");
        });
    }

    @Test
    public void testNonCompositeSlaveKeepsParallelHorizonJoin() throws Exception {
        // NEGATIVE CONTROL: the new supportsConcurrentTimeFrameCursor() default (== supportsTimeFrameCursor)
        // must NOT change plain factory selection -- a plain slave still takes the async/parallel path.
        assertMemoryLeak(() -> {
            createTables();
            sqlExecutionContext.setParallelHorizonJoinEnabled(true);
            assertQuery(TWIN_HORIZON)
                    .noLeakCheck()
                    .assertsPlanContaining("Async", "Horizon Join");
        });
    }

    @Test
    public void testNonCompositeSlaveKeepsParallelWindowJoin() throws Exception {
        // NEGATIVE CONTROL: a plain slave still takes the async/parallel WINDOW join path (byte-identical
        // to before this change).
        assertMemoryLeak(() -> {
            createTables();
            sqlExecutionContext.setParallelWindowJoinEnabled(true);
            assertQuery(TWIN_WINDOW)
                    .noLeakCheck()
                    .assertsPlanContaining("Async Window Join");
        });
    }

    @Test
    public void testWindowJoinCompositeSlaveTakesSerialPathAndMatchesTwin() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // Parallel window join is a real candidate; without the concurrent-cursor guard the composite
            // slave would be selected onto the async path and NPE on the null per-worker cursor.
            sqlExecutionContext.setParallelWindowJoinEnabled(true);

            // Compiles + runs + correct vs the plain twin (RED before Task 3: the slave gate threw).
            assertSqlCursors(TWIN_WINDOW, COMPOSITE_WINDOW);

            // Routed to the SERIAL Window Join over the composite merge scan...
            assertQuery(COMPOSITE_WINDOW)
                    .noLeakCheck()
                    .assertsPlanContaining("Window Join", "Composite cross-cell merge scan");
            // ...and provably NOT the async path.
            assertQuery(COMPOSITE_WINDOW)
                    .noLeakCheck()
                    .assertsPlanNotContaining("Async");
        });
    }

    private void createTables() throws SqlException {
        // Master: plain page-frame table -- supports page frames so the parallel join path is a genuine
        // candidate (a composite master would disable parallelism outright and defeat the negative control).
        execute("CREATE TABLE m (ts TIMESTAMP, mv DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
        // Composite slave (partition by day + exch dimension) and its byte-identical plain twin.
        execute("CREATE TABLE c (ts TIMESTAMP, exch SYMBOL, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY, exch WAL");
        execute("CREATE TABLE p (ts TIMESTAMP, exch SYMBOL, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");

        execute("INSERT INTO m VALUES " +
                "('2024-03-01T00:03:00.000000Z', 2.0)," +
                "('2024-03-01T00:02:00.000000Z', 1.0)," +
                "('2024-03-02T00:02:00.000000Z', 3.0)");

        // Inserted scrambled so the WAL write path O3-sorts every cell; day 2024-03-01 spans three
        // interleaved cells (A/B/C) -- the cross-cell merge -- and 2024-03-02 spans two (A/B).
        final String rows = " VALUES " +
                "('2024-03-01T00:03:00.000000Z','A',103.0)," +
                "('2024-03-02T00:02:00.000000Z','A',202.0)," +
                "('2024-03-01T00:01:00.000000Z','A',101.0)," +
                "('2024-03-01T00:04:00.000000Z','C',104.0)," +
                "('2024-03-02T00:03:00.000000Z','B',203.0)," +
                "('2024-03-01T00:02:00.000000Z','B',102.0)," +
                "('2024-03-02T00:01:00.000000Z','A',201.0)";
        execute("INSERT INTO c" + rows);
        execute("INSERT INTO p" + rows);
        drainWalQueue();
    }
}
