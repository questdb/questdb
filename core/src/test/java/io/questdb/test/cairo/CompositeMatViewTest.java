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

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * Sub-project 7: materialized views over a composite-partitioned base table.
 * <p>
 * The gate here was lifted on MEASUREMENT. Every assertion below is differential -- a view over a
 * composite base against a view over its plain twin, built from the identical rows -- because a
 * mat view can be wrong in two ways that a single-table check cannot see: it can hold the wrong
 * aggregate, or it can hold a CORRECT aggregate that has silently stopped tracking its base.
 * <p>
 * One trap worth recording, since it nearly produced a wrong conclusion: the first probe reported an
 * empty view and a {@code valid} status, which looks exactly like a silent-staleness defect. It was
 * the harness -- {@code drainWalQueue()} does not drive the refresh job. With
 * {@code drainMatViewQueue(engine)} the view populates correctly. Any future test here must drive
 * both queues or it measures its own plumbing.
 */
public class CompositeMatViewTest extends AbstractCairoTest {

    @Test(timeout = 120_000)
    public void testViewOverCompositeBaseMatchesViewOverPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createTwinBases();
            seedBoth();

            execute("CREATE MATERIALIZED VIEW mvc AS (SELECT ts, exch, avg(px) AS ap FROM c SAMPLE BY 1h) PARTITION BY DAY");
            execute("CREATE MATERIALIZED VIEW mvp AS (SELECT ts, exch, avg(px) AS ap FROM p SAMPLE BY 1h) PARTITION BY DAY");
            refresh();

            assertQuery("SELECT view_status FROM materialized_views() WHERE view_name = 'mvc'")
                    .noLeakCheck().noRandomAccess()
                    .returns("view_status\nvalid\n");
            assertSqlCursors("SELECT * FROM mvp ORDER BY ts, exch", "SELECT * FROM mvc ORDER BY ts, exch");
        });
    }

    /**
     * The harder half: a view that is correct at creation but stops tracking its base is a silent
     * staleness bug, and it looks identical to a working one until the base changes. The inserts below
     * deliberately include an OUT-OF-ORDER row into an already-populated cell, not just an append.
     */
    @Test(timeout = 120_000)
    public void testIncrementalRefreshTracksBothCells() throws Exception {
        assertMemoryLeak(() -> {
            createTwinBases();
            seedBoth();
            execute("CREATE MATERIALIZED VIEW mvc AS (SELECT ts, exch, avg(px) AS ap FROM c SAMPLE BY 1h) PARTITION BY DAY");
            execute("CREATE MATERIALIZED VIEW mvp AS (SELECT ts, exch, avg(px) AS ap FROM p SAMPLE BY 1h) PARTITION BY DAY");
            refresh();

            insertBoth("('2023-01-03T01:00:00.000000Z','E1',6.0)");
            insertBoth("('2023-01-01T01:45:00.000000Z','E0',7.0)");   // O3, into an existing cell
            refresh();

            assertSqlCursors("SELECT * FROM mvp ORDER BY ts, exch", "SELECT * FROM mvc ORDER BY ts, exch");
            assertQuery("SELECT view_status FROM materialized_views() WHERE view_name = 'mvc'")
                    .noLeakCheck().noRandomAccess()
                    .returns("view_status\nvalid\n");
        });
    }

    private void createTwinBases() throws Exception {
        execute("CREATE TABLE c (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) PARTITION BY DAY, exch WAL");
        execute("CREATE TABLE p (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
    }

    private void insertBoth(String values) throws Exception {
        execute("INSERT INTO c VALUES " + values);
        execute("INSERT INTO p VALUES " + values);
        drainWalQueue();
    }

    private void refresh() {
        drainWalQueue();
        drainMatViewQueue(engine);
        drainWalQueue();
    }

    private void seedBoth() throws Exception {
        insertBoth("('2023-01-01T01:00:00.000000Z','E0',1.0)");
        insertBoth("('2023-01-01T01:30:00.000000Z','E1',2.0)");
        insertBoth("('2023-01-01T02:00:00.000000Z','E0',3.0)");
        insertBoth("('2023-01-02T01:00:00.000000Z','E1',4.0)");
        insertBoth("('2023-01-02T01:15:00.000000Z','E0',5.0)");
    }
}
