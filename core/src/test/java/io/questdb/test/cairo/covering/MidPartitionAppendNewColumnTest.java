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

package io.questdb.test.cairo.covering;

import io.questdb.cairo.idx.PostingIndexWriter;
import io.questdb.test.AbstractCairoTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * The covered append path may only skip the O3-side index write for a column
 * whose partition ALREADY has index files. O3 is what creates them: for the
 * first rows a column ever has in a partition the copy job opens the posting
 * index with {@code isInit = true} (O3CopyJob's {@code openFromO3Context(row == 0)}),
 * which is what writes the {@code .pk}. The seal cannot stand in for that - it
 * opens an EXISTING index and throws "index does not exist" otherwise - and it
 * does not even skip the column, because {@code updateO3ColumnTops} has by then
 * replaced the column top of -1 with a real one.
 * <p>
 * Both DDL shapes below leave a covering-indexed column with no index files in
 * an older partition; the next append into that partition then has to create
 * them. If the append path claims the column, the commit fails, the writer is
 * distressed and a WAL table suspends - and re-applying reproduces it, so it
 * never heals.
 */
public class MidPartitionAppendNewColumnTest extends AbstractCairoTest {

    @After
    public void resetSwitch() {
        PostingIndexWriter.COVERING_MIDPART_APPEND_DISABLED = false;
    }

    @Test
    public void testAppendAfterAddColumnThenAddIndex() throws Exception {
        assertMemoryLeak(() -> {
            seedTwoPartitions();
            execute("ALTER TABLE t ADD COLUMN sym SYMBOL");
            drainWalQueue();
            // ADD INDEX skips a partition with no data for the column (the
            // ff.exists(dFile(...)) guard in indexHistoricPartitions), so the
            // mid partition still has no .pk afterwards.
            execute("ALTER TABLE t ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (value)");
            drainWalQueue();

            appendIntoMidPartitionAndAssert();
        });
    }

    /**
     * Negative control: with the append path switched off the same DDL and the
     * same append must work, proving any failure above belongs to the append
     * path and not to the DDL sequence.
     */
    @Test
    public void testAppendAfterAddColumnWorksOnResealPath() throws Exception {
        PostingIndexWriter.COVERING_MIDPART_APPEND_DISABLED = true;
        assertMemoryLeak(() -> {
            seedTwoPartitions();
            execute("ALTER TABLE t ADD COLUMN sym SYMBOL");
            drainWalQueue();
            execute("ALTER TABLE t ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (value)");
            drainWalQueue();

            appendIntoMidPartitionAndAssert();
        });
    }

    private void appendIntoMidPartitionAndAssert() throws Exception {
        // Appends into day 2, which is NOT the last partition (day 4 exists) and
        // which holds no rows for `sym` yet.
        execute("INSERT INTO t VALUES ('2024-01-02T23:00:00.000000Z', 1.5, 'S1')");
        execute("INSERT INTO t VALUES ('2024-01-02T23:00:01.000000Z', 2.5, 'S2')");
        drainWalQueue();

        Assert.assertFalse("table must not suspend",
                engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("t")));

        // The index must find exactly what the column holds.
        assertSqlCursors(
                "SELECT /*+ no_index */ ts, sym, value FROM t WHERE sym = 'S1' ORDER BY ts",
                "SELECT ts, sym, value FROM t WHERE sym = 'S1' ORDER BY ts"
        );
        printSql("SELECT count() FROM t WHERE sym = 'S1'");
        Assert.assertEquals("count\n1\n", sink.toString());

        // ... and the partition must keep working for further appends.
        execute("INSERT INTO t VALUES ('2024-01-02T23:00:02.000000Z', 3.5, 'S1')");
        drainWalQueue();
        assertSqlCursors(
                "SELECT /*+ no_index */ ts, sym, value FROM t WHERE sym = 'S1' ORDER BY ts",
                "SELECT ts, sym, value FROM t WHERE sym = 'S1' ORDER BY ts"
        );
    }

    private void seedTwoPartitions() throws Exception {
        execute("CREATE TABLE t (ts TIMESTAMP, value DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO t SELECT dateadd('u', x::INT, '2024-01-02T00:00:00Z'::TIMESTAMP)," +
                " x::DOUBLE FROM long_sequence(1000)");
        // a later day, so day 2 is a mid partition from here on
        execute("INSERT INTO t VALUES ('2024-01-04T00:00:00.000000Z', -1.0)");
        drainWalQueue();
    }
}
