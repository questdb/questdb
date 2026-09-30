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

import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Every SQL route to a POSTING index on a composite table must WORK, on a non-dimension symbol column.
 * <p>
 * This class previously asserted the opposite -- all three routes were refused, because
 * {@code sealPostingIndexForPartition} was cell-blind and SUSPENDED the table on the first merge
 * commit. That seal is now cell-aware, so the refusals are gone and the routes are covered here as
 * working. Kept as one class per route rather than folded into the seal test, because closing one
 * route is not closing the class: when these gates went in, {@code ADD COLUMN} and
 * {@code ALTER COLUMN TYPE} were closed first and probing then showed CREATE and
 * {@code ALTER ... ADD INDEX} were still wide open.
 * <p>
 * Indexing a DIMENSION column is a different question and deliberately not covered: the partitioning
 * already provides that access path, so the case that matters is an ordinary symbol column alongside
 * the dimension.
 */
public class CompositePostingEntryPointsTest extends AbstractCairoTest {

    @Test
    public void testCreateWithPostingIndexWorks() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE c1 (ts TIMESTAMP, exch SYMBOL, sym SYMBOL INDEX TYPE POSTING, px DOUBLE) "
                    + "TIMESTAMP(ts) PARTITION BY DAY, exch WAL");
            execute("INSERT INTO c1 VALUES ('2023-01-01T01:00:00.000000Z','BTC','A',1.0),"
                    + "('2023-01-01T02:00:00.000000Z','ETH','B',2.0)");
            drainWalQueue();
            assertLiveWithRows("c1", "2");
        });
    }

    @Test
    public void testAddColumnWithPostingIndexWorks() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE c2 (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) "
                    + "PARTITION BY DAY, exch WAL");
            execute("INSERT INTO c2 VALUES ('2023-01-01T01:00:00.000000Z','BTC',1.0),"
                    + "('2023-01-01T02:00:00.000000Z','ETH',2.0)");
            drainWalQueue();

            execute("ALTER TABLE c2 ADD COLUMN tag SYMBOL INDEX TYPE POSTING");
            drainWalQueue();
            // An O3 row, so the commit takes the merge path that reaches the seal.
            execute("INSERT INTO c2 VALUES ('2023-01-01T01:30:00.000000Z','BTC',3.0,'X')");
            drainWalQueue();
            assertLiveWithRows("c2", "3");
        });
    }

    @Test
    public void testAlterColumnAddIndexPostingWorks() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE c3 (ts TIMESTAMP, exch SYMBOL, sym SYMBOL, px DOUBLE) TIMESTAMP(ts) "
                    + "PARTITION BY DAY, exch WAL");
            execute("INSERT INTO c3 VALUES ('2023-01-01T01:00:00.000000Z','BTC','A',1.0),"
                    + "('2023-01-01T02:00:00.000000Z','ETH','B',2.0)");
            drainWalQueue();

            execute("ALTER TABLE c3 ALTER COLUMN sym ADD INDEX TYPE POSTING");
            drainWalQueue();
            assertLiveWithRows("c3", "2");
        });
    }

    /**
     * POSITIVE CONTROL for the default index type. Without it the three assertions above could pass
     * because indexed SYMBOL columns were accepted wholesale without the POSTING machinery running.
     */
    @Test
    public void testDefaultBitmapIndexStillWorks() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE c4 (ts TIMESTAMP, exch SYMBOL, sym SYMBOL INDEX, px DOUBLE) "
                    + "TIMESTAMP(ts) PARTITION BY DAY, exch WAL");
            execute("INSERT INTO c4 VALUES ('2023-01-01T01:00:00.000000Z','BTC','A',1.0),"
                    + "('2023-01-01T02:00:00.000000Z','ETH','B',2.0)");
            drainWalQueue();
            assertLiveWithRows("c4", "2");
        });
    }

    private void assertLiveWithRows(String table, String expectedCount) throws Exception {
        Assert.assertFalse(table + " suspended -- the POSTING seal bricked it",
                engine.getTableSequencerAPI().isSuspended(engine.verifyTableName(table)));
        final StringSink sink = new StringSink();
        TestUtils.printSql(engine, sqlExecutionContext, "SELECT count() FROM " + table, sink);
        TestUtils.assertContains(sink, expectedCount);
    }
}
