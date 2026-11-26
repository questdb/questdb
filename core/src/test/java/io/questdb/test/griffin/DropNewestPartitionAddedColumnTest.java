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

import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static io.questdb.cairo.wal.WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE;

/**
 * A column added with ADD COLUMN keeps a default record in _cv naming the partition it was added at. When a
 * DROP PARTITION or a replace-range commit removes the newest partition, the writer moves that record back to
 * the new newest partition. The move must reach _cv on disk, or readers, and the writer after a reopen, treat
 * the column as absent in partitions created later before the old "added at" partition.
 */
@RunWith(Parameterized.class)
public class DropNewestPartitionAddedColumnTest extends AbstractCairoTest {
    private static final String HEADER = "a\tts\tb\ts\tv\n";
    private final boolean isWal;

    public DropNewestPartitionAddedColumnTest(WalMode walMode) {
        this.isWal = walMode == WalMode.WITH_WAL;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{{WalMode.WITH_WAL}, {WalMode.NO_WAL}});
    }

    @Test
    public void testDropEveryPartitionByListThenInsertBeforeAddedAt() throws Exception {
        assertDropEveryPartitionThenInsertBeforeAddedAt("ALTER TABLE t DROP PARTITION LIST '2024-01-01', '2024-01-02'");
    }

    @Test
    public void testDropEveryPartitionByWhereThenInsertBeforeAddedAt() throws Exception {
        assertDropEveryPartitionThenInsertBeforeAddedAt("ALTER TABLE t DROP PARTITION WHERE ts < '2024-01-03'");
    }

    @Test
    public void testDropNewestPartitionsThenInsertBetween() throws Exception {
        assertDropNewestPartitionsThenInsertBetween(false);
    }

    @Test
    public void testDropNewestPartitionsThenInsertBetweenParquetLastPartition() throws Exception {
        assertDropNewestPartitionsThenInsertBetween(true);
    }

    @Test
    public void testReplaceRangeRemovesNewestPartitionThenInsertBetween() throws Exception {
        // only a WAL commit can carry a replace range
        Assume.assumeTrue(isWal);
        assertMemoryLeak(() -> {
            createTableWithAddedColumns();
            // 2024-01-06 holds explicit column tops, so dropping it changes _cv by itself
            ddl("ALTER TABLE t DROP PARTITION LIST '2024-01-06'");
            // A replace range with no rows removes the newest partition, 2024-01-07, which holds no _cv record:
            // moving the "added at" partition back to 2024-01-01 is the only change to _cv
            replaceRange("2024-01-07", "2024-01-08");
            assertInsertBetweenReadsAddedColumns();
        });
    }

    @Test
    public void testReplaceRangeRemovesNewestPartitionsThenInsertBetween() throws Exception {
        // only a WAL commit can carry a replace range
        Assume.assumeTrue(isWal);
        assertMemoryLeak(() -> {
            createTableWithAddedColumns();
            ddl("INSERT INTO t VALUES (7, '2024-01-08T00:00:00.000000Z', 70, 's7', 'v7')");
            ddl("ALTER TABLE t DROP PARTITION LIST '2024-01-06'");
            // one commit removes the two newest partitions, neither of which holds a _cv record
            replaceRange("2024-01-07", "2024-01-09");
            assertInsertBetweenReadsAddedColumns();
        });
    }

    private void assertDropEveryPartitionThenInsertBeforeAddedAt(String dropSql) throws Exception {
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE t (a INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY" + walSuffix());
            ddl("INSERT INTO t VALUES (1, '2024-01-01T00:00:00.000000Z')");
            addColumns();
            ddl("INSERT INTO t VALUES (2, '2024-01-02T00:00:00.000000Z', 20, 's2', 'v2')");
            ddl(dropSql);
            assertTable(HEADER);

            // the column was added at 2024-01-02; no partition is left, so a new partition before it holds it
            ddl("INSERT INTO t VALUES (3, '2023-12-31T00:00:00.000000Z', 30, 's3', 'v3')");
            final String expected = HEADER + "3\t2023-12-31T00:00:00.000000Z\t30\ts3\tv3\n";
            assertTable(expected);
            assertTableAfterReopen(expected);

            ddl("INSERT INTO t VALUES (4, '2023-12-30T00:00:00.000000Z', 40, 's4', 'v4'), (5, '2024-01-03T00:00:00.000000Z', 50, 's5', 'v5')");
            assertTableAfterReopen(
                    HEADER +
                            "4\t2023-12-30T00:00:00.000000Z\t40\ts4\tv4\n" +
                            "3\t2023-12-31T00:00:00.000000Z\t30\ts3\tv3\n" +
                            "5\t2024-01-03T00:00:00.000000Z\t50\ts5\tv5\n"
            );
        });
    }

    private void assertDropNewestPartitionsThenInsertBetween(boolean isLastPartitionParquet) throws Exception {
        assertMemoryLeak(() -> {
            createTableWithAddedColumns();
            if (isLastPartitionParquet) {
                ddl("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            }
            // 2024-01-06 holds explicit column tops, so dropping it changes _cv by itself
            ddl("ALTER TABLE t DROP PARTITION LIST '2024-01-06'");
            // 2024-01-07 holds no _cv record, and 2024-01-01 already has one for each added column:
            // moving the "added at" partition back to 2024-01-01 is the only change to _cv
            ddl("ALTER TABLE t DROP PARTITION LIST '2024-01-07'");
            assertInsertBetweenReadsAddedColumns();
        });
    }

    private void addColumns() throws Exception {
        // one at a time: a WAL table does not take several columns in one ALTER
        ddl("ALTER TABLE t ADD COLUMN b INT");
        ddl("ALTER TABLE t ADD COLUMN s SYMBOL");
        ddl("ALTER TABLE t ADD COLUMN v VARCHAR");
    }

    private void assertInsertBetweenReadsAddedColumns() throws Exception {
        final String before = HEADER + "2\t2024-01-01T00:00:00.000000Z\t20\ts2\tv2\n";
        assertTable(before);

        ddl("INSERT INTO t VALUES (4, '2024-01-03T00:00:00.000000Z', 40, 's4', 'v4')");
        final String expected = before + "4\t2024-01-03T00:00:00.000000Z\t40\ts4\tv4\n";
        assertTable(expected);
        assertTableAfterReopen(expected);

        // the writer reopened from _cv on disk must keep writing the columns in new partitions
        ddl("INSERT INTO t VALUES (5, '2024-01-02T00:00:00.000000Z', 50, 's5', 'v5'), (6, '2024-01-04T00:00:00.000000Z', 60, 's6', 'v6')");
        assertTableAfterReopen(
                before +
                        "5\t2024-01-02T00:00:00.000000Z\t50\ts5\tv5\n" +
                        "4\t2024-01-03T00:00:00.000000Z\t40\ts4\tv4\n" +
                        "6\t2024-01-04T00:00:00.000000Z\t60\ts6\tv6\n"
        );
    }

    private void assertTable(String expected) throws Exception {
        assertQuery("SELECT a, ts, b, s, v FROM t")
                .noLeakCheck()
                .timestamp("ts")
                .expectSize()
                .returns(expected);
    }

    private void assertTableAfterReopen(String expected) throws Exception {
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        assertTable(expected);
    }

    private void createTableWithAddedColumns() throws Exception {
        ddl("CREATE TABLE t (a INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY" + walSuffix());
        ddl("INSERT INTO t VALUES (1, '2024-01-06T00:00:00.000000Z')");
        // added at 2024-01-06, the newest partition
        addColumns();
        // an O3 write before the "added at" partition gives 2024-01-01 explicit records with top 0
        ddl("INSERT INTO t VALUES (2, '2024-01-01T00:00:00.000000Z', 20, 's2', 'v2')");
        ddl("INSERT INTO t VALUES (3, '2024-01-07T00:00:00.000000Z', 30, 's3', 'v3')");
        assertTable(
                HEADER +
                        "2\t2024-01-01T00:00:00.000000Z\t20\ts2\tv2\n" +
                        "1\t2024-01-06T00:00:00.000000Z\tnull\t\t\n" +
                        "3\t2024-01-07T00:00:00.000000Z\t30\ts3\tv3\n"
        );
    }

    private void ddl(String sql) throws Exception {
        execute(sql);
        if (isWal) {
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("t")));
        }
    }

    private void replaceRange(String lo, String hi) throws Exception {
        final TableToken token = engine.verifyTableName("t");
        try (WalWriter w = engine.getWalWriter(token)) {
            w.commitWithParams(MicrosTimestampDriver.floor(lo), MicrosTimestampDriver.floor(hi), WAL_DEDUP_MODE_REPLACE_RANGE);
        }
        drainWalQueue();
        Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
    }

    private String walSuffix() {
        return isWal ? " WAL" : " BYPASS WAL";
    }
}
