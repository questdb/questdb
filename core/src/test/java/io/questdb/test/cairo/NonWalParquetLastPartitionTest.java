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

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * A non-WAL table whose last partition is parquet. The writer keeps no native partition
 * open in that state, so rows at or before the end of that partition have to be merged
 * in through O3, while rows past its end start a new native partition.
 */
public class NonWalParquetLastPartitionTest extends AbstractCairoTest {

    @Test
    public void testInOrderInsertAfterBypassWal() throws Exception {
        assertMemoryLeak(() -> {
            createParquetTableAndBypassWal();

            // separate statements: the second one runs on the writer the first O3 commit left behind
            execute("INSERT INTO t VALUES ('2022-02-26T01:00:00', 2)");
            execute("INSERT INTO t VALUES ('2022-02-26T01:00:00', 3)");
            execute("INSERT INTO t VALUES ('2022-02-26T02:00:00', 4)");

            assertQuery("t")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tv
                            2022-02-26T00:00:00.000000Z\t1
                            2022-02-26T01:00:00.000000Z\t2
                            2022-02-26T01:00:00.000000Z\t3
                            2022-02-26T02:00:00.000000Z\t4
                            """);
            assertPartitions("2022-02-26\ttrue\n");
        });
    }

    @Test
    public void testInOrderInsertAfterDropActivePartition() throws Exception {
        assertInOrderInsertAfterDroppingActivePartition("ALTER TABLE t DROP PARTITION LIST '2022-02-26'");
    }

    @Test
    public void testInOrderInsertAfterForceDropActivePartition() throws Exception {
        assertInOrderInsertAfterDroppingActivePartition("ALTER TABLE t FORCE DROP PARTITION LIST '2022-02-26'");
    }

    @Test
    public void testInOrderInsertAfterO3CommitCreatesParquetLastPartition() throws Exception {
        assertMemoryLeak(() -> {
            createParquetTableAndBypassWal();

            // the first row puts the transaction into O3 mode, so the O3 commit creates the
            // next day, and partitions born on a FORMAT PARQUET table are parquet
            execute("INSERT INTO t VALUES ('2022-02-25T12:00:00', 2), ('2022-02-27T00:00:00', 3)");
            assertPartitions("""
                    2022-02-25\ttrue
                    2022-02-26\ttrue
                    2022-02-27\ttrue
                    """);

            execute("INSERT INTO t VALUES ('2022-02-27T01:00:00', 4)");
            // past the parquet last partition: a new native partition
            execute("INSERT INTO t VALUES ('2022-02-28T00:00:00', 5)");
            execute("INSERT INTO t VALUES ('2022-02-28T01:00:00', 6)");

            assertQuery("t")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tv
                            2022-02-25T12:00:00.000000Z\t2
                            2022-02-26T00:00:00.000000Z\t1
                            2022-02-27T00:00:00.000000Z\t3
                            2022-02-27T01:00:00.000000Z\t4
                            2022-02-28T00:00:00.000000Z\t5
                            2022-02-28T01:00:00.000000Z\t6
                            """);
            assertPartitions("""
                    2022-02-25\ttrue
                    2022-02-26\ttrue
                    2022-02-27\ttrue
                    2022-02-28\tfalse
                    """);
        });
    }

    @Test
    public void testInOrderInsertWithIndexesAfterBypassWal() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t (
                        ts TIMESTAMP,
                        b SYMBOL INDEX TYPE BITMAP,
                        p SYMBOL INDEX TYPE POSTING
                    ) TIMESTAMP(ts) PARTITION BY DAY FORMAT PARQUET WAL
                    """);
            execute("INSERT INTO t VALUES ('2022-02-26T00:00:00', 'x', 'y')");
            drainWalQueue();
            bypassWalAndRestart();

            execute("INSERT INTO t VALUES ('2022-02-26T01:00:00', 'x', 'y')");
            execute("INSERT INTO t VALUES ('2022-02-26T02:00:00', 'z', 'y')");

            assertQuery("SELECT ts, b FROM t WHERE b = 'x'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tb
                            2022-02-26T00:00:00.000000Z\tx
                            2022-02-26T01:00:00.000000Z\tx
                            """);
            assertQuery("SELECT ts, p FROM t WHERE p = 'y'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tp
                            2022-02-26T00:00:00.000000Z\ty
                            2022-02-26T01:00:00.000000Z\ty
                            2022-02-26T02:00:00.000000Z\ty
                            """);
        });
    }

    @Test
    public void testRetriedInsertKeepsParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            // the parquet last partition holds exactly one row
            createParquetTableAndBypassWal();

            // A client retries the INSERT. The server may accept or reject the row,
            // but neither attempt may destroy the row that is already committed.
            for (int attempt = 0; attempt < 2; attempt++) {
                try {
                    execute("INSERT INTO t VALUES ('2022-02-26T01:00:00', 2)");
                } catch (Throwable ignore) {
                }
            }

            assertQuery("SELECT ts, v FROM t WHERE v = 1")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tv
                            2022-02-26T00:00:00.000000Z\t1
                            """);
        });
    }

    private static void bypassWalAndRestart() throws Exception {
        execute("ALTER TABLE t SET TYPE BYPASS WAL");
        // SET TYPE takes effect on the next engine load
        engine.releaseInactive();
        engine.load();
    }

    private static void createParquetTableAndBypassWal() throws Exception {
        execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY FORMAT PARQUET WAL");
        execute("INSERT INTO t VALUES ('2022-02-26T00:00:00', 1)");
        drainWalQueue();
        bypassWalAndRestart();
    }

    private void assertInOrderInsertAfterDroppingActivePartition(String dropSql) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t VALUES ('2022-02-25T00:00:00', 1), ('2022-02-26T00:00:00', 2)");
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2022-02-25'");
            // dropping the active partition makes the parquet partition the last one
            execute(dropSql);

            execute("INSERT INTO t VALUES ('2022-02-25T01:00:00', 3)");
            execute("INSERT INTO t VALUES ('2022-02-25T02:00:00', 4)");

            assertQuery("t")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tv
                            2022-02-25T00:00:00.000000Z\t1
                            2022-02-25T01:00:00.000000Z\t3
                            2022-02-25T02:00:00.000000Z\t4
                            """);
            assertPartitions("2022-02-25\ttrue\n");
        });
    }

    private void assertPartitions(String expected) throws Exception {
        assertQuery("SELECT name, isParquet FROM table_partitions('t')")
                .noLeakCheck()
                .expectSize()
                .noRandomAccess()
                .returns("name\tisParquet\n" + expected);
    }
}
