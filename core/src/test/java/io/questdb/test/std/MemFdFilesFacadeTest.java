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

package io.questdb.test.std;

import io.questdb.std.MemFdFilesFacade;
import io.questdb.test.AbstractCairoTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 * Verifies that CairoEngine operates correctly when all storage is backed by
 * anonymous memfds rather than real files. No data should be written to the
 * root directory on disk.
 */
public class MemFdFilesFacadeTest extends AbstractCairoTest {

    @BeforeClass
    public static void setUpStatic() throws Exception {
        // Inject MemFdFilesFacade before the engine is created so that
        // CairoTestConfiguration picks it up via staticOverrides.getFilesFacade().
        ff = MemFdFilesFacade.INSTANCE;
        AbstractCairoTest.setUpStatic();
    }

    @AfterClass
    public static void tearDownStatic() {
        AbstractCairoTest.tearDownStatic();
    }

    @Test
    public void testCreateInsertSelect() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE trades (" +
                    "    ts TIMESTAMP," +
                    "    symbol SYMBOL," +
                    "    price DOUBLE," +
                    "    qty LONG" +
                    ") TIMESTAMP(ts) PARTITION BY DAY"
            );

            execute(
                    "INSERT INTO trades VALUES" +
                    "    ('2024-01-15T10:00:00.000000Z', 'EURUSD', 1.0850, 100_000)," +
                    "    ('2024-01-15T10:01:00.000000Z', 'GBPUSD', 1.2700, 50_000)," +
                    "    ('2024-01-15T10:02:00.000000Z', 'EURUSD', 1.0855, 200_000)"
            );

            assertQueryNoLeakCheck(
                    "ts\tsymbol\tprice\tqty\n" +
                    "2024-01-15T10:00:00.000000Z\tEURUSD\t1.085\t100000\n" +
                    "2024-01-15T10:01:00.000000Z\tGBPUSD\t1.27\t50000\n" +
                    "2024-01-15T10:02:00.000000Z\tEURUSD\t1.0855\t200000\n",
                    "SELECT * FROM trades ORDER BY ts",
                    "ts",
                    true,
                    true
            );

            execute("DROP TABLE trades");
        });
    }

    @Test
    public void testLatestOn() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE quotes (" +
                    "    ts TIMESTAMP," +
                    "    sym SYMBOL," +
                    "    bid DOUBLE," +
                    "    ask DOUBLE" +
                    ") TIMESTAMP(ts) PARTITION BY DAY"
            );

            execute(
                    "INSERT INTO quotes VALUES" +
                    "    ('2024-01-15T09:00:00.000000Z', 'EURUSD', 1.0848, 1.0850)," +
                    "    ('2024-01-15T09:30:00.000000Z', 'GBPUSD', 1.2698, 1.2702)," +
                    "    ('2024-01-15T10:00:00.000000Z', 'EURUSD', 1.0852, 1.0854)," +
                    "    ('2024-01-15T10:15:00.000000Z', 'GBPUSD', 1.2701, 1.2705)"
            );

            assertQueryNoLeakCheck(
                    "ts\tsym\tbid\task\n" +
                    "2024-01-15T10:00:00.000000Z\tEURUSD\t1.0852\t1.0854\n" +
                    "2024-01-15T10:15:00.000000Z\tGBPUSD\t1.2701\t1.2705\n",
                    "SELECT * FROM quotes LATEST ON ts PARTITION BY sym",
                    "ts",
                    true,
                    true
            );

            execute("DROP TABLE quotes");
        });
    }

    @Test
    public void testNoFilesOnDisk() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE disk_check (ts TIMESTAMP, val LONG) TIMESTAMP(ts) PARTITION BY DAY"
            );
            execute(
                    "INSERT INTO disk_check VALUES ('2024-06-01T00:00:00.000000Z', 42)"
            );

            execute("DROP TABLE disk_check");
        });
    }

    @Test
    public void testSampleBy() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE prices (" +
                    "    ts TIMESTAMP," +
                    "    sym SYMBOL," +
                    "    price DOUBLE" +
                    ") TIMESTAMP(ts) PARTITION BY DAY"
            );

            execute(
                    "INSERT INTO prices VALUES" +
                    "    ('2024-01-15T10:00:00.000000Z', 'EURUSD', 1.0850)," +
                    "    ('2024-01-15T10:05:00.000000Z', 'EURUSD', 1.0852)," +
                    "    ('2024-01-15T10:10:00.000000Z', 'EURUSD', 1.0855)," +
                    "    ('2024-01-15T10:15:00.000000Z', 'EURUSD', 1.0860)"
            );

            assertQueryNoLeakCheck(
                    "ts\tavg_price\n" +
                    "2024-01-15T10:00:00.000000Z\t1.0851\n" +
                    "2024-01-15T10:10:00.000000Z\t1.08575\n",
                    "SELECT ts, avg(price) avg_price FROM prices SAMPLE BY 10m",
                    "ts",
                    true,
                    true
            );

            execute("DROP TABLE prices");
        });
    }

    @Test
    public void testCreateMemoryTable() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE MEMORY TABLE mem_trades (" +
                    "    ts TIMESTAMP," +
                    "    sym SYMBOL," +
                    "    price DOUBLE" +
                    ") TIMESTAMP(ts) PARTITION BY DAY"
            );

            execute(
                    "INSERT INTO mem_trades VALUES" +
                    "    ('2024-06-01T10:00:00.000000Z', 'AAPL', 190.50)," +
                    "    ('2024-06-01T10:01:00.000000Z', 'GOOG', 178.25)"
            );

            assertQueryNoLeakCheck(
                    "ts\tsym\tprice\n" +
                    "2024-06-01T10:00:00.000000Z\tAAPL\t190.5\n" +
                    "2024-06-01T10:01:00.000000Z\tGOOG\t178.25\n",
                    "SELECT * FROM mem_trades ORDER BY ts",
                    "ts",
                    true,
                    true
            );

            execute("DROP TABLE mem_trades");
        });
    }

    @Test
    public void testInVolumeMemory() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE vol_trades (" +
                    "    ts TIMESTAMP," +
                    "    val LONG" +
                    ") TIMESTAMP(ts) PARTITION BY DAY IN VOLUME ':memory:'"
            );

            execute(
                    "INSERT INTO vol_trades VALUES" +
                    "    ('2024-06-01T10:00:00.000000Z', 42)," +
                    "    ('2024-06-01T10:01:00.000000Z', 99)"
            );

            assertQueryNoLeakCheck(
                    "ts\tval\n" +
                    "2024-06-01T10:00:00.000000Z\t42\n" +
                    "2024-06-01T10:01:00.000000Z\t99\n",
                    "SELECT * FROM vol_trades ORDER BY ts",
                    "ts",
                    true,
                    true
            );

            execute("DROP TABLE vol_trades");
        });
    }

    @Test
    public void testMemoryTableWithWal() throws Exception {
        // WAL segments and the sequencer live under the table directory, so
        // the memory-table prefix registered with RoutingFilesFacade catches
        // them and every WAL write/apply/purge is served by memfd pages.
        assertMemoryLeak(() -> {
            execute(
                    "CREATE MEMORY TABLE wal_mem (" +
                    "    ts TIMESTAMP," +
                    "    val LONG" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL"
            );

            execute(
                    "INSERT INTO wal_mem VALUES" +
                    "    ('2024-06-01T10:00:00.000000Z', 1)," +
                    "    ('2024-06-01T10:01:00.000000Z', 2)," +
                    "    ('2024-06-01T10:02:00.000000Z', 3)"
            );

            drainWalQueue();

            assertQueryNoLeakCheck(
                    "ts\tval\n" +
                    "2024-06-01T10:00:00.000000Z\t1\n" +
                    "2024-06-01T10:01:00.000000Z\t2\n" +
                    "2024-06-01T10:02:00.000000Z\t3\n",
                    "SELECT * FROM wal_mem ORDER BY ts",
                    "ts",
                    true,
                    true
            );

            execute("DROP TABLE wal_mem");
            drainWalQueue();
            drainPurgeJob();
        });
    }
}
