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

package io.questdb.test.cairo.wal;

import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * DROP COLUMN of a dedup key leaves the table deduplicating on its other keys. The writer that applies the drop
 * stays open and goes on deduplicating the commits that follow it, so it must compare only the keys that are left.
 */
public class DedupDropKeyColumnTest extends AbstractCairoTest {

    @Test
    public void testDropOneOfTwoKeysInOneCommit() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, k INT, j INT, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, k, j)");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:01', 1, 1, 1)");
            drainWalQueue();

            execute("ALTER TABLE t DROP COLUMN k");
            drainWalQueue();
            execute("INSERT INTO t(ts, j, v) VALUES ('2024-01-01T00:00:02', 1, 2), ('2024-01-01T00:00:02', 2, 3), ('2024-01-01T00:00:02', 1, 4)");
            drainWalQueue();
            assertRows("""
                    ts\tj\tv
                    2024-01-01T00:00:01.000000Z\t1\t1
                    2024-01-01T00:00:02.000000Z\t1\t4
                    2024-01-01T00:00:02.000000Z\t2\t3
                    """, "j");
        });
    }

    @Test
    public void testDropOneOfTwoKeysO3Native() throws Exception {
        assertDropOneOfTwoKeysO3(false);
    }

    @Test
    public void testDropOneOfTwoKeysO3Parquet() throws Exception {
        assertDropOneOfTwoKeysO3(true);
    }

    @Test
    public void testDropOnlyKeyInOneCommit() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, k INT, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, k)");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:01', 1, 1)");
            drainWalQueue();

            execute("ALTER TABLE t DROP COLUMN k");
            drainWalQueue();
            execute("INSERT INTO t(ts, v) VALUES ('2024-01-01T00:00:02', 2), ('2024-01-01T00:00:02', 3), ('2024-01-01T00:00:03', 4)");
            drainWalQueue();
            assertRows("""
                    ts\tv
                    2024-01-01T00:00:01.000000Z\t1
                    2024-01-01T00:00:02.000000Z\t3
                    2024-01-01T00:00:03.000000Z\t4
                    """, null);
        });
    }

    @Test
    public void testDropOnlyKeyO3Native() throws Exception {
        assertDropOnlyKeyO3(false);
    }

    @Test
    public void testDropOnlyKeyO3Parquet() throws Exception {
        assertDropOnlyKeyO3(true);
    }

    private void assertDropOneOfTwoKeysO3(boolean isParquet) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, k INT, j INT, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, k, j)");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:01', 1, 1, 1),
                    ('2024-01-01T00:00:01', 1, 2, 2),
                    ('2024-01-01T00:00:03', 1, 1, 3),
                    ('2024-01-02T00:00:01', 1, 1, 100)
                    """);
            drainWalQueue();
            if (isParquet) {
                execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
                drainWalQueue();
            }

            execute("ALTER TABLE t DROP COLUMN k");
            drainWalQueue();
            execute("INSERT INTO t(ts, j, v) VALUES ('2024-01-01T00:00:01', 1, 10), ('2024-01-01T00:00:02', 1, 20)");
            drainWalQueue();
            assertRows("""
                    ts\tj\tv
                    2024-01-01T00:00:01.000000Z\t1\t10
                    2024-01-01T00:00:01.000000Z\t2\t2
                    2024-01-01T00:00:02.000000Z\t1\t20
                    2024-01-01T00:00:03.000000Z\t1\t3
                    2024-01-02T00:00:01.000000Z\t1\t100
                    """, "j");
        });
    }

    private void assertDropOnlyKeyO3(boolean isParquet) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, k INT, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, k)");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:01', 1, 1),
                    ('2024-01-01T00:00:03', 1, 3),
                    ('2024-01-02T00:00:01', 1, 100)
                    """);
            drainWalQueue();
            if (isParquet) {
                execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
                drainWalQueue();
            }

            execute("ALTER TABLE t DROP COLUMN k");
            drainWalQueue();
            execute("INSERT INTO t(ts, v) VALUES ('2024-01-01T00:00:01', 10), ('2024-01-01T00:00:02', 20)");
            drainWalQueue();
            assertRows("""
                    ts\tv
                    2024-01-01T00:00:01.000000Z\t10
                    2024-01-01T00:00:02.000000Z\t20
                    2024-01-01T00:00:03.000000Z\t3
                    2024-01-02T00:00:01.000000Z\t100
                    """, null);
        });
    }

    private void assertRows(String expected, String key) throws Exception {
        final String columns = key == null ? "ts, v" : "ts, " + key + ", v";
        assertQuery("SELECT " + columns + " FROM t ORDER BY ts, " + (key == null ? "v" : key)).timestamp("ts").expectSize().returns(expected);
        Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("t")));
    }
}
