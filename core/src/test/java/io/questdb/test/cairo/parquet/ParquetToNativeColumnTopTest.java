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

package io.questdb.test.cairo.parquet;

import io.questdb.PropertyKey;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * Converting a parquet partition back to native must produce, for every column, file rows and
 * a column top that add up to the partition's rows.
 * <p>
 * A column added after rows already existed has a column top. Converting the partition to
 * parquet records the top in the file, and the decoder returns empty buffers for every row
 * group that lies entirely inside it. With row groups smaller than the top, several leading
 * row groups decode empty. Converting back to native must still produce a column whose file
 * rows and column top add up to the partition's rows, so that queries and a second conversion
 * to parquet read the right rows.
 */
public class ParquetToNativeColumnTopTest extends AbstractCairoTest {

    @Test
    public void testAllNullRowGroupAfterDataRoundTrip() throws Exception {
        // Row groups of 4: rows 5..8 are NULL in every added column, a whole row group after
        // the first values, so the conversion back to native must write them as NULL rows
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT, vc VARCHAR, s STRING, i INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t
                    SELECT x::INT,
                           CASE WHEN x BETWEEN 5 AND 8 THEN NULL ELSE 'v' || x END,
                           CASE WHEN x BETWEEN 5 AND 8 THEN NULL ELSE 's' || x END,
                           CASE WHEN x BETWEEN 5 AND 8 THEN NULL ELSE x::INT END,
                           '2024-01-01'::TIMESTAMP + x * 60_000_000L
                    FROM long_sequence(12)
                    """);
            execute("INSERT INTO t (x, ts) VALUES (99, '2024-01-02T00:00:00.000000Z')");
            drainWalQueue();

            final String expected = """
                    x\tvc\ts\ti\tts
                    1\tv1\ts1\t1\t2024-01-01T00:01:00.000000Z
                    2\tv2\ts2\t2\t2024-01-01T00:02:00.000000Z
                    3\tv3\ts3\t3\t2024-01-01T00:03:00.000000Z
                    4\tv4\ts4\t4\t2024-01-01T00:04:00.000000Z
                    5\t\t\tnull\t2024-01-01T00:05:00.000000Z
                    6\t\t\tnull\t2024-01-01T00:06:00.000000Z
                    7\t\t\tnull\t2024-01-01T00:07:00.000000Z
                    8\t\t\tnull\t2024-01-01T00:08:00.000000Z
                    9\tv9\ts9\t9\t2024-01-01T00:09:00.000000Z
                    10\tv10\ts10\t10\t2024-01-01T00:10:00.000000Z
                    11\tv11\ts11\t11\t2024-01-01T00:11:00.000000Z
                    12\tv12\ts12\t12\t2024-01-01T00:12:00.000000Z
                    """;
            final String query = "SELECT * FROM t WHERE ts IN '2024-01-01'";
            assertQuery(query).noLeakCheck().timestamp("ts").returns(expected);
            for (int i = 0; i < 2; i++) {
                execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
                drainWalQueue();
                assertQuery(query).noLeakCheck().timestamp("ts").returns(expected);
                execute("ALTER TABLE t CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
                drainWalQueue();
                assertQuery(query).noLeakCheck().timestamp("ts").returns(expected);
            }
        });
    }

    @Test
    public void testColumnTopSpanningSeveralRowGroupsRoundTrip() throws Exception {
        // 10 rows before the columns were added: with row groups of 4, the top spans two
        // whole row groups and part of a third
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t SELECT x::INT, '2024-01-01'::TIMESTAMP + x * 60_000_000L FROM long_sequence(10)");
            drainWalQueue();
            execute("ALTER TABLE t ADD COLUMN vc VARCHAR");
            execute("ALTER TABLE t ADD COLUMN s STRING");
            execute("ALTER TABLE t ADD COLUMN b BOOLEAN");
            execute("ALTER TABLE t ADD COLUMN i INT");
            execute("""
                    INSERT INTO t
                    SELECT (10 + x)::INT, '2024-01-01'::TIMESTAMP + (10 + x) * 60_000_000L,
                           'v' || x, 's' || x, x % 2 = 0, x::INT
                    FROM long_sequence(6)
                    """);
            execute("INSERT INTO t (x, ts) VALUES (99, '2024-01-02T00:00:00.000000Z')");
            drainWalQueue();

            final String expected = """
                    x\tts\tvc\ts\tb\ti
                    1\t2024-01-01T00:01:00.000000Z\t\t\tfalse\tnull
                    2\t2024-01-01T00:02:00.000000Z\t\t\tfalse\tnull
                    3\t2024-01-01T00:03:00.000000Z\t\t\tfalse\tnull
                    4\t2024-01-01T00:04:00.000000Z\t\t\tfalse\tnull
                    5\t2024-01-01T00:05:00.000000Z\t\t\tfalse\tnull
                    6\t2024-01-01T00:06:00.000000Z\t\t\tfalse\tnull
                    7\t2024-01-01T00:07:00.000000Z\t\t\tfalse\tnull
                    8\t2024-01-01T00:08:00.000000Z\t\t\tfalse\tnull
                    9\t2024-01-01T00:09:00.000000Z\t\t\tfalse\tnull
                    10\t2024-01-01T00:10:00.000000Z\t\t\tfalse\tnull
                    11\t2024-01-01T00:11:00.000000Z\tv1\ts1\tfalse\t1
                    12\t2024-01-01T00:12:00.000000Z\tv2\ts2\ttrue\t2
                    13\t2024-01-01T00:13:00.000000Z\tv3\ts3\tfalse\t3
                    14\t2024-01-01T00:14:00.000000Z\tv4\ts4\ttrue\t4
                    15\t2024-01-01T00:15:00.000000Z\tv5\ts5\tfalse\t5
                    16\t2024-01-01T00:16:00.000000Z\tv6\ts6\ttrue\t6
                    """;
            final String query = "SELECT * FROM t WHERE ts IN '2024-01-01'";
            assertQuery(query).noLeakCheck().timestamp("ts").returns(expected);

            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            assertQuery(query).noLeakCheck().timestamp("ts").returns(expected);

            execute("ALTER TABLE t CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
            drainWalQueue();
            assertQuery(query).noLeakCheck().timestamp("ts").returns(expected);

            // the second conversion reads every var-size column up to the partition's last row
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            assertQuery(query).noLeakCheck().timestamp("ts").returns(expected);

            execute("ALTER TABLE t CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
            drainWalQueue();
            assertQuery(query).noLeakCheck().timestamp("ts").returns(expected);
        });
    }
}
