/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class AsOfJoinFuzzTest extends AbstractCairoTest {

    @Test
    public void testFuzzManyDuplicates() throws Exception {
        testFuzz(50);
    }

    @Test
    public void testFuzzNoDuplicates() throws Exception {
        testFuzz(0);
    }

    private void assertResultSetsMatch(String leftTable, String rightTable) throws Exception {
        final StringSink expectedSink = new StringSink();
        // equivalent of the below query, but uses slow factory
        printSql("select * from " + leftTable + " asof join (" + rightTable + " where i >= 0) on s", expectedSink);

        final StringSink actualSink = new StringSink();
        printSql("select * from " + leftTable + " asof join " + rightTable + " on s", actualSink);

        TestUtils.assertEquals(expectedSink, actualSink);
    }

    private void testFuzz(int tsDuplicatePercentage) throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> {
            final int table1Size = rnd.nextPositiveInt() % 1000;
            final int table2Size = rnd.nextPositiveInt() % 1000;

            ddl("CREATE TABLE t1 (ts TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal");
            long ts = TimestampFormatUtils.parseTimestamp("2000-01-01T00:00:00.000Z");
            ts += Timestamps.HOUR_MICROS * (rnd.nextLong() % 48);
            for (int i = 0; i < table1Size; i++) {
                if (rnd.nextInt(100) >= tsDuplicatePercentage) {
                    ts += Timestamps.HOUR_MICROS * rnd.nextLong(24);
                }
                String symbol = "s_" + rnd.nextInt(10);
                insert("INSERT INTO t1 values (" + ts + ", " + i + ", '" + symbol + "');");
            }

            ddl("CREATE TABLE t2 (ts TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal");
            ts = TimestampFormatUtils.parseTimestamp("2000-01-01T00:00:00.000Z");
            ts += Timestamps.HOUR_MICROS * rnd.nextLong(48);
            for (int i = 0; i < table2Size; i++) {
                if (rnd.nextInt(100) >= tsDuplicatePercentage) {
                    ts += Timestamps.HOUR_MICROS * rnd.nextLong(24);
                }
                String symbol = "s_" + rnd.nextInt(10);
                insert("INSERT INTO t2 values (" + ts + ", " + i + ", '" + symbol + "');");
            }

            assertResultSetsMatch("t1", "t2");
        });
    }
}
