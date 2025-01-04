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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableWriter;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class IntervalIntrinsicTest extends AbstractCairoTest {

    @Test
    public void testBasic() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp) timestamp(ts) partition by day;");
            execute("create table oracle (ts timestamp);");

            execute("insert into x values (1)");
            execute("insert into x values (1);");
            execute("insert into x values (1);");
            execute("insert into x values (3);");
            execute("insert into x values (3);");
            execute("insert into x values (3);");

            execute("insert into oracle select * from x;");

            // ASC exact
            String expected = "ts\n" +
                    "1970-01-01T00:00:00.000001Z\n" +
                    "1970-01-01T00:00:00.000001Z\n" +
                    "1970-01-01T00:00:00.000001Z\n" +
                    "1970-01-01T00:00:00.000003Z\n" +
                    "1970-01-01T00:00:00.000003Z\n" +
                    "1970-01-01T00:00:00.000003Z\n";
            assertSql(
                    expected,
                    "x where ts between 1::timestamp and 3::timestamp;"
            );
            assertSql(
                    "count\n" +
                            "6\n",
                    "select count() from x where ts between 1::timestamp and 3::timestamp;"
            );
            assertSql(
                    expected,
                    "oracle where ts between 1::timestamp and 3::timestamp order by ts asc;"
            );

            // ASC overlapping
            expected = "ts\n" +
                    "1970-01-01T00:00:00.000003Z\n" +
                    "1970-01-01T00:00:00.000003Z\n" +
                    "1970-01-01T00:00:00.000003Z\n";
            assertSql(
                    expected,
                    "x where ts between 2::timestamp and 4::timestamp;"
            );
            assertSql(
                    "count\n" +
                            "3\n",
                    "select count() from x where ts between 2::timestamp and 4::timestamp;"
            );
            assertSql(
                    expected,
                    "oracle where ts between 2::timestamp and 4::timestamp order by ts asc;"
            );

            // DESC exact
            expected = "ts\n" +
                    "1970-01-01T00:00:00.000003Z\n" +
                    "1970-01-01T00:00:00.000003Z\n" +
                    "1970-01-01T00:00:00.000003Z\n" +
                    "1970-01-01T00:00:00.000001Z\n" +
                    "1970-01-01T00:00:00.000001Z\n" +
                    "1970-01-01T00:00:00.000001Z\n";
            assertSql(
                    expected,
                    "x where ts between 1::timestamp and 3::timestamp order by ts desc;"
            );
            assertSql(
                    expected,
                    "oracle where ts between 1::timestamp and 3::timestamp order by ts desc;"
            );

            // DESC overlapping
            expected = "ts\n" +
                    "1970-01-01T00:00:00.000003Z\n" +
                    "1970-01-01T00:00:00.000003Z\n" +
                    "1970-01-01T00:00:00.000003Z\n";
            assertSql(
                    expected,
                    "x where ts between 2::timestamp and 4::timestamp order by ts desc;"
            );
            assertSql(
                    expected,
                    "oracle where ts between 2::timestamp and 4::timestamp order by ts desc;"
            );
        });
    }

    @Test
    public void testFuzzAllDuplicates() throws Exception {
        testFuzz(1000, 1000);
    }

    @Test
    public void testFuzzFewDuplicates() throws Exception {
        testFuzz(100, 1);
    }

    @Test
    public void testFuzzSomeDuplicates() throws Exception {
        testFuzz(100, 10);
    }

    private void testFuzz(int rowCount, int duplicatesPerTick) throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> {
            TableModel oracleModel = new TableModel(configuration, "oracle", PartitionBy.NONE).col("timestamp", ColumnType.TIMESTAMP);
            AbstractCairoTest.create(oracleModel);
            TableModel model = new TableModel(configuration, "x", PartitionBy.DAY).timestamp();
            AbstractCairoTest.create(model);

            final long minTimestamp = TimestampFormatUtils.parseTimestamp("1980-01-01T00:00:00.000Z");
            long maxTimestamp = minTimestamp;
            long timestamp = minTimestamp;
            try (
                    TableWriter oracleWriter = newOffPoolWriter(configuration, "oracle");
                    TableWriter writer = newOffPoolWriter(configuration, "x")
            ) {
                int ticks = duplicatesPerTick;
                for (int i = 0; i < rowCount; i++) {
                    TableWriter.Row oracleRow = oracleWriter.newRow();
                    oracleRow.putTimestamp(0, timestamp);
                    oracleRow.append();
                    writer.newRow(timestamp).append();
                    maxTimestamp = timestamp;
                    if (--ticks == 0) {
                        if (duplicatesPerTick > 1) {
                            // we want to be in control of the number of duplicates
                            timestamp += (rnd.nextLong(1) + 1) * Timestamps.MINUTE_MICROS;
                        } else {
                            // extra duplicates are fine
                            timestamp += rnd.nextLong(2) * Timestamps.MINUTE_MICROS;
                        }
                        ticks = duplicatesPerTick;
                    }
                }

                oracleWriter.commit();
                writer.commit();
            }

            StringSink xSink = new StringSink();
            StringSink oracleSink = new StringSink();

            printSql("x where timestamp between " + minTimestamp + " and " + maxTimestamp, xSink);
            printSql("oracle where timestamp between " + minTimestamp + " and " + maxTimestamp, oracleSink);
            TestUtils.assertEquals(oracleSink, xSink);

            long minutes = (maxTimestamp - minTimestamp) / Timestamps.MINUTE_MICROS;

            // ASC
            for (long lo = minTimestamp - Timestamps.MINUTE_MICROS; lo < maxTimestamp + 2 * Timestamps.MINUTE_MICROS; lo += Timestamps.MINUTE_MICROS) {
                long hi = lo + rnd.nextLong(minutes + 1) * Timestamps.MINUTE_MICROS;

                xSink.clear();
                oracleSink.clear();
                printSql("x where timestamp between " + lo + " and " + hi, xSink);
                printSql("oracle where timestamp between " + lo + " and " + hi, oracleSink);
                TestUtils.assertEquals(oracleSink, xSink);

                xSink.clear();
                oracleSink.clear();
                printSql("select count() from x where timestamp between " + lo + " and " + hi, xSink);
                printSql("select count() from oracle where timestamp between " + lo + " and " + hi, oracleSink);
                TestUtils.assertEquals(oracleSink, xSink);
            }

            // DESC
            for (long hi = maxTimestamp + Timestamps.MINUTE_MICROS; hi > minTimestamp - 2 * Timestamps.MINUTE_MICROS; hi -= Timestamps.MINUTE_MICROS) {
                long lo = hi - rnd.nextLong(minutes + 1) * Timestamps.MINUTE_MICROS;

                xSink.clear();
                oracleSink.clear();
                printSql("x where timestamp between " + lo + " and " + hi + " order by timestamp desc", xSink);
                printSql("oracle where timestamp between " + lo + " and " + hi + " order by timestamp desc", oracleSink);
                TestUtils.assertEquals(oracleSink, xSink);

                xSink.clear();
                oracleSink.clear();
                printSql("select count() from x where timestamp between " + lo + " and " + hi, xSink);
                printSql("select count() from oracle where timestamp between " + lo + " and " + hi, oracleSink);
                TestUtils.assertEquals(oracleSink, xSink);
            }
        });
    }
}
