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
import io.questdb.cairo.TimestampDriver;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class IntervalIntrinsicTest extends AbstractCairoTest {
    private final String timestampType;

    public IntervalIntrinsicTest(int timestampType) {
        this.timestampType = ColumnType.nameOf(timestampType);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> testParams() {
        return Arrays.asList(new Object[][]{
                {ColumnType.TIMESTAMP_MICRO}, {ColumnType.TIMESTAMP_NANO}
        });
    }

    @Test
    public void testBasic() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table x (ts #TIMESTAMP) timestamp(ts) partition by day;", timestampType);
            executeWithRewriteTimestamp("create table oracle (ts #TIMESTAMP);", timestampType);

            execute("insert into x values (1)");
            execute("insert into x values (1);");
            execute("insert into x values (1);");
            execute("insert into x values (3);");
            execute("insert into x values (3);");
            execute("insert into x values (3);");

            execute("insert into oracle select * from x;");

            // ASC exact
            String expected = replaceTimestampSuffix("ts\n" +
                    "1970-01-01T00:00:00.000001Z\n" +
                    "1970-01-01T00:00:00.000001Z\n" +
                    "1970-01-01T00:00:00.000001Z\n" +
                    "1970-01-01T00:00:00.000003Z\n" +
                    "1970-01-01T00:00:00.000003Z\n" +
                    "1970-01-01T00:00:00.000003Z\n", timestampType);
            assertSql(
                    expected,
                    "x where ts between 1::" + timestampType + " and 3::" + timestampType + ";"
            );
            assertSql(
                    "count\n" +
                            "6\n",
                    "select count() from x where ts between 1::" + timestampType + " and 3::" + timestampType + ";"
            );
            assertSql(
                    expected,
                    "oracle where ts between 1::" + timestampType + " and 3::" + timestampType + " order by ts asc;"
            );

            // ASC overlapping
            expected = replaceTimestampSuffix("ts\n" +
                    "1970-01-01T00:00:00.000003Z\n" +
                    "1970-01-01T00:00:00.000003Z\n" +
                    "1970-01-01T00:00:00.000003Z\n", timestampType);
            assertSql(
                    expected,
                    "x where ts between 2::" + timestampType + " and 4::" + timestampType + ";"
            );
            assertSql(
                    "count\n" +
                            "3\n",
                    "select count() from x where ts between 2::" + timestampType + " and 4::" + timestampType + ";"
            );
            assertSql(
                    expected,
                    "oracle where ts between 2::" + timestampType + " and 4::" + timestampType + " order by ts asc;"
            );

            // DESC exact
            expected = replaceTimestampSuffix("ts\n" +
                    "1970-01-01T00:00:00.000003Z\n" +
                    "1970-01-01T00:00:00.000003Z\n" +
                    "1970-01-01T00:00:00.000003Z\n" +
                    "1970-01-01T00:00:00.000001Z\n" +
                    "1970-01-01T00:00:00.000001Z\n" +
                    "1970-01-01T00:00:00.000001Z\n", timestampType);
            assertSql(
                    expected,
                    "x where ts between 1::" + timestampType + " and 3::" + timestampType + " order by ts desc;"
            );
            assertSql(
                    expected,
                    "oracle where ts between 1::" + timestampType + " and 3::" + timestampType + " order by ts desc;"
            );

            // DESC overlapping
            expected = replaceTimestampSuffix("ts\n" +
                    "1970-01-01T00:00:00.000003Z\n" +
                    "1970-01-01T00:00:00.000003Z\n" +
                    "1970-01-01T00:00:00.000003Z\n", timestampType);
            assertSql(
                    expected,
                    "x where ts between 2::" + timestampType + " and 4::" + timestampType + " order by ts desc;"
            );
            assertSql(
                    expected,
                    "oracle where ts between 2::" + timestampType + " and 4::" + timestampType + " order by ts desc;"
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
            int timestampTypeValue = ColumnType.typeOf(timestampType);
            TimestampDriver driver = ColumnType.getTimestampDriver(timestampTypeValue);
            TableModel oracleModel = new TableModel(configuration, "oracle", PartitionBy.NONE).col("timestamp", timestampTypeValue);
            AbstractCairoTest.create(oracleModel);
            TableModel model = new TableModel(configuration, "x", PartitionBy.DAY).timestamp(timestampTypeValue);
            AbstractCairoTest.create(model);

            final long minTimestamp = driver.parseFloorLiteral("1980-01-01T00:00:00.000Z");
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
                            timestamp += driver.fromMinutes((int) (rnd.nextLong(1) + 1));
                        } else {
                            // extra duplicates are fine
                            timestamp += driver.fromMinutes((int) rnd.nextLong(2));
                        }
                        ticks = duplicatesPerTick;
                    }
                }

                oracleWriter.commit();
                writer.commit();
            }

            StringSink xSink = new StringSink();
            StringSink oracleSink = new StringSink();

            printSql("x where timestamp between " + minTimestamp + "::" + timestampType + " and " + maxTimestamp + "::" + timestampType, xSink);
            printSql("oracle where timestamp between " + minTimestamp + "::" + timestampType + " and " + maxTimestamp + "::" + timestampType, oracleSink);
            TestUtils.assertEquals(oracleSink, xSink);

            long minutes = (maxTimestamp - minTimestamp) / driver.fromMinutes(1);

            // ASC
            for (long lo = minTimestamp - driver.fromMinutes(1); lo < maxTimestamp + driver.fromMinutes(2); lo += driver.fromMinutes(1)) {
                long hi = lo + driver.fromMinutes((int) rnd.nextLong(minutes + 1));

                xSink.clear();
                oracleSink.clear();
                printSql("x where timestamp between " + lo + "::" + timestampType + " and " + hi + "::" + timestampType, xSink);
                printSql("oracle where timestamp between " + lo + "::" + timestampType + " and " + hi + "::" + timestampType, oracleSink);
                TestUtils.assertEquals(oracleSink, xSink);

                xSink.clear();
                oracleSink.clear();
                printSql("select count() from x where timestamp between " + lo + "::" + timestampType + " and " + hi + "::" + timestampType, xSink);
                printSql("select count() from oracle where timestamp between " + lo + "::" + timestampType + " and " + hi + "::" + timestampType, oracleSink);
                TestUtils.assertEquals(oracleSink, xSink);
            }

            // DESC
            for (long hi = maxTimestamp + driver.fromMinutes(1); hi > minTimestamp - driver.fromMinutes(2); hi -= driver.fromMinutes(1)) {
                long lo = hi - driver.fromMinutes((int) rnd.nextLong(minutes + 1));

                xSink.clear();
                oracleSink.clear();
                printSql("x where timestamp between " + lo + "::" + timestampType + " and " + hi + "::" + timestampType + " order by timestamp desc", xSink);
                printSql("oracle where timestamp between " + lo + "::" + timestampType + " and " + hi + "::" + timestampType + " order by timestamp desc", oracleSink);
                TestUtils.assertEquals(oracleSink, xSink);

                xSink.clear();
                oracleSink.clear();
                printSql("select count() from x where timestamp between " + lo + "::" + timestampType + " and " + hi + "::" + timestampType, xSink);
                printSql("select count() from oracle where timestamp between " + lo + "::" + timestampType + " and " + hi + "::" + timestampType, oracleSink);
                TestUtils.assertEquals(oracleSink, xSink);
            }
        });
    }
}
