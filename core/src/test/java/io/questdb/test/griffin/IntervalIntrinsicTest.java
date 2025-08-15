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

import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TimestampDriver;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class IntervalIntrinsicTest extends AbstractCairoTest {
    private final boolean convertToParquet;
    private final TestTimestampType timestampType;

    public IntervalIntrinsicTest(boolean convertToParquet, TestTimestampType timestampType) {
        this.convertToParquet = convertToParquet;
        this.timestampType = timestampType;
    }

    @Parameterized.Parameters(name = "parquet={0},ts={1}")
    public static Collection<Object[]> testParams() {
        return Arrays.asList(new Object[][]{
                {true, TestTimestampType.MICRO},
                {true, TestTimestampType.NANO},
                {false, TestTimestampType.MICRO},
                {false, TestTimestampType.NANO},
        });
    }

    @Test
    public void testBasic() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table x (ts #TIMESTAMP) timestamp(ts) partition by day;", timestampType.getTypeName());
            executeWithRewriteTimestamp("create table oracle (ts #TIMESTAMP);", timestampType.getTypeName());

            execute("insert into x values (1)");
            execute("insert into x values (1);");
            execute("insert into x values (1);");
            execute("insert into x values (3);");
            execute("insert into x values (3);");
            execute("insert into x values (3);");

            execute("insert into oracle select * from x;");

            if (convertToParquet) {
                // create a new active partition, so that all older partitions are converted to parquet
                execute("insert into x values ('1970-01-02');");
                execute("alter table x convert partition to parquet where ts >= 0;");
            }

            // ASC exact
            String expected = replaceTimestampSuffix("ts\n" +
                    "1970-01-01T00:00:00.000001Z\n" +
                    "1970-01-01T00:00:00.000001Z\n" +
                    "1970-01-01T00:00:00.000001Z\n" +
                    "1970-01-01T00:00:00.000003Z\n" +
                    "1970-01-01T00:00:00.000003Z\n" +
                    "1970-01-01T00:00:00.000003Z\n", timestampType.getTypeName());
            assertSql(
                    expected,
                    "x where ts between 1::" + timestampType.getTypeName() + " and 3::" + timestampType.getTypeName() + ";"
            );
            assertSql(
                    "count\n" +
                            "6\n",
                    "select count() from x where ts between 1::" + timestampType.getTypeName() + " and 3::" + timestampType.getTypeName() + ";"
            );
            assertSql(
                    expected,
                    "oracle where ts between 1::" + timestampType.getTypeName() + " and 3::" + timestampType.getTypeName() + " order by ts asc;"
            );

            // ASC overlapping
            expected = replaceTimestampSuffix("ts\n" +
                    "1970-01-01T00:00:00.000003Z\n" +
                    "1970-01-01T00:00:00.000003Z\n" +
                    "1970-01-01T00:00:00.000003Z\n", timestampType.getTypeName());
            assertSql(
                    expected,
                    "x where ts between 2::" + timestampType.getTypeName() + " and 4::" + timestampType.getTypeName() + ";"
            );
            assertSql(
                    "count\n" +
                            "3\n",
                    "select count() from x where ts between 2::" + timestampType.getTypeName() + " and 4::" + timestampType.getTypeName() + ";"
            );
            assertSql(
                    expected,
                    "oracle where ts between 2::" + timestampType.getTypeName() + " and 4::" + timestampType.getTypeName() + " order by ts asc;"
            );

            // DESC exact
            expected = replaceTimestampSuffix("ts\n" +
                    "1970-01-01T00:00:00.000003Z\n" +
                    "1970-01-01T00:00:00.000003Z\n" +
                    "1970-01-01T00:00:00.000003Z\n" +
                    "1970-01-01T00:00:00.000001Z\n" +
                    "1970-01-01T00:00:00.000001Z\n" +
                    "1970-01-01T00:00:00.000001Z\n", timestampType.getTypeName());
            assertSql(
                    expected,
                    "x where ts between 1::" + timestampType.getTypeName() + " and 3::" + timestampType.getTypeName() + " order by ts desc;"
            );
            assertSql(
                    expected,
                    "oracle where ts between 1::" + timestampType.getTypeName() + " and 3::" + timestampType.getTypeName() + " order by ts desc;"
            );

            // DESC overlapping
            expected = replaceTimestampSuffix("ts\n" +
                    "1970-01-01T00:00:00.000003Z\n" +
                    "1970-01-01T00:00:00.000003Z\n" +
                    "1970-01-01T00:00:00.000003Z\n", timestampType.getTypeName());
            assertSql(
                    expected,
                    "x where ts between 2::" + timestampType.getTypeName() + " and 4::" + timestampType.getTypeName() + " order by ts desc;"
            );
            assertSql(
                    expected,
                    "oracle where ts between 2::" + timestampType.getTypeName() + " and 4::" + timestampType.getTypeName() + " order by ts desc;"
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
            int timestampTypeValue = timestampType.getTimestampType();
            TimestampDriver driver = timestampType.getDriver();
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

            if (convertToParquet) {
                // create a new active partition, so that all older partitions are converted to parquet
                execute("insert into x (timestamp) values ('2000-01-01');");
                execute("alter table x convert partition to parquet where timestamp >= 0;");
            }

            StringSink xSink = new StringSink();
            StringSink oracleSink = new StringSink();

            printSql("x where timestamp between " + minTimestamp + "::" + timestampType.getTypeName() + " and " + maxTimestamp + "::" + timestampType.getTypeName(), xSink);
            printSql("oracle where timestamp between " + minTimestamp + "::" + timestampType.getTypeName() + " and " + maxTimestamp + "::" + timestampType.getTypeName(), oracleSink);
            TestUtils.assertEquals(oracleSink, xSink);

            long minutes = (maxTimestamp - minTimestamp) / driver.fromMinutes(1);

            // ASC
            for (long lo = minTimestamp - driver.fromMinutes(1); lo < maxTimestamp + driver.fromMinutes(2); lo += driver.fromMinutes(1)) {
                long hi = lo + driver.fromMinutes((int) rnd.nextLong(minutes + 1));

                xSink.clear();
                oracleSink.clear();
                printSql("x where timestamp between " + lo + "::" + timestampType.getTypeName() + " and " + hi + "::" + timestampType.getTypeName(), xSink);
                printSql("oracle where timestamp between " + lo + "::" + timestampType.getTypeName() + " and " + hi + "::" + timestampType.getTypeName(), oracleSink);
                TestUtils.assertEquals(oracleSink, xSink);

                xSink.clear();
                oracleSink.clear();
                printSql("select count() from x where timestamp between " + lo + "::" + timestampType.getTypeName() + " and " + hi + "::" + timestampType.getTypeName(), xSink);
                printSql("select count() from oracle where timestamp between " + lo + "::" + timestampType.getTypeName() + " and " + hi + "::" + timestampType.getTypeName(), oracleSink);
                TestUtils.assertEquals(oracleSink, xSink);

                // repeat with difference scales
                if (timestampType == TestTimestampType.MICRO) {
                    xSink.clear();
                    long loNs = timestampType.getDriver().toNanos(lo);
                    long hiNs = timestampType.getDriver().toNanos(hi);
                    printSql("select count() from x where timestamp between " + loNs + "::TIMESTAMP_NS and " + hiNs + "::TIMESTAMP_NS", xSink);
                    TestUtils.assertEquals(oracleSink, xSink);
                }
            }

            // DESC
            for (long hi = maxTimestamp + driver.fromMinutes(1); hi > minTimestamp - driver.fromMinutes(2); hi -= driver.fromMinutes(1)) {
                long lo = hi - driver.fromMinutes((int) rnd.nextLong(minutes + 1));

                xSink.clear();
                oracleSink.clear();
                printSql("x where timestamp between " + lo + "::" + timestampType.getTypeName() + " and " + hi + "::" + timestampType.getTypeName() + " order by timestamp desc", xSink);
                printSql("oracle where timestamp between " + lo + "::" + timestampType.getTypeName() + " and " + hi + "::" + timestampType.getTypeName() + " order by timestamp desc", oracleSink);
                TestUtils.assertEquals(oracleSink, xSink);
            }
        });
    }
}
