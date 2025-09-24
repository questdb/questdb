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

package io.questdb.test.cairo.mv;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class MatViewIdenticalReplaceTest extends AbstractCairoTest {
    private final TestTimestampType timestampType;

    public MatViewIdenticalReplaceTest(TestTimestampType timestampType) {
        this.timestampType = timestampType;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> testParams() {
        return Arrays.asList(new Object[][]{
                {TestTimestampType.MICRO}, {TestTimestampType.NANO}
        });
    }

    @Test
    public void testReplaceNoPartitionRewriteArray() throws Exception {
        testSameAndShuffledInserts("double[]", "null", "ARRAY[1.0,1.0]", "ARRAY[2.0,2.0]", "ARRAY[3.0,3.0]", "first");
    }

    @Test
    public void testReplaceNoPartitionRewriteByte() throws Exception {
        testSameAndShuffledInserts("byte", "0", "123", "127", "22", "first");
    }

    @Test
    public void testReplaceNoPartitionRewriteDesignatedTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table test (ts #TIMESTAMP, x int, v string) timestamp(ts) partition by DAY WAL dedup upsert keys (ts, x) ");
            execute("create materialized view test_mv as select ts, x, first(v) as v from test sample by 1s");
            execute("insert into test(ts,x,v) values ('2022-02-24', 1, '123'), ('2022-02-24T01', 2, null), ('2022-02-24T02', 3, '2345567')");

            drainWalAndMatViewQueues();
            assertSql(
                    replaceExpectedTimestamp(
                            "ts\tx\tv\n" +
                                    "2022-02-24T00:00:00.000000Z\t1\t123\n" +
                                    "2022-02-24T01:00:00.000000Z\t2\t\n" +
                                    "2022-02-24T02:00:00.000000Z\t3\t2345567\n"),
                    "test_mv"
            );

            try (WalWriter ww = engine.getWalWriter(engine.verifyTableName("test"))) {
                var row = ww.newRow(timestampType.getDriver().parseFloorLiteral("2022-02-24"));
                row.putInt(1, 1);
                row.putStr(2, "123");
                row.append();

                row = ww.newRow(timestampType.getDriver().parseFloorLiteral("2022-02-24T00:55"));
                row.putInt(1, 2);
                row.putStr(2, null);
                row.append();

                row = ww.newRow(timestampType.getDriver().parseFloorLiteral("2022-02-24T02"));
                row.putInt(1, 3);
                row.putStr(2, "2345567");
                row.append();

                ww.commitWithParams(
                        timestampType.getDriver().parseFloorLiteral("2022-02-24"),
                        timestampType.getDriver().parseFloorLiteral("2022-02-24T02:00:00.000001Z"),
                        WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE
                );
            }

            drainWalAndMatViewQueues();
            assertSql(
                    replaceExpectedTimestamp(
                            "ts\tx\tv\n" +
                                    "2022-02-24T00:00:00.000000Z\t1\t123\n" +
                                    "2022-02-24T00:55:00.000000Z\t2\t\n" +
                                    "2022-02-24T02:00:00.000000Z\t3\t2345567\n"),
                    "test_mv"
            );

        });
    }

    @Test
    public void testReplaceNoPartitionRewriteInt() throws Exception {
        testSameAndShuffledInserts("int", "null", "123", "2345567", "22", "first");
    }

    @Test
    public void testReplaceNoPartitionRewriteLong() throws Exception {
        testSameAndShuffledInserts("long", "null", "123", "2345567", "22", "first");
    }

    @Test
    public void testReplaceNoPartitionRewriteLong256() throws Exception {
        testSameAndShuffledInserts("Long256", "", "0x01", "0x02", "0x03", "sum"
        );
    }

    @Test
    public void testReplaceNoPartitionRewriteShort() throws Exception {
        testSameAndShuffledInserts("short", "0", "123", "2342", "22", "first");
    }

    @Test
    public void testReplaceNoPartitionRewriteString() throws Exception {
        testSameAndShuffledInserts("string", "", "'123'", "'2345567'", "'22'", "first");
    }

    @Test
    public void testReplaceNoPartitionRewriteUUID() throws Exception {
        testSameAndShuffledInserts(
                "UUID",
                "",
                "'00000000-0000-0006-0000-000000000006'",
                "'00000000-0000-0005-0000-000000000005'",
                "'00000000-0000-0007-0000-000000000007'",
                "first"
        );
    }

    @Test
    public void testReplaceNoPartitionRewriteVarchar() throws Exception {
        testSameAndShuffledInserts("varchar", "", "'123'", "'2345567'", "'22'", "first");
    }

    private void executeWithRewriteTimestamp(CharSequence sqlText) throws SqlException {
        sqlText = sqlText.toString().replaceAll("#TIMESTAMP", timestampType.getTypeName());
        engine.execute(sqlText, sqlExecutionContext);
    }

    private String replaceExpectedTimestamp(String expected) {
        return ColumnType.isTimestampMicro(timestampType.getDriver().getTimestampType()) ? expected : expected.replaceAll(".000000Z", ".000000000Z");
    }

    private void testSameAndShuffledInserts(String columnType, String nullValue, String value1, String value2, String nullValueUpdated, String mvGroupFunction) throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table test (ts #TIMESTAMP, x int, v " + columnType + ") timestamp(ts) partition by DAY WAL dedup upsert keys (ts, x) ");
            execute("create materialized view test_mv as select ts, x, " + mvGroupFunction + "(v) as v from test sample by 1s");
            execute("insert into test(ts,x,v) values ('2022-02-24', 1, " + value1 + "), ('2022-02-24', 2, null), ('2022-02-24', 3, " + value2 + ")");

            drainWalAndMatViewQueues();

            String value1Unquoted = unquote(value1);
            String value2Unquoted = unquote(value2);
            assertSql(
                    replaceExpectedTimestamp("ts\tx\tv\n" +
                            "2022-02-24T00:00:00.000000Z\t1\t" + value1Unquoted + "\n" +
                            "2022-02-24T00:00:00.000000Z\t2\t" + nullValue + "\n" +
                            "2022-02-24T00:00:00.000000Z\t3\t" + value2Unquoted + "\n"),
                    "test"
            );

            assertSql(
                    replaceExpectedTimestamp("ts\tx\tv\n" +
                            "2022-02-24T00:00:00.000000Z\t1\t" + value1Unquoted + "\n" +
                            "2022-02-24T00:00:00.000000Z\t2\t" + nullValue + "\n" +
                            "2022-02-24T00:00:00.000000Z\t3\t" + value2Unquoted + "\n"),
                    "test_mv"
            );

            TableToken tt = engine.verifyTableName("test_mv");
            String partitionsTxnFile = readTxnToString(tt, false, true, true);

            // Insert same values
            execute("insert into test(ts,x,v) values ('2022-02-24', 1, " + value1 + "), ('2022-02-24', 2, null), ('2022-02-24', 3, " + value2 + ")");
            drainWalAndMatViewQueues();

            assertSql(
                    replaceExpectedTimestamp("ts\tx\tv\n" +
                            "2022-02-24T00:00:00.000000Z\t1\t" + value1Unquoted + "\n" +
                            "2022-02-24T00:00:00.000000Z\t2\t" + nullValue + "\n" +
                            "2022-02-24T00:00:00.000000Z\t3\t" + value2Unquoted + "\n"),
                    "test_mv"
            );
            Assert.assertEquals(partitionsTxnFile, readTxnToString(tt, false, true, true));

            // Insert same values recorded
            execute("insert into test(ts,x,v) values ('2022-02-24', 3, " + value2 + "), ('2022-02-24', 2, null)");
            drainWalAndMatViewQueues();

            assertSql(
                    replaceExpectedTimestamp("ts\tx\tv\n" +
                            "2022-02-24T00:00:00.000000Z\t1\t" + value1Unquoted + "\n" +
                            "2022-02-24T00:00:00.000000Z\t2\t" + nullValue + "\n" +
                            "2022-02-24T00:00:00.000000Z\t3\t" + value2Unquoted + "\n"),
                    "test_mv"
            );
            Assert.assertEquals(partitionsTxnFile, readTxnToString(tt, false, true, true));

            // Change one varchar
            execute("insert into test(ts,x,v) values ('2022-02-24', 3, " + value2 + "), ('2022-02-24', 2, " + nullValueUpdated + ")");
            drainWalAndMatViewQueues();

            assertSql(
                    replaceExpectedTimestamp("ts\tx\tv\n" +
                            "2022-02-24T00:00:00.000000Z\t1\t" + value1Unquoted + "\n" +
                            "2022-02-24T00:00:00.000000Z\t2\t" + unquote(nullValueUpdated) + "\n" +
                            "2022-02-24T00:00:00.000000Z\t3\t" + value2Unquoted + "\n"),
                    "test_mv"
            );
        });
    }

    private String unquote(String v) {
        if (v.startsWith("'") && v.endsWith("'")) {
            return v.substring(1, v.length() - 1);
        }
        if (v.startsWith("ARRAY")) {
            return v.substring(5); // remove ARRAY
        }
        return v;
    }
}
