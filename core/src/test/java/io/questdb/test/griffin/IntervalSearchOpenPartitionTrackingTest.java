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

import io.questdb.griffin.SqlException;
import io.questdb.std.IntList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class IntervalSearchOpenPartitionTrackingTest extends AbstractCairoTest {
    private final TestTimestampType timestampType;

    public IntervalSearchOpenPartitionTrackingTest(TestTimestampType timestampType) {
        this.timestampType = timestampType;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> testParams() {
        return Arrays.asList(new Object[][]{
                {TestTimestampType.MICRO}, {TestTimestampType.NANO}
        });
    }

    @Test
    public void testBwdInterval() throws SqlException {
        var partitionList = setupTableAndOpenPartitionList();
        var sql = "select * from tmp where ts in '2025-07-11T03' order by ts desc";
        assertSql(
                replaceTimestampSuffix("ts\td\n" +
                        "2025-07-11T03:54:00.000000Z\t0.5522494170511608\n" +
                        "2025-07-11T03:48:00.000000Z\t0.5298405941762054\n" +
                        "2025-07-11T03:42:00.000000Z\t0.7664256753596138\n" +
                        "2025-07-11T03:36:00.000000Z\t0.26922103479744897\n" +
                        "2025-07-11T03:30:00.000000Z\t0.42281342727402726\n" +
                        "2025-07-11T03:24:00.000000Z\t0.3435685332942956\n" +
                        "2025-07-11T03:18:00.000000Z\t0.8912587536603974\n" +
                        "2025-07-11T03:12:00.000000Z\t0.6761934857077543\n" +
                        "2025-07-11T03:06:00.000000Z\t0.12026122412833129\n" +
                        "2025-07-11T03:00:00.000000Z\t0.9038068796506872\n", timestampType.getTypeName()),
                sql
        );
        Assert.assertEquals("[3]", partitionList.toString());
        assertSql(
                "QUERY PLAN\n" +
                        "PageFrame\n" +
                        "    Row backward scan\n" +
                        "    Interval backward scan on: tmp\n" +
                        (timestampType == TestTimestampType.MICRO ?
                                "      intervals: [(\"2025-07-11T03:00:00.000000Z\",\"2025-07-11T03:59:59.999999Z\")]\n" :
                                "      intervals: [(\"2025-07-11T03:00:00.000000000Z\",\"2025-07-11T03:59:59.999999999Z\")]\n"),
                "explain " + sql
        );
    }

    @Test
    public void testBwdLimit() throws SqlException {
        var partitionList = setupTableAndOpenPartitionList();
        var sql = "select * from tmp order by ts desc limit 20,30";
        assertSql(
                replaceTimestampSuffix("ts\td\n" +
                        "2025-07-11T07:54:00.000000Z\t0.19751370382305056\n" +
                        "2025-07-11T07:48:00.000000Z\t0.8940917126581895\n" +
                        "2025-07-11T07:42:00.000000Z\t0.03167026265669903\n" +
                        "2025-07-11T07:36:00.000000Z\t0.44804689668613573\n" +
                        "2025-07-11T07:30:00.000000Z\t0.9540069089049732\n" +
                        "2025-07-11T07:24:00.000000Z\t0.48558682958070665\n" +
                        "2025-07-11T07:18:00.000000Z\t0.2820020716674768\n" +
                        "2025-07-11T07:12:00.000000Z\t0.5811247005631662\n" +
                        "2025-07-11T07:06:00.000000Z\t0.6551335839796312\n" +
                        "2025-07-11T07:00:00.000000Z\t0.42020442539326086\n", timestampType.getTypeName()),
                sql
        );

        Assert.assertEquals("[7]", partitionList.toString());
        assertSql(
                "QUERY PLAN\n" +
                        "Limit lo: 20 hi: 30 skip-over-rows: 20 limit: 10\n" +
                        "    PageFrame\n" +
                        "        Row backward scan\n" +
                        "        Frame backward scan on: tmp\n",
                "explain " + sql
        );
    }

    @Test
    public void testFwdInterval() throws SqlException {
        var partitionList = setupTableAndOpenPartitionList();
        var sql = "select * from tmp where ts in '2025-07-11T05'";
        assertSql(
                replaceTimestampSuffix("ts\td\n" +
                        "2025-07-11T05:00:00.000000Z\t0.16381374773748514\n" +
                        "2025-07-11T05:06:00.000000Z\t0.456344569609078\n" +
                        "2025-07-11T05:12:00.000000Z\t0.8664158914718532\n" +
                        "2025-07-11T05:18:00.000000Z\t0.40455469747939254\n" +
                        "2025-07-11T05:24:00.000000Z\t0.4149517697653501\n" +
                        "2025-07-11T05:30:00.000000Z\t0.5659429139861241\n" +
                        "2025-07-11T05:36:00.000000Z\t0.05384400312338511\n" +
                        "2025-07-11T05:42:00.000000Z\t0.6821660861001273\n" +
                        "2025-07-11T05:48:00.000000Z\t0.7230015763133606\n" +
                        "2025-07-11T05:54:00.000000Z\t0.9644183832564398\n", timestampType.getTypeName()),
                sql
        );
        Assert.assertEquals("[5]", partitionList.toString());
        assertSql(
                "QUERY PLAN\n" +
                        "PageFrame\n" +
                        "    Row forward scan\n" +
                        "    Interval forward scan on: tmp\n" +
                        (timestampType == TestTimestampType.MICRO ?
                                "      intervals: [(\"2025-07-11T05:00:00.000000Z\",\"2025-07-11T05:59:59.999999Z\")]\n" :
                                "      intervals: [(\"2025-07-11T05:00:00.000000000Z\",\"2025-07-11T05:59:59.999999999Z\")]\n"),
                "explain " + sql
        );
    }

    @Test
    public void testFwdLimit() throws SqlException {
        var partitionList = setupTableAndOpenPartitionList();
        var sql = "select * from tmp limit -20";
        assertSql(
                replaceTimestampSuffix("ts\td\n" +
                        "2025-07-11T08:00:00.000000Z\t0.9441658975532605\n" +
                        "2025-07-11T08:06:00.000000Z\t0.6806873134626418\n" +
                        "2025-07-11T08:12:00.000000Z\t0.7176053468281931\n" +
                        "2025-07-11T08:18:00.000000Z\t0.24008362859107102\n" +
                        "2025-07-11T08:24:00.000000Z\t0.8733293804420821\n" +
                        "2025-07-11T08:30:00.000000Z\t0.9455893004802433\n" +
                        "2025-07-11T08:36:00.000000Z\t0.17833722747266334\n" +
                        "2025-07-11T08:42:00.000000Z\t0.6247427794126656\n" +
                        "2025-07-11T08:48:00.000000Z\t0.3045253310626277\n" +
                        "2025-07-11T08:54:00.000000Z\t0.3901731258748704\n" +
                        "2025-07-11T09:00:00.000000Z\t0.03993124821273464\n" +
                        "2025-07-11T09:06:00.000000Z\t0.10643046345788132\n" +
                        "2025-07-11T09:12:00.000000Z\t0.8438459563914771\n" +
                        "2025-07-11T09:18:00.000000Z\t0.07246172621937097\n" +
                        "2025-07-11T09:24:00.000000Z\t0.0171850098561398\n" +
                        "2025-07-11T09:30:00.000000Z\t0.3679848625908545\n" +
                        "2025-07-11T09:36:00.000000Z\t0.8231249461985348\n" +
                        "2025-07-11T09:42:00.000000Z\t0.6697969295620055\n" +
                        "2025-07-11T09:48:00.000000Z\t0.4295631643526773\n" +
                        "2025-07-11T09:54:00.000000Z\t0.26369335635512836\n", timestampType.getTypeName()),
                sql
        );

        Assert.assertEquals("[8,9]", partitionList.toString());
        assertSql(
                "QUERY PLAN\n" +
                        "Limit lo: -20 skip-over-rows: 80 limit: 20\n" +
                        "    PageFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tmp\n",
                "explain " + sql
        );
    }

    private @NotNull IntList setupTableAndOpenPartitionList() throws SqlException {
        execute("create table tmp as (select timestamp_sequence('2025-07-11'::timestamp, 360_000_000)::" + timestampType.getTypeName() + " ts, rnd_double() d from long_sequence(100)) timestamp(ts) partition by hour");
        assertSql(
                "count\n" +
                        "10\n",
                "select count from table_partitions('tmp')"
        );

        engine.releaseAllReaders();
        var partitionList = new IntList();

        engine.setReaderListener((tableToken, partitionIndex) -> {
            Assert.assertEquals("tmp", tableToken.getTableName());
            partitionList.add(partitionIndex);
        });
        return partitionList;
    }
}
