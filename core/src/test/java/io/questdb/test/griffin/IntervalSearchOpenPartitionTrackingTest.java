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
                replaceTimestampSuffix("""
                        ts\td
                        2025-07-11T03:54:00.000000Z\t0.5522494170511608
                        2025-07-11T03:48:00.000000Z\t0.5298405941762054
                        2025-07-11T03:42:00.000000Z\t0.7664256753596138
                        2025-07-11T03:36:00.000000Z\t0.26922103479744897
                        2025-07-11T03:30:00.000000Z\t0.42281342727402726
                        2025-07-11T03:24:00.000000Z\t0.3435685332942956
                        2025-07-11T03:18:00.000000Z\t0.8912587536603974
                        2025-07-11T03:12:00.000000Z\t0.6761934857077543
                        2025-07-11T03:06:00.000000Z\t0.12026122412833129
                        2025-07-11T03:00:00.000000Z\t0.9038068796506872
                        """, timestampType.getTypeName()),
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
                replaceTimestampSuffix("""
                        ts\td
                        2025-07-11T07:54:00.000000Z\t0.19751370382305056
                        2025-07-11T07:48:00.000000Z\t0.8940917126581895
                        2025-07-11T07:42:00.000000Z\t0.03167026265669903
                        2025-07-11T07:36:00.000000Z\t0.44804689668613573
                        2025-07-11T07:30:00.000000Z\t0.9540069089049732
                        2025-07-11T07:24:00.000000Z\t0.48558682958070665
                        2025-07-11T07:18:00.000000Z\t0.2820020716674768
                        2025-07-11T07:12:00.000000Z\t0.5811247005631662
                        2025-07-11T07:06:00.000000Z\t0.6551335839796312
                        2025-07-11T07:00:00.000000Z\t0.42020442539326086
                        """, timestampType.getTypeName()),
                sql
        );

        Assert.assertEquals("[7]", partitionList.toString());
        assertSql(
                """
                        QUERY PLAN
                        Limit left: 20 right: 30 skip-rows: 20 take-rows: 10
                            PageFrame
                                Row backward scan
                                Frame backward scan on: tmp
                        """,
                "explain " + sql
        );
    }

    @Test
    public void testFwdInterval() throws SqlException {
        var partitionList = setupTableAndOpenPartitionList();
        var sql = "select * from tmp where ts in '2025-07-11T05'";
        assertSql(
                replaceTimestampSuffix("""
                        ts\td
                        2025-07-11T05:00:00.000000Z\t0.16381374773748514
                        2025-07-11T05:06:00.000000Z\t0.456344569609078
                        2025-07-11T05:12:00.000000Z\t0.8664158914718532
                        2025-07-11T05:18:00.000000Z\t0.40455469747939254
                        2025-07-11T05:24:00.000000Z\t0.4149517697653501
                        2025-07-11T05:30:00.000000Z\t0.5659429139861241
                        2025-07-11T05:36:00.000000Z\t0.05384400312338511
                        2025-07-11T05:42:00.000000Z\t0.6821660861001273
                        2025-07-11T05:48:00.000000Z\t0.7230015763133606
                        2025-07-11T05:54:00.000000Z\t0.9644183832564398
                        """, timestampType.getTypeName()),
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
                replaceTimestampSuffix("""
                        ts\td
                        2025-07-11T08:00:00.000000Z\t0.9441658975532605
                        2025-07-11T08:06:00.000000Z\t0.6806873134626418
                        2025-07-11T08:12:00.000000Z\t0.7176053468281931
                        2025-07-11T08:18:00.000000Z\t0.24008362859107102
                        2025-07-11T08:24:00.000000Z\t0.8733293804420821
                        2025-07-11T08:30:00.000000Z\t0.9455893004802433
                        2025-07-11T08:36:00.000000Z\t0.17833722747266334
                        2025-07-11T08:42:00.000000Z\t0.6247427794126656
                        2025-07-11T08:48:00.000000Z\t0.3045253310626277
                        2025-07-11T08:54:00.000000Z\t0.3901731258748704
                        2025-07-11T09:00:00.000000Z\t0.03993124821273464
                        2025-07-11T09:06:00.000000Z\t0.10643046345788132
                        2025-07-11T09:12:00.000000Z\t0.8438459563914771
                        2025-07-11T09:18:00.000000Z\t0.07246172621937097
                        2025-07-11T09:24:00.000000Z\t0.0171850098561398
                        2025-07-11T09:30:00.000000Z\t0.3679848625908545
                        2025-07-11T09:36:00.000000Z\t0.8231249461985348
                        2025-07-11T09:42:00.000000Z\t0.6697969295620055
                        2025-07-11T09:48:00.000000Z\t0.4295631643526773
                        2025-07-11T09:54:00.000000Z\t0.26369335635512836
                        """, timestampType.getTypeName()),
                sql
        );

        Assert.assertEquals("[8,9]", partitionList.toString());
        assertSql(
                """
                        QUERY PLAN
                        Limit value: -20 skip-rows: 80 take-rows: 20
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tmp
                        """,
                "explain " + sql
        );
    }

    private @NotNull IntList setupTableAndOpenPartitionList() throws SqlException {
        execute("create table tmp as (select timestamp_sequence('2025-07-11'::timestamp, 360_000_000)::" + timestampType.getTypeName() + " ts, rnd_double() d from long_sequence(100)) timestamp(ts) partition by hour");
        assertSql(
                """
                        count
                        10
                        """,
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
