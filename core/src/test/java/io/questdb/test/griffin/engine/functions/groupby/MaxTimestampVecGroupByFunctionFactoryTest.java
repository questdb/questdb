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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.PropertyKey;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class MaxTimestampVecGroupByFunctionFactoryTest extends AbstractCairoTest {
    private final TestTimestampType timestampType;

    public MaxTimestampVecGroupByFunctionFactoryTest(TestTimestampType timestampType) {
        this.timestampType = timestampType;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> testParams() {
        return Arrays.asList(new Object[][]{
                {TestTimestampType.MICRO}, {TestTimestampType.NANO}
        });
    }

    @Test
    public void testAddColumn() throws Exception {
        // fix page frame size, because it affects AVG accuracy
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 10_000);
        assertQuery(
                "avg\n" +
                        "5261.376146789\n",
                "select round(avg(f),9) avg from tab",
                "create table tab as (select rnd_int(-55, 9009, 2) f from long_sequence(131))",
                null,
                "alter table tab add column b " + timestampType.getTypeName(),
                "avg\n" +
                        "5261.376146789\n",
                false,
                true,
                false
        );

        assertQuery(
                "avg\tmax\n" +
                        (timestampType == TestTimestampType.MICRO ? "14.792007\t1970-01-01T00:00:00.088964Z\n" : "14.792007\t1970-01-01T00:00:00.000088964Z\n"),
                "select round(avg(f),6) avg, max(b) max from tab",
                "insert into tab select rnd_int(2, 10, 2), rnd_long(16772, 88965, 4) from long_sequence(78057)",
                null,
                false,
                true
        );
    }

    @Test
    public void testAllNullThenOne() throws Exception {
        assertQuery(
                "max\n" +
                        "\n",
                "select max(f) from tab",
                "create table tab as (select cast(null as " + timestampType.getTypeName() + ") f from long_sequence(33))",
                null,
                "insert into tab select 999999999999999L::timestamp from long_sequence(1)",
                "max\n" + (timestampType == TestTimestampType.MICRO ? "2001-09-09T01:46:39.999999Z\n" : "2001-09-09T01:46:39.999999000Z\n"),
                false,
                true,
                false
        );
    }

    @Test
    public void testKeyedMaxTimestampOrNullThenMaxLong() throws Exception {
        assertQuery(
                "i\tmax\n" +
                        "1\t\n",
                "select i, max(f) from tab",
                "create table tab as (select cast(1 as int) i, cast(null as " + timestampType.getTypeName() + ") f from long_sequence(33))",
                null,
                "insert into tab select 1, 9223372036854775807L from long_sequence(1)",
                "i\tmax\n" + (timestampType == TestTimestampType.MICRO ? "1\t294247-01-10T04:00:54.775807Z\n" : "1\t2262-04-11T23:47:16.854775807Z\n"),
                true,
                true,
                false
        );
    }

    @Test
    public void testMaxTimestampOrNullThenMaxLong() throws Exception {
        assertQuery(
                "max\n" +
                        "\n",
                "select max(f) from tab",
                "create table tab as (select cast(null as " + timestampType.getTypeName() + ") f from long_sequence(33))",
                null,
                "insert into tab select 9223372036854775807L from long_sequence(1)",
                "max\n" + (timestampType == TestTimestampType.MICRO ? "294247-01-10T04:00:54.775807Z\n" : "2262-04-11T23:47:16.854775807Z\n"),
                false,
                true,
                false
        );
    }

    @Test
    public void testSimple() throws Exception {
        assertQuery(
                "max\n" +
                        (timestampType == TestTimestampType.MICRO ? "1970-01-01T00:00:00.008826Z\n" : "1970-01-01T00:00:00.000008826Z\n"),
                "select max(f) from tab",
                "create table tab as (select cast(rnd_long(-55, 9009, 2) as " + timestampType.getTypeName() + ") f from long_sequence(131))",
                null,
                false,
                true
        );
    }
}
