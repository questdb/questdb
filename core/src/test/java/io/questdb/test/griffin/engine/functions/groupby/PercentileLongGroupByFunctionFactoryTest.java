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
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANYIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * Temporary tables are used in this class, generated via union and long_sequence.
 * Please check the comment on `testModeWithGroupBy` for clarity.
 */
public class PercentileLongGroupByFunctionFactoryTest extends AbstractCairoTest {

    const String txDdl = """
            CREATE TABLE tx_traffic (timestamp TIMESTAMP, value VALUE);
            """;

    const String txDml = """
            INSERT INTO tx_traffic (timestamp, value) VALUES
            ('2022-05-02T14:51:28Z', '10.978'),
            ('2022-05-02T14:52:28Z', '85.063'),
            ('2022-05-02T14:52:58Z', '148.83'),
            ('2022-05-02T14:53:28Z', '135.881'),
            ('2022-05-02T14:54:28Z', '47.674'),
            ('2022-05-02T14:54:58Z', '61.336'),
            ('2022-05-02T14:54:58Z', '63.451'),
            ('2022-05-02T14:55:28Z', '8.618'),
            ('2022-05-02T14:55:58Z', '61.774'),
            ('2022-05-02T14:56:28Z', '8.1'),
            ('2022-05-02T14:56:58Z', '177.434'),
            ('2022-05-02T14:57:28Z', '12.955'),
            ('2022-05-02T14:57:58Z', '62.164'),
            ('2022-05-02T14:58:28Z', '161.288'),
            ('2022-05-02T14:58:58Z', '63.363'),
            ('2022-05-02T14:59:28Z', '10.228'),
            ('2022-05-02T14:59:58Z', '60.801'),
            ('2022-05-02T15:00:28Z', '14.053'),
            ('2022-05-02T15:01:28Z', '406.977'),
            ('2022-05-02T15:01:58Z', '849.035'),
            ('2022-05-02T15:02:28Z', '14.674'),
            ('2022-05-02T15:02:58Z', '126.668'),
            ('2022-05-02T15:03:28Z', '217.782'),
            ('2022-05-02T15:03:28Z', '66.75'),
            ('2022-05-02T15:04:28Z', '39.856'),
            ('2022-05-02T15:04:58Z', '289.615'),
            ('2022-05-02T15:05:28Z', '182.917'),
            ('2022-05-02T15:05:58Z', '219.222'),
            ('2022-05-02T15:06:28Z', '125.86'),
            ('2022-05-02T15:06:58Z', '87.316'),
            ('2022-05-02T15:07:28Z', '191.085'),
            ('2022-05-02T15:07:58Z', '242.04'),
            ('2022-05-02T15:08:28Z', '224.195'),
            ('2022-05-02T15:08:58Z', '240.752'),
            ('2022-05-02T15:09:28Z', '70.349'),
            ('2022-05-02T15:09:58Z', '177.227'),
            ('2022-05-02T15:10:28Z', '184.439'),
            ('2022-05-02T15:10:58Z', '118.329'),
            ('2022-05-02T15:11:28Z', '8.316'),
            ('2022-05-02T15:11:58Z', '63.863'),
            ('2022-05-02T15:12:58Z', '60.102'),
            ('2022-05-02T15:13:28Z', '47.509'),
            ('2022-05-02T15:13:58Z', '62.541'),
            ('2022-05-02T15:14:28Z', '11.976'),
            ('2022-05-02T15:14:58Z', '59.689'),
            ('2022-05-02T15:15:28Z', '8.939'),
            ('2022-05-02T15:15:58Z', '65.81');
            """;


    @Test
    public void testModeAllNull() throws Exception {
        assertQuery(
                "mode\n" +
                        "null\n",
                "select mode(f) from tab",
                "create table tab as (select null::long as f from long_sequence(5))",
                null,
                false,
                true
        );
    }

    @Test
    public void testModeBasic() throws Exception {
        assertQuery(
                "mode\n" +
                        "1\n",
                "select mode(f) from tab",
                "create table tab as (" +
                        "select x::long as f from long_sequence(5) " +
                        "union all " +
                        "select 1L as f from long_sequence(3)" +
                        ")",
                null,
                false,
                true
        );
    }

    @Test
    public void testModeEmpty() throws Exception {
        assertQuery(
                "mode\n" +
                        "null\n",
                "select mode(f) from tab",
                "create table tab (f long)",
                null,
                false,
                true
        );
    }

    @Test
    public void testModeSingleValue() throws Exception {
        assertQuery(
                "mode\n" +
                        "42\n",
                "select mode(f) from tab",
                "create table tab as (select 42L as f from long_sequence(1))",
                null,
                false,
                true
        );
    }

    @Test
    public void testModeSomeNull() throws Exception {
        assertQuery(
                "mode\n" +
                        "5\n",
                "select mode(f) from tab",
                "create table tab as (" +
                        "select null::long as f from long_sequence(2) " +
                        "union all " +
                        "select 5L as f from long_sequence(3) " +
                        "union all " +
                        "select 7L as f from long_sequence(1)" +
                        ")",
                null,
                false,
                true
        );
    }


    /**
     * The temporary table unrolls to this:
     * <p>
     * | g | f  |<br>
     * | - | -- |<br>
     * | A | 10 |<br>
     * | A | 10 |<br>
     * | A | 10 |<br>
     * | A | 20 |<br>
     * | B | 20 |<br>
     * | B | 20 |<br>
     * | B | 30 |<br>
     * </p>
     */
    @Test
    public void testModeWithGroupBy() throws Exception {
        assertQuery(
                "g\tmode\n" +
                        "A\t10\n" +
                        "B\t20\n",
                "select g, mode(f) from tab order by g",
                "create table tab as (" +
                        "select 'A' as g, 10L as f from long_sequence(3) " +
                        "union all " +
                        "select 'A' as g, 20L as f from long_sequence(1) " +
                        "union all " +
                        "select 'B' as g, 20L as f from long_sequence(2) " +
                        "union all " +
                        "select 'B' as g, 30L as f from long_sequence(1)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testModeWithLargeValues() throws Exception {
        assertQuery(
                "mode\n" +
                        "9.223372036854776E18\n",
                "select mode(f) from tab",
                "create table tab as (" +
                        "select 9223372036854775807 as f from long_sequence(3) " +
                        "union all " +
                        "select -9223372036854775808 as f from long_sequence(2)" +
                        ")",
                null,
                false,
                true
        );
    }

    @Test
    public void testModeWithManyGroups() throws Exception {
        assertQuery(
                "g\tmode\n" +
                        "0\t0\n" +
                        "1\t1000\n" +
                        "2\t2000\n" +
                        "3\t3000\n" +
                        "4\t4000\n",
                "select g, mode(f) from tab order by g limit 5",
                "create table tab as (" +
                        "select " +
                        "x % 100 as g, " +
                        "case when (x % 10) < 5 then (x % 100) * 1000L else (x % 100) * 1000L + x end as f " +
                        "from long_sequence(2000)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testModeWithNegativeValues() throws Exception {
        assertQuery(
                "mode\n" +
                        "-1\n",
                "select mode(f) from tab",
                "create table tab as (" +
                        "select -1L as f from long_sequence(3) " +
                        "union all " +
                        "select -2L as f from long_sequence(2) " +
                        "union all " +
                        "select -3L as f from long_sequence(1)" +
                        ")",
                null,
                false,
                true
        );
    }

    @Test
    public void testModeWithOrderByAndLimit() throws Exception {
        assertQuery(
                "g\tmode\n" +
                        "E\t40\n" +
                        "D\t30\n" +
                        "C\t20\n",
                "select g, mode(f) from tab order by g desc limit 3",
                "create table tab as (" +
                        "select " +
                        "case when x % 5 = 1 then 'A' " +
                        "when x % 5 = 2 then 'B' " +
                        "when x % 5 = 3 then 'C' " +
                        "when x % 5 = 4 then 'D' " +
                        "else 'E' end as g, " +
                        "case when x % 5 = 1 then 10L " +
                        "when x % 5 = 2 then 20L " +
                        "when x % 5 = 3 then 20L " +
                        "when x % 5 = 4 then 30L " +
                        "else 40L end as f " +
                        "from long_sequence(100)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testModeWithRandomData() throws Exception {
        assertQuery(
                "mode\n" +
                        "123456\n",
                "select mode(f) from tab",
                "create table tab as (" +
                        "select 123456L as f from long_sequence(10) " +
                        "union all " +
                        "select rnd_long(1, 999999, 0) as f from long_sequence(50)" +
                        ")",
                null,
                false,
                true
        );
    }

    @Test
    public void testModeWithSampleBy() throws Exception {
        assertQuery(
                "k\tmode\n" +
                        "1970-01-01T00:00:00.000000Z\t1\n" +
                        "1970-01-01T01:00:00.000000Z\t2\n" +
                        "1970-01-01T02:00:00.000000Z\t3\n",
                "select, mode(f) from tab sample by 1h",
                "create table tab as (" +
                        "select " +
                        "case when x % 3 = 1 then 1L " +
                        "when x % 3 = 2 then 2L " +
                        "else 3L end as f, " +
                        "timestamp_sequence(0, 60*60*1000000L/10) " +
                        "from long_sequence(30)" +
                        ") timestamp(k) partition by HOUR",
                "k",
                true,
                true
        );
    }

    @Test
    public void testModeWithWhereClause() throws Exception {
        assertQuery(
                "mode\n" +
                        "100\n",
                "select mode(f) from tab where filter < 5",
                "create table tab as (" +
                        "select " +
                        "x % 10 as filter, " +
                        "case when x % 10 < 5 then 100L else x end as f " +
                        "from long_sequence(100)" +
                        ")",
                null,
                false,
                true
        );
    }
}