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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class CorrGroupByFunctionFactoryTest extends AbstractCairoTest {

    private static final String TRADES_DATA = """
            INSERT INTO trades(timestamp,price,amount) VALUES ('2022-03-08T18:03:57.609765Z','2615.54','4.4E-4'),
            ('2022-03-08T18:03:57.710419Z','39269.98','0.001'),
            ('2022-03-08T18:03:57.764098Z','2615.4','0.001'),
            ('2022-03-08T18:03:57.764098Z','2615.4','0.002'),
            ('2022-03-08T18:03:57.764098Z','2615.4','4.2698000000000004E-4'),
            ('2022-03-08T18:03:58.194582Z','2615.36','0.02593599'),
            ('2022-03-08T18:03:58.194582Z','2615.37','0.03500836'),
            ('2022-03-08T18:03:58.194582Z','2615.46','0.17260246'),
            ('2022-03-08T18:03:58.194582Z','2615.470000000000','0.14810976'),
            ('2022-03-08T18:03:58.357448Z','39263.28','0.00392897'),
            ('2022-03-08T18:03:58.357448Z','39265.31','1.27E-4'),
            ('2022-03-08T18:03:58.357448Z','39265.31','2.45E-4'),
            ('2022-03-08T18:03:58.357448Z','39265.31','7.3E-5'),
            ('2022-03-08T18:03:58.612275Z','2615.35','0.02245868'),
            ('2022-03-08T18:03:58.612275Z','2615.36','0.0324461300000'),
            ('2022-03-08T18:03:58.660121Z','39262.42','4.6562000000000003E-4'),
            ('2022-03-08T18:03:58.660121Z','39265.270000000004','6.847E-5'),
            ('2022-03-08T18:03:58.682070Z','2615.62','0.02685107'),
            ('2022-03-08T18:03:58.682070Z','2615.62','4.4E-4'),
            ('2022-03-08T18:03:58.682070Z','2615.62','4.4E-4'),
            ('2022-03-08T18:03:58.682070Z','2615.62','4.4E-4'),
            ('2022-03-08T18:03:58.682070Z','2615.62','4.4E-4'),
            ('2022-03-08T18:03:58.682070Z','2615.63','0.00828692'),
            ('2022-03-08T18:03:59.093929Z','2615.08','0.0182400000000'),
            ('2022-03-08T18:03:59.093929Z','2615.36','4.4E-4'),
            ('2022-03-08T18:03:59.093929Z','2615.38','4.4E-4'),
            ('2022-03-08T18:03:59.093929Z','2615.43','4.4E-4'),
            ('2022-03-08T18:03:59.093929Z','2615.43','4.4E-4'),
            ('2022-03-08T18:03:59.355334Z','39263.24','0.0127958999999'),
            ('2022-03-08T18:03:59.608328Z','2615.450000000000','0.001'),
            ('2022-03-08T18:03:59.608328Z','2615.450000000000','0.0440829'),
            ('2022-03-08T18:03:59.608328Z','2615.46','4.4E-4'),
            ('2022-03-08T18:03:59.608328Z','2615.55','4.4E-4'),
            ('2022-03-08T18:03:59.608328Z','2615.55','4.4E-4'),
            ('2022-03-08T18:03:59.608328Z','2615.55','4.4E-4'),
            ('2022-03-08T18:03:59.608328Z','2615.56','7.011200000000001E-4'),
            ('2022-03-08T18:03:59.727709Z','2615.44','4.4E-4'),
            ('2022-03-08T18:03:59.727709Z','2615.46','0.00556635'),
            ('2022-03-08T18:04:00.200434Z','39263.71','0.00207171'),
            ('2022-03-08T18:04:00.286031Z','2615.490000000000','0.001'),
            ('2022-03-08T18:04:00.286031Z','2615.5','4.4E-4'),
            ('2022-03-08T18:04:00.286031Z','2615.5','4.4E-4'),
            ('2022-03-08T18:04:00.286031Z','2615.5','4.4E-4'),
            ('2022-03-08T18:04:00.286031Z','2615.5','4.4E-4'),
            ('2022-03-08T18:04:00.286031Z','2615.51','0.03560969'),
            ('2022-03-08T18:04:00.286031Z','2615.52','0.03448545'),
            ('2022-03-08T18:04:00.286031Z','2615.66','0.05214486'),
            ('2022-03-08T18:04:00.326210Z','2615.46','0.05398012'),
            ('2022-03-08T18:04:00.395576Z','39268.89','0.00137114'),
            ('2022-03-08T18:04:00.395576Z','39268.89','0.00874886'),
            ('2022-03-08T18:04:00.399099Z','2615.46','0.20830946'),
            ('2022-03-08T18:04:00.399099Z','2615.470000000000','0.001'),
            ('2022-03-08T18:04:00.399099Z','2615.470000000000','0.001'),
            ('2022-03-08T18:04:00.431068Z','2615.48','0.00283596'),
            ('2022-03-08T18:04:00.583472Z','39268.89','1.6998E-4'),
            ('2022-03-08T18:04:00.583472Z','39269.03','4.2543E-4'),
            ('2022-03-08T18:04:00.652059Z','39269.03','0.00243515'),
            ('2022-03-08T18:04:00.678509Z','39269.03','0.0059'),
            ('2022-03-08T18:04:00.690258Z','39269.03','2.83E-6'),
            ('2022-03-08T18:04:00.690258Z','39269.520000000004','0.00764717'),
            ('2022-03-08T18:04:00.769190Z','2615.48','4.4E-4'),
            ('2022-03-08T18:04:00.769190Z','2615.490000000000','4.4E-4'),
            ('2022-03-08T18:04:00.769190Z','2615.490000000000','4.4E-4'),
            ('2022-03-08T18:04:00.769190Z','2615.490000000000','4.4E-4'),
            ('2022-03-08T18:04:00.769190Z','2615.5','0.0384595200000'),
            ('2022-03-08T18:04:00.797517Z','39269.520000000004','1.274E-5'),
            ('2022-03-08T18:04:00.797517Z','39269.66','0.0175104600000'),
            ('2022-03-08T18:04:00.822053Z','39271.15','0.038'),
            ('2022-03-08T18:04:00.825881Z','2615.52','4.4E-4'),
            ('2022-03-08T18:04:00.825881Z','2615.52','4.4E-4'),
            ('2022-03-08T18:04:00.826507Z','2615.66','4.4E-4'),
            ('2022-03-08T18:04:00.826507Z','2615.66','4.4E-4'),
            ('2022-03-08T18:04:00.826507Z','2615.67','0.570000000000'),
            ('2022-03-08T18:04:00.826507Z','2616.220000000000','0.479120000000'),
            ('2022-03-08T18:04:00.976207Z','39275.08','1.4401E-4'),
            ('2022-03-08T18:04:01.000524Z','39268.13','0.01281'),
            ('2022-03-08T18:04:01.004211Z','2615.66','4.4E-4'),
            ('2022-03-08T18:04:01.004211Z','2615.66','4.4E-4'),
            ('2022-03-08T18:04:01.004211Z','2615.66','4.4E-4'),
            ('2022-03-08T18:04:01.062339Z','39275.08','0.09985599'),
            ('2022-03-08T18:04:01.082274Z','39277.11','0.00876115'),
            ('2022-03-08T18:04:01.164363Z','39279.17','0.0479300000000'),
            ('2022-03-08T18:04:01.164363Z','39279.18','0.0270700000000'),
            ('2022-03-08T18:04:01.370105Z','39284.23','0.00243635'),
            ('2022-03-08T18:04:01.529881Z','39284.090000000004','0.01747499'),
            ('2022-03-08T18:04:01.617122Z','39272.05','0.02625746'),
            ('2022-03-08T18:04:01.783673Z','2615.8','0.0180000000000'),
            ('2022-03-08T18:04:01.783673Z','2615.950000000000','0.001'),
            ('2022-03-08T18:04:01.783673Z','2615.950000000000','0.001'),
            ('2022-03-08T18:04:01.787719Z','2616.08','0.001'),
            ('2022-03-08T18:04:01.787719Z','2616.09','0.001'),
            ('2022-03-08T18:04:01.787719Z','2616.100000000000','0.00732372000000'),
            ('2022-03-08T18:04:01.991343Z','2615.69','1.91228355'),
            ('2022-03-08T18:04:01.991343Z','2615.700000000000','0.12193348'),
            ('2022-03-08T18:04:01.991343Z','2615.77','0.15516619'),
            ('2022-03-08T18:04:01.991343Z','2615.81','0.001'),
            ('2022-03-08T18:04:01.991343Z','2615.81','0.001'),
            ('2022-03-08T18:04:01.991343Z','2615.81','0.001'),
            ('2022-03-08T18:04:02.006053Z','2616.02','0.001'),
            ('2022-03-08T18:04:02.006053Z','2616.02','0.00318975');""";
    private static final String TRADES_TABLE = """
            CREATE TABLE trades(
               timestamp TIMESTAMP
              ,price     DOUBLE
              ,amount    DOUBLE
            );""";

    @Test
    public void testCorrAllNull() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "corr\nnull\n", "select corr(x, y) from (select cast(null as double) x, cast(null as double) y from long_sequence(100))"
        ));
    }

    @Test
    public void testCorrAllSameValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1 as (select 17.2151921 x, 17.2151921 y from long_sequence(100))");
            assertSql(
                    "corr\nnull\n", "select corr(x, y) from tbl1"
            );
        });
    }

    @Test
    public void testCorrDoubleValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1 as (select cast(x as double) x, cast(x as double) y from long_sequence(100))");
            assertSql(
                    "corr\n1.0\n", "select corr(x, y) from tbl1"
            );
        });
    }

    @Test
    public void testCorrFirstNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1(x double, y double)");
            execute("insert into 'tbl1' VALUES (null, null)");
            execute("insert into 'tbl1' select x, x as y from long_sequence(100)");
            assertSql(
                    "corr\n1.0\n", "select corr(x, y) from tbl1"
            );
        });
    }

    @Test
    public void testCorrFloatValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1 as (select cast(x as float) x, cast(x as float) y from long_sequence(100))");
            assertSql(
                    "corr\n1.0\n", "select corr(x, y) from tbl1"
            );
        });
    }

    @Test
    public void testCorrIntValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1 as (select cast(x as int) x, cast(x as int) y from long_sequence(100))");
            assertSql(
                    "corr\n1.0\n", "select corr(x, y) from tbl1"
            );
        });
    }

    @Test
    public void testCorrNoValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1(x int, y int)");
            assertSql(
                    "corr\nnull\n", "select corr(x, y) from tbl1"
            );
        });
    }

    @Test
    public void testCorrOneColumnAllNull() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "corr\nnull\n", "select corr(x, y) from (select cast(null as double) x, x as y from long_sequence(100))"
        ));
    }

    @Test
    public void testCorrOneValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1(x int, y int)");
            execute("insert into 'tbl1' VALUES " +
                    "(1, 1)");
            assertSql(
                    "corr\nnull\n", "select corr(x, y) from tbl1"
            );
        });
    }

    @Test
    public void testCorrOverflow() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1 as (select 100000000 x, 100000000 y from long_sequence(1000000))");
            assertSql(
                    "corr\nnull\n", "select corr(x, y) from tbl1"
            );
        });
    }

    @Test
    public void testCorrSomeNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1 as (select cast(x as double) x, cast(x as double) y from long_sequence(100))");
            execute("insert into 'tbl1' VALUES (null, null)");
            assertSql(
                    "corr\n1.0\n", "select corr(x, y) from tbl1"
            );
        });
    }

    @Test
    public void testCorrTwoValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl1(x int, y int)");
            execute("insert into 'tbl1' VALUES " +
                    "(1, 1), (2, 2)");
            assertSql(
                    "corr\n1.0\n", "select corr(x, y) from tbl1"
            );
        });
    }

    @Test
    public void testTradingData() throws Exception {
        execute(TRADES_TABLE);
        execute(TRADES_DATA);

        assertSql("""
                corr
                -0.10692047006371702
                """, "select corr(price, amount) from trades");
        assertSql("""
                corr
                -0.10692047006371702
                """, "select corr(price, amount) from trades");
    }
}