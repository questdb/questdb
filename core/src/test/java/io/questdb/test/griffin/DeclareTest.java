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

import io.questdb.cairo.SqlJitMode;
import io.questdb.griffin.model.ExecutionModel;
import org.junit.Test;

public class DeclareTest extends AbstractSqlParserTest {

    private static final String aaplDdl = "CREATE TABLE 'AAPL_orderbook' (\n" +
            "  timestamp TIMESTAMP,\n" +
            "  ts_recv VARCHAR,\n" +
            "  ts_event VARCHAR,\n" +
            "  rtype LONG,\n" +
            "  symbol VARCHAR,\n" +
            "  publisher_id LONG,\n" +
            "  instrument_id LONG,\n" +
            "  action VARCHAR,\n" +
            "  side VARCHAR,\n" +
            "  depth LONG,\n" +
            "  price DOUBLE,\n" +
            "  size LONG,\n" +
            "  flags LONG,\n" +
            "  ts_in_delta LONG,\n" +
            "  sequence LONG,\n" +
            "  bid_px_00 DOUBLE,\n" +
            "  ask_px_00 DOUBLE,\n" +
            "  bid_sz_00 LONG,\n" +
            "  ask_sz_00 LONG,\n" +
            "  bid_ct_00 LONG,\n" +
            "  ask_ct_00 LONG,\n" +
            "  bid_px_01 DOUBLE,\n" +
            "  ask_px_01 DOUBLE,\n" +
            "  bid_sz_01 LONG,\n" +
            "  ask_sz_01 LONG,\n" +
            "  bid_ct_01 LONG,\n" +
            "  ask_ct_01 LONG,\n" +
            "  bid_px_02 DOUBLE,\n" +
            "  ask_px_02 DOUBLE,\n" +
            "  bid_sz_02 LONG,\n" +
            "  ask_sz_02 LONG,\n" +
            "  bid_ct_02 LONG,\n" +
            "  ask_ct_02 LONG,\n" +
            "  bid_px_03 DOUBLE,\n" +
            "  ask_px_03 DOUBLE,\n" +
            "  bid_sz_03 LONG,\n" +
            "  ask_sz_03 LONG,\n" +
            "  bid_ct_03 LONG,\n" +
            "  ask_ct_03 LONG,\n" +
            "  bid_px_04 DOUBLE,\n" +
            "  ask_px_04 DOUBLE,\n" +
            "  bid_sz_04 LONG,\n" +
            "  ask_sz_04 LONG,\n" +
            "  bid_ct_04 LONG,\n" +
            "  ask_ct_04 LONG,\n" +
            "  bid_px_05 DOUBLE,\n" +
            "  ask_px_05 DOUBLE,\n" +
            "  bid_sz_05 LONG,\n" +
            "  ask_sz_05 LONG,\n" +
            "  bid_ct_05 LONG,\n" +
            "  ask_ct_05 LONG,\n" +
            "  bid_px_06 DOUBLE,\n" +
            "  ask_px_06 DOUBLE,\n" +
            "  bid_sz_06 LONG,\n" +
            "  ask_sz_06 LONG,\n" +
            "  bid_ct_06 LONG,\n" +
            "  ask_ct_06 LONG,\n" +
            "  bid_px_07 DOUBLE,\n" +
            "  ask_px_07 DOUBLE,\n" +
            "  bid_sz_07 LONG,\n" +
            "  ask_sz_07 LONG,\n" +
            "  bid_ct_07 LONG,\n" +
            "  ask_ct_07 LONG,\n" +
            "  bid_px_08 DOUBLE,\n" +
            "  ask_px_08 DOUBLE,\n" +
            "  bid_sz_08 LONG,\n" +
            "  ask_sz_08 LONG,\n" +
            "  bid_ct_08 LONG,\n" +
            "  ask_ct_08 LONG,\n" +
            "  bid_px_09 DOUBLE,\n" +
            "  ask_px_09 DOUBLE,\n" +
            "  bid_sz_09 LONG,\n" +
            "  ask_sz_09 LONG,\n" +
            "  bid_ct_09 LONG,\n" +
            "  ask_ct_09 LONG\n" +
            ") timestamp (timestamp) PARTITION BY HOUR WAL;";
    private static final String tradesData = "INSERT INTO trades(timestamp,price,amount) VALUES ('2022-03-08T18:03:57.609765Z','2615.54','4.4E-4'),\n" +
            "('2022-03-08T18:03:57.710419Z','39269.98','0.001'),\n" +
            "('2022-03-08T18:03:57.764098Z','2615.4','0.001'),\n" +
            "('2022-03-08T18:03:57.764098Z','2615.4','0.002'),\n" +
            "('2022-03-08T18:03:57.764098Z','2615.4','4.2698000000000004E-4'),\n" +
            "('2022-03-08T18:03:58.194582Z','2615.36','0.02593599'),\n" +
            "('2022-03-08T18:03:58.194582Z','2615.37','0.03500836'),\n" +
            "('2022-03-08T18:03:58.194582Z','2615.46','0.17260246'),\n" +
            "('2022-03-08T18:03:58.194582Z','2615.470000000000','0.14810976'),\n" +
            "('2022-03-08T18:03:58.357448Z','39263.28','0.00392897'),\n" +
            "('2022-03-08T18:03:58.357448Z','39265.31','1.27E-4'),\n" +
            "('2022-03-08T18:03:58.357448Z','39265.31','2.45E-4'),\n" +
            "('2022-03-08T18:03:58.357448Z','39265.31','7.3E-5'),\n" +
            "('2022-03-08T18:03:58.612275Z','2615.35','0.02245868'),\n" +
            "('2022-03-08T18:03:58.612275Z','2615.36','0.0324461300000'),\n" +
            "('2022-03-08T18:03:58.660121Z','39262.42','4.6562000000000003E-4'),\n" +
            "('2022-03-08T18:03:58.660121Z','39265.270000000004','6.847E-5'),\n" +
            "('2022-03-08T18:03:58.682070Z','2615.62','0.02685107'),\n" +
            "('2022-03-08T18:03:58.682070Z','2615.62','4.4E-4'),\n" +
            "('2022-03-08T18:03:58.682070Z','2615.62','4.4E-4'),\n" +
            "('2022-03-08T18:03:58.682070Z','2615.62','4.4E-4'),\n" +
            "('2022-03-08T18:03:58.682070Z','2615.62','4.4E-4'),\n" +
            "('2022-03-08T18:03:58.682070Z','2615.63','0.00828692'),\n" +
            "('2022-03-08T18:03:59.093929Z','2615.08','0.0182400000000'),\n" +
            "('2022-03-08T18:03:59.093929Z','2615.36','4.4E-4'),\n" +
            "('2022-03-08T18:03:59.093929Z','2615.38','4.4E-4'),\n" +
            "('2022-03-08T18:03:59.093929Z','2615.43','4.4E-4'),\n" +
            "('2022-03-08T18:03:59.093929Z','2615.43','4.4E-4'),\n" +
            "('2022-03-08T18:03:59.355334Z','39263.24','0.0127958999999'),\n" +
            "('2022-03-08T18:03:59.608328Z','2615.450000000000','0.001'),\n" +
            "('2022-03-08T18:03:59.608328Z','2615.450000000000','0.0440829'),\n" +
            "('2022-03-08T18:03:59.608328Z','2615.46','4.4E-4'),\n" +
            "('2022-03-08T18:03:59.608328Z','2615.55','4.4E-4'),\n" +
            "('2022-03-08T18:03:59.608328Z','2615.55','4.4E-4'),\n" +
            "('2022-03-08T18:03:59.608328Z','2615.55','4.4E-4'),\n" +
            "('2022-03-08T18:03:59.608328Z','2615.56','7.011200000000001E-4'),\n" +
            "('2022-03-08T18:03:59.727709Z','2615.44','4.4E-4'),\n" +
            "('2022-03-08T18:03:59.727709Z','2615.46','0.00556635'),\n" +
            "('2022-03-08T18:04:00.200434Z','39263.71','0.00207171'),\n" +
            "('2022-03-08T18:04:00.286031Z','2615.490000000000','0.001'),\n" +
            "('2022-03-08T18:04:00.286031Z','2615.5','4.4E-4'),\n" +
            "('2022-03-08T18:04:00.286031Z','2615.5','4.4E-4'),\n" +
            "('2022-03-08T18:04:00.286031Z','2615.5','4.4E-4'),\n" +
            "('2022-03-08T18:04:00.286031Z','2615.5','4.4E-4'),\n" +
            "('2022-03-08T18:04:00.286031Z','2615.51','0.03560969'),\n" +
            "('2022-03-08T18:04:00.286031Z','2615.52','0.03448545'),\n" +
            "('2022-03-08T18:04:00.286031Z','2615.66','0.05214486'),\n" +
            "('2022-03-08T18:04:00.326210Z','2615.46','0.05398012'),\n" +
            "('2022-03-08T18:04:00.395576Z','39268.89','0.00137114'),\n" +
            "('2022-03-08T18:04:00.395576Z','39268.89','0.00874886'),\n" +
            "('2022-03-08T18:04:00.399099Z','2615.46','0.20830946'),\n" +
            "('2022-03-08T18:04:00.399099Z','2615.470000000000','0.001'),\n" +
            "('2022-03-08T18:04:00.399099Z','2615.470000000000','0.001'),\n" +
            "('2022-03-08T18:04:00.431068Z','2615.48','0.00283596'),\n" +
            "('2022-03-08T18:04:00.583472Z','39268.89','1.6998E-4'),\n" +
            "('2022-03-08T18:04:00.583472Z','39269.03','4.2543E-4'),\n" +
            "('2022-03-08T18:04:00.652059Z','39269.03','0.00243515'),\n" +
            "('2022-03-08T18:04:00.678509Z','39269.03','0.0059'),\n" +
            "('2022-03-08T18:04:00.690258Z','39269.03','2.83E-6'),\n" +
            "('2022-03-08T18:04:00.690258Z','39269.520000000004','0.00764717'),\n" +
            "('2022-03-08T18:04:00.769190Z','2615.48','4.4E-4'),\n" +
            "('2022-03-08T18:04:00.769190Z','2615.490000000000','4.4E-4'),\n" +
            "('2022-03-08T18:04:00.769190Z','2615.490000000000','4.4E-4'),\n" +
            "('2022-03-08T18:04:00.769190Z','2615.490000000000','4.4E-4'),\n" +
            "('2022-03-08T18:04:00.769190Z','2615.5','0.0384595200000'),\n" +
            "('2022-03-08T18:04:00.797517Z','39269.520000000004','1.274E-5'),\n" +
            "('2022-03-08T18:04:00.797517Z','39269.66','0.0175104600000'),\n" +
            "('2022-03-08T18:04:00.822053Z','39271.15','0.038'),\n" +
            "('2022-03-08T18:04:00.825881Z','2615.52','4.4E-4'),\n" +
            "('2022-03-08T18:04:00.825881Z','2615.52','4.4E-4'),\n" +
            "('2022-03-08T18:04:00.826507Z','2615.66','4.4E-4'),\n" +
            "('2022-03-08T18:04:00.826507Z','2615.66','4.4E-4'),\n" +
            "('2022-03-08T18:04:00.826507Z','2615.67','0.570000000000'),\n" +
            "('2022-03-08T18:04:00.826507Z','2616.220000000000','0.479120000000'),\n" +
            "('2022-03-08T18:04:00.976207Z','39275.08','1.4401E-4'),\n" +
            "('2022-03-08T18:04:01.000524Z','39268.13','0.01281'),\n" +
            "('2022-03-08T18:04:01.004211Z','2615.66','4.4E-4'),\n" +
            "('2022-03-08T18:04:01.004211Z','2615.66','4.4E-4'),\n" +
            "('2022-03-08T18:04:01.004211Z','2615.66','4.4E-4'),\n" +
            "('2022-03-08T18:04:01.062339Z','39275.08','0.09985599'),\n" +
            "('2022-03-08T18:04:01.082274Z','39277.11','0.00876115'),\n" +
            "('2022-03-08T18:04:01.164363Z','39279.17','0.0479300000000'),\n" +
            "('2022-03-08T18:04:01.164363Z','39279.18','0.0270700000000'),\n" +
            "('2022-03-08T18:04:01.370105Z','39284.23','0.00243635'),\n" +
            "('2022-03-08T18:04:01.529881Z','39284.090000000004','0.01747499'),\n" +
            "('2022-03-08T18:04:01.617122Z','39272.05','0.02625746'),\n" +
            "('2022-03-08T18:04:01.783673Z','2615.8','0.0180000000000'),\n" +
            "('2022-03-08T18:04:01.783673Z','2615.950000000000','0.001'),\n" +
            "('2022-03-08T18:04:01.783673Z','2615.950000000000','0.001'),\n" +
            "('2022-03-08T18:04:01.787719Z','2616.08','0.001'),\n" +
            "('2022-03-08T18:04:01.787719Z','2616.09','0.001'),\n" +
            "('2022-03-08T18:04:01.787719Z','2616.100000000000','0.00732372000000'),\n" +
            "('2022-03-08T18:04:01.991343Z','2615.69','1.91228355'),\n" +
            "('2022-03-08T18:04:01.991343Z','2615.700000000000','0.12193348'),\n" +
            "('2022-03-08T18:04:01.991343Z','2615.77','0.15516619'),\n" +
            "('2022-03-08T18:04:01.991343Z','2615.81','0.001'),\n" +
            "('2022-03-08T18:04:01.991343Z','2615.81','0.001'),\n" +
            "('2022-03-08T18:04:01.991343Z','2615.81','0.001'),\n" +
            "('2022-03-08T18:04:02.006053Z','2616.02','0.001'),\n" +
            "('2022-03-08T18:04:02.006053Z','2616.02','0.00318975');";
    private static final String tradesDdl = "CREATE TABLE 'trades' (\n" +
            "  symbol SYMBOL,\n" +
            "  side SYMBOL,\n" +
            "  price DOUBLE,\n" +
            "  amount DOUBLE,\n" +
            "  timestamp TIMESTAMP\n" +
            ") timestamp (timestamp) PARTITION BY DAY WAL;";

    @Test
    public void testDeclareCreateAsSelect() throws Exception {
        assertMemoryLeak(() -> {
            execute(tradesDdl);
            drainWalQueue();
            assertModel("create batch 1000000 table foo as (select-virtual 1 + 2 column from (long_sequence(1)))",
                    "CREATE TABLE foo AS (DECLARE @x := 1, @y := 2 SELECT @x + @y)", ExecutionModel.CREATE_TABLE);
        });
    }

    @Test
    public void testDeclareGivesMoreUsefulErrorWhenMispellingDeclare() throws Exception {
        assertMemoryLeak(() -> {
            execute(tradesDdl);
            drainWalQueue();
            assertException("delcare @ts := timestamp select @ts from trades", 12, "perhaps `DECLARE` was misspelled?");
        });
    }

    @Test
    public void testDeclareGivesMoreUsefulErrorWhenUsingTheWrongBindOperator() throws Exception {
        assertMemoryLeak(() -> {
            execute(tradesDdl);
            drainWalQueue();
            assertException("declare @ts = timestamp select @ts from trades", 12, "expected variable assignment operator `:=`");
        });
    }

    @Test
    public void testDeclareInsertIntoSelect() throws Exception {
        assertMemoryLeak(() -> {
            execute(tradesDdl);
            drainWalQueue();
            execute("create table foo (x int)");
            drainWalQueue();
            String query = "INSERT INTO foo SELECT * FROM (DECLARE @x := 1, @y := 2 SELECT @x + @y as x)";
            assertModel("insert batch 1000000 into foo select-choose x from (select-virtual [1 + 2 x] 1 + 2 x from (long_sequence(1)))",
                    query, ExecutionModel.INSERT);
            execute(query);
            drainWalQueue();
            assertSql("x\n" +
                    "3\n", "select * from foo");
        });
    }

    @Test
    public void testDeclareSelectAsofJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table foo (ts timestamp, x int) timestamp(ts) partition by day wal;");
            execute("create table bah (ts timestamp, y int) timestamp(ts) partition by day wal;");
            drainWalQueue();
            assertModel("select-choose foo.ts ts, foo.x x from (select [ts, x] from foo timestamp (ts) asof join bah timestamp (ts))",
                    "DECLARE @foo := foo, @bah := bah SELECT foo.ts, foo.x FROM @foo ASOF JOIN @bah", ExecutionModel.QUERY);
        });
    }

    @Test
    public void testDeclareSelectCTE() throws Exception {
        assertModel("select-choose column from (select-virtual [2 + 5 column] 2 + 5 column from (long_sequence(1))) a",
                "DECLARE @x := 2, @y := 5 WITH a AS (SELECT @x + @y) SELECT * FROM a", ExecutionModel.QUERY);
    }

    @Test
    public void testDeclareSelectCase() throws Exception {
        assertModel("select-virtual case(1 = 1,5,2) case from (long_sequence(1))",
                "DECLARE @x := 1, @y := 5, @z := 2 SELECT CASE WHEN @x = @X THEN @y ELSE @z END", ExecutionModel.QUERY);
    }

    @Test
    public void testDeclareSelectCast1() throws Exception {
        assertModel("select-virtual cast(2,timestamp) cast from (long_sequence(1))",
                "DECLARE @x := 2::timestamp SELECT @x", ExecutionModel.QUERY);
    }

    @Test
    public void testDeclareSelectCast2() throws Exception {
        assertModel("select-virtual cast(5,timestamp) cast from (long_sequence(1))",
                "DECLARE @x := 5 SELECT CAST(@x AS timestamp)", ExecutionModel.QUERY);
    }

    @Test
    public void testDeclareSelectDistinct() throws Exception {
        assertMemoryLeak(() -> {
            execute(tradesDdl);
            drainWalQueue();
            assertModel("select-distinct [symbol] symbol from (select-choose [symbol] symbol from (select [symbol] from trades timestamp (timestamp)))",
                    "DECLARE @x := symbol SELECT DISTINCT symbol FROM trades", ExecutionModel.QUERY);
        });
    }

    @Test
    public void testDeclareSelectDouble() throws Exception {
        assertModel("select-virtual 123.456 column1 from (long_sequence(1))",
                "DECLARE @x := 123.456 SELECT @x", ExecutionModel.QUERY);
    }

    @Test
    public void testDeclareSelectExcept() throws Exception {
        assertModel("select-choose [column] column from (select-virtual [1 + 2 column] 1 + 2 column from (long_sequence(1))) except select-choose [column] column from (select-virtual [1 + 2 column] 1 + 2 column from (long_sequence(1)))",
                "DECLARE @a := 1, @b := 2 (SELECT @a + @b) EXCEPT (SELECT @a + @b)", ExecutionModel.QUERY);
    }

    @Test
    public void testDeclareSelectExceptAll() throws Exception {
        assertModel("select-choose [column] column from (select-virtual [1 + 2 column] 1 + 2 column from (long_sequence(1))) except all select-choose [column] column from (select-virtual [1 + 2 column] 1 + 2 column from (long_sequence(1)))",
                "DECLARE @a := 1, @b := 2 (SELECT @a + @b) EXCEPT ALL (SELECT @a + @b)", ExecutionModel.QUERY);
    }

    @Test
    public void testDeclareSelectExplainPlan() throws Exception {
        assertModel("EXPLAIN (FORMAT TEXT) ", "EXPLAIN DECLARE @x := 5 SELECT @x", ExecutionModel.EXPLAIN);
        assertSql("QUERY PLAN\n" +
                "VirtualRecord\n" +
                "  functions: [5]\n" +
                "    long_sequence count: 1\n", "EXPLAIN DECLARE @x := 5 SELECT @x");
    }

    @Test
    public void testDeclareSelectGroupByNames() throws Exception {
        assertMemoryLeak(() -> {
            execute(tradesDdl);
            drainWalQueue();
            assertModel("select-group-by timestamp, symbol, price from (select [timestamp, symbol, price] from trades timestamp (timestamp))",
                    "DECLARE @x := timestamp, @y := symbol SELECT timestamp, symbol, price FROM trades GROUP BY @x, @y, price", ExecutionModel.QUERY);
        });
    }

    @Test
    public void testDeclareSelectGroupByNumbers() throws Exception {
        assertMemoryLeak(() -> {
            execute(tradesDdl);
            drainWalQueue();
            assertModel("select-group-by timestamp, symbol, price from (select [timestamp, symbol, price] from trades timestamp (timestamp))",
                    "DECLARE @x := 1, @y := 2 SELECT timestamp, symbol, price FROM trades GROUP BY @x, @y, 3", ExecutionModel.QUERY);
        });
    }

    @Test
    public void testDeclareSelectInt() throws Exception {
        assertModel("select-virtual 5 5 from (long_sequence(1))",
                "DECLARE @x := 5 SELECT @x", ExecutionModel.QUERY);
    }

    @Test
    public void testDeclareSelectIntersect() throws Exception {
        assertModel("select-choose [column] column from (select-virtual [1 + 2 column] 1 + 2 column from (long_sequence(1))) intersect select-choose [column] column from (select-virtual [1 + 2 column] 1 + 2 column from (long_sequence(1)))",
                "DECLARE @a := 1, @b := 2 (SELECT @a + @b) INTERSECT (SELECT @a + @b)", ExecutionModel.QUERY);
    }

    @Test
    public void testDeclareSelectIntersectAll() throws Exception {
        assertModel("select-choose [column] column from (select-virtual [1 + 2 column] 1 + 2 column from (long_sequence(1))) intersect all select-choose [column] column from (select-virtual [1 + 2 column] 1 + 2 column from (long_sequence(1)))",
                "DECLARE @a := 1, @b := 2 (SELECT @a + @b) INTERSECT ALL (SELECT @a + @b)", ExecutionModel.QUERY);
    }

    @Test
    public void testDeclareSelectJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table foo (ts timestamp, x int) timestamp(ts) partition by day wal;");
            execute("create table bah (ts timestamp, y int) timestamp(ts) partition by day wal;");
            drainWalQueue();
            assertModel("select-choose foo.ts ts, foo.x x from (select [ts, x] from foo timestamp (ts) join select [y] from bah timestamp (ts) on bah.y = foo.x)",
                    "DECLARE @x := foo.x, @y := bah.y SELECT foo.ts, foo.x FROM foo JOIN bah on @x = @y", ExecutionModel.QUERY);
        });
    }

    @Test
    public void testDeclareSelectKeyedBindVariables() throws Exception {
        assertModel("select-virtual $1 $1, $2 $2 from (long_sequence(1))",
                "DECLARE @x := $1, @y := $2 SELECT @x, @y", ExecutionModel.QUERY);
    }

    @Test
    public void testDeclareSelectLatestBy() throws Exception {
        assertMemoryLeak(() -> {
            execute(tradesDdl);
            drainWalQueue();
            assertModel("select-choose symbol, side, price, amount, timestamp from (select [symbol, side, price, amount, timestamp] from trades timestamp (timestamp) latest by timestamp)",
                    "DECLARE @ts := timestamp SELECT * FROM trades LATEST BY @ts;", ExecutionModel.QUERY);
        });
    }

    @Test
    public void testDeclareSelectLatestOn() throws Exception {
        assertMemoryLeak(() -> {
            execute(tradesDdl);
            drainWalQueue();
            assertModel("select-choose symbol, side, price, amount, timestamp from (select [symbol, side, price, amount, timestamp] from trades latest on timestamp partition by symbol)",
                    "DECLARE @ts := timestamp, @sym := symbol SELECT * FROM trades LATEST ON @ts PARTITION BY @sym;", ExecutionModel.QUERY);
        });
    }

    @Test
    public void testDeclareSelectLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute(tradesDdl);
            drainWalQueue();
            assertModel("select-choose symbol, side, price, amount, timestamp from (select [symbol, side, price, amount, timestamp] from trades timestamp (timestamp)) limit 2,5",
                    "DECLARE @lo := 2, @hi := 5 SELECT * FROM trades LIMIT @lo, @hi", ExecutionModel.QUERY);
        });
    }

    @Test
    public void testDeclareSelectMultipleCTEs() throws Exception {
        String query = "DECLARE @x := 2, @y := 5 WITH a AS (SELECT @x + @y as col1), b AS (SELECT (@x - @y) + col1 as col2 FROM a) SELECT * FROM b";
        assertModel("select-choose col2 from (select-virtual [2 - 5 + col1 col2] 2 - 5 + col1 col2 from (select-virtual [2 + 5 col1] 2 + 5 col1 from (long_sequence(1))) a) b",
                query
                , ExecutionModel.QUERY);
        assertSql("col2\n" +
                        "4\n",
                query);
    }

    @Test
    public void testDeclareSelectMultipleColumns() throws Exception {
        assertModel("select-virtual 1 1, 2 2 from (long_sequence(1))",
                "DECLARE @x := 1, @y := 2 SELECT @x, @y", ExecutionModel.QUERY);
    }

    @Test
    public void testDeclareSelectMultipleColumnsBinaryExpr() throws Exception {
        assertModel("select-virtual 1 + 2 column from (long_sequence(1))",
                "DECLARE @x := 1, @y := 2 SELECT @x + @y", ExecutionModel.QUERY);
    }

    @Test
    public void testDeclareSelectMultipleColumnsComplexNesting() throws Exception {
        assertModel("select-virtual 1 * 2 + 1 / 2 column from (long_sequence(1))",
                "DECLARE @x := 1, @y := 2 SELECT @x * @y + @x / @y", ExecutionModel.QUERY);
    }

    @Test
    public void testDeclareSelectNegativeLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute(tradesDdl);
            execute(tradesData);
            drainWalQueue();
            String query = "DECLARE @lo := -5, @hi := -2 SELECT * FROM trades LIMIT @lo, @hi";
            assertModel("select-choose symbol, side, price, amount, timestamp from (select [symbol, side, price, amount, timestamp] from trades timestamp (timestamp)) limit -(5),-(2)",
                    query, ExecutionModel.QUERY);
            assertSql("symbol\tside\tprice\tamount\ttimestamp\n" +
                    "\t\t2615.81\t0.001\t2022-03-08T18:04:01.991343Z\n" +
                    "\t\t2615.81\t0.001\t2022-03-08T18:04:01.991343Z\n" +
                    "\t\t2615.81\t0.001\t2022-03-08T18:04:01.991343Z\n", query);
        });
    }

    @Test
    public void testDeclareSelectNegativeLimitUnary() throws Exception {
        assertMemoryLeak(() -> {
            execute(tradesDdl);
            drainWalQueue();
            assertModel("select-choose symbol, side, price, amount, timestamp from (select [symbol, side, price, amount, timestamp] from trades timestamp (timestamp)) limit -(2),-(5)",
                    "DECLARE @lo := 2, @hi := 5 SELECT * FROM trades LIMIT -@lo, -@hi", ExecutionModel.QUERY);
        });
    }

    @Test
    public void testDeclareSelectOrderByNames() throws Exception {
        assertMemoryLeak(() -> {
            execute(tradesDdl);
            drainWalQueue();
            assertModel("select-choose timestamp, symbol, price from (select [timestamp, symbol, price] from trades timestamp (timestamp)) order by timestamp, symbol, price",
                    "DECLARE @x := timestamp, @y := symbol SELECT timestamp, symbol, price FROM trades ORDER BY @x, @y, price", ExecutionModel.QUERY);
        });
    }

    @Test
    public void testDeclareSelectOrderByNumbers() throws Exception {
        assertMemoryLeak(() -> {
            execute(tradesDdl);
            drainWalQueue();
            assertModel("select-choose timestamp, symbol, price from (select [timestamp, symbol, price] from trades timestamp (timestamp)) order by timestamp, symbol, price",
                    "DECLARE @x := 1, @y := 2 SELECT timestamp, symbol, price FROM trades ORDER BY @x, @y, 3", ExecutionModel.QUERY);
        });
    }

    @Test
    public void testDeclareSelectPositionalBindVariables() throws Exception {
        assertException("DECLARE @x := ?, @y := ? SELECT @x, @y", 14, "Invalid column: ?");
    }

    @Test
    public void testDeclareSelectRequiredComma() throws Exception {
        assertModel("select-virtual 5 + 2 column from (long_sequence(1))", "DECLARE \n" +
                "  @x := 5,\n" +
                "  @y := 2\n" +
                "SELECT\n" +
                "  @x + @y", ExecutionModel.QUERY);
    }

    @Test
    public void testDeclareSelectSampleByBasic() throws Exception {
        assertMemoryLeak(() -> {
            execute(tradesDdl);
            drainWalQueue();
            assertModel("select-group-by timestamp_floor('1h',timestamp) timestamp, symbol, avg(price) avg from (select [timestamp, symbol, price] from trades timestamp (timestamp) stride 1h) order by timestamp",
                    "DECLARE @unit := 1h SELECT timestamp, symbol, avg(price) FROM trades SAMPLE BY @unit", ExecutionModel.QUERY);
        });
    }

    @Test
    public void testDeclareSelectSampleByFirstObservation() throws Exception {
        assertMemoryLeak(() -> {
            execute(tradesDdl);
            drainWalQueue();
            assertModel("select-group-by timestamp, symbol, avg(price) avg from (select [timestamp, symbol, price] from trades timestamp (timestamp)) sample by 1h",
                    "DECLARE @unit := 1h SELECT timestamp, symbol, avg(price) FROM trades SAMPLE BY @unit ALIGN TO FIRST OBSERVATION", ExecutionModel.QUERY);
        });
    }

    @Test
    public void testDeclareSelectSampleByFromToFill() throws Exception {
        assertMemoryLeak(() -> {
            execute(tradesDdl);
            drainWalQueue();
            assertModel("select-group-by timestamp, symbol, avg(price) avg from (select [timestamp, symbol, price] from trades timestamp (timestamp) where timestamp >= '2008-12-28' and timestamp < '2009-01-05') sample by 1h from '2008-12-28' to '2009-01-05' fill(null) align to calendar with offset '00:00'",
                    "DECLARE @unit := 1h, @from := '2008-12-28', @to := '2009-01-05', @fill := null SELECT timestamp, symbol, avg(price) FROM trades SAMPLE BY @unit FROM @from TO @to FILL(@fill)", ExecutionModel.QUERY);
        });
    }

    @Test
    public void testDeclareSelectSampleByWithOffset() throws Exception {
        assertMemoryLeak(() -> {
            execute(tradesDdl);
            drainWalQueue();
            assertModel("select-group-by timestamp, symbol, avg(price) avg from (select [timestamp, symbol, price] from trades timestamp (timestamp)) sample by 1h align to calendar with offset '10:00'",
                    "DECLARE @offset := '10:00' SELECT timestamp, symbol, avg(price) FROM trades SAMPLE BY 1h ALIGN TO CALENDAR WITH OFFSET @offset", ExecutionModel.QUERY);
        });
    }

    @Test
    public void testDeclareSelectSampleByWithTimezone() throws Exception {
        assertMemoryLeak(() -> {
            execute(tradesDdl);
            drainWalQueue();
            assertModel("select-group-by timestamp, symbol, avg(price) avg from (select [timestamp, symbol, price] from trades timestamp (timestamp)) sample by 1h align to calendar time zone 'Antarctica/McMurdo' with offset '00:00'",
                    "DECLARE @tz := 'Antarctica/McMurdo' SELECT timestamp, symbol, avg(price) FROM trades SAMPLE BY 1h ALIGN TO CALENDAR TIME ZONE @tz", ExecutionModel.QUERY);
        });
    }

    @Test
    public void testDeclareSelectSubQuery() throws Exception {
        assertModel("select-choose column from (select-virtual [2 + 5 column] 2 + 5 column from (long_sequence(1)))",
                "DECLARE @x := 2, @y := 5 SELECT * FROM (SELECT @x + @y)", ExecutionModel.QUERY);
    }

    @Test
    public void testDeclareSelectSubQueryAndShadowedVariable() throws Exception {
        assertModel("select-choose column from (select-virtual [7 + 5 column] 7 + 5 column from (long_sequence(1)))",
                "DECLARE @x := 2, @y := 5 SELECT * FROM (DECLARE @x:= 7 SELECT @x + @y)", ExecutionModel.QUERY);
    }

    @Test
    public void testDeclareSelectSubQueryAndShadowedVariableAndOuterUsage() throws Exception {
        assertModel("select-virtual 2 - 5 foo, column from (select-virtual [7 + 5 column] 7 + 5 column from (long_sequence(1)))",
                "DECLARE @x := 2, @y := 5 SELECT @x - @y as foo, * FROM (DECLARE @x:= 7 SELECT @x + @y)", ExecutionModel.QUERY);
    }

    @Test
    public void testDeclareSelectTableNameInFrom() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table foo (ts timestamp, x int) timestamp(ts) partition by day wal;");
            drainWalQueue();
            assertModel("select-choose ts, x from (select [ts, x] from foo timestamp (ts))",
                    "DECLARE @table_name := foo, @ts := ts, @x := x, SELECT @ts, @x FROM @table_name", ExecutionModel.QUERY);
        });
    }

    @Test
    public void testDeclareSelectUnion() throws Exception {
        assertModel("select-choose [column] column from (select-virtual [1 + 2 column] 1 + 2 column from (long_sequence(1))) union select-choose [column] column from (select-virtual [1 + 2 column] 1 + 2 column from (long_sequence(1)))",
                "DECLARE @a := 1, @b := 2 (SELECT @a + @b) UNION (SELECT @a + @b)", ExecutionModel.QUERY);
    }

    @Test
    public void testDeclareSelectUnionAll() throws Exception {
        assertModel("select-choose column from (select-virtual [1 + 2 column] 1 + 2 column from (long_sequence(1))) union all select-choose column from (select-virtual [1 + 2 column] 1 + 2 column from (long_sequence(1)))",
                "DECLARE @a := 1, @b := 2 (SELECT @a + @b) UNION ALL (SELECT @a + @b)", ExecutionModel.QUERY);
    }

    @Test
    public void testDeclareSelectWhere() throws Exception {
        assertModel("select-virtual 2 + 5 column from (long_sequence(1) where 2 < 5)",
                "DECLARE @x := 2, @y := 5 SELECT @x + @y FROM long_sequence(1) WHERE @x < @y", ExecutionModel.QUERY);
    }

    @Test
    public void testDeclareSelectWhereComplex() throws Exception {
        assertModel("select-virtual cast(2,timestamp) + cast(5,timestamp) column from (long_sequence(1) where cast(2,timestamp) < cast(5,timestamp))",
                "DECLARE @x := 2::timestamp, @y := 5::timestamp SELECT @x + @y FROM long_sequence(1) WHERE @x < @y", ExecutionModel.QUERY);
    }

    @Test
    public void testDeclareSelectWithFunction() throws Exception {
        assertMemoryLeak(() -> {
            execute(tradesDdl);
            drainWalQueue();
            assertModel("select-group-by timestamp, symbol, max(price) max from (select [timestamp, symbol, price] from trades timestamp (timestamp))",
                    "DECLARE @max_price := max(price) SELECT timestamp, symbol, @max_price FROM trades", ExecutionModel.QUERY);
        });
    }

    @Test
    public void testDeclareSelectWithFunction2() throws Exception {
        assertMemoryLeak(() -> {
            execute(tradesDdl);
            drainWalQueue();
            assertSql("column\n" +
                            "true\n",
                    "DECLARE\n" +
                            "    @today := today(),\n" +
                            "    @start := interval_start(@today),\n" +
                            "    @end := interval_end(@today)\n" +
                            "    SELECT @today = interval(@start, @end)");
        });
    }

    @Test
    public void testDeclareSelectWithWindowFunction() throws Exception {
        assertMemoryLeak(() -> {
            execute(aaplDdl);
            drainWalQueue();
            assertModel("select-window timestamp, bid_px_00, " +
                            "AVG(bid_px_00) avg_5min over (order by timestamp range between '5' minute preceding and current row exclude no others)," +
                            " COUNT() updates_100ms over (order by timestamp range between '100' millisecond preceding and current row exclude no others)," +
                            " SUM(bid_sz_00) volume_2sec over (order by timestamp range between '2' second preceding and current row exclude no others)" +
                            " from (select [timestamp, bid_px_00, bid_sz_00] from AAPL_orderbook timestamp (timestamp) where bid_px_00 > 0) limit 10",
                    "DECLARE\n" +
                            "    @ts := timestamp,\n" +
                            "    @bid_price := bid_px_00,\n" +
                            "    @bid_size := bid_sz_00,\n" +
                            "    @avg_time_range := '5',\n" +
                            "    @updates_period := '100',\n" +
                            "    @volume_2sec := '2'\n" +
                            "SELECT\n" +
                            "    @ts,\n" +
                            "    @bid_price,\n" +
                            "    AVG(@bid_price) OVER (\n" +
                            "        ORDER BY @ts\n" +
                            "        RANGE BETWEEN @avg_time_range MINUTE PRECEDING AND CURRENT ROW\n" +
                            "    ) AS avg_5min,\n" +
                            "    COUNT(*) OVER (\n" +
                            "        ORDER BY @ts\n" +
                            "        RANGE BETWEEN @updates_period MILLISECOND PRECEDING AND CURRENT ROW\n" +
                            "    ) AS updates_100ms,\n" +
                            "    SUM(@bid_size) OVER (\n" +
                            "        ORDER BY @ts\n" +
                            "        RANGE BETWEEN @volume_2sec SECOND PRECEDING AND CURRENT ROW\n" +
                            "    ) AS volume_2sec\n" +
                            "FROM AAPL_orderbook\n" +
                            "WHERE @bid_price > 0\n" +
                            "LIMIT 10;"
                    , ExecutionModel.QUERY);
        });
    }

    @Test
    public void testDeclareSelectWrongAssignmentOperator() throws Exception {
        assertException("DECLARE @x = 5 SELECT @x;", 11, "expected variable assignment operator");
    }

    @Test
    public void testDeclareVariableAsSubQuery() throws Exception {
        String targetModel = "select-choose y from (select-virtual [1 y] 1 y from (long_sequence(1)))";
        assertModel(targetModel,
                "SELECT * FROM (SELECT 1 as y)", ExecutionModel.QUERY);
        assertModel(targetModel,
                "DECLARE @x := (SELECT 1 as y) SELECT * FROM @x", ExecutionModel.QUERY);
    }

    @Test
    public void testDeclareVariableAsSubQueryWithNestedVariable() throws Exception {
        assertModel("select-choose y from (select-virtual [4 y] 4 y from (long_sequence(1)))",
                "DECLARE @x := (DECLARE @y := 4 SELECT @y as y) SELECT * FROM @x", ExecutionModel.QUERY);
    }

    @Test
    public void testDeclareVariableAsSubQueryWithNestedVariableAndPredeclaredVariable() throws Exception {
        assertModel("select-choose z from (select-virtual [4 + 5 z] 4 + 5 z from (long_sequence(1)))",
                "DECLARE @x := 5, @y := (DECLARE @y := 4 SELECT @y + @x as z) SELECT * FROM @y", ExecutionModel.QUERY);
    }

    @Test
    public void testDeclareVariableDefinedByAnotherVariable() throws Exception {
        assertModel("select-virtual 2 2, 2 * 2 column from (long_sequence(1))",
                "DECLARE @y := 2, @y2 := (@y * @y) SELECT @y, @y2", ExecutionModel.QUERY);
    }

    @Test
    public void testDeclareVariableWithBracketedExpression() throws Exception {
        assertMemoryLeak(() -> {
            execute(tradesDdl);
            assertException("DECLARE @symbols := ('ETH-USD', 'BTC-USD') " +
                            "SELECT * FROM trades WHERE @symbols IN @symbols",
                    43, "bracket lists");

        });
    }

    @Test
    public void testDeclareWorksWithIntrinsics01() throws Exception {
        assertMemoryLeak(() -> {
            execute(tradesDdl);
            drainWalQueue();
            String plan = "Async Group By workers: 1\n" +
                    "  keys: [timestamp]\n" +
                    "  values: [count(*)]\n" +
                    "  filter: null\n" +
                    "    PageFrame\n" +
                    "        Row forward scan\n" +
                    "        Interval forward scan on: trades\n" +
                    "          intervals: [(\"2024-01-01T00:00:00.000001Z\",\"MAX\")]\n";
            assertPlanNoLeakCheck("declare @ts := (timestamp > '2024-01-01') select timestamp, count() from trades where @ts;", plan);
            assertPlanNoLeakCheck("declare @lo := '2024-01-01' select timestamp, count() from trades where timestamp > @lo;", plan);
        });
    }

    @Test
    public void testDeclareWorksWithIntrinsics02() throws Exception {
        assertMemoryLeak(() -> {
            execute(tradesDdl);
            drainWalQueue();
            String plan = "Async Group By workers: 1\n" +
                    "  keys: [timestamp]\n" +
                    "  values: [count(*)]\n" +
                    "  filter: null\n" +
                    "    PageFrame\n" +
                    "        Row forward scan\n" +
                    "        Interval forward scan on: trades\n" +
                    "          intervals: [(\"2024-01-01T00:00:00.000001Z\",\"2024-08-22T23:59:59.999999Z\")]\n";
            assertPlanNoLeakCheck("declare @ts := (timestamp > '2024-01-01' and timestamp < '2024-08-23') select timestamp, count() from trades where @ts;", plan);
            assertPlanNoLeakCheck("declare @lo := '2024-01-01', @hi := '2024-08-23' select timestamp, count() from trades where timestamp > @lo and timestamp < @hi;", plan);
        });
    }

    @Test
    public void testDeclareWorksWithIntrinsics03() throws Exception {
        assertMemoryLeak(() -> {
            execute(tradesDdl);
            drainWalQueue();
            String plan = "Async Group By workers: 1\n" +
                    "  keys: [timestamp]\n" +
                    "  values: [count(*)]\n" +
                    "  filter: null\n" +
                    "    PageFrame\n" +
                    "        Row forward scan\n" +
                    "        Interval forward scan on: trades\n" +
                    "          intervals: [(\"MIN\",\"2024-08-22T23:59:59.999999Z\")]\n";
            assertPlanNoLeakCheck("declare @ts := (timestamp < '2024-08-23')  select timestamp, count() from trades where @ts;", plan);
            assertPlanNoLeakCheck("declare @hi := '2024-08-23' select timestamp, count() from trades where timestamp < @hi;", plan);
        });
    }

    @Test
    public void testDeclareWorksWithIntrinsics04() throws Exception {
        assertMemoryLeak(() -> {
            execute(tradesDdl);
            drainWalQueue();
            String plan = "Async Group By workers: 1\n" +
                    "  keys: [timestamp]\n" +
                    "  values: [count(*)]\n" +
                    "  filter: null\n" +
                    "    PageFrame\n" +
                    "        Row forward scan\n" +
                    "        Interval forward scan on: trades\n" +
                    "          intervals: [(\"2024-01-01T00:00:00.000001Z\",\"MAX\")]\n";
            assertPlanNoLeakCheck("declare @ts := (timestamp > '2024-01-01') select timestamp, count() from trades where @ts;", plan);
            assertPlanNoLeakCheck("declare @lo := '2024-01-01' select timestamp, count() from trades where timestamp > @lo;", plan);
        });
    }

    @Test
    public void testDeclareWorksWithIntrinsics05() throws Exception {
        assertMemoryLeak(() -> {
            execute(tradesDdl);
            drainWalQueue();
            String plan = "Async Group By workers: 1\n" +
                    "  keys: [timestamp]\n" +
                    "  values: [count(*)]\n" +
                    "  filter: null\n" +
                    "    PageFrame\n" +
                    "        Row forward scan\n" +
                    "        Interval forward scan on: trades\n" +
                    "          intervals: [(\"2024-01-01T00:00:00.000000Z\",\"2024-08-23T00:00:00.000000Z\")]\n";
            assertPlanNoLeakCheck("declare @ts := (timestamp >= '2024-01-01' and timestamp <= '2024-08-23') select timestamp, count() from trades where @ts;", plan);
            assertPlanNoLeakCheck("declare @lo := '2024-01-01', @hi := '2024-08-23' select timestamp, count() from trades where timestamp >= @lo and timestamp <= @hi;", plan);
        });
    }

    @Test
    public void testDeclareWorksWithIntrinsics06() throws Exception {
        assertMemoryLeak(() -> {
            execute(tradesDdl);
            drainWalQueue();
            String plan = "Async Group By workers: 1\n" +
                    "  keys: [timestamp]\n" +
                    "  values: [count(*)]\n" +
                    "  filter: null\n" +
                    "    PageFrame\n" +
                    "        Row forward scan\n" +
                    "        Interval forward scan on: trades\n" +
                    "          intervals: [(\"MIN\",\"2024-08-23T00:00:00.000000Z\")]\n";
            assertPlanNoLeakCheck("declare @ts := (timestamp <= '2024-08-23') select timestamp, count() from trades where @ts;", plan);
            assertPlanNoLeakCheck("declare @hi := '2024-08-23' select timestamp, count() from trades where timestamp <= @hi;", plan);
        });
    }

    @Test
    public void testDeclareWorksWithIntrinsics07() throws Exception {
        assertMemoryLeak(() -> {
            execute(tradesDdl);
            drainWalQueue();
            String plan = "Async Group By workers: 1\n" +
                    "  keys: [timestamp]\n" +
                    "  values: [count(*)]\n" +
                    "  filter: null\n" +
                    "    PageFrame\n" +
                    "        Row forward scan\n" +
                    "        Interval forward scan on: trades\n" +
                    "          intervals: [(\"2024-01-01T00:00:00.000000Z\",\"MAX\")]\n";
            assertPlanNoLeakCheck("declare @ts := (timestamp >= '2024-01-01') select timestamp, count() from trades where @ts;", plan);
            assertPlanNoLeakCheck("declare @lo := '2024-01-01' select timestamp, count() from trades where timestamp >= @lo;", plan);

        });
    }

    @Test
    public void testDeclareWorksWithIntrinsics08() throws Exception {
        assertMemoryLeak(() -> {
            execute(tradesDdl);
            drainWalQueue();
            String plan = "Async Group By workers: 1\n" +
                    "  keys: [timestamp]\n" +
                    "  values: [count(*)]\n" +
                    "  filter: null\n" +
                    "    PageFrame\n" +
                    "        Row forward scan\n" +
                    "        Interval forward scan on: trades\n" +
                    "          intervals: [(\"2024-01-01T00:00:00.000000Z\",\"2024-08-23T00:00:00.000000Z\")]\n";
            assertPlanNoLeakCheck("declare @ts := (timestamp between '2024-01-01' and '2024-08-23') select timestamp, count() from trades where @ts;", plan);
            assertPlanNoLeakCheck("declare @lo := '2024-01-01', @hi := '2024-08-23' select timestamp, count() from trades where timestamp between @lo and @hi;", plan);
        });
    }

    @Test
    public void testDeclareWorksWithIntrinsics09() throws Exception {
        assertMemoryLeak(() -> {
            execute(tradesDdl);
            drainWalQueue();
            String plan = "Async Group By workers: 1\n" +
                    "  keys: [timestamp]\n" +
                    "  values: [count(*)]\n" +
                    "  filter: null\n" +
                    "    PageFrame\n" +
                    "        Row forward scan\n" +
                    "        Interval forward scan on: trades\n" +
                    "          intervals: [(\"2024-01-01T00:00:00.000000Z\",\"2024-01-01T00:00:00.000000Z\"),(\"2024-08-23T00:00:00.000000Z\",\"2024-08-23T00:00:00.000000Z\")]\n";
            assertPlanNoLeakCheck("declare @ts1 := '2024-01-01', @ts2 := '2024-08-23' select timestamp, count() from trades where timestamp IN (@ts1, @ts2);", plan);
            assertException("declare @ts := ('2024-01-01', '2024-08-23') select timestamp, count() from trades where timestamp IN @ts", 44, "bracket lists are not supported");
        });
    }

    @Test
    public void testDeclareWorksWithJit() throws Exception {
        assertMemoryLeak(() -> {
            String plan = "Async{JIT}Filter workers: 1\n" +
                    "  filter: id<4\n" +
                    "    PageFrame\n" +
                    "        Row forward scan\n" +
                    "        Frame forward scan on: x\n";

            String replacement = sqlExecutionContext.getJitMode() == SqlJitMode.JIT_MODE_ENABLED ?
                    " JIT " : " ";
            plan = plan.replace("{JIT}", replacement);

            execute(
                    "create table x as (\n" +
                            "  select x id, timestamp_sequence(0,1000000000) as ts\n" +
                            "  from long_sequence(10)\n" +
                            ") timestamp(ts) partition by hour;"
            );
            assertPlanNoLeakCheck("declare @id := id, @val := 4 select * from x where @id < @val", plan);
            assertPlanNoLeakCheck("declare @expr := (id < 4) select * from x where @expr", plan);
            assertSql(
                    "id\tts\n" +
                            "1\t1970-01-01T00:00:00.000000Z\n" +
                            "2\t1970-01-01T00:16:40.000000Z\n" +
                            "3\t1970-01-01T00:33:20.000000Z\n",
                    "x where id < 4"
            );
        });
    }

}
