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

import io.questdb.cairo.SqlJitMode;
import io.questdb.griffin.model.ExecutionModel;
import org.junit.Test;

public class DeclareTest extends AbstractSqlParserTest {
    private static final String AAPL_DDL = """
            CREATE TABLE 'AAPL_orderbook' (
              timestamp TIMESTAMP,
              ts_recv VARCHAR,
              ts_event VARCHAR,
              rtype LONG,
              symbol VARCHAR,
              publisher_id LONG,
              instrument_id LONG,
              action VARCHAR,
              side VARCHAR,
              depth LONG,
              price DOUBLE,
              size LONG,
              flags LONG,
              ts_in_delta LONG,
              sequence LONG,
              bid_px_00 DOUBLE,
              ask_px_00 DOUBLE,
              bid_sz_00 LONG,
              ask_sz_00 LONG,
              bid_ct_00 LONG,
              ask_ct_00 LONG,
              bid_px_01 DOUBLE,
              ask_px_01 DOUBLE,
              bid_sz_01 LONG,
              ask_sz_01 LONG,
              bid_ct_01 LONG,
              ask_ct_01 LONG,
              bid_px_02 DOUBLE,
              ask_px_02 DOUBLE,
              bid_sz_02 LONG,
              ask_sz_02 LONG,
              bid_ct_02 LONG,
              ask_ct_02 LONG,
              bid_px_03 DOUBLE,
              ask_px_03 DOUBLE,
              bid_sz_03 LONG,
              ask_sz_03 LONG,
              bid_ct_03 LONG,
              ask_ct_03 LONG,
              bid_px_04 DOUBLE,
              ask_px_04 DOUBLE,
              bid_sz_04 LONG,
              ask_sz_04 LONG,
              bid_ct_04 LONG,
              ask_ct_04 LONG,
              bid_px_05 DOUBLE,
              ask_px_05 DOUBLE,
              bid_sz_05 LONG,
              ask_sz_05 LONG,
              bid_ct_05 LONG,
              ask_ct_05 LONG,
              bid_px_06 DOUBLE,
              ask_px_06 DOUBLE,
              bid_sz_06 LONG,
              ask_sz_06 LONG,
              bid_ct_06 LONG,
              ask_ct_06 LONG,
              bid_px_07 DOUBLE,
              ask_px_07 DOUBLE,
              bid_sz_07 LONG,
              ask_sz_07 LONG,
              bid_ct_07 LONG,
              ask_ct_07 LONG,
              bid_px_08 DOUBLE,
              ask_px_08 DOUBLE,
              bid_sz_08 LONG,
              ask_sz_08 LONG,
              bid_ct_08 LONG,
              ask_ct_08 LONG,
              bid_px_09 DOUBLE,
              ask_px_09 DOUBLE,
              bid_sz_09 LONG,
              ask_sz_09 LONG,
              bid_ct_09 LONG,
              ask_ct_09 LONG
            ) timestamp (timestamp) PARTITION BY HOUR WAL;""";
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
    private static final String TRADES_DDL = """
            CREATE TABLE 'trades' (
              symbol SYMBOL,
              side SYMBOL,
              price DOUBLE,
              amount DOUBLE,
              timestamp TIMESTAMP
            ) timestamp (timestamp) PARTITION BY DAY WAL;""";

    @Test
    public void testDeclareCreateAsSelect() throws Exception {
        assertMemoryLeak(() -> {
            execute(TRADES_DDL);
            drainWalQueue();
            assertModel("create batch 1000000 table foo as (select-virtual 1 + 2 column from (long_sequence(1)))",
                    "CREATE TABLE foo AS (DECLARE @x := 1, @y := 2 SELECT @x + @y)", ExecutionModel.CREATE_TABLE);
        });
    }

    @Test
    public void testDeclareCreateView() throws Exception {
        assertMemoryLeak(() ->
                assertModel("create view foo as (select-virtual 1 + 2 column from (long_sequence(1)))",
                        "CREATE VIEW foo AS (DECLARE @x := 1, @y := 2 SELECT @x + @y)", ExecutionModel.CREATE_VIEW)
        );
    }

    @Test
    public void testDeclareGivesMoreUsefulErrorWhenMispellingDeclare() throws Exception {
        assertMemoryLeak(() -> {
            execute(TRADES_DDL);
            drainWalQueue();
            assertException("delcare @ts := timestamp select @ts from trades", 12, "perhaps `DECLARE` was misspelled?");
        });
    }

    @Test
    public void testDeclareGivesMoreUsefulErrorWhenUsingTheWrongBindOperator() throws Exception {
        assertMemoryLeak(() -> {
            execute(TRADES_DDL);
            drainWalQueue();
            assertException("declare @ts = timestamp select @ts from trades", 12, "expected variable assignment operator `:=`");
        });
    }

    @Test
    public void testDeclareInsertIntoSelect() throws Exception {
        assertMemoryLeak(() -> {
            execute(TRADES_DDL);
            drainWalQueue();
            execute("create table foo (x int)");
            drainWalQueue();
            String query = "INSERT INTO foo SELECT * FROM (DECLARE @x := 1, @y := 2 SELECT @x + @y as x)";
            assertModel("insert batch 1000000 into foo select-choose x from (select-virtual [1 + 2 x] 1 + 2 x from (long_sequence(1)))",
                    query, ExecutionModel.INSERT);
            execute(query);
            drainWalQueue();
            assertSql("""
                    x
                    3
                    """, "select * from foo");
        });
    }

    @Test
    public void testDeclareMultiDeclares() throws Exception {
        assertException(
                "DECLARE @from_time := dateadd('h',-1,now()), DECLARE @to_time := now() WITH t as ( select now() as ts ) select * from t where ts between @from_time and @to_time;",
                45,
                "unexpected token [DECLARE] - Multiple DECLARE statements are not allowed. Use single DECLARE block: DECLARE @a := 1, @b := 1, @c := 1");
    }

    @Test
    public void testDeclareOverridable() throws Exception {
        assertModel("select-virtual 5 5 from (long_sequence(1))",
                "DECLARE OVERRIDABLE @x := 5 SELECT @x", ExecutionModel.QUERY);
    }

    @Test
    public void testDeclareOverridableCaseInsensitive() throws Exception {
        assertModel("select-virtual 5 5 from (long_sequence(1))",
                "DECLARE overridable @x := 5 SELECT @x", ExecutionModel.QUERY);
        assertModel("select-virtual 5 5 from (long_sequence(1))",
                "DECLARE Overridable @x := 5 SELECT @x", ExecutionModel.QUERY);
        assertModel("select-virtual 5 5 from (long_sequence(1))",
                "DECLARE OVERRIDABLE @x := 5 SELECT @x", ExecutionModel.QUERY);
    }

    @Test
    public void testDeclareOverridableMissingVariable() throws Exception {
        assertException("DECLARE OVERRIDABLE SELECT 1", 20, "variable name expected after OVERRIDABLE");
    }

    @Test
    public void testDeclareOverridableMixed() throws Exception {
        assertModel("select-virtual 5 5, 10 10 from (long_sequence(1))",
                "DECLARE OVERRIDABLE @x := 5, @y := 10 SELECT @x, @y", ExecutionModel.QUERY);
    }

    @Test
    public void testDeclareOverridableMultiple() throws Exception {
        assertModel("select-virtual 5 5, 10 10 from (long_sequence(1))",
                "DECLARE OVERRIDABLE @x := 5, OVERRIDABLE @y := 10 SELECT @x, @y", ExecutionModel.QUERY);
    }

    @Test
    public void testDeclareReuseVariable() throws Exception {
        assertSql("interval\n('2025-07-02T13:00:00.000Z', '2025-07-02T13:00:00.000Z')\n",
                "declare " +
                        "@ts := '2025-07-02T13:00:00.000000Z', " +
                        "@int := interval(@ts, @ts)" +
                        "select @int");
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
        assertModel("select-virtual case when 1 = 1 then 5 else 2 end case from (long_sequence(1))",
                "DECLARE @x := 1, @y := 5, @z := 2 SELECT CASE WHEN @x = @X THEN @y ELSE @z END", ExecutionModel.QUERY);
    }

    @Test
    public void testDeclareSelectCast1() throws Exception {
        assertModel("select-virtual 2::timestamp cast from (long_sequence(1))",
                "DECLARE @x := 2::timestamp SELECT @x", ExecutionModel.QUERY);
    }

    @Test
    public void testDeclareSelectCast2() throws Exception {
        assertModel("select-virtual 5::timestamp cast from (long_sequence(1))",
                "DECLARE @x := 5 SELECT CAST(@x AS timestamp)", ExecutionModel.QUERY);
    }

    @Test
    public void testDeclareSelectDistinct() throws Exception {
        assertMemoryLeak(() -> {
            execute(TRADES_DDL);
            drainWalQueue();
            assertModel("select-choose symbol from (select-group-by [symbol] symbol, count() count from (select [symbol] from trades timestamp (timestamp)))",
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
        assertSql("""
                QUERY PLAN
                VirtualRecord
                  functions: [5]
                    long_sequence count: 1
                """, "EXPLAIN DECLARE @x := 5 SELECT @x");
    }

    @Test
    public void testDeclareSelectFromGenerateSeries() throws Exception {
        // Test for issue #6547: DECLARE substitution should work for function arguments in FROM clause
        assertMemoryLeak(() -> assertSql(
                """
                        generate_series
                        2025-01-01T00:00:00.000000Z
                        2025-01-02T00:00:00.000000Z
                        """,
                "DECLARE @lo := '2025-01-01', @hi := '2025-01-02', @unit := '1d' SELECT * FROM generate_series(@lo, @hi, @unit)"
        ));
    }

    @Test
    public void testDeclareSelectGroupByNames() throws Exception {
        assertMemoryLeak(() -> {
            execute(TRADES_DDL);
            drainWalQueue();
            assertModel("select-group-by timestamp, symbol, price from (select [timestamp, symbol, price] from trades timestamp (timestamp))",
                    "DECLARE @x := timestamp, @y := symbol SELECT timestamp, symbol, price FROM trades GROUP BY @x, @y, price", ExecutionModel.QUERY);
        });
    }

    @Test
    public void testDeclareSelectGroupByNumbers() throws Exception {
        assertMemoryLeak(() -> {
            execute(TRADES_DDL);
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
            execute(TRADES_DDL);
            drainWalQueue();
            assertModel("select-choose symbol, side, price, amount, timestamp from (select [symbol, side, price, amount, timestamp] from trades timestamp (timestamp) latest by timestamp)",
                    "DECLARE @ts := timestamp SELECT * FROM trades LATEST BY @ts;", ExecutionModel.QUERY);
        });
    }

    @Test
    public void testDeclareSelectLatestOn() throws Exception {
        assertMemoryLeak(() -> {
            execute(TRADES_DDL);
            drainWalQueue();
            assertModel("select-choose symbol, side, price, amount, timestamp from (select [symbol, side, price, amount, timestamp] from trades latest on timestamp partition by symbol)",
                    "DECLARE @ts := timestamp, @sym := symbol SELECT * FROM trades LATEST ON @ts PARTITION BY @sym;", ExecutionModel.QUERY);
        });
    }

    @Test
    public void testDeclareSelectLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute(TRADES_DDL);
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
        assertSql("""
                        col2
                        4
                        """,
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
            execute(TRADES_DDL);
            execute(TRADES_DATA);
            drainWalQueue();
            String query = "DECLARE @lo := -5, @hi := -2 SELECT * FROM trades LIMIT @lo, @hi";
            assertModel("select-choose symbol, side, price, amount, timestamp from (select [symbol, side, price, amount, timestamp] from trades timestamp (timestamp)) limit -(5),-(2)",
                    query, ExecutionModel.QUERY);
            assertSql("""
                    symbol\tside\tprice\tamount\ttimestamp
                    \t\t2615.81\t0.001\t2022-03-08T18:04:01.991343Z
                    \t\t2615.81\t0.001\t2022-03-08T18:04:01.991343Z
                    \t\t2615.81\t0.001\t2022-03-08T18:04:01.991343Z
                    """, query);
        });
    }

    @Test
    public void testDeclareSelectNegativeLimitUnary() throws Exception {
        assertMemoryLeak(() -> {
            execute(TRADES_DDL);
            drainWalQueue();
            assertModel("select-choose symbol, side, price, amount, timestamp from (select [symbol, side, price, amount, timestamp] from trades timestamp (timestamp)) limit -(2),-(5)",
                    "DECLARE @lo := 2, @hi := 5 SELECT * FROM trades LIMIT -@lo, -@hi", ExecutionModel.QUERY);
        });
    }

    @Test
    public void testDeclareSelectOrderByNames() throws Exception {
        assertMemoryLeak(() -> {
            execute(TRADES_DDL);
            drainWalQueue();
            assertModel("select-choose timestamp, symbol, price from (select [timestamp, symbol, price] from trades timestamp (timestamp)) order by timestamp, symbol, price",
                    "DECLARE @x := timestamp, @y := symbol SELECT timestamp, symbol, price FROM trades ORDER BY @x, @y, price", ExecutionModel.QUERY);
        });
    }

    @Test
    public void testDeclareSelectOrderByNumbers() throws Exception {
        assertMemoryLeak(() -> {
            execute(TRADES_DDL);
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
        assertModel("select-virtual 5 + 2 column from (long_sequence(1))", """
                DECLARE\s
                  @x := 5,
                  @y := 2
                SELECT
                  @x + @y""", ExecutionModel.QUERY);
    }

    @Test
    public void testDeclareSelectSampleByBasic() throws Exception {
        assertMemoryLeak(() -> {
            execute(TRADES_DDL);
            drainWalQueue();
            assertModel("select-group-by timestamp_floor('1h', timestamp, null, '00:00', null) timestamp, symbol, avg(price) avg from (select [timestamp, symbol, price] from trades timestamp (timestamp) stride 1h) order by timestamp",
                    "DECLARE @unit := 1h SELECT timestamp, symbol, avg(price) FROM trades SAMPLE BY @unit", ExecutionModel.QUERY);
        });
    }

    @Test
    public void testDeclareSelectSampleByFirstObservation() throws Exception {
        assertMemoryLeak(() -> {
            execute(TRADES_DDL);
            drainWalQueue();
            assertModel("select-group-by timestamp, symbol, avg(price) avg from (select [timestamp, symbol, price] from trades timestamp (timestamp)) sample by 1h",
                    "DECLARE @unit := 1h SELECT timestamp, symbol, avg(price) FROM trades SAMPLE BY @unit ALIGN TO FIRST OBSERVATION", ExecutionModel.QUERY);
        });
    }

    @Test
    public void testDeclareSelectSampleByFromToFill() throws Exception {
        assertMemoryLeak(() -> {
            execute(TRADES_DDL);
            drainWalQueue();
            assertModel("select-group-by timestamp, symbol, avg(price) avg from (select [timestamp, symbol, price] from trades timestamp (timestamp) where timestamp >= '2008-12-28' and timestamp < '2009-01-05') sample by 1h from '2008-12-28' to '2009-01-05' fill(null) align to calendar with offset '00:00'",
                    "DECLARE @unit := 1h, @from := '2008-12-28', @to := '2009-01-05', @fill := null SELECT timestamp, symbol, avg(price) FROM trades SAMPLE BY @unit FROM @from TO @to FILL(@fill)", ExecutionModel.QUERY);
        });
    }

    @Test
    public void testDeclareSelectSampleByWithOffset() throws Exception {
        assertMemoryLeak(() -> {
            execute(TRADES_DDL);
            drainWalQueue();
            assertModel("select-group-by timestamp_floor('1h', timestamp, null, '10:00', null) timestamp, symbol, avg(price) avg from (select [timestamp, symbol, price] from trades timestamp (timestamp) stride 1h) order by timestamp",
                    "DECLARE @offset := '10:00' SELECT timestamp, symbol, avg(price) FROM trades SAMPLE BY 1h ALIGN TO CALENDAR WITH OFFSET @offset", ExecutionModel.QUERY);
        });
    }

    @Test
    public void testDeclareSelectSampleByWithTimezone() throws Exception {
        assertMemoryLeak(() -> {
            execute(TRADES_DDL);
            drainWalQueue();
            assertModel("select-virtual to_utc(timestamp, 'Antarctica/McMurdo') timestamp, symbol, avg from (select-group-by [timestamp_floor('1h', timestamp, null, '00:00', 'Antarctica/McMurdo') timestamp, symbol, avg(price) avg] timestamp_floor('1h', timestamp, null, '00:00', 'Antarctica/McMurdo') timestamp, symbol, avg(price) avg from (select [timestamp, symbol, price] from trades timestamp (timestamp) stride 1h)) timestamp (timestamp) order by timestamp",
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
        assertModel("select-virtual 2::timestamp + 5::timestamp column from (long_sequence(1) where 2::timestamp < 5::timestamp)",
                "DECLARE @x := 2::timestamp, @y := 5::timestamp SELECT @x + @y FROM long_sequence(1) WHERE @x < @y", ExecutionModel.QUERY);
    }

    @Test
    public void testDeclareSelectWithFunction() throws Exception {
        assertMemoryLeak(() -> {
            execute(TRADES_DDL);
            drainWalQueue();
            assertModel("select-group-by timestamp, symbol, max(price) max from (select [timestamp, symbol, price] from trades timestamp (timestamp))",
                    "DECLARE @max_price := max(price) SELECT timestamp, symbol, @max_price FROM trades", ExecutionModel.QUERY);
        });
    }

    @Test
    public void testDeclareSelectWithFunction2() throws Exception {
        assertMemoryLeak(() -> {
            execute(TRADES_DDL);
            drainWalQueue();
            assertSql("""
                            column
                            true
                            """,
                    """
                            DECLARE
                                @today := today(),
                                @start := interval_start(@today),
                                @end := interval_end(@today)
                                SELECT @today = interval(@start, @end)""");
        });
    }

    @Test
    public void testDeclareSelectWithWindowFunction() throws Exception {
        assertMemoryLeak(() -> {
            execute(AAPL_DDL);
            drainWalQueue();
            assertModel("select-window timestamp, bid_px_00, " +
                            "AVG(bid_px_00) avg_5min over (order by timestamp range between '5' minute preceding and current row exclude no others)," +
                            " COUNT() updates_100ms over (order by timestamp range between '100' millisecond preceding and current row exclude no others)," +
                            " SUM(bid_sz_00) volume_2sec over (order by timestamp range between '2' second preceding and current row exclude no others)" +
                            " from (select [timestamp, bid_px_00, bid_sz_00] from AAPL_orderbook timestamp (timestamp) where bid_px_00 > 0) limit 10",
                    """
                            DECLARE
                                @ts := timestamp,
                                @bid_price := bid_px_00,
                                @bid_size := bid_sz_00,
                                @avg_time_range := '5',
                                @updates_period := '100',
                                @volume_2sec := '2'
                            SELECT
                                @ts,
                                @bid_price,
                                AVG(@bid_price) OVER (
                                    ORDER BY @ts
                                    RANGE BETWEEN @avg_time_range MINUTE PRECEDING AND CURRENT ROW
                                ) AS avg_5min,
                                COUNT(*) OVER (
                                    ORDER BY @ts
                                    RANGE BETWEEN @updates_period MILLISECOND PRECEDING AND CURRENT ROW
                                ) AS updates_100ms,
                                SUM(@bid_size) OVER (
                                    ORDER BY @ts
                                    RANGE BETWEEN @volume_2sec SECOND PRECEDING AND CURRENT ROW
                                ) AS volume_2sec
                            FROM AAPL_orderbook
                            WHERE @bid_price > 0
                            LIMIT 10;"""
                    , ExecutionModel.QUERY);
        });
    }

    @Test
    public void testDeclareSelectWithWindowFunctionPartitionBy() throws Exception {
        assertMemoryLeak(() -> {
            execute(AAPL_DDL);
            drainWalQueue();
            // Test declared variable in PARTITION BY clause
            assertModel("select-window timestamp, bid_px_00, " +
                            "ROW_NUMBER() row_num over (partition by bid_px_00 order by timestamp) " +
                            "from (select [timestamp, bid_px_00] from AAPL_orderbook timestamp (timestamp)) limit 5",
                    """
                            DECLARE
                                @partition_col := bid_px_00,
                                @order_col := timestamp
                            SELECT
                                @order_col,
                                @partition_col,
                                ROW_NUMBER() OVER (
                                    PARTITION BY @partition_col
                                    ORDER BY @order_col
                                ) AS row_num
                            FROM AAPL_orderbook
                            LIMIT 5;"""
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
            execute(TRADES_DDL);
            assertException("DECLARE @symbols := ('ETH-USD', 'BTC-USD') " +
                            "SELECT * FROM trades WHERE @symbols IN @symbols",
                    43, "bracket lists");

        });
    }

    @Test
    public void testDeclareWorksWithIntrinsics01() throws Exception {
        assertMemoryLeak(() -> {
            execute(TRADES_DDL);
            drainWalQueue();
            String plan = """
                    Async Group By workers: 1
                      keys: [timestamp]
                      values: [count(*)]
                      filter: null
                        PageFrame
                            Row forward scan
                            Interval forward scan on: trades
                              intervals: [("2024-01-01T00:00:00.000001Z","MAX")]
                    """;
            assertPlanNoLeakCheck("declare @ts := (timestamp > '2024-01-01') select timestamp, count() from trades where @ts;", plan);
            assertPlanNoLeakCheck("declare @lo := '2024-01-01' select timestamp, count() from trades where timestamp > @lo;", plan);
        });
    }

    @Test
    public void testDeclareWorksWithIntrinsics02() throws Exception {
        assertMemoryLeak(() -> {
            execute(TRADES_DDL);
            drainWalQueue();
            String plan = """
                    Async Group By workers: 1
                      keys: [timestamp]
                      values: [count(*)]
                      filter: null
                        PageFrame
                            Row forward scan
                            Interval forward scan on: trades
                              intervals: [("2024-01-01T00:00:00.000001Z","2024-08-22T23:59:59.999999Z")]
                    """;
            assertPlanNoLeakCheck("declare @ts := (timestamp > '2024-01-01' and timestamp < '2024-08-23') select timestamp, count() from trades where @ts;", plan);
            assertPlanNoLeakCheck("declare @lo := '2024-01-01', @hi := '2024-08-23' select timestamp, count() from trades where timestamp > @lo and timestamp < @hi;", plan);
        });
    }

    @Test
    public void testDeclareWorksWithIntrinsics03() throws Exception {
        assertMemoryLeak(() -> {
            execute(TRADES_DDL);
            drainWalQueue();
            String plan = """
                    Async Group By workers: 1
                      keys: [timestamp]
                      values: [count(*)]
                      filter: null
                        PageFrame
                            Row forward scan
                            Interval forward scan on: trades
                              intervals: [("MIN","2024-08-22T23:59:59.999999Z")]
                    """;
            assertPlanNoLeakCheck("declare @ts := (timestamp < '2024-08-23')  select timestamp, count() from trades where @ts;", plan);
            assertPlanNoLeakCheck("declare @hi := '2024-08-23' select timestamp, count() from trades where timestamp < @hi;", plan);
        });
    }

    @Test
    public void testDeclareWorksWithIntrinsics04() throws Exception {
        assertMemoryLeak(() -> {
            execute(TRADES_DDL);
            drainWalQueue();
            String plan = """
                    Async Group By workers: 1
                      keys: [timestamp]
                      values: [count(*)]
                      filter: null
                        PageFrame
                            Row forward scan
                            Interval forward scan on: trades
                              intervals: [("2024-01-01T00:00:00.000001Z","MAX")]
                    """;
            assertPlanNoLeakCheck("declare @ts := (timestamp > '2024-01-01') select timestamp, count() from trades where @ts;", plan);
            assertPlanNoLeakCheck("declare @lo := '2024-01-01' select timestamp, count() from trades where timestamp > @lo;", plan);
        });
    }

    @Test
    public void testDeclareWorksWithIntrinsics05() throws Exception {
        assertMemoryLeak(() -> {
            execute(TRADES_DDL);
            drainWalQueue();
            String plan = """
                    Async Group By workers: 1
                      keys: [timestamp]
                      values: [count(*)]
                      filter: null
                        PageFrame
                            Row forward scan
                            Interval forward scan on: trades
                              intervals: [("2024-01-01T00:00:00.000000Z","2024-08-23T00:00:00.000000Z")]
                    """;
            assertPlanNoLeakCheck("declare @ts := (timestamp >= '2024-01-01' and timestamp <= '2024-08-23') select timestamp, count() from trades where @ts;", plan);
            assertPlanNoLeakCheck("declare @lo := '2024-01-01', @hi := '2024-08-23' select timestamp, count() from trades where timestamp >= @lo and timestamp <= @hi;", plan);
        });
    }

    @Test
    public void testDeclareWorksWithIntrinsics06() throws Exception {
        assertMemoryLeak(() -> {
            execute(TRADES_DDL);
            drainWalQueue();
            String plan = """
                    Async Group By workers: 1
                      keys: [timestamp]
                      values: [count(*)]
                      filter: null
                        PageFrame
                            Row forward scan
                            Interval forward scan on: trades
                              intervals: [("MIN","2024-08-23T00:00:00.000000Z")]
                    """;
            assertPlanNoLeakCheck("declare @ts := (timestamp <= '2024-08-23') select timestamp, count() from trades where @ts;", plan);
            assertPlanNoLeakCheck("declare @hi := '2024-08-23' select timestamp, count() from trades where timestamp <= @hi;", plan);
        });
    }

    @Test
    public void testDeclareWorksWithIntrinsics07() throws Exception {
        assertMemoryLeak(() -> {
            execute(TRADES_DDL);
            drainWalQueue();
            String plan = """
                    Async Group By workers: 1
                      keys: [timestamp]
                      values: [count(*)]
                      filter: null
                        PageFrame
                            Row forward scan
                            Interval forward scan on: trades
                              intervals: [("2024-01-01T00:00:00.000000Z","MAX")]
                    """;
            assertPlanNoLeakCheck("declare @ts := (timestamp >= '2024-01-01') select timestamp, count() from trades where @ts;", plan);
            assertPlanNoLeakCheck("declare @lo := '2024-01-01' select timestamp, count() from trades where timestamp >= @lo;", plan);

        });
    }

    @Test
    public void testDeclareWorksWithIntrinsics08() throws Exception {
        assertMemoryLeak(() -> {
            execute(TRADES_DDL);
            drainWalQueue();
            String plan = """
                    Async Group By workers: 1
                      keys: [timestamp]
                      values: [count(*)]
                      filter: null
                        PageFrame
                            Row forward scan
                            Interval forward scan on: trades
                              intervals: [("2024-01-01T00:00:00.000000Z","2024-08-23T00:00:00.000000Z")]
                    """;
            assertPlanNoLeakCheck("declare @ts := (timestamp between '2024-01-01' and '2024-08-23') select timestamp, count() from trades where @ts;", plan);
            assertPlanNoLeakCheck("declare @lo := '2024-01-01', @hi := '2024-08-23' select timestamp, count() from trades where timestamp between @lo and @hi;", plan);
        });
    }

    @Test
    public void testDeclareWorksWithIntrinsics09() throws Exception {
        assertMemoryLeak(() -> {
            execute(TRADES_DDL);
            drainWalQueue();
            String plan = """
                    Async Group By workers: 1
                      keys: [timestamp]
                      values: [count(*)]
                      filter: null
                        PageFrame
                            Row forward scan
                            Interval forward scan on: trades
                              intervals: [("2024-01-01T00:00:00.000000Z","2024-01-01T00:00:00.000000Z"),("2024-08-23T00:00:00.000000Z","2024-08-23T00:00:00.000000Z")]
                    """;
            assertPlanNoLeakCheck("declare @ts1 := '2024-01-01', @ts2 := '2024-08-23' select timestamp, count() from trades where timestamp IN (@ts1, @ts2);", plan);
            assertException("declare @ts := ('2024-01-01', '2024-08-23') select timestamp, count() from trades where timestamp IN @ts", 44, "bracket lists are not supported");
        });
    }

    @Test
    public void testDeclareWorksWithJit() throws Exception {
        assertMemoryLeak(() -> {
            String plan = """
                    Async{JIT}Filter workers: 1
                      filter: id<4
                        PageFrame
                            Row forward scan
                            Frame forward scan on: x
                    """;

            String replacement = sqlExecutionContext.getJitMode() == SqlJitMode.JIT_MODE_ENABLED ?
                    " JIT " : " ";
            plan = plan.replace("{JIT}", replacement);

            execute(
                    """
                            create table x as (
                              select x id, timestamp_sequence(0,1000000000) as ts
                              from long_sequence(10)
                            ) timestamp(ts) partition by hour;"""
            );
            assertPlanNoLeakCheck("declare @id := id, @val := 4 select * from x where @id < @val", plan);
            assertPlanNoLeakCheck("declare @expr := (id < 4) select * from x where @expr", plan);
            assertSql(
                    """
                            id\tts
                            1\t1970-01-01T00:00:00.000000Z
                            2\t1970-01-01T00:16:40.000000Z
                            3\t1970-01-01T00:33:20.000000Z
                            """,
                    "x where id < 4"
            );
        });
    }
}
