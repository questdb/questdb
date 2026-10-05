/*+*****************************************************************************
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

import io.questdb.PropertyKey;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.view.ViewDefinition;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlExecutionRequirements;
import io.questdb.griffin.SqlParser;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.griffin.model.ExecutionModel;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TableFunctionTestUtils;
import io.questdb.test.tools.TableFunctionTestUtils.CloseCountingRecordCursorFactory;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;

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
    public void testBracketedSubqueryIsNotAList() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE k (s SYMBOL, l LONG)");
            execute("INSERT INTO k VALUES ('a', 1), ('b', 2)");
            drainWalQueue();
            // A subquery puts commas at the same bracket depth a list separator sits at, so the
            // lookahead has to recognise it rather than count commas. Declaring one is not
            // supported either way - what matters is that it still says so, instead of blaming a
            // list the user did not write.
            assertQuery("DECLARE @x := (SELECT max(l), min(l) FROM k) SELECT @x")
                    .fails(15, "query is not expected");
            assertQuery("DECLARE @x := (SELECT l FROM k ORDER BY l, s LIMIT 1) SELECT @x")
                    .fails(15, "query is not expected");
            // ...and a subquery that IS usable with IN keeps working.
            assertQuery("DECLARE @x := (SELECT s FROM k) SELECT s FROM k WHERE s IN @x")
                    .noLeakCheck()
                    .returns("""
                            s
                            a
                            b
                            """);
        });
    }

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
            assertQuery("delcare @ts := timestamp select @ts from trades")
                    .fails(12, "perhaps `DECLARE` was misspelled?");
        });
    }

    @Test
    public void testDeclareGivesMoreUsefulErrorWhenUsingTheWrongBindOperator() throws Exception {
        assertMemoryLeak(() -> {
            execute(TRADES_DDL);
            drainWalQueue();
            assertQuery("declare @ts = timestamp select @ts from trades")
                    .fails(12, "expected variable assignment operator `:=`");
        });
    }

    @Test
    public void testDeclareGluedToComment() throws Exception {
        assertMemoryLeak(() -> {
            // With no space between DECLARE, or a marker, and a comment, the lexer holds the
            // comment's opener as its next token and has already moved past it. The parser used to
            // parse the value again from there, reading the comment's text as code. Each form
            // returns what the form with a space returns.
            final String expected = """
                    1
                    1
                    """;
            assertQuery("DECLARE--@x := 2,\n@x := 1 SELECT @x")
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
            assertQuery("DECLARE --@x := 2,\n@x := 1 SELECT @x")
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
            assertQuery("DECLARE AUDITED--@x := 2,\n@x := 1 SELECT @x")
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
            assertQuery("DECLARE AUDITED --@x := 2,\n@x := 1 SELECT @x")
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
            assertQuery("SELECT * FROM (DECLARE--@x := 2,\n@x := 1 SELECT @x)")
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
            assertQuery("SELECT * FROM (DECLARE --@x := 2,\n@x := 1 SELECT @x)")
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
            assertQuery("WITH c AS (DECLARE--@x := 2,\n@x := 1 SELECT @x) SELECT * FROM c")
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
            assertQuery("WITH c AS (DECLARE --@x := 2,\n@x := 1 SELECT @x) SELECT * FROM c")
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
            // A comment that declares nothing failed with a NullPointerException.
            assertQuery("DECLARE/* c */@x := 1 SELECT @x")
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
            assertQuery("DECLARE--c\n@x := 1 SELECT @x")
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
            assertQuery("DECLARE/**/@x := 1 SELECT @x")
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
            assertQuery("DECLARE/*+ hint */@x := 1 SELECT @x")
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
            assertQuery("DECLARE OVERRIDABLE/* c */ @x := 1 SELECT @x")
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
            // A copy of a declared sub-query parses the DECLARE nested in it again.
            assertQuery("DECLARE @q := (DECLARE/* c */@z := 5 SELECT @z AS z) SELECT * FROM @q UNION ALL SELECT * FROM @q")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            z
                            5
                            5
                            """);
            // The declaration in a comment declares nothing.
            assertQuery("DECLARE--@y := 2,\n@x := 1 SELECT @y")
                    .fails(33, "tried to use undeclared variable `@y`");
        });
    }

    @Test
    public void testDeclareGluedToCommentInViewBody() throws Exception {
        assertMemoryLeak(() -> {
            // A view body parses at CREATE VIEW and again at every read. Both parses skip a comment
            // glued to DECLARE or a marker.
            execute("CREATE VIEW v_glued AS (DECLARE OVERRIDABLE--@x := 2,\n@x := 1 SELECT @x AS v)");
            drainWalAndViewQueues();
            assertQuery("SELECT * FROM v_glued")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            v
                            1
                            """);
            assertQuery("DECLARE @x := 9 SELECT * FROM v_glued")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            v
                            9
                            """);
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
            assertQuery("select * from foo")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x
                            3
                            """);
        });
    }

    @Test
    public void testDeclareMultiDeclares() throws Exception {
        assertQuery("DECLARE @from_time := dateadd('h',-1,now()), DECLARE @to_time := now() WITH t as ( select now() as ts ) select * from t where ts between @from_time and @to_time;")
                .fails(45, "unexpected token [DECLARE] - Multiple DECLARE statements are not allowed. Use single DECLARE block: DECLARE @a := 1, @b := 1, @c := 1");
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
        assertQuery("DECLARE OVERRIDABLE SELECT 1")
                .fails(20, "variable name expected after OVERRIDABLE");
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
        assertQuery("declare " +
                "@ts := '2025-07-02T13:00:00.000000Z', " +
                "@int := interval(@ts, @ts)" +
                "select @int")
                .noLeakCheck()
                .expectSize()
                .returns("interval\n('2025-07-02T13:00:00.000Z', '2025-07-02T13:00:00.000Z')\n");
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
        assertQuery("EXPLAIN DECLARE @x := 5 SELECT @x")
                .noLeakCheck()
                .expectSize()
                .noRandomAccess()
                .returns("""
                        QUERY PLAN
                        VirtualRecord
                          functions: [5]
                            long_sequence count: 1
                        """);
    }

    @Test
    public void testDeclareSelectFromGenerateSeries() throws Exception {
        // Test for issue #6547: DECLARE substitution should work for function arguments in FROM clause
        assertMemoryLeak(() -> assertQuery("DECLARE @lo := '2025-01-01', @hi := '2025-01-02', @unit := '1d' SELECT * FROM generate_series(@lo, @hi, @unit)")
                .noLeakCheck()
                .expectSize()
                .timestamp("generate_series")
                .returns("""
                        generate_series
                        2025-01-01T00:00:00.000000Z
                        2025-01-02T00:00:00.000000Z
                        """));
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
        assertQuery(query)
                .noLeakCheck()
                .expectSize()
                .returns("""
                        col2
                        4
                        """);
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
            assertQuery(query)
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("timestamp")
                    .returns("""
                            symbol\tside\tprice\tamount\ttimestamp
                            \t\t2615.81\t0.001\t2022-03-08T18:04:01.991343Z
                            \t\t2615.81\t0.001\t2022-03-08T18:04:01.991343Z
                            \t\t2615.81\t0.001\t2022-03-08T18:04:01.991343Z
                            """);
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
        assertQuery("DECLARE @x := ?, @y := ? SELECT @x, @y")
                .fails(14, "Invalid column: ?");
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
            assertModel("select-group-by timestamp_floor_utc('1h', timestamp, null, '00:00', null) timestamp, symbol, avg(price) avg from (select [timestamp, symbol, price] from trades timestamp (timestamp) stride 1h) order by timestamp",
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
            assertModel("select-group-by timestamp_floor_utc('1h', timestamp, '2008-12-28', '00:00', null) timestamp, symbol, avg(price) avg from (select [timestamp, symbol, price] from trades timestamp (timestamp) where timestamp >= '2008-12-28' and timestamp < '2009-01-05' fill(null) from '2008-12-28' to '2009-01-05' stride 1h) order by timestamp",
                    "DECLARE @unit := 1h, @from := '2008-12-28', @to := '2009-01-05', @fill := null SELECT timestamp, symbol, avg(price) FROM trades SAMPLE BY @unit FROM @from TO @to FILL(@fill)", ExecutionModel.QUERY);
        });
    }

    @Test
    public void testDeclareSelectSampleByWithOffset() throws Exception {
        assertMemoryLeak(() -> {
            execute(TRADES_DDL);
            drainWalQueue();
            assertModel("select-group-by timestamp_floor_utc('1h', timestamp, null, '10:00', null) timestamp, symbol, avg(price) avg from (select [timestamp, symbol, price] from trades timestamp (timestamp) offset '10:00' stride 1h) order by timestamp",
                    "DECLARE @offset := '10:00' SELECT timestamp, symbol, avg(price) FROM trades SAMPLE BY 1h ALIGN TO CALENDAR WITH OFFSET @offset", ExecutionModel.QUERY);
        });
    }

    @Test
    public void testDeclareSelectSampleByWithTimezone() throws Exception {
        assertMemoryLeak(() -> {
            execute(TRADES_DDL);
            drainWalQueue();
            assertModel("select-group-by timestamp_floor_utc('1h', timestamp, null, '00:00', 'Antarctica/McMurdo') timestamp, symbol, avg(price) avg from (select [timestamp, symbol, price] from trades timestamp (timestamp) stride 1h) order by timestamp",
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
            assertQuery("""
                    DECLARE
                        @today := today(),
                        @start := interval_start(@today),
                        @end := interval_end(@today)
                        SELECT @today = interval(@start, @end)""")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            column
                            true
                            """);
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
        assertQuery("DECLARE @x = 5 SELECT @x;")
                .fails(11, "expected variable assignment operator");
    }

    @Test
    public void testDeclareVariableAfterMissingComma() throws Exception {
        assertMemoryLeak(() -> {
            // The value of @a ends before @x, and the parse of that value hands @x back to the
            // lexer. The parser then parses the value of @x again from past the variable, finds no
            // left operand for `:=` and fails the declaration.
            assertQuery("DECLARE @a := 1 @x := 2 SELECT @x")
                    .fails(19, "too few arguments for ':='");
            // A comment glued to the variable puts the start of that parse inside the comment, where
            // the parse finds no left operand at all. The check of the left operand failed on that
            // with a NullPointerException.
            assertQuery("DECLARE @a := 1 @x/*c*/:= 2 SELECT @x")
                    .fails(21, "unexpected token [@x] - unexpected bind expression");
        });
    }

    @Test
    public void testDeclareVariableAsComparisonUnderNot() throws Exception {
        assertMemoryLeak(() -> {
            // The optimiser folds NOT into a comparison by rewriting the comparison node in place.
            // Every reference to a variable used to be the declared node itself, so `NOT @f` turned
            // `@f` into its own negation for the other reference as well.
            assertQuery("DECLARE @f := (1 = 1) SELECT count() FROM long_sequence(3) WHERE NOT @f OR @f")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count
                            3
                            """);
            assertQuery("DECLARE @f := (x < 2) SELECT x, NOT @f AS n FROM long_sequence(3) WHERE @f OR NOT @f")
                    .noLeakCheck()
                    .returns("""
                            x	n
                            1	false
                            2	true
                            3	true
                            """);
            // A view body substitutes its variables the same way, the caller's value included.
            execute("CREATE VIEW v_not AS (DECLARE OVERRIDABLE @f := (x < 2) SELECT x FROM long_sequence(3) WHERE NOT @f AND x > 2 OR @f)");
            drainWalAndViewQueues();
            assertQuery("SELECT * FROM v_not")
                    .noLeakCheck()
                    .returns("""
                            x
                            1
                            3
                            """);
            assertQuery("DECLARE @f := (x > 2) SELECT * FROM v_not")
                    .noLeakCheck()
                    .returns("""
                            x
                            3
                            """);
        });
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
    public void testDeclareVariableAsSubQueryKeepsOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            // FROM @x used to read the model the declaration parsed while the optimiser also rewrote
            // that model as a standalone query, which left it without its ORDER BY but kept its LIMIT:
            // the rows came back unsorted, and a top-N read returned the wrong rows.
            assertQuery("DECLARE @x := (SELECT x FROM long_sequence(3) ORDER BY x DESC) SELECT * FROM @x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x
                            3
                            2
                            1
                            """);
            assertQuery("DECLARE @x := (DECLARE @n := 2 SELECT x FROM long_sequence(3) ORDER BY x % @n, x LIMIT 1) SELECT * FROM @x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x
                            2
                            """);
            execute("CREATE VIEW v_top AS (DECLARE OVERRIDABLE @x := (SELECT x FROM long_sequence(3) ORDER BY x DESC LIMIT 1) SELECT * FROM @x)");
            drainWalAndViewQueues();
            assertQuery("SELECT * FROM v_top")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x
                            3
                            """);
            assertQuery("DECLARE @x := (SELECT x FROM long_sequence(3) ORDER BY x DESC LIMIT 2) SELECT * FROM v_top")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x
                            3
                            2
                            """);
        });
    }

    @Test
    public void testDeclareVariableAsSubQueryOverTableFunctionClosesEachFactoryOnce() throws Exception {
        assertMemoryLeak(() -> {
            // owned_cursor() counts the closes of every cursor factory it hands out, which the
            // memory check cannot do: a factory ignores a second close. The function returns no
            // rows, so @q is 0, no x equals it, and all rows share one partition.
            final ObjList<CloseCountingRecordCursorFactory> factories = new ObjList<>();
            final String functionName = "owned_cursor";
            TableFunctionTestUtils.register(engine, functionName, SqlExecutionRequirements.NONE, factories);
            try {
                final String declare = """
                        DECLARE
                            @q := (SELECT count() FROM owned_cursor()),
                            @w := row_number() OVER (PARTITION BY x = @q)
                        """;
                // The optimiser drops the second window as a duplicate. Code generation takes the
                // factory of the first read's model, which the compiled query closes, and never
                // generates the second read's model.
                assertQuery(declare + "SELECT x, @w a, @w b FROM long_sequence(2)")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                x\ta\tb
                                1\t1\t1
                                2\t2\t2
                                """);
                assertEachClosedOnce(factories, 2);

                // the first read sits in a CTE nothing references
                factories.clear();
                assertQuery(declare + """
                        WITH c AS (SELECT @w r FROM long_sequence(1))
                        SELECT x, @w r FROM long_sequence(2)
                        """)
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                x\tr
                                1\t1
                                2\t2
                                """);
                assertEachClosedOnce(factories, 2);

                // nothing reads the variable
                factories.clear();
                assertQuery("DECLARE @q := (SELECT count() FROM owned_cursor()) SELECT 1")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                1
                                1
                                """);
                assertEachClosedOnce(factories, 1);

                // The next compile borrows the same pooled compiler and clears its optimiser state. A
                // reference left behind there must not close a factory a second time.
                execute("CREATE TABLE other (x LONG)");
                assertEachClosedOnce(factories, 1);
            } finally {
                TableFunctionTestUtils.unregister(engine, functionName);
            }
        });
    }

    @Test
    public void testDeclareVariableAsSubQueryOverTableFunctionFailingCompile() throws Exception {
        assertMemoryLeak(() -> {
            createParquetFile();
            execute("CREATE TABLE base AS (SELECT (x * 1_000_000)::TIMESTAMP ts, x FROM long_sequence(4)) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE VIEW v_plain AS (SELECT x FROM long_sequence(1))");
            drainWalAndViewQueues();
            // The optimiser opens the table function of every declared sub-query, read or not, and
            // a compile that fails has to close the ones code generation never took, wherever it
            // fails. @unread is never read, so no walk from the statement's model reaches it.
            final String declare = """
                    DECLARE
                        @q := (SELECT x FROM read_parquet('q.parquet') LIMIT 1),
                        @unread := (SELECT x FROM read_parquet('q.parquet'))
                    """;
            for (int i = 0; i < 3; i++) {
                // the optimiser fails, after it opened both table functions
                assertQuery(declare + "SELECT nonexistent, x = @q b FROM long_sequence(4)")
                        .noLeakCheck()
                        .fails(133, "Invalid column: nonexistent");
                // code generation fails, before and after it took the table function @q reads
                assertQuery(declare + "SELECT sin(x, x) a, x = @q b FROM long_sequence(4)")
                        .noLeakCheck()
                        .fails(133, "wrong number of arguments for function `sin`");
                assertQuery(declare + "SELECT x = @q b, sin(x, x) a FROM long_sequence(4)")
                        .noLeakCheck()
                        .fails(143, "wrong number of arguments for function `sin`");
                // Code generation rejects a window's ORDER BY before it generates the sub-query
                // the ORDER BY reads. With two such windows, each read has a model of its own.
                assertQuery(declare + "SELECT x, row_number() OVER (ORDER BY x = @q, x) a FROM long_sequence(4)")
                        .noLeakCheck()
                        .fails(166, "Invalid column: =");
                assertQuery(declare + "SELECT x, row_number() OVER (ORDER BY x = @q, x) a, rank() OVER (ORDER BY x = @q, x) b FROM long_sequence(4)")
                        .noLeakCheck()
                        .fails(166, "Invalid column: =");
                // CREATE VIEW and CREATE MATERIALIZED VIEW each compile the body on a path of
                // their own.
                assertExceptionNoLeakCheck(
                        "CREATE VIEW v_bad AS (" + declare + "SELECT sin(x, x) a, x = @q b FROM long_sequence(4))",
                        155,
                        "wrong number of arguments for function `sin`"
                );
                assertExceptionNoLeakCheck(
                        "CREATE MATERIALIZED VIEW mv_bad AS (" + declare + "SELECT ts, max(sin(x, x)) m FROM base SAMPLE BY 1d) PARTITION BY DAY",
                        177,
                        "wrong number of arguments for function `sin`"
                );
                // The statement fails between the two: the optimiser has returned and code
                // generation has not started.
                assertExceptionNoLeakCheck(
                        "INSERT INTO v_plain SELECT * FROM (" + declare + "SELECT x FROM long_sequence(4) WHERE x = @q)",
                        12,
                        "cannot modify view [view=v_plain]"
                );
            }
            // a compile that succeeds after the failed ones reads the file
            assertQuery(declare + "SELECT x, x = @q b FROM long_sequence(4)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x\tb
                            1\tfalse
                            2\tfalse
                            3\ttrue
                            4\tfalse
                            """);
        });
    }

    @Test
    public void testDeclareVariableAsSubQueryOverTableFunctionInOtherStatements() throws Exception {
        assertMemoryLeak(() -> {
            createParquetFile();
            // @q is 3. The second read of @w is a duplicate window column, which the optimiser
            // drops, so nothing generates the copy of @q that read parsed.
            final String declare = """
                    DECLARE
                        @q := (SELECT x FROM read_parquet('q.parquet') LIMIT 1),
                        @w := row_number() OVER (PARTITION BY x = @q)
                    """;
            final String select = declare + "SELECT x, @w a, @w b FROM long_sequence(4)";
            final String expected = """
                    x\ta\tb
                    1\t1\t1
                    2\t2\t2
                    3\t1\t1
                    4\t3\t3
                    """;
            execute("CREATE TABLE dst (x LONG, a LONG, b LONG)");
            execute("CREATE TABLE upd AS (SELECT x, 0L v FROM long_sequence(4))");
            execute("CREATE TABLE base AS (SELECT (x * 1_000_000)::TIMESTAMP ts, x FROM long_sequence(4)) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE VIEW v_q AS (" + select + ")");
            drainWalAndViewQueues();
            for (int i = 0; i < 3; i++) {
                // EXPLAIN generates the plan it prints
                assertQuery("DECLARE @q := (SELECT x FROM read_parquet('q.parquet')) SELECT 1")
                        .noLeakCheck()
                        .assertsPlan("""
                                VirtualRecord
                                  functions: [1]
                                    long_sequence count: 1
                                """);
                // a view body, parsed again on every read
                assertQuery("SELECT * FROM v_q")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns(expected);
                execute("CREATE TABLE ctas" + i + " AS (" + select + ")");
                assertQuery("SELECT * FROM ctas" + i)
                        .noLeakCheck()
                        .expectSize()
                        .returns(expected);
                execute("INSERT INTO dst SELECT * FROM (" + select + ")");
                // the sub-query in FROM declares a sub-query of its own, which nothing reads
                execute("""
                        UPDATE upd SET v = v + s.x
                        FROM (DECLARE @q := (SELECT x FROM read_parquet('q.parquet')) SELECT x FROM long_sequence(4)) s
                        WHERE upd.x = s.x
                        """);
                execute("CREATE MATERIALIZED VIEW mv" + i + " AS ("
                        + "DECLARE @q := (SELECT x FROM read_parquet('q.parquet')) SELECT ts, count() c FROM base SAMPLE BY 1d"
                        + ") PARTITION BY DAY");
            }
            drainWalAndMatViewQueues();
            assertQuery("SELECT x, a, b, count() c FROM dst ORDER BY x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x\ta\tb\tc
                            1\t1\t1\t3
                            2\t2\t2\t3
                            3\t1\t1\t3
                            4\t3\t3\t3
                            """);
            assertQuery("SELECT * FROM upd")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x\tv
                            1\t3
                            2\t6
                            3\t9
                            4\t12
                            """);
            assertQuery("SELECT * FROM mv2")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tc
                            1970-01-01T00:00:00.000000Z\t4
                            """);
        });
    }

    @Test
    public void testDeclareVariableAsSubQueryOverTableFunctionNeverRead() throws Exception {
        assertMemoryLeak(() -> {
            createParquetFile();
            // The optimiser opens the table function of every declared sub-query, and code
            // generation takes over only the ones something reads.
            for (int i = 0; i < 3; i++) {
                assertQuery("DECLARE @q := (SELECT x FROM read_parquet('q.parquet')) SELECT 1")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                1
                                1
                                """);
                // beside a declared sub-query that is read, whose table function has to stay open
                assertQuery("""
                        DECLARE
                            @q := (SELECT x FROM read_parquet('q.parquet') LIMIT 1),
                            @unread := (SELECT x FROM read_parquet('q.parquet'))
                        SELECT x, x = @q b FROM long_sequence(4)
                        """)
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                x\tb
                                1\tfalse
                                2\tfalse
                                3\ttrue
                                4\tfalse
                                """);
                // declared in a sub-query, and read from the file itself
                assertQuery("SELECT * FROM (DECLARE @q := (SELECT x FROM read_parquet('q.parquet')) SELECT x FROM read_parquet('q.parquet'))")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                x
                                3
                                2
                                1
                                """);
            }
        });
    }

    @Test
    public void testDeclareVariableAsSubQueryOverTableFunctionReadInDuplicateWindows() throws Exception {
        assertMemoryLeak(() -> {
            createParquetFile();
            // Each window read of @q has a model of its own, and the optimiser opens the table
            // function of each. It then drops a window column identical to an earlier one, and
            // with it the only reader of that column's model, so code generation never takes
            // that table function. @q is 3, so x = 3 sits alone in its partition.
            final String declare = """
                    DECLARE
                        @q := (SELECT x FROM read_parquet('q.parquet') LIMIT 1),
                        @w := row_number() OVER (PARTITION BY x = @q)
                    """;
            final String expected = """
                    x\ta\tb
                    1\t1\t1
                    2\t2\t2
                    3\t1\t1
                    4\t3\t3
                    """;
            for (int i = 0; i < 3; i++) {
                // both windows written out
                assertQuery(declare + """
                        SELECT
                            x,
                            row_number() OVER (PARTITION BY x = @q) a,
                            row_number() OVER (PARTITION BY x = @q) b
                        FROM long_sequence(4)
                        """)
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns(expected);
                // both read from a variable
                assertQuery(declare + "SELECT x, @w a, @w b FROM long_sequence(4)")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns(expected);
                // three reads, two of them dropped
                assertQuery(declare + "SELECT x, @w a, @w b, @w c FROM long_sequence(4)")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                x\ta\tb\tc
                                1\t1\t1\t1
                                2\t2\t2\t2
                                3\t1\t1\t1
                                4\t3\t3\t3
                                """);
                // a plain read takes the declaration's model ahead of the window reads
                assertQuery(declare + "SELECT x, x = @q y, @w a, @w b FROM long_sequence(4)")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                x\ty\ta\tb
                                1\tfalse\t1\t1
                                2\tfalse\t2\t2
                                3\ttrue\t1\t1
                                4\tfalse\t3\t3
                                """);
                // Two windows that differ both stay, and code generation takes both table functions.
                assertQuery(declare + """
                        SELECT
                            x,
                            row_number() OVER (PARTITION BY x = @q) a,
                            rank() OVER (PARTITION BY x = @q ORDER BY x) b
                        FROM long_sequence(4)
                        """)
                        .noLeakCheck()
                        .expectSize()
                        .returns(expected);
            }
        });
    }

    @Test
    public void testDeclareVariableAsSubQueryOverTableFunctionReadInUnusedCte() throws Exception {
        assertMemoryLeak(() -> {
            createParquetFile();
            // The first read in parse order takes the model the declaration parsed. When that
            // read sits in a CTE nothing references, the optimiser still opens the model's table
            // function, as it does for every declared sub-query, and code generation never
            // reaches the CTE. @q is 3.
            final String declare = """
                    DECLARE
                        @q := (SELECT x FROM read_parquet('q.parquet') LIMIT 1),
                        @w := row_number() OVER (PARTITION BY x = @q)
                    """;
            final String expected = """
                    x\tr
                    1\t1
                    2\t2
                    3\t1
                    4\t3
                    """;
            for (int i = 0; i < 3; i++) {
                // the window read from a variable
                assertQuery(declare + """
                        WITH c AS (SELECT @w r FROM long_sequence(1))
                        SELECT x, @w r FROM long_sequence(4)
                        """)
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns(expected);
                // both windows written out
                assertQuery(declare + """
                        WITH c AS (SELECT row_number() OVER (PARTITION BY x = @q) r FROM long_sequence(1))
                        SELECT x, row_number() OVER (PARTITION BY x = @q) r FROM long_sequence(4)
                        """)
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns(expected);
                // no window at all
                assertQuery(declare + """
                        WITH c AS (SELECT x = @q a FROM long_sequence(1))
                        SELECT x, x = @q b FROM long_sequence(4)
                        """)
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                x\tb
                                1\tfalse
                                2\tfalse
                                3\ttrue
                                4\tfalse
                                """);
                // The outer read comes first and takes the declaration's model. The unused CTE,
                // inside FROM, parses a copy the optimiser never visits.
                assertQuery(declare + """
                        SELECT x, @w AS r
                        FROM (WITH c AS (SELECT @w r2 FROM long_sequence(1)) SELECT x FROM long_sequence(4))
                        """)
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns(expected);
            }
        });
    }

    @Test
    public void testDeclareVariableAsSubQueryOverViewSharesLexers() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE k (l LONG)");
            execute("INSERT INTO k VALUES (1), (2), (3)");
            execute("CREATE VIEW v_k AS (SELECT l FROM k)");
            execute("CREATE VIEW v_wrap AS (SELECT l FROM v_k WHERE l > 1)");
            execute("CREATE VIEW v_aud AS (DECLARE OVERRIDABLE AUDITED @lo := 0 SELECT l FROM k WHERE l > @lo)");
            execute("""
                    CREATE VIEW v_tail AS (
                        DECLARE @own := (SELECT l FROM k), OVERRIDABLE @lim := 3
                        SELECT l FROM (SELECT * FROM @own UNION ALL SELECT * FROM @own) WHERE l <= @lim AND l <= @lim ORDER BY l DESC LIMIT 3
                    )
                    """);
            drainWalAndViewQueues();
            // OSS has no syntax that makes a view audited, so the test sets the flag on the
            // definition the view graph holds.
            final ViewDefinition audited = engine.getViewGraph().getViewDefinition(engine.verifyTableName("v_aud"));
            audited.init(audited.getViewToken(), audited.getViewSql(), audited.getSeqTxn(), true);

            // A read of @q5 comes to 32 reads of @q0, and each of them expands the view that the
            // text of @q0 reads. Every expansion used to take a lexer from a pool that only grows
            // and that the compiler keeps for as long as it lives, so the statement held 37
            // lexers: 32 for the view's body and 5 for the copies nested in one another. A lexer
            // that has parsed the body parses it again for the next expansion, so the body takes
            // one lexer.
            final String overView = declaredQueryChainOver("v_k");
            assertQuery(overView)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count\tsum
                            96\t192
                            """);
            Assert.assertEquals(6, countViewLexersHeld(overView));

            // A view that reads a view: the parser reads both bodies at the same time, so each
            // takes a lexer. The statement held 69.
            final String overViewOverView = declaredQueryChainOver("v_wrap");
            assertQuery(overViewOverView)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count\tsum
                            64\t160
                            """);
            Assert.assertEquals(7, countViewLexersHeld(overViewOverView));

            // An audited view expands the same way, and each of the 32 reads still records its own
            // audit.
            final String overAuditedView = declaredQueryChainOver("v_aud");
            assertQuery(overAuditedView)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count\tsum
                            96\t192
                            """);
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                Assert.assertEquals(32, compiler.generateExecutionModel(overAuditedView, sqlExecutionContext).getQueryModel().getViewAudits().size());
                Assert.assertEquals(6, countViewLexersHeld(compiler));
            }

            // 33 reads of a sub-query whose text reads a view take one lexer for the copies and
            // one for the view's body, where they held 34. The reads need no DECLARE to share a
            // lexer: 33 reads of a view that reads a view take the two lexers one read takes,
            // where they held 66.
            final StringBuilder subQueryReads = new StringBuilder("DECLARE @x := (SELECT l FROM v_k WHERE l > 1) SELECT count(), sum(l) FROM (SELECT * FROM @x");
            final StringBuilder viewReads = new StringBuilder("SELECT count(), sum(l) FROM (SELECT * FROM v_wrap");
            for (int i = 1; i < 33; i++) {
                subQueryReads.append(" UNION ALL SELECT * FROM @x");
                viewReads.append(" UNION ALL SELECT * FROM v_wrap");
            }
            subQueryReads.append(')');
            viewReads.append(')');
            final String flatReadsExpected = """
                    count\tsum
                    66\t165
                    """;
            assertQuery(subQueryReads)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns(flatReadsExpected);
            Assert.assertEquals(2, countViewLexersHeld(subQueryReads.toString()));
            assertQuery(viewReads)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns(flatReadsExpected);
            Assert.assertEquals(2, countViewLexersHeld(viewReads.toString()));

            // A view expanded inside its own body. v_tail reads @lim twice, so the second read
            // parses a copy of the caller's sub-query while the parser is in the middle of
            // v_tail's body, and the copy expands v_tail again. The lexer of the outer expansion
            // is still reading, so the inner one cannot take it, and the outer one goes on to its
            // ORDER BY and LIMIT from where it stopped: the rows come back unordered if it loses
            // its place. The inner expansion takes the lexer that parsed the copy of @own for the
            // outer one, and that copy's model keeps its tokens: the statement fails if the lexer
            // hands them out again. The inner v_tail keeps its default 3, so the caller's @lim is 2.
            final String sameViewInsideItself = "DECLARE @lim := (SELECT max(l) - 1 FROM v_tail) SELECT * FROM v_tail";
            assertQuery(sameViewInsideItself)
                    .noLeakCheck()
                    .returns("""
                            l
                            2
                            2
                            1
                            """);
            // Three for the body's text, because the outer expansion, the inner one and the copy
            // of @own inside the inner one are all being parsed at once, and one for the copy of
            // the caller's sub-query.
            Assert.assertEquals(4, countViewLexersHeld(sameViewInsideItself));
        });
    }

    @Test
    public void testDeclareVariableAsSubQueryReadInWindowClause() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE k (s SYMBOL, l LONG)");
            execute("INSERT INTO k VALUES ('a', 1), ('b', 2), ('c', 3)");
            drainWalQueue();
            // A read in a window clause takes the declaration's model or parses a copy, like any
            // other read. Window reads used to go uncounted and share the declaration's model, so
            // a FROM read took that model from under them and code generation met the window's
            // sub-query without one.
            //
            // @q is the two rows with the highest l, c and b. FROM reads them, both are IN @q, so
            // they share a partition. A read that loses the ORDER BY yields a and b instead: read
            // by FROM those are the rows returned, and read by the window they leave c in a
            // partition of its own.
            assertQuery("DECLARE @q := (SELECT s FROM k ORDER BY l DESC LIMIT 2) SELECT s, row_number() OVER (PARTITION BY s IN @q) FROM @q")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            s\trow_number
                            c\t1
                            b\t2
                            """);
            assertQuery("""
                    DECLARE @q := (SELECT 1L x)
                    SELECT x, row_number() OVER (PARTITION BY x = @q)
                    FROM (SELECT * FROM @q UNION ALL SELECT x FROM long_sequence(3))
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            x\trow_number
                            1\t1
                            1\t2
                            2\t1
                            3\t2
                            """);
            // The window read takes the declaration's model and FROM parses a copy, which keeps
            // its ORDER BY and LIMIT.
            assertQuery("""
                    DECLARE @q := (SELECT x FROM long_sequence(3) ORDER BY x DESC LIMIT 1)
                    SELECT x, row_number() OVER (PARTITION BY x = @q)
                    FROM (SELECT * FROM @q UNION ALL SELECT x FROM long_sequence(3))
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            x\trow_number
                            3\t1
                            1\t1
                            2\t2
                            3\t2
                            """);
            // Each case below reads four rows, and @q is 3, so the two rows with x = 3 share a
            // partition. A read that runs a model the optimiser never saw loses the ORDER BY and
            // yields 1 instead, and two reads that share one model do not both get its one row.
            // Either changes the output, whereas row_number() over a single row is 1 whatever
            // the reads did.
            //
            // A plain read ahead of the window read takes the model, so the window read parses a
            // copy of its own.
            assertQuery("""
                    DECLARE @q := (SELECT x FROM long_sequence(3) ORDER BY x DESC LIMIT 1)
                    SELECT x, x = @q AS y, row_number() OVER (PARTITION BY x = @q)
                    FROM (SELECT * FROM @q UNION ALL SELECT x FROM long_sequence(3))
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            x\ty\trow_number
                            3\ttrue\t1
                            1\tfalse\t1
                            2\tfalse\t2
                            3\ttrue\t2
                            """);
            // A FROM read in a CTE takes the model ahead of the window read, so the window read
            // parses the copy and registers it for the optimiser.
            assertQuery("""
                    DECLARE @q := (SELECT x FROM long_sequence(3) ORDER BY x DESC LIMIT 1)
                    WITH c AS (SELECT * FROM @q UNION ALL SELECT x FROM long_sequence(3))
                    SELECT x, row_number() OVER (PARTITION BY x = @q) FROM c
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            x\trow_number
                            3\t1
                            1\t1
                            2\t2
                            3\t2
                            """);
            // Every branch of a union reads the sub-query twice, once in the window, once in FROM.
            assertQuery("""
                    DECLARE @q := (SELECT x FROM long_sequence(3) ORDER BY x DESC LIMIT 1)
                    SELECT x, row_number() OVER (PARTITION BY x = @q)
                    FROM (SELECT * FROM @q UNION ALL SELECT x FROM long_sequence(3))
                    UNION ALL
                    SELECT x, row_number() OVER (PARTITION BY x = @q)
                    FROM (SELECT * FROM @q UNION ALL SELECT x FROM long_sequence(3))
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            x\trow_number
                            3\t1
                            1\t1
                            2\t2
                            3\t2
                            3\t1
                            1\t1
                            2\t2
                            3\t2
                            """);
            // A declared value holding a window function reads the sub-query in its window.
            assertQuery("""
                    DECLARE
                        @q := (SELECT x FROM long_sequence(3) ORDER BY x DESC LIMIT 1),
                        @w := row_number() OVER (PARTITION BY x = @q)
                    SELECT x, @w AS r
                    FROM (SELECT * FROM @q UNION ALL SELECT x FROM long_sequence(3))
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            x\tr
                            3\t1
                            1\t1
                            2\t2
                            3\t2
                            """);
            // A frame bound is a read too. It has to be a constant, which the sub-query is not.
            assertQuery("DECLARE @q := (SELECT 1L x) SELECT x, sum(x) OVER (ORDER BY x ROWS BETWEEN @q PRECEDING AND CURRENT ROW) FROM @q")
                    .noLeakCheck()
                    .fails(15, "constant expression expected");
            assertQuery("DECLARE @q := (SELECT 1L x) SELECT x, sum(x) OVER (ORDER BY x ROWS BETWEEN CURRENT ROW AND @q FOLLOWING) FROM @q")
                    .noLeakCheck()
                    .fails(15, "constant expression expected");
            // The bound is the sub-query itself here, so when FROM reads first, in a CTE, the copy
            // parsed for the bound has to take the bound's place in the window. Left in place, the
            // declaration's node has no model once FROM took it, and code generation tripped over
            // it instead of reporting the bound.
            assertQuery("DECLARE @q := (SELECT 1L x) WITH c AS (SELECT * FROM @q) SELECT x, sum(x) OVER (ORDER BY x ROWS BETWEEN @q PRECEDING AND CURRENT ROW) FROM c")
                    .noLeakCheck()
                    .fails(15, "constant expression expected");
            assertQuery("DECLARE @q := (SELECT 1L x) WITH c AS (SELECT * FROM @q) SELECT x, sum(x) OVER (ORDER BY x ROWS BETWEEN 2 PRECEDING AND @q PRECEDING) FROM c")
                    .noLeakCheck()
                    .fails(15, "constant expression expected");
            // A PARTITION BY key that is the sub-query itself needs the same write: FROM reads
            // first, in a CTE, so the copy parsed for the key has to take the key's place in the
            // window. Code generation compiles the keys in order, so it compiles the copy and then
            // reports the unknown function in the next key. Left in place, the declaration's node
            // has no model once FROM took it, and code generation trips over it before it reaches
            // that key. The second key is there to end the compilation with an error: nothing
            // refuses a declared sub-query key on its own yet, and the partition key sink throws
            // on its CURSOR type.
            assertQuery("DECLARE @q := (SELECT 1L x) WITH c AS (SELECT * FROM @q) SELECT x, row_number() OVER (PARTITION BY @q, nosuchfn(x)) FROM c")
                    .noLeakCheck()
                    .fails(103, "unknown function name: nosuchfn(LONG)");
        });
    }

    @Test
    public void testDeclareVariableAsSubQueryReadInWindowClauseInsideView() throws Exception {
        assertMemoryLeak(() -> {
            // Every read of a view parses its stored body again, so a body that reads a declared
            // sub-query in a window clause and in FROM has to parse at CREATE and at every read.
            // The body reads four rows and partitions them by the sub-query's value, 3 by default,
            // so a read that yields anything else moves rows between the partitions. The two rows
            // that equal the value share a partition and tie on x, so o, which tells the two
            // branches of the union apart, orders them: no row number depends on how the window's
            // sort breaks a tie.
            execute("""
                    CREATE VIEW v_win AS (
                        DECLARE OVERRIDABLE @m := (SELECT x FROM long_sequence(3) ORDER BY x DESC LIMIT 1)
                        SELECT x, row_number() OVER (PARTITION BY x = @m ORDER BY x, o) r
                        FROM (SELECT x, 0 o FROM @m UNION ALL SELECT x, 1 o FROM long_sequence(3))
                    )
                    """);
            drainWalAndViewQueues();
            assertQuery("SELECT * FROM v_win")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x\tr
                            3\t1
                            1\t1
                            2\t2
                            3\t2
                            """);
            assertQuery("SELECT count() FROM v_win")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count
                            4
                            """);
            // A caller's sub-query for the overridable variable, read in the window and in FROM.
            // It is 2, and 1 if a read loses its ORDER BY.
            assertQuery("DECLARE @m := (SELECT x FROM long_sequence(2) ORDER BY x DESC LIMIT 1) SELECT * FROM v_win")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x\tr
                            2\t1
                            1\t1
                            2\t2
                            3\t2
                            """);
        });
    }

    @Test
    public void testDeclareVariableAsSubQueryReadInWindowClauseWhereNoQueryIsAllowed() throws Exception {
        assertMemoryLeak(() -> {
            // An expression without a model has nowhere to register a copy of a declared
            // sub-query, so a read there that would need one is refused, as a sub-query written in
            // place is. FROM takes the declaration's model here, which leaves the window in the
            // SAMPLE BY time zone needing a copy. A read counts in every clause of the window:
            // PARTITION BY, ORDER BY and either frame bound.
            final String error = "query is not allowed here";
            assertQuery("""
                    DECLARE @q := (SELECT 1L x), @w := row_number() OVER (PARTITION BY x = @q)
                    SELECT x FROM @q SAMPLE BY 1h ALIGN TO CALENDAR TIME ZONE @w
                    """)
                    .noLeakCheck()
                    .fails(15, error);
            // Code generation looks a window's ORDER BY entry up as a column, by the entry's token,
            // and compiles no expression for it. A statement that reads a sub-query there compiles
            // only when a column carries that name or nothing selects the window's column, and
            // nothing runs the sub-query either way, so its rows do not show whether the read
            // counted. This refusal is where a read there shows.
            assertQuery("""
                    DECLARE @q := (SELECT 1L x), @w := row_number() OVER (ORDER BY x = @q)
                    SELECT x FROM @q SAMPLE BY 1h ALIGN TO CALENDAR TIME ZONE @w
                    """)
                    .noLeakCheck()
                    .fails(15, error);
            assertQuery("""
                    DECLARE @q := (SELECT 1L x), @w := sum(x) OVER (ORDER BY x ROWS BETWEEN @q PRECEDING AND CURRENT ROW)
                    SELECT x FROM @q SAMPLE BY 1h ALIGN TO CALENDAR TIME ZONE @w
                    """)
                    .noLeakCheck()
                    .fails(15, error);
            assertQuery("""
                    DECLARE @q := (SELECT 1L x), @w := sum(x) OVER (ORDER BY x ROWS BETWEEN 2 PRECEDING AND @q PRECEDING)
                    SELECT x FROM @q SAMPLE BY 1h ALIGN TO CALENDAR TIME ZONE @w
                    """)
                    .noLeakCheck()
                    .fails(15, error);
            // the window written in place of the variable
            assertQuery("""
                    DECLARE @q := (SELECT 1L x)
                    SELECT x FROM @q SAMPLE BY 1h ALIGN TO CALENDAR TIME ZONE row_number() OVER (PARTITION BY x = @q)
                    """)
                    .noLeakCheck()
                    .fails(15, error);
        });
    }

    @Test
    public void testDeclareVariableAsSubQueryReadManyTimesFailingInLaterCopy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE k (l LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO k VALUES (1, '2024-01-01T00:00:00Z'), (2, '2024-01-01T01:00:00Z'), (3, '2024-01-02T00:00:00Z')");
            execute("CREATE VIEW v_lim AS (DECLARE OVERRIDABLE @lim := 0 SELECT l FROM k WHERE l >= @lim AND l <= @lim)");
            drainWalAndViewQueues();
            final String error = "query is not allowed here";
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                // The second and the third read of @one parse a copy each, the third with the lexer
                // the second used. The second read of @lim in v_lim parses a copy with that lexer
                // too, and this copy fails: it reads @tz a second time, as a SAMPLE BY time zone,
                // which has no model to hold a copy of @tz. A lexer that has parsed other copies
                // reports the failure where a new one does, at the sub-query of @tz.
                assertQuery("""
                        DECLARE
                            @one := (SELECT 1L l),
                            @tz := (SELECT 'UTC'),
                            @lim := (SELECT max(c) FROM (SELECT count() c FROM k SAMPLE BY 1d ALIGN TO CALENDAR TIME ZONE @tz))
                        SELECT * FROM @one UNION ALL SELECT * FROM @one UNION ALL SELECT * FROM @one UNION ALL SELECT * FROM v_lim
                        """)
                        .withCompiler(compiler)
                        .noLeakCheck()
                        .fails(47, error);
                // One lexer for the three copies and one for v_lim's body: the failing copy took
                // none of its own.
                Assert.assertEquals(2, countViewLexersHeld(compiler));

                // The same failure inside the third expansion of v_lim, which parses the body with
                // the lexer the first two used. Only that expansion sees the declarations of the
                // sub-query it sits in.
                assertQuery("""
                        DECLARE @one := (SELECT 1L l)
                        SELECT * FROM @one UNION ALL SELECT * FROM @one UNION ALL SELECT * FROM @one
                        UNION ALL SELECT * FROM v_lim UNION ALL SELECT * FROM v_lim
                        UNION ALL SELECT * FROM (
                            DECLARE
                                @tz := (SELECT 'UTC'),
                                @lim := (SELECT max(c) FROM (SELECT count() c FROM k SAMPLE BY 1d ALIGN TO CALENDAR TIME ZONE @tz))
                            SELECT * FROM v_lim
                        )
                        """)
                        .withCompiler(compiler)
                        .noLeakCheck()
                        .fails(221, error);
                Assert.assertEquals(2, countViewLexersHeld(compiler));

                // The lexers a failed parse leaves behind do not reach the next statement.
                assertQuery("DECLARE @lim := (SELECT max(l) FROM k) SELECT * FROM v_lim UNION ALL SELECT * FROM v_lim")
                        .withCompiler(compiler)
                        .noLeakCheck()
                        .noRandomAccess()
                        .returns("""
                                l
                                3
                                3
                                """);
            }
        });
    }

    @Test
    public void testDeclareVariableAsSubQueryReadManyTimesSharesLexers() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE k (s SYMBOL, l LONG)");
            execute("INSERT INTO k VALUES ('a', 1), ('b', 2), ('c', 3)");
            // Every read after the first parses a copy of the sub-query, with a lexer over the
            // declaration's text. The copies used to take a lexer each from a pool that only grows
            // and that the compiler keeps for as long as it lives, so 33 reads held 32 lexers.
            // The copies come one after another here, so one lexer parses them all.
            final StringBuilder fromReads = new StringBuilder("DECLARE @x := (SELECT l FROM k WHERE l > 1) SELECT sum(l) FROM (SELECT * FROM @x");
            final StringBuilder exprReads = new StringBuilder("DECLARE @x := (SELECT s FROM k WHERE l > 1) SELECT count() FROM k WHERE s IN @x");
            for (int i = 1; i < 33; i++) {
                fromReads.append(" UNION ALL SELECT * FROM @x");
                exprReads.append(" AND s IN @x");
            }
            fromReads.append(')');
            assertQuery(fromReads)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            sum
                            165
                            """);
            Assert.assertEquals(1, countViewLexersHeld(fromReads.toString()));
            assertQuery(exprReads)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count
                            2
                            """);
            Assert.assertEquals(1, countViewLexersHeld(exprReads.toString()));

            // Copies of two texts: a view body reads its own sub-query twice, and reads the caller's
            // twice. The two expansions of the view come one after the other, so one lexer parses
            // the body for both. The copies of the body's own sub-query take one more, because
            // the body's lexer is still reading when they are parsed, and the copies of the
            // caller's text take a third.
            execute("""
                    CREATE VIEW v_two AS (
                        DECLARE OVERRIDABLE @x := (SELECT l FROM k WHERE l > 2), @y := (SELECT l + 10 l FROM k WHERE l < 2)
                        SELECT * FROM @x UNION ALL SELECT * FROM @x UNION ALL SELECT * FROM @y UNION ALL SELECT * FROM @y
                    )
                    """);
            drainWalAndViewQueues();
            final String viewReads = "DECLARE @x := (SELECT l FROM k WHERE l = 2) SELECT * FROM v_two UNION ALL SELECT * FROM v_two";
            assertQuery(viewReads)
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            l
                            2
                            2
                            11
                            11
                            2
                            2
                            11
                            11
                            """);
            Assert.assertEquals(3, countViewLexersHeld(viewReads));
        });
    }

    @Test
    public void testDeclareVariableAsSubQueryReadMoreThanOnce() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE k (s SYMBOL, l LONG)");
            execute("INSERT INTO k VALUES ('a', 1), ('b', 2), ('c', 3)");
            drainWalQueue();
            // The optimiser and the code generator rewrite a model in place for the one place it is
            // read from, so every read of a declared sub-query needs a model of its own. The reads
            // used to share the declaration's model, and all but one of them read it wrong.
            assertQuery("DECLARE @x := (SELECT l FROM k WHERE l > 1 ORDER BY l DESC LIMIT 1) SELECT * FROM @x UNION ALL SELECT * FROM @x")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            l
                            3
                            3
                            """);
            assertQuery("DECLARE @x := (SELECT s FROM k WHERE l > 1) WITH w AS (SELECT * FROM @x) SELECT * FROM w UNION ALL SELECT * FROM w")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            s
                            b
                            c
                            b
                            c
                            """);
            assertQuery("DECLARE @x := (SELECT s FROM k WHERE l > 1) SELECT * FROM k WHERE s IN @x OR s IN @x")
                    .noLeakCheck()
                    .returns("""
                            s	l
                            b	2
                            c	3
                            """);
            // A read in an expression and a read in FROM, in either order.
            assertQuery("DECLARE @x := (SELECT s FROM k WHERE l > 1) SELECT * FROM @x WHERE s IN @x")
                    .noLeakCheck()
                    .returns("""
                            s
                            b
                            c
                            """);
            assertQuery("DECLARE @x := (SELECT s FROM k WHERE l > 1), @f := (s IN @x) SELECT s FROM k WHERE @f UNION ALL SELECT * FROM @x")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            s
                            b
                            c
                            b
                            c
                            """);
            // Every copy resolves variables as the declaration did, not as the scope it is read in.
            assertQuery("DECLARE @n := 1, @x := (SELECT l FROM k WHERE l > @n) SELECT * FROM @x UNION ALL SELECT * FROM (DECLARE @n := 2 SELECT * FROM @x)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            l
                            2
                            3
                            2
                            3
                            """);
            // A view body parses a copy of a caller's value from the caller's text.
            execute("CREATE VIEW v_sub AS (DECLARE OVERRIDABLE @x := (SELECT l FROM k ORDER BY l DESC LIMIT 1) SELECT * FROM @x)");
            drainWalAndViewQueues();
            assertQuery("DECLARE @x := (SELECT l FROM k ORDER BY l LIMIT 2) SELECT * FROM v_sub UNION ALL SELECT * FROM v_sub")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            l
                            1
                            2
                            1
                            2
                            """);
        });
    }

    @Test
    public void testDeclareVariableAsSubQueryReadMoreThanOnceKeepsGeneratedColumnNames() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE k (l LONG)");
            execute("INSERT INTO k VALUES (1), (2), (3), (4), (5)");
            // The test configuration names an unaliased constant with a dot in it column1, column2
            // and so on, from a counter the parser keeps for the whole statement. Every read of a
            // declared sub-query after the first parses a copy of it, and the copy used to go on
            // counting where the statement had got to: the second read of (SELECT 2.5, 4.5) saw
            // column2 and column3, so the upper bound below read 2.5 and no row passed.
            assertQuery("DECLARE @thr := (SELECT 2.5, 4.5) SELECT l FROM k WHERE l > (SELECT column1 FROM @thr) AND l < (SELECT column2 FROM @thr)")
                    .noLeakCheck()
                    .returns("""
                            l
                            3
                            4
                            """);
            // The second read returned the other column under the same name.
            assertQuery("DECLARE @q := (SELECT 1.5, 2.5) SELECT column2 FROM @q UNION ALL SELECT column2 FROM @q")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            column2
                            2.5
                            2.5
                            """);
            // The second read had no column1 at all.
            assertQuery("DECLARE @q := (SELECT 1.5, 2.5) SELECT column1 FROM @q UNION ALL SELECT column1 FROM @q")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            column1
                            1.5
                            1.5
                            """);
            // Quoted constants with a dot in them are named the same way.
            assertQuery("DECLARE @q := (SELECT '1.2.3.4', '2024-01-01T00:00:00.000Z') SELECT column2 FROM @q UNION ALL SELECT column2 FROM @q")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            column2
                            2024-01-01T00:00:00.000Z
                            2024-01-01T00:00:00.000Z
                            """);
            // The reads expose the same names, so a join of them suffixes the second and the third
            // read's. The second read used to expose column2 and column3, and the join named them
            // column21 and column3.
            assertQuery("DECLARE @q := (SELECT 1.5, 2.5) SELECT * FROM @q a CROSS JOIN (SELECT * FROM @q) b CROSS JOIN (SELECT * FROM @q) c")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            column1	column2	column11	column21	column12	column22
                            1.5	2.5	1.5	2.5	1.5	2.5
                            """);
            // The declaration skips the number an alias written beside the constants has taken,
            // and so does every copy.
            assertQuery("DECLARE @q := (SELECT 1.5 column1, 2.5, 3.5) SELECT * FROM @q a CROSS JOIN (SELECT * FROM @q) b")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            column1	column2	column3	column11	column21	column31
                            1.5	2.5	3.5	1.5	2.5	3.5
                            """);
        });
    }

    @Test
    public void testDeclareVariableAsSubQueryReadMoreThanOnceKeepsGeneratedColumnNamesInCreateTableAndInsert() throws Exception {
        assertMemoryLeak(() -> {
            // CREATE TABLE AS keeps the names the reads expose. The second read used to expose
            // column2 and column3, which the join named column21 and column3.
            execute("CREATE TABLE two_reads AS (DECLARE @q := (SELECT 1.5, 2.5) SELECT * FROM @q a CROSS JOIN (SELECT * FROM @q) b)");
            assertQuery("SELECT \"column\" FROM table_columns('two_reads')")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            column
                            column1
                            column2
                            column11
                            column21
                            """);
            assertQuery("SELECT * FROM two_reads")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            column1	column2	column11	column21
                            1.5	2.5	1.5	2.5
                            """);
            // The same when the declared sub-query reads a CTE that names its columns.
            execute("CREATE TABLE two_cte_reads AS (WITH w AS (SELECT 1.5, 2.5) SELECT * FROM (DECLARE @q := (SELECT * FROM w) SELECT * FROM @q a CROSS JOIN (SELECT * FROM @q) b))");
            assertQuery("SELECT \"column\" FROM table_columns('two_cte_reads')")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            column
                            column1
                            column2
                            column11
                            column21
                            """);
            assertQuery("SELECT * FROM two_cte_reads")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            column1	column2	column11	column21
                            1.5	2.5	1.5	2.5
                            """);
            // INSERT ... SELECT keeps the rows the reads return. The second read of column2 used
            // to return 1.5.
            execute("CREATE TABLE stored (x DOUBLE)");
            execute("WITH w AS (SELECT 1.5, 2.5) INSERT INTO stored SELECT * FROM (DECLARE @q := (SELECT * FROM w) SELECT column2 FROM @q UNION ALL SELECT column2 FROM @q)");
            assertQuery("SELECT * FROM stored")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x
                            2.5
                            2.5
                            """);
        });
    }

    @Test
    public void testDeclareVariableAsSubQueryReadMoreThanOnceKeepsGeneratedColumnNamesInNestedValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE k (l LONG)");
            execute("INSERT INTO k VALUES (1), (2), (3), (4), (5)");
            // @b reads @a twice, so the declaration of @b parses a copy of @a, and the copy of @b
            // parses two more. A read of a declared sub-query leaves the count of generated names
            // where it found it, whether it takes the parsed model or parses a copy: 5.5 and 6.5
            // are column3 and column4 in every parse of @b, after the constants of @a and of @m.
            assertQuery("""
                    DECLARE
                        @a := (SELECT 1.5, 2.5),
                        @m := (SELECT 0.5, 0.75),
                        @b := (SELECT x.column2 c, z.column4 d FROM @a x CROSS JOIN (SELECT column1 e FROM @a) y CROSS JOIN (SELECT 5.5, 6.5) z)
                    SELECT * FROM @b UNION ALL SELECT c, d FROM @b
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            c	d
                            2.5	6.5
                            2.5	6.5
                            """);
            // A value that holds two sub-queries: the second one starts counting where the first
            // one stopped, in the declaration and in every copy. The constants of the first are
            // column1 and column2, and the ones of the second are column2 and column3.
            assertQuery("""
                    DECLARE @in := l > (SELECT column2 FROM (SELECT 0.5, 1.5)) AND l < (SELECT column3 FROM (SELECT 0.5, 4.5))
                    SELECT l FROM k WHERE @in UNION ALL SELECT l FROM k WHERE @in
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            l
                            2
                            3
                            4
                            2
                            3
                            4
                            """);
            // A declared sub-query with a DECLARE block of its own: the copy of @q declares @z
            // again, and reads that @z twice, as the declaration of @q did.
            assertQuery("""
                    DECLARE @q := (DECLARE @z := (SELECT 1.5, 2.5) SELECT column2 FROM @z UNION ALL SELECT column1 FROM @z)
                    SELECT * FROM @q UNION ALL SELECT * FROM @q
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            column2
                            2.5
                            1.5
                            2.5
                            1.5
                            """);
            // A declaration that shadows an earlier one is parsed after it, so its constants are
            // column2 and column3, in every read.
            assertQuery("""
                    DECLARE @q := (SELECT 1.5, 2.5)
                    SELECT * FROM (DECLARE @q := (SELECT 7.5, 8.5) SELECT column2 FROM @q UNION ALL SELECT column2 FROM @q)
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            column2
                            7.5
                            7.5
                            """);
            // A view body reads its own sub-query twice, and a caller's value for it twice. The
            // caller's value keeps the names its declaration gave it, although the body parses its
            // own declaration in between.
            execute("CREATE VIEW v_q AS (DECLARE OVERRIDABLE @q := (SELECT 1.5, 2.5) SELECT column2 FROM @q UNION ALL SELECT column2 FROM @q)");
            drainWalAndViewQueues();
            assertQuery("SELECT * FROM v_q")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            column2
                            2.5
                            2.5
                            """);
            assertQuery("DECLARE @q := (SELECT 7.5, 8.5) SELECT * FROM v_q")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            column2
                            8.5
                            8.5
                            """);
        });
    }

    @Test
    public void testDeclareVariableAsSubQueryReadMoreThanOnceKeepsGeneratedColumnNamesOfCte() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE k (l LONG)");
            execute("INSERT INTO k VALUES (1), (2), (3), (4), (5)");
            // The declaration of @q makes the first reference to w, so it takes the model the
            // definition of w parsed, where 1.5 and 2.5 are column1 and column2. A copy of @q
            // finds that model gone and parses w again, and it used to count the names of w from
            // where the declaration of @q began: the second read saw w as column2 and column3,
            // and returned 1.5 for column2.
            assertQuery("WITH w AS (SELECT 1.5, 2.5) SELECT * FROM (DECLARE @q := (SELECT column2 c FROM w) SELECT c FROM @q UNION ALL SELECT c FROM @q)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            c
                            2.5
                            2.5
                            """);
            assertQuery("WITH w AS (SELECT 1.5, 2.5) SELECT * FROM (DECLARE @q := (SELECT * FROM w) SELECT column2 FROM @q UNION ALL SELECT column2 FROM @q)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            column2
                            2.5
                            2.5
                            """);
            // The reads sit in expressions here. The upper bound used to read 2.5, and no row
            // passed.
            assertQuery("WITH w AS (SELECT 2.5, 4.5) SELECT * FROM (DECLARE @thr := (SELECT * FROM w) SELECT l FROM k WHERE l > (SELECT column1 FROM @thr) AND l < (SELECT column2 FROM @thr))")
                    .noLeakCheck()
                    .returns("""
                            l
                            3
                            4
                            """);
            // The reads expose the names of w, so a join of them suffixes the second read's. The
            // second read used to expose column2 and column3, which the join named column21 and
            // column3.
            assertQuery("WITH w AS (SELECT 1.5, 2.5) SELECT * FROM (DECLARE @q := (SELECT * FROM w) SELECT * FROM @q a CROSS JOIN (SELECT * FROM @q) b)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            column1	column2	column11	column21
                            1.5	2.5	1.5	2.5
                            """);
            // The sub-query's own constants follow its reference to w. Taking the model of w
            // counts nothing, so the declaration names 3.5 and 4.5 column2 and column3. Parsing w
            // again used to move the count on, and the copy named them column3 and column4.
            assertQuery("WITH w AS (SELECT 1.5, 2.5) SELECT * FROM (DECLARE @q := (SELECT * FROM (SELECT * FROM w) x CROSS JOIN (SELECT 3.5, 4.5) y) SELECT column3 FROM @q UNION ALL SELECT column3 FROM @q)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            column3
                            4.5
                            4.5
                            """);
            assertQuery("WITH w AS (SELECT 1.5, 2.5) SELECT * FROM (DECLARE @q := (SELECT column2 c, 3.5, 4.5 FROM w) SELECT * FROM @q UNION ALL SELECT c, column2, column3 FROM @q)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            c	column2	column3
                            2.5	3.5	4.5
                            2.5	3.5	4.5
                            """);
            // Two CTEs with constants between the references to them: the declaration takes both
            // models, and the copy parses each CTE from where its own definition began.
            assertQuery("""
                    WITH w AS (SELECT 1.5, 2.5), v AS (SELECT 3.5, 4.5)
                    SELECT * FROM (
                        DECLARE @q := (SELECT * FROM w a CROSS JOIN (SELECT 0.5, 0.25) m CROSS JOIN v b)
                        SELECT * FROM @q UNION ALL SELECT column1, column2, column3, column4, column21, column31 FROM @q
                    )
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            column1	column2	column3	column4	column21	column31
                            1.5	2.5	0.5	0.25	3.5	4.5
                            1.5	2.5	0.5	0.25	3.5	4.5
                            """);
            // The sub-query refers to w twice. Its first reference takes the model of w, and its
            // second parses w again, from the count the declaration has reached: column2 and
            // column3, which the join names column21 and column3. The copy does the same at each
            // reference.
            assertQuery("""
                    WITH w AS (SELECT 1.5, 2.5)
                    SELECT * FROM (
                        DECLARE @q := (SELECT * FROM w a CROSS JOIN w b)
                        SELECT column2, column21, column3 FROM @q UNION ALL SELECT column2, column21, column3 FROM @q
                    )
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            column2	column21	column3
                            2.5	1.5	2.5
                            2.5	1.5	2.5
                            """);
            // The same with the name of w read from a variable: the two references are two places
            // in the text, although the name they resolve to sits in one.
            assertQuery("""
                    WITH w AS (SELECT 1.5, 2.5)
                    SELECT * FROM (
                        DECLARE @t := w, @q := (SELECT * FROM @t a CROSS JOIN @t b)
                        SELECT column2, column21, column3 FROM @q UNION ALL SELECT column2, column21, column3 FROM @q
                    )
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            column2	column21	column3
                            2.5	1.5	2.5
                            2.5	1.5	2.5
                            """);
            // Another sub-query took the model of w before the declaration of @q, so the
            // declaration parses w again itself, as column2 and column3, and so does the copy.
            assertQuery("WITH w AS (SELECT 1.5, 2.5) SELECT * FROM (SELECT column1 a FROM w) z CROSS JOIN (DECLARE @q := (SELECT * FROM w) SELECT column3 FROM @q UNION ALL SELECT column3 FROM @q)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            a	column3
                            1.5	2.5
                            1.5	2.5
                            """);
            // The statement's own constants after the reads get the names they get with one read:
            // a copy leaves the count where it found it, although it parses w.
            assertQuery("WITH w AS (SELECT 1.5, 2.5) SELECT * FROM (DECLARE @q := (SELECT * FROM w) SELECT * FROM (SELECT * FROM @q) y CROSS JOIN (SELECT 5.5, 6.5) z)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            column1	column2	column21	column3
                            1.5	2.5	5.5	6.5
                            """);
            assertQuery("WITH w AS (SELECT 1.5, 2.5) SELECT * FROM (DECLARE @q := (SELECT * FROM w) SELECT * FROM (SELECT * FROM @q UNION ALL SELECT * FROM @q) y CROSS JOIN (SELECT 5.5, 6.5) z)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            column1	column2	column21	column3
                            1.5	2.5	5.5	6.5
                            1.5	2.5	5.5	6.5
                            """);
            // A reference to w outside the declared sub-query parses w from the current count, as
            // it does in a statement without declared sub-queries: column2 and column3 here.
            assertQuery("WITH w AS (SELECT 1.5, 2.5) SELECT * FROM (DECLARE @q := (SELECT * FROM w) SELECT * FROM @q a CROSS JOIN (SELECT * FROM @q) b CROSS JOIN w c)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            column1	column2	column11	column21	column22	column3
                            1.5	2.5	1.5	2.5	1.5	2.5
                            """);
            // A view body reads a caller's value for its variable twice, and the value reads a
            // CTE of the caller's statement. The copy parses the caller's text, w included.
            execute("CREATE VIEW v_q AS (DECLARE OVERRIDABLE @q := (SELECT 1.5, 2.5) SELECT column2 FROM @q UNION ALL SELECT column2 FROM @q)");
            drainWalAndViewQueues();
            assertQuery("WITH w AS (SELECT 6.5, 7.5) SELECT * FROM (DECLARE @q := (SELECT * FROM w) SELECT * FROM v_q)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            column2
                            7.5
                            7.5
                            """);
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                // The definition of w begins at the count of 1 here, and the second reference to
                // u declares @q once more, in a body parsed again.
                assertQuery("""
                        WITH
                            a AS (SELECT 1),
                            w AS (SELECT 1.5, 2.5),
                            u AS (DECLARE @q := (SELECT * FROM w) SELECT column2 FROM @q EXCEPT SELECT column2 FROM @q)
                        SELECT * FROM u UNION ALL SELECT * FROM u
                        """)
                        .withCompiler(compiler)
                        .noLeakCheck()
                        .noRandomAccess()
                        .returns("""
                                column2
                                """);
                // Nothing the parser keeps about the CTEs and the declarations of one statement
                // reaches the next one. The definition of this w begins after the constants of a,
                // at the count of 2. This @q takes the model of w, and it is the second sub-query
                // its statement declares, as the @q of the body parsed again was above.
                assertQuery("""
                        WITH a AS (SELECT 0.5, 0.75), w AS (SELECT 1.5, 2.5)
                        SELECT * FROM (
                            DECLARE @m := (SELECT 3.5), @q := (SELECT * FROM w)
                            SELECT column3 FROM @q UNION ALL SELECT column3 FROM @q
                        )
                        """)
                        .withCompiler(compiler)
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                column3
                                2.5
                                2.5
                                """);
            }
        });
    }

    @Test
    public void testDeclareVariableAsSubQueryReadMoreThanOnceKeepsGeneratedColumnNamesOfCteReadThroughAnother() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE k (l LONG)");
            execute("INSERT INTO k VALUES (1), (2), (3), (4), (5)");
            // The declaration of @a takes the model of w. @b reads @a twice, so its declaration
            // parses a copy of @a, and a copy of @b parses two more. Before the copies started
            // from the count of their declarations the statement failed; once they did, the
            // second read of @b returned 1.5 twice.
            assertQuery("""
                    WITH w AS (SELECT 1.5, 2.5)
                    SELECT * FROM (
                        DECLARE @a := (SELECT * FROM w), @b := (SELECT * FROM @a UNION ALL SELECT * FROM @a)
                        SELECT column2 FROM @b UNION ALL SELECT column2 FROM @b
                    )
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            column2
                            2.5
                            2.5
                            2.5
                            2.5
                            """);
            // u reads w, and its definition takes the model of w. The declaration of @q takes the
            // model of u at its first reference, and at its second parses u again, and w inside
            // it, from the current count. The copy parses u at its first reference as the
            // definition of u did, w included, and at its second as the declaration did.
            assertQuery("""
                    WITH w AS (SELECT 1.5, 2.5), u AS (SELECT * FROM w)
                    SELECT * FROM (
                        DECLARE @q := (SELECT * FROM u UNION ALL SELECT * FROM u)
                        SELECT column2 FROM @q UNION ALL SELECT column2 FROM @q
                    )
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            column2
                            2.5
                            2.5
                            2.5
                            2.5
                            """);
            // The same in a join, where the names of the second reference show: w is column1 and
            // column2 under the first reference to u, and column2 and column3 under the second,
            // which the join names column21 and column3.
            assertQuery("""
                    WITH w AS (SELECT 1.5, 2.5), u AS (SELECT * FROM w)
                    SELECT * FROM (
                        DECLARE @q := (SELECT * FROM u a CROSS JOIN u b)
                        SELECT column2, column21, column3 FROM @q UNION ALL SELECT column2, column21, column3 FROM @q
                    )
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            column2	column21	column3
                            2.5	1.5	2.5
                            2.5	1.5	2.5
                            """);
            // Three CTEs, each reading the one before it, and three reads.
            assertQuery("""
                    WITH w AS (SELECT 1.5, 2.5), u AS (SELECT * FROM w), t AS (SELECT * FROM u)
                    SELECT * FROM (
                        DECLARE @q := (SELECT * FROM t)
                        SELECT column2 FROM @q UNION ALL SELECT column2 FROM @q UNION ALL SELECT column2 FROM @q
                    )
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            column2
                            2.5
                            2.5
                            2.5
                            """);
            assertQuery("""
                    WITH w AS (SELECT 2.5, 4.5), u AS (SELECT * FROM w)
                    SELECT * FROM (
                        DECLARE @thr := (SELECT * FROM u)
                        SELECT l FROM k WHERE l > (SELECT column1 FROM @thr) AND l < (SELECT column2 FROM @thr)
                    )
                    """)
                    .noLeakCheck()
                    .returns("""
                            l
                            3
                            4
                            """);
            // A declared sub-query with a DECLARE block of its own, whose sub-query takes the
            // model of w. The copy of @q declares @z again, and both reads of that @z see w as
            // the first @z did.
            assertQuery("""
                    WITH w AS (SELECT 1.5, 2.5)
                    SELECT * FROM (
                        DECLARE @q := (DECLARE @z := (SELECT * FROM w) SELECT column2 FROM @z UNION ALL SELECT column1 FROM @z)
                        SELECT * FROM @q UNION ALL SELECT * FROM @q
                    )
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            column2
                            2.5
                            1.5
                            2.5
                            1.5
                            """);
            // The reads sit in the body of a CTE that the statement refers to three times, so the
            // second and the third reference parse the body again, from the current count. The
            // copy of @q they parse reads w as the declaration of @q did all the same.
            assertQuery("""
                    WITH w AS (SELECT 1.5, 2.5)
                    SELECT * FROM (
                        DECLARE @q := (SELECT * FROM w)
                        WITH x AS (SELECT column2 c FROM @q)
                        SELECT * FROM x UNION ALL SELECT * FROM x UNION ALL SELECT * FROM x
                    )
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            c
                            2.5
                            2.5
                            2.5
                            """);
            // The declaration sits in the body of a CTE.
            assertQuery("""
                    WITH
                        w AS (SELECT 1.5, 2.5),
                        u AS (DECLARE @q := (SELECT * FROM w) SELECT column2 FROM @q UNION ALL SELECT column2 FROM @q)
                    SELECT * FROM u
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            column2
                            2.5
                            2.5
                            """);
            // The second reference to u parses its body again, declaration included, and that
            // declaration of @q finds the model of w gone: it parses w from the current count,
            // and so does its copy. Both reads of each @q return the same value, so neither
            // EXCEPT leaves a row.
            assertQuery("""
                    WITH
                        w AS (SELECT 1.5, 2.5),
                        u AS (DECLARE @q := (SELECT * FROM w) SELECT column2 FROM @q EXCEPT SELECT column2 FROM @q)
                    SELECT * FROM u UNION ALL SELECT * FROM u
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            column2
                            """);
        });
    }

    @Test
    public void testDeclareVariableAsSubQueryReadMoreThanOnceKeepsGeneratedColumnNamesOfStatement() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE k (l LONG)");
            execute("INSERT INTO k VALUES (1), (2), (3), (4), (5)");
            // The declaration's parse counts 1.5 and 2.5, so the statement's own constants are
            // column2 and column3 before the reads, and column3 and column4 after them, however
            // many times the sub-query is read: not at all, once, or twice. A copy used to count
            // its constants as well, and the second read moved the constants after it on to
            // column5 and column6.
            assertQuery("DECLARE @q := (SELECT 1.5, 2.5) SELECT * FROM (SELECT 3.5, 4.5) x CROSS JOIN (SELECT 5.5, 6.5) z")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            column2	column3	column31	column4
                            3.5	4.5	5.5	6.5
                            """);
            assertQuery("DECLARE @q := (SELECT 1.5, 2.5) SELECT * FROM (SELECT 3.5, 4.5) x CROSS JOIN (SELECT * FROM @q) y CROSS JOIN (SELECT 5.5, 6.5) z")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            column2	column3	column1	column21	column31	column4
                            3.5	4.5	1.5	2.5	5.5	6.5
                            """);
            assertQuery("DECLARE @q := (SELECT 1.5, 2.5) SELECT * FROM (SELECT 3.5, 4.5) x CROSS JOIN (SELECT * FROM @q UNION ALL SELECT * FROM @q) y CROSS JOIN (SELECT 5.5, 6.5) z")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            column2	column3	column1	column21	column31	column4
                            3.5	4.5	1.5	2.5	5.5	6.5
                            3.5	4.5	1.5	2.5	5.5	6.5
                            """);
            // The reads sit in the select list here, beside an alias that takes a generated name
            // and before a constant that needs one. The copy for b parses while the list is still
            // open, and names its constants column1 and column2 all the same.
            assertQuery("DECLARE @q := (SELECT 1.5, 2.5) SELECT 7.5 column1, l > (SELECT column1 FROM @q) a, l > (SELECT column2 FROM @q) b, 8.5 FROM k")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            column1	a	b	column2
                            7.5	false	false	8.5
                            7.5	true	false	8.5
                            7.5	true	true	8.5
                            7.5	true	true	8.5
                            7.5	true	true	8.5
                            """);
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                // The copy of @q fails: it reads @tz a second time, as a SAMPLE BY time zone, which
                // has no model to hold a copy of @tz.
                assertQuery("""
                        DECLARE
                            @tz := (SELECT 'UTC'),
                            @q := (SELECT 1.5, 2.5, count() c FROM k SAMPLE BY 1d ALIGN TO CALENDAR TIME ZONE @tz)
                        SELECT * FROM @q UNION ALL SELECT * FROM @q
                        """)
                        .withCompiler(compiler)
                        .noLeakCheck()
                        .fails(20, "query is not allowed here");
                // Nothing of the failed statement reaches the next one: its constants are counted
                // from one, and the copy of @q is counted from where its own declaration began,
                // after the constants of @a.
                assertQuery("DECLARE @a := (SELECT 0.5, 0.75), @q := (SELECT 1.5, 2.5) SELECT column3 FROM @q UNION ALL SELECT column3 FROM @q")
                        .withCompiler(compiler)
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                column3
                                2.5
                                2.5
                                """);
            }
        });
    }

    @Test
    public void testDeclareVariableAsSubQueryReadMoreThanOnceWithExpressionColumnAliases() throws Exception {
        // The default configuration names an unaliased column after its expression, and counts
        // nothing through the statement, so the reads of a declared sub-query always agreed there.
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, "true");
        assertMemoryLeak(() -> {
            assertQuery("DECLARE @q := (SELECT 1.5, 2.5) SELECT * FROM @q a CROSS JOIN (SELECT * FROM @q) b")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            1.5	2.5	1.51	2.51
                            1.5	2.5	1.5	2.5
                            """);
            assertQuery("DECLARE @q := (SELECT 1.5, 2.5) SELECT column1 FROM @q UNION ALL SELECT column1 FROM @q")
                    .noLeakCheck()
                    .fails(72, "Invalid column: column1");
            // The same holds for a declared sub-query that reads a CTE.
            assertQuery("WITH w AS (SELECT 1.5, 2.5) SELECT * FROM (DECLARE @q := (SELECT * FROM w) SELECT * FROM @q a CROSS JOIN (SELECT * FROM @q) b)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            1.5	2.5	1.51	2.51
                            1.5	2.5	1.5	2.5
                            """);
        });
    }

    @Test
    public void testDeclareVariableAsSubQueryReadTooManyTimes() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE k (l LONG)");
            execute("INSERT INTO k VALUES (1), (2), (3)");
            // Every read of a declared sub-query after the first parses a copy of it, and each
            // sub-query here reads the one before it twice, so the copies double with every level:
            // 120 at six levels, and over 2,000 at ten, each with query models of its own in a
            // pool that never shrinks for as long as the compiler lives. The parser refuses the
            // copy that would take the statement past its budget, the 101st, a copy of @q0, at
            // the sub-query it would copy.
            assertExceptionNoLeakCheck(
                    """
                            DECLARE
                                @q0 := (SELECT l FROM k),
                                @q1 := (SELECT * FROM @q0 UNION ALL SELECT * FROM @q0),
                                @q2 := (SELECT * FROM @q1 UNION ALL SELECT * FROM @q1),
                                @q3 := (SELECT * FROM @q2 UNION ALL SELECT * FROM @q2),
                                @q4 := (SELECT * FROM @q3 UNION ALL SELECT * FROM @q3),
                                @q5 := (SELECT * FROM @q4 UNION ALL SELECT * FROM @q4),
                                @q6 := (SELECT * FROM @q5 UNION ALL SELECT * FROM @q5)
                            SELECT count(), sum(l) FROM @q6
                            """,
                    20,
                    "declared sub-queries are read too many times [max=100]"
            );
            // The same chain written with CTEs parses as many copies, and the budget does not
            // apply to it.
            assertQuery("""
                    WITH
                        q0 AS (SELECT l FROM k),
                        q1 AS (SELECT * FROM q0 UNION ALL SELECT * FROM q0),
                        q2 AS (SELECT * FROM q1 UNION ALL SELECT * FROM q1),
                        q3 AS (SELECT * FROM q2 UNION ALL SELECT * FROM q2),
                        q4 AS (SELECT * FROM q3 UNION ALL SELECT * FROM q3),
                        q5 AS (SELECT * FROM q4 UNION ALL SELECT * FROM q4),
                        q6 AS (SELECT * FROM q5 UNION ALL SELECT * FROM q5)
                    SELECT count(), sum(l) FROM q6
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count\tsum
                            192\t384
                            """);
        });
    }

    @Test
    public void testDeclareVariableAsSubQueryReadTwiceAtEveryLevelSharesLexers() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE k (l LONG)");
            execute("INSERT INTO k VALUES (1), (2), (3)");
            // Each sub-query reads the one before it twice, so the copies double with every level:
            // 57 of them at five levels, and each used to take a lexer of its own from a pool that
            // only grows, which the compiler keeps for as long as it lives. A lexer can parse the
            // same text again once its copy is parsed, so the statement needs one lexer for each
            // copy being parsed at the same time: one per level.
            final String sql = """
                    DECLARE
                        @q0 := (SELECT l FROM k),
                        @q1 := (SELECT * FROM @q0 UNION ALL SELECT * FROM @q0),
                        @q2 := (SELECT * FROM @q1 UNION ALL SELECT * FROM @q1),
                        @q3 := (SELECT * FROM @q2 UNION ALL SELECT * FROM @q2),
                        @q4 := (SELECT * FROM @q3 UNION ALL SELECT * FROM @q3),
                        @q5 := (SELECT * FROM @q4 UNION ALL SELECT * FROM @q4)
                    SELECT count(), sum(l) FROM @q5
                    """;
            assertQuery(sql)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count	sum
                            96	192
                            """);
            Assert.assertEquals(5, countViewLexersHeld(sql));
        });
    }

    @Test
    public void testDeclareVariableAsSubQueryReadTwiceFailingInsideView() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE k (l LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO k VALUES (1, '2024-01-01T00:00:00Z'), (2, '2024-01-01T01:00:00Z'), (3, '2024-01-02T00:00:00Z')");
            execute("CREATE VIEW v_lim AS (DECLARE OVERRIDABLE @lim := 0 SELECT l FROM k WHERE l >= @lim AND l <= @lim)");
            drainWalAndViewQueues();
            // The second read of @lim in v_lim parses a copy of the caller's sub-query outside
            // v_lim. The copy reads @tz a second time, as a SAMPLE BY time zone, which has no model
            // to hold a copy of @tz, so the copy's parse fails. The views it set aside go back on
            // the way out, and v_lim's expansion unwinds to the parse error.
            assertQuery("""
                    DECLARE
                        @tz := (SELECT 'UTC'),
                        @lim := (SELECT max(c) FROM (SELECT count() c FROM k SAMPLE BY 1d ALIGN TO CALENDAR TIME ZONE @tz))
                    SELECT * FROM v_lim
                    """)
                    .noLeakCheck()
                    .failsWith("query is not allowed here");
            assertQuery("DECLARE @lim := (SELECT max(l) FROM k) SELECT * FROM v_lim")
                    .noLeakCheck()
                    .returns("""
                            l
                            3
                            """);
        });
    }

    @Test
    public void testDeclareVariableAsSubQueryReadTwiceInNestedSelectWithoutFrom() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE k (l LONG)");
            execute("INSERT INTO k VALUES (1), (2), (3), (4)");
            drainWalQueue();
            // Every read of a declared sub-query after the first parses a copy of it. That parse
            // must leave the enclosing sub-query still parsing as one, or a select list without
            // FROM rejects the ')' that closes it.
            assertQuery("DECLARE @q := (SELECT max(l) FROM k) SELECT * FROM (SELECT 4 = @q a, 3 = @q b)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a\tb
                            true\tfalse
                            """);
            assertQuery("DECLARE @q := (SELECT max(l) FROM k) WITH w AS (SELECT 4 = @q a, 3 = @q b) SELECT * FROM w")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a\tb
                            true\tfalse
                            """);
            assertQuery("DECLARE @q := (SELECT max(l) FROM k) SELECT * FROM (SELECT 4 = @q a UNION ALL SELECT 3 = @q a)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            a
                            true
                            false
                            """);
            assertQuery("DECLARE @q := (SELECT max(l) FROM k) SELECT * FROM (SELECT 4 = @q a) CROSS JOIN (SELECT 3 = @q b)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            a\tb
                            true\tfalse
                            """);
            // A later read in FROM parses a copy too.
            assertQuery("DECLARE @x := (SELECT max(l) m FROM k) SELECT * FROM (SELECT * FROM @x UNION ALL SELECT * FROM @x UNION ALL SELECT 5)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            m
                            4
                            4
                            5
                            """);
        });
    }

    @Test
    public void testDeclareVariableAsSubQueryReadTwiceInViewSelectWithoutFrom() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE k (l LONG)");
            execute("INSERT INTO k VALUES (1), (2), (3), (4)");
            // Every read of a view parses its stored body again, so a body that reads a declared
            // sub-query twice in a select list without FROM has to parse at CREATE and at every read.
            execute("CREATE VIEW v_decl AS (DECLARE @q := (SELECT max(l) FROM k) SELECT * FROM (SELECT 4 = @q a, 3 = @q b))");
            execute("CREATE VIEW v_over AS (DECLARE OVERRIDABLE @q := 4 SELECT * FROM (SELECT 4 = @q a, 3 = @q b))");
            drainWalAndViewQueues();
            assertQuery("SELECT * FROM v_decl")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a\tb
                            true\tfalse
                            """);
            // A caller's sub-query for an overridable variable, read twice in the view body.
            assertQuery("DECLARE @q := (SELECT max(l) - 1 FROM k) SELECT * FROM v_over")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a\tb
                            false\ttrue
                            """);
        });
    }

    @Test
    public void testDeclareVariableAsSubQueryReadTwiceReadingSameView() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE k (l LONG)");
            execute("INSERT INTO k VALUES (1), (2), (3), (4), (5)");
            execute("CREATE VIEW v_lim AS (DECLARE OVERRIDABLE @lim := 0 SELECT l FROM k WHERE l >= @lim AND l <= @lim)");
            execute("CREATE VIEW v_eq AS (DECLARE OVERRIDABLE @v := 2 SELECT l FROM k WHERE l >= @v AND l <= @v)");
            drainWalAndViewQueues();
            // The view reads the caller's value twice, so the second read parses a copy of the
            // caller's sub-query while the parser is still expanding the view. The sub-query reads
            // that same view, which is no cycle: the caller declared the value outside the view,
            // and the sub-query reads the view with its own default.
            // The inner v_lim keeps only l = 0, which k lacks, so min(l) is NULL and no row
            // passes a comparison with NULL.
            assertQuery("DECLARE @lim := (SELECT min(l) FROM v_lim) SELECT * FROM v_lim")
                    .noLeakCheck()
                    .returns("""
                            l
                            """);
            // The inner v_eq keeps its default 2, so the caller's @v is 3, and the outer read
            // keeps 3 where the view's default keeps 2.
            assertQuery("DECLARE @v := (SELECT max(l) + 1 FROM v_eq) SELECT * FROM v_eq")
                    .noLeakCheck()
                    .returns("""
                            l
                            3
                            """);
            // The same value reaching v_eq through a view that wraps it: the second read of @v
            // happens two views deep, so the copy of the caller's sub-query sets both views aside.
            execute("CREATE VIEW v_wrap AS (SELECT * FROM v_eq)");
            drainWalAndViewQueues();
            assertQuery("DECLARE @v := (SELECT max(l) + 1 FROM v_eq) SELECT * FROM v_wrap")
                    .noLeakCheck()
                    .returns("""
                            l
                            3
                            """);
            // A sub-query that reads the outer view passes only if the copy sets aside every view,
            // not just the innermost one.
            assertQuery("DECLARE @v := (SELECT max(l) + 1 FROM v_wrap) SELECT * FROM v_wrap")
                    .noLeakCheck()
                    .returns("""
                            l
                            3
                            """);
            // A view body that hands such a value to the view it reads: the second read happens
            // two views deep, at CREATE VIEW and at every read of the outer view.
            execute("CREATE VIEW v_outer AS (DECLARE @v := (SELECT max(l) + 1 FROM v_eq) SELECT * FROM v_eq)");
            drainWalAndViewQueues();
            assertQuery("SELECT * FROM v_outer")
                    .noLeakCheck()
                    .returns("""
                            l
                            3
                            """);
        });
    }

    @Test
    public void testDeclareVariableAsSubQueryReadTwiceWithViewCycle() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE k (l LONG)");
            execute("INSERT INTO k VALUES (1), (2), (3)");
            execute("CREATE VIEW v_cyc AS (DECLARE OVERRIDABLE @lim := 0 SELECT l FROM k WHERE l >= @lim AND l <= @lim)");
            execute("CREATE VIEW v_back AS (SELECT * FROM v_cyc)");
            execute("CREATE VIEW v_self AS (SELECT l FROM k)");
            execute("CREATE VIEW v_lim AS (DECLARE OVERRIDABLE @lim := 0 SELECT l FROM k WHERE l >= @lim AND l <= @lim)");
            drainWalAndViewQueues();
            // DDL refuses a view cycle, and a concurrent ALTER VIEW is the only way past that check.
            // The test stands in for the race: it swaps the bodies the view graph holds, closing
            // v_cyc -> v_back -> v_cyc and v_self -> v_self through a declared sub-query that the
            // view body reads twice.
            final ViewDefinition cyc = engine.getViewGraph().getViewDefinition(engine.verifyTableName("v_cyc"));
            cyc.init(
                    cyc.getViewToken(),
                    "DECLARE OVERRIDABLE @lim := (SELECT min(l) FROM v_back) SELECT l FROM k WHERE l >= @lim AND l <= @lim",
                    cyc.getSeqTxn(),
                    cyc.isAudited()
            );
            final ViewDefinition self = engine.getViewGraph().getViewDefinition(engine.verifyTableName("v_self"));
            self.init(
                    self.getViewToken(),
                    "DECLARE @lim := (SELECT min(l) FROM v_self) SELECT l FROM k WHERE l >= @lim AND l <= @lim",
                    self.getSeqTxn(),
                    self.isAudited()
            );
            assertQuery("SELECT * FROM v_cyc")
                    .noLeakCheck()
                    .failsWith("circular view reference detected: v_cyc");
            assertQuery("SELECT * FROM v_back")
                    .noLeakCheck()
                    .failsWith("circular view reference detected: v_back");
            assertQuery("SELECT * FROM v_self")
                    .noLeakCheck()
                    .failsWith("circular view reference detected: v_self");
            // A caller's value does not stop the view parsing its own declaration.
            assertQuery("DECLARE @lim := 2 SELECT * FROM v_cyc")
                    .noLeakCheck()
                    .failsWith("circular view reference detected: v_cyc");
            // A caller's sub-query, read twice in v_lim, that reads a view in a cycle.
            assertQuery("DECLARE @lim := (SELECT min(l) FROM v_cyc) SELECT * FROM v_lim")
                    .noLeakCheck()
                    .failsWith("circular view reference detected: v_cyc");
            assertQuery("DECLARE @lim := (SELECT min(l) FROM v_self) SELECT * FROM v_lim")
                    .noLeakCheck()
                    .failsWith("circular view reference detected: v_self");
        });
    }

    @Test
    public void testDeclareVariableAsSubQueryWithEmptyLimit() throws Exception {
        assertQuery("declare @pair := (select symbol from fx_trades limit ), " +
                "with bids as (select symbol, bids[1,1] from market_data where symbol = @pair), " +
                "asks as (select symbol, asks[1,1] from market_data where symbol = @pair) " +
                "select symbol, * from bids")
                .fails(53, "limit expression expected");
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
    public void testDeclareVariableAsSubQueryWithTopLevelComma() throws Exception {
        assertMemoryLeak(() -> {
            // A subquery that opens with its own DECLARE is still a subquery, not a value list,
            // however many commas its select list, its DECLARE or its ORDER BY puts directly inside
            // the brackets.
            assertQuery("DECLARE @x := (DECLARE @y := 4 SELECT @y AS a, 5 AS b) SELECT * FROM @x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a\tb
                            4\t5
                            """);
            assertQuery("DECLARE @x := (DECLARE @a := 1, @b := 2 SELECT @a + @b AS s) SELECT * FROM @x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            s
                            3
                            """);
            assertQuery("DECLARE @x := (/* leading */ DECLARE @y := 4 SELECT @y AS a, 5 AS b) SELECT * FROM @x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a\tb
                            4\t5
                            """);
            assertQuery("DECLARE @x := (DECLARE @n := 2 SELECT x FROM long_sequence(3) ORDER BY x % @n, x) SELECT * FROM @x ORDER BY x DESC")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x
                            3
                            2
                            1
                            """);
            // A view body is parsed again on every read, so a stored body of this shape has to
            // keep parsing too.
            execute("CREATE VIEW v_decl AS (DECLARE @x := (DECLARE @y := 4 SELECT @y AS a, 5 AS b) SELECT * FROM @x)");
            drainWalAndViewQueues();
            assertQuery("SELECT * FROM v_decl")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a\tb
                            4\t5
                            """);
        });
    }

    @Test
    public void testDeclareVariableAsWindowFunctionKeepsItsAnchor() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // Only a live view takes ANCHOR, and only on a named window, so it refuses the inline
            // window a declared value holds, at the ANCHOR. Each read's copy of the window has to
            // keep the anchor and where it sits for the refusal to see it and to point at it.
            // Without the anchor a copy read as a bare unbounded window, and without its position
            // the refusal pointed at the first PARTITION BY key.
            assertExceptionNoLeakCheck(
                    """
                            CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS
                            DECLARE @w := row_number() OVER (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00')
                            SELECT ts, sym, @w AS a, @w AS b FROM base
                            """,
                    115,
                    "ANCHOR is only supported on named WINDOW clauses"
            );
            Assert.assertNull(engine.getTableTokenIfExists("lv"));
        });
    }

    @Test
    public void testDeclareVariableAsWindowFunctionKeepsItsFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                        (1, '2024-01-01T00:00:00Z'),
                        (2, '2024-01-01T00:00:01Z'),
                        (3, '2024-01-01T00:00:02Z'),
                        (4, '2024-01-01T00:00:03Z'),
                        (5, '2024-01-01T00:00:04Z'),
                        (6, '2024-01-01T00:00:05Z')
                    """);
            // Each read of a variable that holds a window function copies the window, and the
            // copy has to carry every part of the frame. Each frame below differs from the default
            // in the part it names, so a copy that falls back to the default returns other sums.
            //
            // A frame that ends before the current row: the end's kind and its offset. Ending at
            // the current row, the default, the sums are 1, 3, 6, 9, 12.
            assertQuery("""
                    DECLARE @w := sum(x) OVER (ORDER BY x ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING)
                    SELECT x, @w a, @w b FROM long_sequence(5)
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x\ta\tb
                            1\tnull\tnull
                            2\t1.0\t1.0
                            3\t3.0\t3.0
                            4\t5.0\t5.0
                            5\t7.0\t7.0
                            """);
            // A frame that starts at the current row: the start's kind. Starting at UNBOUNDED
            // PRECEDING, the default, the sums are 1, 3, 6, 10, 15.
            assertQuery("""
                    DECLARE @w := sum(x) OVER (ORDER BY x ROWS BETWEEN CURRENT ROW AND CURRENT ROW)
                    SELECT x, @w a, @w b FROM long_sequence(5)
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x\ta\tb
                            1\t1.0\t1.0
                            2\t2.0\t2.0
                            3\t3.0\t3.0
                            4\t4.0\t4.0
                            5\t5.0\t5.0
                            """);
            // The exclusion. With the current row in the frame, the default, the sums are
            // 1, 3, 5, 7, 9.
            assertQuery("""
                    DECLARE @w := sum(x) OVER (ORDER BY x ROWS BETWEEN 1 PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW)
                    SELECT x, @w a, @w b FROM long_sequence(5)
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x\ta\tb
                            1\tnull\tnull
                            2\t1.0\t1.0
                            3\t2.0\t2.0
                            4\t3.0\t3.0
                            5\t4.0\t4.0
                            """);
            // The time unit of each bound of a RANGE frame. The rows are a second apart, so the
            // frame holds the rows two and three seconds back. Read in microseconds, the default,
            // the start bound leaves the frame empty and the end bound lets in the row before.
            assertQuery("""
                    DECLARE @w := sum(x) OVER (ORDER BY ts RANGE BETWEEN 3 SECOND PRECEDING AND 2 SECOND PRECEDING)
                    SELECT x, @w a, @w b FROM t
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            x\ta\tb
                            1\tnull\tnull
                            2\tnull\tnull
                            3\t1.0\t1.0
                            4\t3.0\t3.0
                            5\t5.0\t5.0
                            6\t7.0\t7.0
                            """);
        });
    }

    @Test
    public void testDeclareVariableAsWindowFunctionKeepsItsPositions() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            // A window that a function or the optimiser refuses is reported at the part of it the
            // refusal names, which a read's copy of the window keeps from the declaration. A copy
            // without the position reported it at 0, and IGNORE NULLS, which a function finds by
            // its position, went unnoticed.
            assertQuery("DECLARE @w := row_number() OVER nosuch SELECT x, @w a, @w b FROM long_sequence(3)")
                    .noLeakCheck()
                    .fails(32, "window 'nosuch' is not defined");
            assertQuery("DECLARE @w := rank() IGNORE NULLS OVER (ORDER BY x) SELECT x, @w a, @w b FROM long_sequence(3)")
                    .noLeakCheck()
                    .fails(21, "RESPECT/IGNORE NULLS is not supported for current window function");
            // the frame start's kind and the frame end's kind
            assertQuery("DECLARE @w := ntile(2) OVER (ORDER BY x ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) SELECT x, @w a, @w b FROM long_sequence(3)")
                    .noLeakCheck()
                    .fails(55, "ntile() does not support framing; remove the frame clause");
            assertQuery("DECLARE @w := sum(x) OVER (ORDER BY x ROWS BETWEEN 2 PRECEDING AND UNBOUNDED FOLLOWING) SELECT x, @w a, @w b FROM long_sequence(3)")
                    .noLeakCheck()
                    .fails(77, "frame end supports UNBOUNDED FOLLOWING only when frame start is UNBOUNDED PRECEDING");
            // the exclusion
            assertQuery("""
                    DECLARE @w := sum(x) OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW)
                    SELECT x, @w a, @w b FROM long_sequence(3)
                    """)
                    .noLeakCheck()
                    .fails(95, "EXCLUDE CURRENT ROW not supported with UNBOUNDED FOLLOWING frame boundary");
            // the offset of the frame start and of the frame end
            assertQuery("""
                    DECLARE @w := sum(x) OVER (ORDER BY ts RANGE BETWEEN 9_223_372_036_854_775_807 SECOND PRECEDING AND CURRENT ROW)
                    SELECT x, @w a, @w b FROM t
                    """)
                    .noLeakCheck()
                    .fails(53, "RANGE frame start is out of range for the designated timestamp");
            assertQuery("""
                    DECLARE @w := sum(x) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 9_223_372_036_854_775_807 SECOND PRECEDING)
                    SELECT x, @w a, @w b FROM t
                    """)
                    .noLeakCheck()
                    .fails(77, "RANGE frame end is out of range for the designated timestamp");
        });
    }

    @Test
    public void testDeclareVariableAsWindowFunctionReadMoreThanOnce() throws Exception {
        assertMemoryLeak(() -> {
            // Every read of a variable that holds a window function gets a window of its own. The
            // reads used to share one, so the second read in a select list reused the first read's
            // column and the parser rejected its alias as a duplicate.
            assertQuery("DECLARE @w := row_number() OVER (PARTITION BY x % 2 ORDER BY x) SELECT x, @w a, @w b FROM long_sequence(4)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x\ta\tb
                            1\t1\t1
                            2\t1\t1
                            3\t2\t2
                            4\t2\t2
                            """);
            // A variable built on another one copies its window as well.
            assertQuery("DECLARE @w := row_number() OVER (PARTITION BY x % 2 ORDER BY x), @y := @w SELECT x, @w a, @y b FROM long_sequence(4)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x\ta\tb
                            1\t1\t1
                            2\t1\t1
                            3\t2\t2
                            4\t2\t2
                            """);
            // The copy keeps the ORDER BY direction, the frame, IGNORE NULLS and a reference to a
            // named window.
            assertQuery("DECLARE @w := row_number() OVER (PARTITION BY x % 2 ORDER BY x DESC) SELECT x, @w a, @w b FROM long_sequence(4)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x\ta\tb
                            1\t2\t2
                            2\t2\t2
                            3\t1\t1
                            4\t1\t1
                            """);
            assertQuery("""
                    DECLARE @w := sum(x) OVER (ORDER BY x ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
                    SELECT x, @w a, @w b FROM (SELECT x * 2 x FROM long_sequence(4))
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x\ta\tb
                            2\t2.0\t2.0
                            4\t6.0\t6.0
                            6\t10.0\t10.0
                            8\t14.0\t14.0
                            """);
            assertQuery("""
                    DECLARE @w := last_value(nullif(x, 3)) IGNORE NULLS OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                    SELECT x, @w a, @w b FROM long_sequence(4)
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x\ta\tb
                            1\t1\t1
                            2\t2\t2
                            3\t2\t2
                            4\t4\t4
                            """);
            assertQuery("DECLARE @w := row_number() OVER w1 SELECT x, @w a, @w b FROM long_sequence(4) WINDOW w1 AS (PARTITION BY x % 2 ORDER BY x)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x\ta\tb
                            1\t1\t1
                            2\t1\t1
                            3\t2\t2
                            4\t2\t2
                            """);
            // Each read names its own column. A shared window took the alias of whichever read
            // the parser met last, the inner r2 here.
            assertQuery("DECLARE @w := row_number() OVER (PARTITION BY x % 2 ORDER BY x) SELECT x, @w r FROM (SELECT x, @w r2 FROM long_sequence(4))")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x\tr
                            1\t1
                            2\t1
                            3\t2
                            4\t2
                            """);
            // Each read's window points at the read's own function call. The optimiser rewrites
            // that call in place, qualified columns included. Pointed at the declared call, the
            // reads shared it, and one read's rewrite of t1.x left the other with an ambiguous x.
            execute("CREATE TABLE t1 AS (SELECT x, x * 10 y FROM long_sequence(3))");
            execute("CREATE TABLE t2 AS (SELECT x * 100 x, x y FROM long_sequence(3))");
            assertQuery("DECLARE @w := sum(t1.x) OVER (ORDER BY t1.y) SELECT t1.y, @w a, @w b FROM t1 JOIN t2 ON t1.x = t2.y")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            y\ta\tb
                            10\t1.0\t1.0
                            20\t3.0\t3.0
                            30\t6.0\t6.0
                            """);
        });
    }

    @Test
    public void testDeclareVariableAsWindowFunctionReadingSubQuery() throws Exception {
        assertMemoryLeak(() -> {
            // @q is 3, so x = 3 sits alone in its partition. The unoptimised sub-query loses its
            // ORDER BY and yields 1 instead, which puts x = 1 alone.
            final String declare = """
                    DECLARE
                        @q := (SELECT x FROM long_sequence(3) ORDER BY x DESC LIMIT 1),
                        @w := row_number() OVER (PARTITION BY x = @q)
                    """;
            final String expected = """
                    x\tr
                    1\t1
                    2\t2
                    3\t1
                    4\t3
                    """;
            // a single read
            assertQuery(declare + "SELECT x, @w r FROM long_sequence(4)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns(expected);
            // Each read of @w reads @q in its window, and each read of @q needs a model of its own.
            // The reads used to share one window, so the read in the unused CTE, second in parse
            // order, wrote its copy of @q into the window of the outer read too. The optimiser
            // never visits the unused CTE, so the outer window ran the copy unoptimised.
            assertQuery(declare + """
                    SELECT x, @w AS r
                    FROM (WITH c AS (SELECT @w r2 FROM long_sequence(1)) SELECT x FROM long_sequence(4))
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns(expected);
            // the same, with the unused CTE in a UNION ALL branch
            assertQuery(declare + """
                    SELECT x, @w r
                    FROM (
                        SELECT x FROM long_sequence(3)
                        UNION ALL
                        (WITH c AS (SELECT @w r2 FROM long_sequence(1)) SELECT x FROM long_sequence(1))
                    )
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            x\tr
                            1\t1
                            2\t2
                            3\t1
                            1\t3
                            """);
            // The unused CTE read first, the outer read second. The outer read names no column,
            // so its column takes the function's name. Over a shared window it took r2, the alias
            // the read before it had set.
            assertQuery(declare + """
                    WITH c AS (SELECT @w r2 FROM long_sequence(1))
                    SELECT x, @w FROM long_sequence(4)
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            x\trow_number
                            1\t1
                            2\t2
                            3\t1
                            4\t3
                            """);
            // Two unused CTEs read first. The second one's read parses a copy of @q, which over a
            // shared window was the sub-query the outer read ran, unoptimised as before.
            assertQuery(declare + """
                    WITH c AS (SELECT @w r2 FROM long_sequence(1)), d AS (SELECT @w r3 FROM long_sequence(1))
                    SELECT x, @w r FROM long_sequence(4)
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns(expected);
            // A plain read in the unused CTE takes @q's model ahead of both window reads, so the
            // outer window's copy of @q has to come from the outer read itself, whichever window
            // read the parser meets first.
            assertQuery(declare + """
                    WITH c AS (SELECT x = @q a, @w r2 FROM long_sequence(1))
                    SELECT x, @w r FROM long_sequence(4)
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns(expected);
            // An aggregate the optimiser never rewrote failed code generation instead.
            assertQuery("""
                    DECLARE
                        @q := (SELECT max(x) FROM long_sequence(3)),
                        @w := row_number() OVER (PARTITION BY x = @q)
                    SELECT x, @w r
                    FROM (WITH c AS (SELECT @w r2 FROM long_sequence(1)) SELECT x FROM long_sequence(4))
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns(expected);
            // A view body parses the same way at CREATE and at every read.
            execute("CREATE VIEW v_win_q AS (" + declare + """
                    SELECT x, @w r
                    FROM (WITH c AS (SELECT @w r2 FROM long_sequence(1)) SELECT x FROM long_sequence(4))
                    )""");
            drainWalAndViewQueues();
            assertQuery("SELECT * FROM v_win_q")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns(expected);
        });
    }

    @Test
    public void testDeclareVariableDefinedByAnotherVariable() throws Exception {
        assertModel("select-virtual 2 2, 2 * 2 column from (long_sequence(1))",
                "DECLARE @y := 2, @y2 := (@y * @y) SELECT @y, @y2", ExecutionModel.QUERY);
    }

    @Test
    public void testDeclareVariableWithArrayCastBeforeBracketedQuery() throws Exception {
        assertMemoryLeak(() -> {
            // An array type ends at its `[]`, which completes the declared value, so a bracket
            // after it starts the statement, as it does after any other complete value.
            assertQuery("DECLARE @a := '{1,2}'::double[] (SELECT @a AS a)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a
                            [1.0,2.0]
                            """);
            assertQuery("DECLARE @a := '{1,2}'::double[](SELECT @a AS a)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a
                            [1.0,2.0]
                            """);
            assertQuery("DECLARE @a := '{{1,2},{3,4}}'::double[][] (SELECT @a AS a)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a
                            [[1.0,2.0],[3.0,4.0]]
                            """);
            assertQuery("DECLARE @a := 1, @b := '{1,2}'::double[] (SELECT @a AS a, @b AS b)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a\tb
                            1\t[1.0,2.0]
                            """);
            // The array type still may not contain whitespace.
            assertQuery("DECLARE @a := '{1,2}'::double [] (SELECT @a AS a)")
                    .fails(30, "array type requires no whitespace");
        });
    }

    @Test
    public void testDeclareVariableWithBracketedExpression() throws Exception {
        assertMemoryLeak(() -> {
            execute(TRADES_DDL);
            // A list is the set of values IN tests against, so it cannot also be the value under
            // test. The position is the declaration's opening bracket, as it is for any substituted
            // variable - the misuse site is not where the list was written.
            assertQuery("DECLARE @symbols := ('ETH-USD', 'BTC-USD') " +
                    "SELECT * FROM trades WHERE @symbols IN @symbols")
                    .fails(20, "declared list can only be used on the right-hand side of IN");
        });
    }

    @Test
    public void testDeclareVariableWithBracketedOperand() throws Exception {
        assertMemoryLeak(() -> {
            // A bracket used to end a declared value after anything but a bracket, a literal or the
            // `:=` itself, so a bracketed operand after an operator, a comma or a keyword cut the
            // value short. Only a complete value, outside every bracket, ends at one.
            assertQuery("""
                    DECLARE @a := (1 + 2) * (3), @b := abs(1 - (5)), @c := NOT (1 = 1), @d := -(1),
                        @e := CASE WHEN (1 = 1) THEN 1 END, @f := 2 BETWEEN (1) AND (3), @g := cast((1) AS LONG)
                    SELECT @a a, @b b, @c c, @d d, @e e, @f f, @g g
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a	b	c	d	e	f	g
                            9	4	false	-1	1	true	1
                            """);
            assertQuery("DECLARE @x := 1 (SELECT @x AS x)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x
                            1
                            """);
            // A sub-query nested in a declared value now gets the error it gets written in place.
            execute("CREATE TABLE k (l LONG, ts TIMESTAMP)");
            assertQuery("DECLARE @v := dateadd('d', -1, (SELECT max(ts) FROM k)) SELECT * FROM k WHERE ts > @v")
                    .fails(14, "there is no matching function `dateadd` with the argument types: (CHAR, INT, CURSOR)");
            assertQuery("DECLARE @v := 1 + (SELECT max(l) FROM k) SELECT * FROM k WHERE l = @v")
                    .fails(16, "there is no matching operator `+` with the argument types: INT + CURSOR");
        });
    }

    @Test
    public void testDeclareVariableWithClosingBracketBeforeBracketedQuery() throws Exception {
        assertMemoryLeak(() -> {
            // A call's `)`, an array's `]` and the `)` of a sized type such as `decimal(10,2)` or
            // `geohash(6c)` each complete the declared value, so a bracket after them starts the
            // statement, as it does after any other complete value.
            assertQuery("DECLARE @x := abs(-1) (SELECT @x)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            abs
                            1
                            """);
            assertQuery("DECLARE @x := ARRAY[1.0,2.0] (SELECT @x)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            ARRAY
                            [1.0,2.0]
                            """);
            assertQuery("DECLARE @x := 1.5::decimal(10,2) (SELECT @x)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            cast
                            1.50
                            """);
            assertQuery("DECLARE @x := 'sp052w'::geohash(6c) (SELECT @x)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            cast
                            sp052w
                            """);
        });
    }

    @Test
    public void testDeclareVariableWithMalformedSelfReferencingValue() throws Exception {
        assertMemoryLeak(() -> {
            // A bracketed list after an operator leaves an operand other than the variable on the
            // left of `:=`. When the value reads its own variable, that operand is the reference:
            // it has the variable's token, but not its position. The parser used to accept the
            // declaration and give the variable the rest of the value, such as `2 = 3`.
            assertQuery("DECLARE @x := @x = (2, 3) SELECT @x")
                    .fails(26, "unexpected token [@x] - unexpected bind expression");
            assertQuery("DECLARE @x := @x + (2, 3) SELECT @x")
                    .fails(26, "unexpected token [@x] - unexpected bind expression");
            assertQuery("DECLARE @X := @x = (2, 3) SELECT @x")
                    .fails(26, "unexpected token [@X] - unexpected bind expression");
            // The sub-query used to drop out of the value without notice.
            assertQuery("DECLARE @x := -((SELECT 1), @x, 1) SELECT @x")
                    .fails(35, "unexpected token [@x] - unexpected bind expression");
            // The cast to a sub-query used to fail later with a NullPointerException.
            assertQuery("DECLARE @x := @x :: (1, (SELECT 1)) SELECT @x")
                    .fails(36, "unexpected token [@x] - unexpected bind expression");
            assertQuery("DECLARE @x := 1, @x := @x = (2, 3) SELECT @x")
                    .fails(35, "unexpected token [@x] - unexpected bind expression");
            assertQuery("DECLARE OVERRIDABLE AUDITED @x := @x = (2, 3) SELECT @x")
                    .fails(46, "unexpected token [@x] - unexpected bind expression");
            // Substituting @y puts a copy of the reference in the first declaration on the left.
            assertQuery("DECLARE @x := @x, @y := @x, @x := @y = (2, 3) SELECT @x")
                    .fails(46, "unexpected token [@x] - unexpected bind expression");
            assertQuery("SELECT * FROM (DECLARE @x := @x = (2, 3) SELECT @x)")
                    .fails(41, "unexpected token [@x] - unexpected bind expression");
            assertQuery("WITH c AS (DECLARE @x := @x = (2, 3) SELECT @x) SELECT * FROM c")
                    .fails(37, "unexpected token [@x] - unexpected bind expression");
            assertExceptionNoLeakCheck(
                    "CREATE VIEW v_self AS (DECLARE @x := @x = (2, 3) SELECT @x AS v)",
                    49,
                    "unexpected token [@x] - unexpected bind expression"
            );
            assertQuery("SELECT * FROM v_self").fails(14, "table does not exist [table=v_self]");
            // A well-formed value that names its own variable still passes the check.
            assertQuery("DECLARE @x := 1, @x := @x + 1 SELECT 1 AS v")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            v
                            1
                            """);
        });
    }

    @Test
    public void testDeclareVariableWithMalformedValueAroundSubQuery() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE k (l LONG)");
            // A bracketed list compared with `=`, or a bracket after a bind variable, leaves the
            // parser with an operand other than the variable on the left of `:=`. The declaration
            // reports that where its value's parse ended, whatever kind of operand took the
            // variable's place.
            assertQuery("DECLARE @x := 1 = (2, 3) SELECT 1")
                    .fails(25, "unexpected token [@x] - unexpected bind expression");
            // A sub-query has no token, and the same check failed with a NullPointerException
            // when one took the variable's place.
            assertQuery("DECLARE @x := (SELECT 1) = (2, 3) SELECT 1")
                    .fails(34, "unexpected token [@x] - unexpected bind expression");
            assertQuery("DECLARE @x := (SELECT max(l) FROM k) > (1, 2) SELECT 1")
                    .fails(46, "unexpected token [@x] - unexpected bind expression");
            assertQuery("DECLARE @x := $1 (SELECT @x)")
                    .fails(27, "unexpected token [@x] - unexpected bind expression");
            assertQuery("DECLARE @x := :a (SELECT @x)")
                    .fails(27, "unexpected token [@x] - unexpected bind expression");
            assertQuery("DECLARE OVERRIDABLE AUDITED @x := (SELECT 1) = (2, 3) SELECT 1")
                    .fails(54, "unexpected token [@x] - unexpected bind expression");
            // Substituting a variable whose value is a sub-query puts that sub-query on the left of
            // `:=` as well.
            assertQuery("DECLARE @q := (SELECT 1), @x := @q = (2, 3) SELECT 1")
                    .fails(44, "unexpected token [@x] - unexpected bind expression");
            assertQuery("SELECT * FROM (DECLARE @x := (SELECT 1) = (2, 3) SELECT 1)")
                    .fails(49, "unexpected token [@x] - unexpected bind expression");
        });
    }

    @Test
    public void testDeclareVariableWithTrailingDotBeforeBracketedQuery() throws Exception {
        assertMemoryLeak(() -> {
            // A number may end at its decimal point, which completes the declared value, so a
            // bracket after it starts the statement. The bracket used to open the value's next
            // operand instead, and the parse failed with a NullPointerException.
            assertQuery("DECLARE @a := 1. (SELECT @a AS a)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a
                            1.0
                            """);
            assertQuery("DECLARE @a := -1., @b := 1 + 2. (SELECT @a AS a, @b AS b)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a\tb
                            -1.0\t3.0
                            """);
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
            assertQuery("declare @ts := (timestamp > '2024-01-01') select timestamp, count() from trades where @ts;")
                    .noLeakCheck()
                    .assertsPlan(plan);
            assertQuery("declare @lo := '2024-01-01' select timestamp, count() from trades where timestamp > @lo;")
                    .noLeakCheck()
                    .assertsPlan(plan);
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
            assertQuery("declare @ts := (timestamp > '2024-01-01' and timestamp < '2024-08-23') select timestamp, count() from trades where @ts;")
                    .noLeakCheck()
                    .assertsPlan(plan);
            assertQuery("declare @lo := '2024-01-01', @hi := '2024-08-23' select timestamp, count() from trades where timestamp > @lo and timestamp < @hi;")
                    .noLeakCheck()
                    .assertsPlan(plan);
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
            assertQuery("declare @ts := (timestamp < '2024-08-23')  select timestamp, count() from trades where @ts;")
                    .noLeakCheck()
                    .assertsPlan(plan);
            assertQuery("declare @hi := '2024-08-23' select timestamp, count() from trades where timestamp < @hi;")
                    .noLeakCheck()
                    .assertsPlan(plan);
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
            assertQuery("declare @ts := (timestamp > '2024-01-01') select timestamp, count() from trades where @ts;")
                    .noLeakCheck()
                    .assertsPlan(plan);
            assertQuery("declare @lo := '2024-01-01' select timestamp, count() from trades where timestamp > @lo;")
                    .noLeakCheck()
                    .assertsPlan(plan);
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
            assertQuery("declare @ts := (timestamp >= '2024-01-01' and timestamp <= '2024-08-23') select timestamp, count() from trades where @ts;")
                    .noLeakCheck()
                    .assertsPlan(plan);
            assertQuery("declare @lo := '2024-01-01', @hi := '2024-08-23' select timestamp, count() from trades where timestamp >= @lo and timestamp <= @hi;")
                    .noLeakCheck()
                    .assertsPlan(plan);
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
            assertQuery("declare @ts := (timestamp <= '2024-08-23') select timestamp, count() from trades where @ts;")
                    .noLeakCheck()
                    .assertsPlan(plan);
            assertQuery("declare @hi := '2024-08-23' select timestamp, count() from trades where timestamp <= @hi;")
                    .noLeakCheck()
                    .assertsPlan(plan);
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
            assertQuery("declare @ts := (timestamp >= '2024-01-01') select timestamp, count() from trades where @ts;")
                    .noLeakCheck()
                    .assertsPlan(plan);
            assertQuery("declare @lo := '2024-01-01' select timestamp, count() from trades where timestamp >= @lo;")
                    .noLeakCheck()
                    .assertsPlan(plan);

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
            assertQuery("declare @ts := (timestamp between '2024-01-01' and '2024-08-23') select timestamp, count() from trades where @ts;")
                    .noLeakCheck()
                    .assertsPlan(plan);
            assertQuery("declare @lo := '2024-01-01', @hi := '2024-08-23' select timestamp, count() from trades where timestamp between @lo and @hi;")
                    .noLeakCheck()
                    .assertsPlan(plan);
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
            assertQuery("declare @ts1 := '2024-01-01', @ts2 := '2024-08-23' select timestamp, count() from trades where timestamp IN (@ts1, @ts2);")
                    .noLeakCheck()
                    .assertsPlan(plan);
            // A declared list is spliced into the IN it is used with, so it plans identically to
            // the written-out list above - including the interval scan the TIMESTAMP overload of
            // IN gives, which is the whole point of declaring the list rather than a string.
            assertQuery("declare @ts := ('2024-01-01', '2024-08-23') select timestamp, count() from trades where timestamp IN @ts")
                    .noLeakCheck()
                    .assertsPlan(plan);
            assertQuery("declare @ts := ('2024-01-01', '2024-08-23') select timestamp, count() from trades where timestamp IN (@ts)")
                    .noLeakCheck()
                    .assertsPlan(plan);
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
            assertQuery("declare @id := id, @val := 4 select * from x where @id < @val")
                    .noLeakCheck()
                    .assertsPlan(plan);
            assertQuery("declare @expr := (id < 4) select * from x where @expr")
                    .noLeakCheck()
                    .assertsPlan(plan);
            assertQuery("x where id < 4")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            id\tts
                            1\t1970-01-01T00:00:00.000000Z
                            2\t1970-01-01T00:16:40.000000Z
                            3\t1970-01-01T00:33:20.000000Z
                            """);
        });
    }

    @Test
    public void testDeclaredEmptyListNamesTheMistake() throws Exception {
        assertMemoryLeak(() -> {
            execute(TRADES_DDL);
            // An empty bracket pair is a list with nothing in it, never a scalar, so the lookahead
            // claims it. Left to the scalar parse it complained about ':=' having one argument,
            // which describes the parser's predicament rather than the user's mistake.
            assertQuery("DECLARE @s := () SELECT * FROM trades WHERE symbol IN @s")
                    .fails(15, "value expected in list");
        });
    }

    @Test
    public void testDeclaredListCannotNestInAnotherList() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE k (s SYMBOL)");
            // Flattening one list into another is not supported; `IN (@a, 'z')` already covers it.
            assertQuery("DECLARE @a := ('x','y'), @b := (@a, 'z') SELECT s FROM k WHERE s IN @b")
                    .fails(14, "declared list can only be used on the right-hand side of IN");
        });
    }

    @Test
    public void testDeclaredListInWindowClause() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE k (s SYMBOL)");
            execute("INSERT INTO k VALUES ('a'), ('b')");
            drainWalQueue();
            final String expected = """
                    s\tcount
                    a\t2
                    b\t2
                    """;
            // Declared variables are substituted inside window clauses, so a list reaches them too
            // and the splice pass has to walk them as well. It did not, and an IN in a window
            // partition failed with the marker's own token showing through as
            // `unknown function name: ()()`. It has to match the list written out in full.
            assertQuery("SELECT s, count() OVER (PARTITION BY s IN ('a','b')) FROM k")
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
            assertQuery("DECLARE @x := ('a','b') SELECT s, count() OVER (PARTITION BY s IN @x) FROM k")
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
            // A bare list is no more usable here than anywhere else, and has to say so rather than
            // leak the marker downstream.
            assertQuery("DECLARE @x := ('a','b') SELECT row_number() OVER (PARTITION BY @x) FROM k")
                    .fails(14, "declared list can only be used on the right-hand side of IN");
            assertQuery("DECLARE @x := ('a','b') SELECT count() OVER (ORDER BY @x) FROM k")
                    .fails(14, "declared list can only be used on the right-hand side of IN");
        });
    }

    @Test
    public void testDeclaredListInsideView() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE k (s SYMBOL)");
            execute("INSERT INTO k VALUES ('a'), ('b'), ('c')");
            drainWalQueue();
            // Reading a view parses its stored body as a subquery, where a bare ')' marks the end
            // of that subquery. A list has to keep its own closing bracket there, so this fails
            // even when the identical DECLARE works as a top-level query.
            execute("CREATE VIEW v_list AS (DECLARE @s := ('a','b') SELECT s FROM k WHERE s IN @s)");
            drainWalAndViewQueues();
            assertQuery("SELECT * FROM v_list")
                    .noLeakCheck()
                    .returns("""
                            s
                            a
                            b
                            """);
            execute("CREATE VIEW v_list_ovr AS (DECLARE OVERRIDABLE @s := ('a','b') SELECT s FROM k WHERE s IN @s)");
            drainWalAndViewQueues();
            assertQuery("DECLARE @s := ('b','c') SELECT * FROM v_list_ovr")
                    .noLeakCheck()
                    .returns("""
                            s
                            b
                            c
                            """);
        });
    }

    @Test
    public void testDeclaredListIsRejectedOutsideIn() throws Exception {
        assertMemoryLeak(() -> {
            execute(TRADES_DDL);
            // A list has no value of its own, so anything other than IN is a mistake worth naming
            // at parse time rather than leaving to fail obscurely further down.
            assertQuery("DECLARE @s := ('ETH-USD', 'BTC-USD') SELECT @s FROM trades")
                    .fails(14, "declared list can only be used on the right-hand side of IN");
            assertQuery("DECLARE @s := ('ETH-USD', 'BTC-USD') SELECT * FROM trades WHERE symbol = @s")
                    .fails(14, "declared list can only be used on the right-hand side of IN");
        });
    }

    @Test
    public void testDeclaredListKeepsElementTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE k (l LONG, d DOUBLE, c CHAR)");
            execute("INSERT INTO k VALUES (1, 1.5, 'a'), (2, 2.5, 'b'), (3, 3.5, 'c')");
            drainWalQueue();
            // Each element keeps its own type and picks the matching IN overload - there is no
            // array in the middle forcing them to a single element type.
            assertQuery("DECLARE @l := (1,3) SELECT l FROM k WHERE l IN @l")
                    .noLeakCheck()
                    .returns("""
                            l
                            1
                            3
                            """);
            assertQuery("DECLARE @c := ('a','c') SELECT c FROM k WHERE c IN @c")
                    .noLeakCheck()
                    .returns("""
                            c
                            a
                            c
                            """);
        });
    }

    @Test
    public void testDeclaredListLookaheadReadsCommentsLikeTheLexer() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE k (s SYMBOL, l LONG)");
            execute("INSERT INTO k VALUES ('a', 1), ('b', 2), ('c', 3)");
            drainWalQueue();
            // The lookahead that tells a list from a scalar or a sub-query has to read comments
            // exactly as the lexer does: a `--` comment ends at a lone '\r' as well as at '\n',
            // block comments nest, and quoted text inside a block comment does not close it.
            // Reading one differently hides a separator, or invents one inside the comment.
            final String expectedList = """
                    s
                    a
                    b
                    """;
            assertQuery("DECLARE @x := --c\r('a', 'b') SELECT s FROM k WHERE s IN @x")
                    .noLeakCheck()
                    .returns(expectedList);
            assertQuery("DECLARE @x := /* a /* b */ c */ ('a', 'b') SELECT s FROM k WHERE s IN @x")
                    .noLeakCheck()
                    .returns(expectedList);
            assertQuery("DECLARE @x := /* '*/' */ ('a', 'b') SELECT s FROM k WHERE s IN @x")
                    .noLeakCheck()
                    .returns(expectedList);
            assertQuery("DECLARE @x := (--c\r'a', 'b') SELECT s FROM k WHERE s IN @x")
                    .noLeakCheck()
                    .returns(expectedList);
            assertQuery("DECLARE @x := (/* '*/' */ 'a', 'b') SELECT s FROM k WHERE s IN @x")
                    .noLeakCheck()
                    .returns(expectedList);
            assertQuery("DECLARE @x := ('a' --c\r, 'b') SELECT s FROM k WHERE s IN @x")
                    .noLeakCheck()
                    .returns(expectedList);
            assertQuery("DECLARE @x := ('a' /* a /* b */ ) */ , 'b') SELECT s FROM k WHERE s IN @x")
                    .noLeakCheck()
                    .returns(expectedList);
            assertQuery("DECLARE @x := ('a' /* '*/' ) */ , 'b') SELECT s FROM k WHERE s IN @x")
                    .noLeakCheck()
                    .returns(expectedList);
            // A comma inside a nested comment is not a separator, so these stay a scalar and a
            // sub-query.
            assertQuery("DECLARE @x := (1 /* a /* b */ , */ + 2) SELECT l FROM k WHERE l = @x")
                    .noLeakCheck()
                    .returns("""
                            l
                            3
                            """);
            assertQuery("DECLARE @x := (/* a /* b */ c */ SELECT s, l FROM k WHERE l > 1) SELECT * FROM @x")
                    .noLeakCheck()
                    .returns("""
                            s\tl
                            b\t2
                            c\t3
                            """);
            // A comment in front of the first member must not hide a nested list, which the
            // expression parser would otherwise evaluate to its last member and silently drop 'b'.
            assertQuery("DECLARE @x := (/* a /* b */ c */ ('b','c'), 'a') SELECT s FROM k WHERE s IN @x")
                    .fails(33, "nested lists are not supported");
            assertQuery("DECLARE @x := (--c\r('b','c'), 'a') SELECT s FROM k WHERE s IN @x")
                    .fails(19, "nested lists are not supported");
            assertQuery("DECLARE @x := (/* '*/' */ ('b','c'), 'a') SELECT s FROM k WHERE s IN @x")
                    .fails(26, "nested lists are not supported");
        });
    }

    @Test
    public void testDeclaredListLookaheadSkipsCommentsAndQuotes() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE k (s SYMBOL)");
            execute("INSERT INTO k VALUES ('a'), ('b')");
            drainWalQueue();
            // Whether brackets hold a list is decided by looking ahead for a separator, so a comma
            // that only looks like one has to be skipped. A comma inside a comment or a
            // string is not a separator, and mistaking it for one turns a scalar into a list of
            // one - which reads the same but audits as an array instead of a value.
            assertQuery("DECLARE @x := ('a' /* , */) SELECT s FROM k WHERE s = @x")
                    .noLeakCheck()
                    .returns("""
                            s
                            a
                            """);
            assertQuery("DECLARE @x := ('a' -- ,\n) SELECT s FROM k WHERE s = @x")
                    .noLeakCheck()
                    .returns("""
                            s
                            a
                            """);
            assertQuery("DECLARE @x := ('a,b') SELECT s FROM k WHERE s = @x")
                    .noLeakCheck()
                    .returns("s\n");
            // ...and a real separator still makes a list, comments between members included.
            assertQuery("DECLARE @x := ('a', /* keep */ 'b') SELECT s FROM k WHERE s IN @x")
                    .noLeakCheck()
                    .returns("""
                            s
                            a
                            b
                            """);
            assertQuery("DECLARE @x := /* before */ ('a','b') SELECT s FROM k WHERE s IN @x")
                    .noLeakCheck()
                    .returns("""
                            s
                            a
                            b
                            """);
            // A member may itself contain the characters the scan is looking for.
            assertQuery("DECLARE @x := ('a)b', 'a') SELECT s FROM k WHERE s IN @x")
                    .noLeakCheck()
                    .returns("""
                            s
                            a
                            """);
            assertQuery("DECLARE @x := ('it''s', 'a') SELECT s FROM k WHERE s IN @x")
                    .noLeakCheck()
                    .returns("""
                            s
                            a
                            """);
        });
    }

    @Test
    public void testDeclaredListMatchesWrittenOutList() throws Exception {
        assertMemoryLeak(() -> {
            execute(TRADES_DDL);
            execute("""
                    INSERT INTO trades VALUES
                        ('ETH-USD','buy',1,1,'2024-01-01T00:00:00.000000Z'),
                        ('BTC-USD','sell',2,2,'2024-01-02T00:00:00.000000Z'),
                        ('SOL-USD','buy',3,3,'2024-01-03T00:00:00.000000Z')
                    """);
            drainWalQueue();
            final String expected = """
                    symbol
                    ETH-USD
                    BTC-USD
                    """;
            // Both spellings, and both must agree with the list written out in full.
            assertQuery("SELECT symbol FROM trades WHERE symbol IN ('ETH-USD','BTC-USD')")
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("DECLARE @s := ('ETH-USD','BTC-USD') SELECT symbol FROM trades WHERE symbol IN @s")
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("DECLARE @s := ('ETH-USD','BTC-USD') SELECT symbol FROM trades WHERE symbol IN (@s)")
                    .noLeakCheck()
                    .returns(expected);
            // NOT IN has to see the same expansion.
            assertQuery("DECLARE @s := ('ETH-USD','BTC-USD') SELECT symbol FROM trades WHERE symbol NOT IN @s")
                    .noLeakCheck()
                    .returns("""
                            symbol
                            SOL-USD
                            """);
        });
    }

    @Test
    public void testDeclaredListOfBindVariables() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE k (s SYMBOL, l LONG)");
            execute("INSERT INTO k VALUES ('a',1), ('b',2), ('c',3)");
            drainWalQueue();
            // Splicing at parse time is what lets a list hold bind variables: each one lands in IN
            // as its own argument and binds to its own type, which a typed-array literal carrying
            // the list could not do.
            bindVariableService.clear();
            bindVariableService.setStr(0, "a");
            bindVariableService.setStr(1, "c");
            assertQuery("DECLARE @s := ($1, $2) SELECT s FROM k WHERE s IN @s ORDER BY s")
                    .noLeakCheck()
                    .returns("""
                            s
                            a
                            c
                            """);
            // Re-binding the same plan to different values is the point of leaving them unbound.
            bindVariableService.setStr(0, "b");
            bindVariableService.setStr(1, "c");
            assertQuery("DECLARE @s := ($1, $2) SELECT s FROM k WHERE s IN @s ORDER BY s")
                    .noLeakCheck()
                    .returns("""
                            s
                            b
                            c
                            """);
            // A one-member list of a bind variable needs the trailing comma like any other.
            bindVariableService.clear();
            bindVariableService.setLong(0, 2L);
            assertQuery("DECLARE @l := ($1,) SELECT l FROM k WHERE l IN @l")
                    .noLeakCheck()
                    .returns("""
                            l
                            2
                            """);
            // Bind variables mix with literals on either side of the list.
            bindVariableService.clear();
            bindVariableService.setLong(0, 1L);
            assertQuery("DECLARE @l := ($1, 2) SELECT l FROM k WHERE l IN (@l, 3) ORDER BY l")
                    .noLeakCheck()
                    .returns("""
                            l
                            1
                            2
                            3
                            """);
        });
    }

    @Test
    public void testDeclaredListOfOnePlansLikeTheWrittenOutList() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE k (s SYMBOL, l LONG)");
            execute("INSERT INTO k VALUES ('a',1),('b',2),('c',3)");
            drainWalQueue();
            // A one-member list is the case where the splice can produce the right rows through the
            // wrong node: `IN @x` parses as an operator, `IN (2)` as a function, and only the
            // second reaches the JIT filter. Rows cannot tell the two apart, so assert the plan -
            // this is the shape that regressed while every row-level test stayed green.
            final String longPlan = """
                    Async JIT Filter workers: 1
                      filter: l in [2]
                        PageFrame
                            Row forward scan
                            Frame forward scan on: k
                    """;
            assertQuery("SELECT l FROM k WHERE l IN (2)").noLeakCheck().assertsPlan(longPlan);
            assertQuery("DECLARE @x := (2,) SELECT l FROM k WHERE l IN @x").noLeakCheck().assertsPlan(longPlan);
            assertQuery("DECLARE @x := (2,) SELECT l FROM k WHERE l IN (@x)").noLeakCheck().assertsPlan(longPlan);

            final String symbolPlan = """
                    Async JIT Filter workers: 1
                      filter: s in [a]
                        PageFrame
                            Row forward scan
                            Frame forward scan on: k
                    """;
            assertQuery("SELECT s FROM k WHERE s IN ('a')").noLeakCheck().assertsPlan(symbolPlan);
            assertQuery("DECLARE @x := ('a',) SELECT s FROM k WHERE s IN @x").noLeakCheck().assertsPlan(symbolPlan);
        });
    }

    @Test
    public void testDeclaredListSplicesInSourceOrder() throws Exception {
        assertMemoryLeak(() -> {
            execute(TRADES_DDL);
            execute("""
                    INSERT INTO trades VALUES
                        ('ETH-USD','buy',1,1,'2024-01-01T00:00:00.000000Z'),
                        ('BTC-USD','sell',2,2,'2024-01-02T00:00:00.000000Z'),
                        ('SOL-USD','buy',3,3,'2024-01-03T00:00:00.000000Z')
                    """);
            drainWalQueue();
            final String expected = """
                    symbol
                    ETH-USD
                    BTC-USD
                    SOL-USD
                    """;
            // A list mixes with literals on either side of it; IN holds its arguments in reverse,
            // so getting this wrong silently reorders or drops members.
            assertQuery("DECLARE @s := ('BTC-USD','SOL-USD') SELECT symbol FROM trades WHERE symbol IN ('ETH-USD', @s)")
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("DECLARE @s := ('ETH-USD','BTC-USD') SELECT symbol FROM trades WHERE symbol IN (@s, 'SOL-USD')")
                    .noLeakCheck()
                    .returns(expected);
            // One list may stand in for another.
            assertQuery("DECLARE @a := ('ETH-USD','BTC-USD'), @b := @a SELECT symbol FROM trades WHERE symbol IN @b")
                    .noLeakCheck()
                    .returns("""
                            symbol
                            ETH-USD
                            BTC-USD
                            """);
        });
    }

    @Test
    public void testDeclaredListTrailingCommaIsOptional() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE k (s SYMBOL)");
            execute("INSERT INTO k VALUES ('a'), ('b'), ('c')");
            drainWalQueue();
            final String expected = """
                    s
                    a
                    b
                    """;
            // The trailing comma is only needed to say "a list of one", which brackets alone cannot.
            // On a longer list it is accepted and means nothing, so neither spelling is the one way
            // to write a list.
            assertQuery("DECLARE @s := ('a','b') SELECT s FROM k WHERE s IN @s")
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("DECLARE @s := ('a','b',) SELECT s FROM k WHERE s IN @s")
                    .noLeakCheck()
                    .returns(expected);
        });
    }

    @Test
    public void testDeclaredListWithNullMembersMatchesWrittenOutList() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE n (s SYMBOL, l LONG, v VARCHAR, d DOUBLE, ts TIMESTAMP)");
            execute("""
                    INSERT INTO n VALUES
                        ('a', 1, 'a', 1.5, '2024-01-01T00:00:00.000000Z'),
                        (NULL, NULL, NULL, NULL, NULL),
                        ('b', 2, 'b', 2.5, '2024-01-02T00:00:00.000000Z')
                    """);
            drainWalQueue();
            // IN treats NULL as equal to NULL, so a NULL member of a written-out list matches the
            // NULL row and NOT IN drops that row. The splice has to hand a NULL member to IN like
            // any other member, neither dropping nor refusing it. Each column type reaches its
            // own IN overload, and the lists cover a lone NULL, a NULL on either side of a value,
            // and nothing but NULLs, under both operators.
            assertDeclaredListMatchesWrittenOut("s", " IN ", "NULL, 'a'", """
                    s
                    a

                    """);
            assertDeclaredListMatchesWrittenOut("s", " NOT IN ", "NULL,", """
                    s
                    a
                    b
                    """);
            assertDeclaredListMatchesWrittenOut("l", " IN ", "NULL,", """
                    l
                    null
                    """);
            assertDeclaredListMatchesWrittenOut("l", " NOT IN ", "1, NULL", """
                    l
                    2
                    """);
            assertDeclaredListMatchesWrittenOut("v", " IN ", "NULL, NULL", """
                    v

                    """);
            assertDeclaredListMatchesWrittenOut("v", " NOT IN ", "NULL, 'a'", """
                    v
                    b
                    """);
            assertDeclaredListMatchesWrittenOut("d", " IN ", "1.5, NULL", """
                    d
                    1.5
                    null
                    """);
            assertDeclaredListMatchesWrittenOut("d", " NOT IN ", "NULL, NULL", """
                    d
                    1.5
                    2.5
                    """);
            assertDeclaredListMatchesWrittenOut("ts", " IN ", "NULL,", """
                    ts

                    """);
            assertDeclaredListMatchesWrittenOut("ts", " NOT IN ", "NULL, '2024-01-01T00:00:00.000000Z'", """
                    ts
                    2024-01-02T00:00:00.000000Z
                    """);
        });
    }

    @Test
    public void testDeclaredVariableCanBeMarkedAudited() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE k (s SYMBOL)");
            execute("INSERT INTO k VALUES ('a'), ('b')");
            drainWalQueue();
            // AUDITED only marks the variable here; what a read of an audited view does with the
            // marking is an Enterprise concern. What OSS owns is that the marking parses, in
            // either order with OVERRIDABLE and on a list as readily as on a scalar.
            final String expected = """
                    s
                    a
                    """;
            assertQuery("DECLARE AUDITED @s := 'a' SELECT s FROM k WHERE s = @s")
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("DECLARE OVERRIDABLE AUDITED @s := 'a' SELECT s FROM k WHERE s = @s")
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("DECLARE AUDITED OVERRIDABLE @s := 'a' SELECT s FROM k WHERE s = @s")
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("DECLARE AUDITED @s := ('a',) SELECT s FROM k WHERE s IN @s")
                    .noLeakCheck()
                    .returns(expected);
        });
    }

    @Test
    public void testDeclaredVariableMarkedAuditedBesideNestedDeclares() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE audited (x LONG)");
            execute("INSERT INTO audited VALUES (1), (2), (3)");
            drainWalQueue();
            final String expected = """
                    x
                    2
                    3
                    """;
            // Only the AUDITED marking is confined to the top-level block. The top-level block keeps
            // it whatever follows, a set operation or a sub-query with a DECLARE of its own.
            assertQuery("DECLARE AUDITED @x := 1 SELECT @x a UNION ALL SELECT 2")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            a
                            1
                            2
                            """);
            assertQuery("""
                    DECLARE AUDITED @lim := 1
                    SELECT * FROM (DECLARE OVERRIDABLE @lo := 0 SELECT * FROM audited WHERE x > @lo)
                    WHERE x > @lim""")
                    .noLeakCheck()
                    .returns(expected);
            // Where no variable follows the word, it names a table in a nested DECLARE too, so
            // there is no marker to refuse.
            assertQuery("SELECT * FROM (DECLARE @lim := 1 audited WHERE x > @lim)")
                    .noLeakCheck()
                    .returns(expected);
        });
    }

    @Test
    public void testDeclaredVariableMarkedAuditedOnlyInTopLevelBlock() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE k (s SYMBOL)");
            // A read of an audited view records the AUDITED variables of the DECLARE block that
            // opens the view body, and no others. A marking in any other DECLARE would parse and
            // never be recorded, so the parser refuses it at the marker. It does so in every
            // statement, so a view body is refused on its own exactly when it is refused as a view.
            final String error = "AUDITED is only allowed in the top-level DECLARE block";
            // a sub-query in FROM, and a CTE
            assertQuery("SELECT * FROM (DECLARE AUDITED @x := 1 SELECT @x a)")
                    .noLeakCheck()
                    .fails(23, error);
            assertQuery("WITH c AS (DECLARE OVERRIDABLE AUDITED @x := 1 SELECT @x a) SELECT * FROM c")
                    .noLeakCheck()
                    .fails(31, error);
            // a set operation branch, which a caller's value for a view's variable still reaches
            assertQuery("SELECT 1 a UNION ALL DECLARE OVERRIDABLE AUDITED @x := 2 SELECT @x")
                    .noLeakCheck()
                    .fails(41, error);
            assertQuery("SELECT 1 a EXCEPT DECLARE AUDITED @x := 1 SELECT @x")
                    .noLeakCheck()
                    .fails(26, error);
            assertQuery("SELECT 1 a INTERSECT DECLARE AUDITED @x := 1 SELECT @x")
                    .noLeakCheck()
                    .fails(29, error);
            // a sub-query a join reads, lateral or not
            assertQuery("SELECT * FROM k JOIN (DECLARE AUDITED @x := 'a' SELECT @x s) j ON k.s = j.s")
                    .noLeakCheck()
                    .fails(30, error);
            assertQuery("SELECT * FROM k JOIN LATERAL (DECLARE AUDITED @x := 'a' SELECT count() c FROM k k2 WHERE k2.s = k.s OR k2.s = @x) j ON true")
                    .noLeakCheck()
                    .fails(38, error);
            // a sub-query in a declared value, and one in an expression
            assertQuery("DECLARE @q := (DECLARE AUDITED @y := 1 SELECT @y) SELECT * FROM @q")
                    .noLeakCheck()
                    .fails(23, error);
            assertQuery("SELECT s FROM k WHERE s IN (DECLARE AUDITED @y := 'b'::STRING SELECT @y)")
                    .noLeakCheck()
                    .fails(36, error);
            // the marker on a later declaration of the block, after OVERRIDABLE
            assertQuery("SELECT * FROM (DECLARE @a := 1, OVERRIDABLE AUDITED @x := 2 SELECT @a + @x)")
                    .noLeakCheck()
                    .fails(44, error);
            // Brackets around a whole statement make it a sub-query, as they do around a view body
            // inside the brackets CREATE VIEW takes.
            assertQuery("(DECLARE AUDITED @x := 1 SELECT @x a)")
                    .noLeakCheck()
                    .fails(9, error);
        });
    }

    @Test
    public void testDeclaredVariableMarkedAuditedOnlyInTopLevelBlockOfOtherStatements() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE k (s SYMBOL, l LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE dest (s SYMBOL, l LONG)");
            // The refusal belongs to the DECLARE block, not to the statement around it: a nested
            // AUDITED marker is refused at the marker in every statement that parses a query.
            final String error = "AUDITED is only allowed in the top-level DECLARE block";
            // UPDATE: a sub-query in WHERE, and one in FROM
            assertExceptionNoLeakCheck("UPDATE dest SET l = 1 WHERE l < (DECLARE AUDITED @x := 1 SELECT @x)", 41, error);
            assertExceptionNoLeakCheck("UPDATE dest SET l = 1 FROM (DECLARE AUDITED @x := 'a' SELECT @x s) v WHERE dest.s = v.s", 36, error);
            // INSERT ... SELECT
            assertExceptionNoLeakCheck("INSERT INTO dest SELECT * FROM (DECLARE AUDITED @x := 'a' SELECT @x s, 1L l)", 40, error);
            // CREATE TABLE AS: a sub-query of the body, and a set operation branch of it
            assertExceptionNoLeakCheck("CREATE TABLE c_sub AS (SELECT * FROM (DECLARE AUDITED @x := 1 SELECT @x a))", 46, error);
            assertExceptionNoLeakCheck("CREATE TABLE c_union AS (SELECT 1 a UNION ALL DECLARE AUDITED @x := 2 SELECT @x)", 54, error);
            // a materialized view body: a sub-query in FROM, one in WHERE, and a CTE
            assertExceptionNoLeakCheck(
                    "CREATE MATERIALIZED VIEW mv_sub AS (SELECT ts, count() c FROM (DECLARE AUDITED @x := 1 SELECT ts, l FROM k WHERE l > @x) SAMPLE BY 1h) PARTITION BY DAY",
                    71,
                    error
            );
            assertExceptionNoLeakCheck(
                    "CREATE MATERIALIZED VIEW mv_where AS (SELECT ts, count() c FROM k WHERE l > (DECLARE AUDITED @x := 1 SELECT @x) SAMPLE BY 1h) PARTITION BY DAY",
                    85,
                    error
            );
            assertExceptionNoLeakCheck(
                    "CREATE MATERIALIZED VIEW mv_cte AS (WITH c AS (DECLARE AUDITED @x := 1 SELECT ts, l FROM k WHERE l > @x) SELECT ts, count() c FROM c SAMPLE BY 1h) PARTITION BY DAY",
                    55,
                    error
            );
            // EXPLAIN parses the statement it explains
            assertExceptionNoLeakCheck("EXPLAIN SELECT * FROM (DECLARE AUDITED @x := 1 SELECT @x a)", 31, error);

            // None of them created anything.
            drainWalAndMatViewQueues();
            Assert.assertNull(engine.getTableTokenIfExists("c_sub"));
            Assert.assertNull(engine.getTableTokenIfExists("c_union"));
            Assert.assertNull(engine.getTableTokenIfExists("mv_sub"));
            Assert.assertNull(engine.getTableTokenIfExists("mv_where"));
            Assert.assertNull(engine.getTableTokenIfExists("mv_cte"));
        });
    }

    @Test
    public void testDeclaredVariableMarkerLookaheadReadsCommentsLikeTheLexer() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE audited (x LONG)");
            execute("INSERT INTO audited VALUES (1), (2), (3)");
            drainWalQueue();
            // Whether AUDITED or OVERRIDABLE marks a declaration or names a table depends on what
            // follows the word, so the lookahead has to read any comment in between exactly as the
            // lexer does: a `--` comment ends at a lone '\r' as well as at '\n', block comments
            // nest, and quoted text inside a block comment does not close it.
            final String expected = """
                    y
                    2
                    """;
            assertQuery("DECLARE OVERRIDABLE --c\r@y := 2 SELECT @y AS y")
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
            assertQuery("DECLARE OVERRIDABLE /* a /* b */ c */ @y := 2 SELECT @y AS y")
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
            assertQuery("DECLARE OVERRIDABLE /* '*/' */ @y := 2 SELECT @y AS y")
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
            assertQuery("DECLARE AUDITED --c\r@y := 2 SELECT @y AS y")
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
            assertQuery("DECLARE AUDITED /* a /* b */ c */ @y := 2 SELECT @y AS y")
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
            assertQuery("DECLARE AUDITED /* '*/' */ @y := 2 SELECT @y AS y")
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
            assertQuery("DECLARE @x := 1, OVERRIDABLE --c\r@y := 2 SELECT @x + @y AS y")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            y
                            3
                            """);
            // A comment that hides what only looks like a variable leaves the word to the query.
            assertQuery("DECLARE @lim := 1 audited /* x /* y */ @ */ WHERE x > @lim")
                    .noLeakCheck()
                    .returns("""
                            x
                            2
                            3
                            """);
            // The view body is parsed again at every read, and the caller's value shows the
            // OVERRIDABLE marking taking effect behind the comment.
            execute("CREATE VIEW v_marked AS (DECLARE OVERRIDABLE /* '*/' */ @lim := 1 SELECT * FROM audited WHERE x > @lim)");
            drainWalAndViewQueues();
            assertQuery("SELECT * FROM v_marked")
                    .noLeakCheck()
                    .returns("""
                            x
                            2
                            3
                            """);
            assertQuery("DECLARE @lim := 2 SELECT * FROM v_marked")
                    .noLeakCheck()
                    .returns("""
                            x
                            3
                            """);
        });
    }

    @Test
    public void testDeclaredVariableMarkerMisuse() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE k (s SYMBOL)");
            // A repeated marker is a typo, not a stronger marking, and saying which one repeated
            // is the whole value of the message.
            assertQuery("DECLARE AUDITED AUDITED @s := 'a' SELECT s FROM k")
                    .fails(16, "duplicate AUDITED");
            assertQuery("DECLARE OVERRIDABLE OVERRIDABLE @s := 'a' SELECT s FROM k")
                    .fails(20, "duplicate OVERRIDABLE");
            assertQuery("DECLARE AUDITED OVERRIDABLE AUDITED @s := 'a' SELECT s FROM k")
                    .fails(28, "duplicate AUDITED");
            // A marker with nothing to mark names what it was expecting.
            assertQuery("DECLARE AUDITED := 'a' SELECT s FROM k")
                    .fails(16, "variable name expected after AUDITED");
            assertQuery("DECLARE OVERRIDABLE := 'a' SELECT s FROM k")
                    .fails(20, "variable name expected after OVERRIDABLE");
            assertQuery("DECLARE OVERRIDABLE AUDITED := 'a' SELECT s FROM k")
                    .fails(28, "variable name expected after OVERRIDABLE/AUDITED");
            // A variable that lacks its '@' is still a declaration, not a table named `audited`.
            assertQuery("DECLARE AUDITED s := 'a' SELECT s FROM k")
                    .fails(16, "variable name expected after AUDITED");
            // The error points at the token in the variable's place, past any whitespace or
            // comment after the marker.
            assertQuery("DECLARE AUDITED    s := 'a' SELECT s FROM k")
                    .fails(19, "variable name expected after AUDITED");
            assertQuery("DECLARE OVERRIDABLE    := 'a' SELECT s FROM k")
                    .fails(23, "variable name expected after OVERRIDABLE");
            assertQuery("DECLARE AUDITED--c\ns := 'a' SELECT s FROM k")
                    .fails(19, "variable name expected after AUDITED");
        });
    }

    @Test
    public void testDeclaredVariableMarkerMisuseOutsideTopLevelBlock() throws Exception {
        assertMemoryLeak(() -> {
            // Outside the top-level block AUDITED is refused where it stands, whatever follows it.
            // So a nested marker that is also malformed reports the refusal at its first AUDITED
            // rather than the duplicate or the missing variable name further on: the marker has to
            // go, and mending it would only bring the refusal up next.
            final String error = "AUDITED is only allowed in the top-level DECLARE block";
            assertQuery("SELECT * FROM (DECLARE AUDITED AUDITED @x := 1 SELECT @x a)")
                    .noLeakCheck()
                    .fails(23, error);
            assertQuery("SELECT * FROM (DECLARE AUDITED OVERRIDABLE AUDITED @x := 1 SELECT @x a)")
                    .noLeakCheck()
                    .fails(23, error);
            assertQuery("SELECT * FROM (DECLARE AUDITED := 1 SELECT 1 a)")
                    .noLeakCheck()
                    .fails(23, error);
            assertQuery("SELECT * FROM (DECLARE OVERRIDABLE AUDITED := 1 SELECT 1 a)")
                    .noLeakCheck()
                    .fails(35, error);
            assertQuery("SELECT * FROM (DECLARE AUDITED x := 1 SELECT 1 a)")
                    .noLeakCheck()
                    .fails(23, error);
            // in a CTE and in a set operation branch
            assertQuery("WITH c AS (DECLARE AUDITED AUDITED @x := 1 SELECT @x a) SELECT * FROM c")
                    .noLeakCheck()
                    .fails(19, error);
            assertQuery("SELECT 1 a UNION ALL DECLARE AUDITED := 2 SELECT 2")
                    .noLeakCheck()
                    .fails(29, error);
            // Markers are read left to right, so a mistake ahead of the first AUDITED still wins.
            assertQuery("SELECT * FROM (DECLARE OVERRIDABLE OVERRIDABLE AUDITED @x := 1 SELECT @x a)")
                    .noLeakCheck()
                    .fails(35, "duplicate OVERRIDABLE");
            // OVERRIDABLE is not confined to the top-level block and keeps its own errors there.
            assertQuery("SELECT * FROM (DECLARE OVERRIDABLE OVERRIDABLE @x := 1 SELECT @x a)")
                    .noLeakCheck()
                    .fails(35, "duplicate OVERRIDABLE");
            assertQuery("SELECT * FROM (DECLARE OVERRIDABLE := 1 SELECT 1 a)")
                    .noLeakCheck()
                    .fails(35, "variable name expected after OVERRIDABLE");
        });
    }

    @Test
    public void testDeclaredVariableMarkerWordNamesTableInImplicitSelect() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE audited (x LONG)");
            execute("CREATE TABLE overridable (x LONG)");
            execute("INSERT INTO audited VALUES (1), (2), (3)");
            execute("INSERT INTO overridable VALUES (1), (2), (3)");
            // AUDITED and OVERRIDABLE mark a declaration only when a variable follows them. Where
            // the query begins, the same words are table names in the implicit SELECT * FROM form,
            // whether or not a comma closes the DECLARE block.
            final String expected = """
                    x
                    2
                    3
                    """;
            assertQuery("DECLARE @lim := 1 audited WHERE x > @lim")
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("DECLARE @lim := 1 overridable WHERE x > @lim")
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("DECLARE @lim := 1, audited WHERE x > @lim")
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("DECLARE @lim := 1, overridable WHERE x > @lim")
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("DECLARE @lim := 1 audited overridable WHERE overridable.x > @lim")
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("DECLARE @lim := 1, overridable")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x
                            1
                            2
                            3
                            """);
            // Marked declarations may still precede such a table name. The view below shows the
            // OVERRIDABLE marking still taking effect there.
            assertQuery("DECLARE @lo := 1, OVERRIDABLE AUDITED @hi := 3 audited WHERE x > @lo AND x < @hi")
                    .noLeakCheck()
                    .returns("""
                            x
                            2
                            """);
            execute("CREATE VIEW v_audited AS (DECLARE OVERRIDABLE @lim := 1 audited WHERE x > @lim)");
            drainWalAndViewQueues();
            assertQuery("SELECT * FROM v_audited")
                    .noLeakCheck()
                    .returns(expected);
            assertQuery("DECLARE @lim := 2 SELECT * FROM v_audited")
                    .noLeakCheck()
                    .returns("""
                            x
                            3
                            """);
        });
    }

    @Test
    public void testDeclaredVariablesExpandToTooManyNodes() throws Exception {
        assertMemoryLeak(() -> {
            // Every reference to a variable reads a copy of its value, and each declaration here
            // reads the one before it twice, so the copies double with every level: 131,038 nodes
            // at 15 levels, and 4.2 million at 20, held in a pool that never shrinks for as long as
            // the compiler lives. The parser refuses the reference whose copy would take the
            // statement past its budget, the second read of @v14, before it copies anything.
            assertExceptionNoLeakCheck(
                    """
                            DECLARE
                                @v0 := 1,
                                @v1 := @v0 + @v0,
                                @v2 := @v1 + @v1,
                                @v3 := @v2 + @v2,
                                @v4 := @v3 + @v3,
                                @v5 := @v4 + @v4,
                                @v6 := @v5 + @v5,
                                @v7 := @v6 + @v6,
                                @v8 := @v7 + @v7,
                                @v9 := @v8 + @v8,
                                @v10 := @v9 + @v9,
                                @v11 := @v10 + @v10,
                                @v12 := @v11 + @v11,
                                @v13 := @v12 + @v12,
                                @v14 := @v13 + @v13,
                                @v15 := @v14 + @v14
                            SELECT 1
                            """,
                    362,
                    "declared variables expand to too many expression nodes [max=100000]"
            );

            // A view's body expands with the statement that reads it, and its copies count against
            // that statement: one read of this body copies 65,507 nodes, and a second read crosses
            // the budget.
            execute("CREATE VIEW v_chain AS (" + declaredVariableChain(14) + " SELECT @v1 x FROM long_sequence(1))");
            drainWalAndViewQueues();
            assertQuery("SELECT * FROM v_chain")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x
                            2
                            """);
            assertQuery("SELECT * FROM v_chain UNION ALL SELECT * FROM v_chain")
                    .noLeakCheck()
                    .failsWith("declared variables expand to too many expression nodes [max=100000]");
        });
    }

    @Test
    public void testDeclaredVariablesExpandToTooManyNodesThroughSubQueryReferences() throws Exception {
        assertMemoryLeak(() -> {
            // A copy of a value shares the value's sub-queries, but holds a reference to each of
            // them, and a value can hold many: each read of @y below copies one node that refers
            // to @x 400 times. The budget counts each reference as a node, so @y's declaration
            // counts 400 and each read of @y 401, and the 249th read is the one it refuses, when
            // the statement has copied only 248 nodes.
            final StringBuilder sql = new StringBuilder("DECLARE @x := (SELECT 1 x), @y := coalesce(@x");
            for (int i = 1; i < 400; i++) {
                sql.append(", @x");
            }
            sql.append("), @z := @y");
            int refusedReadPosition = -1;
            for (int i = 1; i < 300; i++) {
                sql.append(" + ");
                if (i == 248) {
                    refusedReadPosition = sql.length();
                }
                sql.append("@y");
            }
            sql.append(" SELECT 1");
            assertExceptionNoLeakCheck(
                    sql,
                    refusedReadPosition,
                    "declared variables expand to too many expression nodes [max=100000]"
            );
        });
    }

    @Test
    public void testDeclaredVariablesWithinExpansionBudget() throws Exception {
        assertMemoryLeak(() -> {
            // Declaring @v0 to @v14 copies 65,504 nodes, and the reads copy 34,496 more: 32,767
            // for @v14, then 1,023, 511, 127, 63, 3, 1 and 1. That is the budget exactly.
            final String atBudget = declaredVariableChain(14) + " SELECT @v14 + @v9 + @v8 + @v6 + @v5 + @v1 + @v0 + @v0 x";
            assertQuery(atBudget)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x
                            17252
                            """);
            // One more read of @v0 copies one node too many.
            final String pastBudget = atBudget.substring(0, atBudget.length() - 2) + " + @v0 x";
            assertExceptionNoLeakCheck(
                    pastBudget,
                    pastBudget.length() - 5,
                    "declared variables expand to too many expression nodes [max=100000]"
            );

            // 101 reads of a declared sub-query parse 100 copies, the budget exactly.
            execute("CREATE TABLE k (s SYMBOL, l LONG)");
            execute("INSERT INTO k VALUES ('a', 1), ('b', 2), ('c', 3)");
            final StringBuilder reads = new StringBuilder("DECLARE @x := (SELECT s FROM k WHERE l > 1) SELECT count() FROM k WHERE s IN @x");
            for (int i = 1; i < 101; i++) {
                reads.append(" AND s IN @x");
            }
            assertQuery(reads)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count
                            2
                            """);
            // A 102nd read is one copy too many.
            assertExceptionNoLeakCheck(
                    reads.append(" AND s IN @x"),
                    15,
                    "declared sub-queries are read too many times [max=100]"
            );
        });
    }

    @Test
    public void testLiteralListCannotNestInAnotherList() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE k (s SYMBOL, l LONG)");
            execute("INSERT INTO k VALUES ('a',1),('b',2)");
            drainWalQueue();
            // The same mistake written out rather than through a variable. It has to be refused for
            // the same reason, and more sharply: the expression parser reads `('b','c')` as a
            // parenthesised scalar and evaluates it to its last member, so this silently meant
            // `('a','c')` and dropped 'b'. Losing members without saying so is the behaviour a
            // declared list exists to replace, so it cannot be the behaviour a declared list has.
            assertQuery("DECLARE @x := ('a', ('b','c')) SELECT s FROM k WHERE s IN @x")
                    .fails(20, "nested lists are not supported");
            // ...and in first position, where the element start is found differently.
            assertQuery("DECLARE @x := (('x','y'), 'z') SELECT s FROM k WHERE s IN @x")
                    .fails(15, "nested lists are not supported");
            // A bracketed element with no separator is not a nested list. It is a parenthesised
            // scalar, and it keeps working exactly as it does anywhere else.
            assertQuery("DECLARE @x := ((1+1), 3) SELECT l FROM k WHERE l IN @x")
                    .noLeakCheck()
                    .returns("""
                            l
                            2
                            """);
            // Nor is a function call, whose commas belong to the call.
            assertQuery("DECLARE @x := (greatest(1, 2), 9) SELECT l FROM k WHERE l IN @x")
                    .noLeakCheck()
                    .returns("""
                            l
                            2
                            """);
        });
    }

    @Test
    public void testParenthesisedScalarIsNotAList() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE k (l LONG)");
            execute("INSERT INTO k VALUES (1), (2), (3)");
            drainWalQueue();
            // Only a comma directly inside the brackets makes a list. Arithmetic, a single value
            // and a call's own argument commas must all stay scalar.
            assertQuery("DECLARE @x := (1+2) SELECT l FROM k WHERE l = @x")
                    .noLeakCheck()
                    .returns("""
                            l
                            3
                            """);
            // ...but a trailing comma makes it a list of one, which IN treats the same way.
            assertQuery("DECLARE @x := (2,) SELECT l FROM k WHERE l IN @x")
                    .noLeakCheck()
                    .returns("""
                            l
                            2
                            """);
            assertQuery("DECLARE @x := (2) SELECT l FROM k WHERE l IN @x")
                    .noLeakCheck()
                    .returns("""
                            l
                            2
                            """);
            assertQuery("DECLARE @x := (greatest(1, 3)) SELECT l FROM k WHERE l = @x")
                    .noLeakCheck()
                    .returns("""
                            l
                            3
                            """);
            // A comma inside quoted text is not a separator either.
            assertQuery("DECLARE @x := ('a,b') SELECT @x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a,b
                            a,b
                            """);
        });
    }

    private static void assertEachClosedOnce(ObjList<CloseCountingRecordCursorFactory> factories, int minFactoryCount) {
        Assert.assertTrue("instantiated factories: " + factories.size(), factories.size() >= minFactoryCount);
        for (int i = 0, n = factories.size(); i < n; i++) {
            Assert.assertEquals("factory " + i + " of " + n, 1, factories.getQuick(i).getCloseCount());
        }
    }

    // Returns how many lexers the compiler's parser holds from the pool that view bodies and
    // copies of declared sub-queries parse with. The parser holds them from one compile until the
    // next begins, whether the compile fails or not. Neither the compiler nor the parser exposes
    // the pool, hence the reflection.
    private static int countViewLexersHeld(SqlCompilerImpl compiler) throws Exception {
        final Field parserField = SqlCompilerImpl.class.getDeclaredField("parser");
        parserField.setAccessible(true);
        final Field viewLexersField = SqlParser.class.getDeclaredField("viewLexers");
        viewLexersField.setAccessible(true);
        return ((ObjectPool<?>) viewLexersField.get(parserField.get(compiler))).getPos();
    }

    // Compiles the statement with a compiler of its own, so the count is that of this statement
    // alone.
    private static int countViewLexersHeld(String sql) throws Exception {
        try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
            Misc.free(compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory());
            return countViewLexersHeld(compiler);
        }
    }

    // Writes x = 3, 2, 1 to q.parquet, so that the first row read from the file is not the
    // first of long_sequence().
    private static void createParquetFile() throws Exception {
        execute("CREATE TABLE src AS (SELECT 4 - x x FROM long_sequence(3))");
        try (
                Path path = new Path();
                PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                TableReader reader = engine.getReader("src")
        ) {
            path.of(root).concat("q.parquet");
            PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
            PartitionEncoder.encode(partitionDescriptor, path);
        }
        inputRoot = root;
    }

    // Declares a sub-query that reads the given table or view and five more on top of it, each
    // reading the one before it twice, and reads the last: 32 reads of the first.
    private static String declaredQueryChainOver(String source) {
        final StringBuilder sql = new StringBuilder("DECLARE @q0 := (SELECT l FROM ").append(source).append(')');
        for (int i = 1; i < 6; i++) {
            sql.append(", @q").append(i).append(" := (SELECT * FROM @q").append(i - 1).append(" UNION ALL SELECT * FROM @q").append(i - 1).append(')');
        }
        return sql.append(" SELECT count(), sum(l) FROM @q5").toString();
    }

    // Declares @v0 := 1 and, up to the given depth, variables that each read the one before them
    // twice, so @vN is 2^N. Declaring them copies 2^(depth+2) - 4 - 2 * depth nodes, and a read
    // of @vN copies 2^(N+1) - 1 more.
    private static String declaredVariableChain(int depth) {
        final StringBuilder sql = new StringBuilder("DECLARE @v0 := 1");
        for (int i = 1; i <= depth; i++) {
            sql.append(", @v").append(i).append(" := @v").append(i - 1).append(" + @v").append(i - 1);
        }
        return sql.toString();
    }

    // Asserts that the list written out in full and the same members declared as @x return the
    // expected rows of table n. A trailing comma marks a declared list of one, which the
    // written-out list spells without it.
    private void assertDeclaredListMatchesWrittenOut(String column, String operator, String members, String expected) throws Exception {
        final String writtenMembers = members.endsWith(",") ? members.substring(0, members.length() - 1) : members;
        assertQuery("SELECT " + column + " FROM n WHERE " + column + operator + "(" + writtenMembers + ")")
                .noLeakCheck()
                .returns(expected);
        assertQuery("DECLARE @x := (" + members + ") SELECT " + column + " FROM n WHERE " + column + operator + "@x")
                .noLeakCheck()
                .returns(expected);
    }
}
