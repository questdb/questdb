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

import io.questdb.mp.WorkerPool;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import java.util.UUID;

public class FirstNotNullGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testAllNull() throws Exception {
        assertQuery(
                """
                        a0\ta1\ta2\ta3\ta4\ta5\ta6\ta7\ta8\ta9\ta10\ta11\ta12\ta13\ta14
                        \t\tnull\tnull\tnull\tnull\t\t\t\t\t\t\t\t\t
                        """,
                "select first_not_null(a0) a0," +
                        "     first_not_null(a1) a1," +
                        "     first_not_null(a2) a2," +
                        "     first_not_null(a3) a3," +
                        "     first_not_null(a4) a4," +
                        "     first_not_null(a5) a5," +
                        "     first_not_null(a6) a6," +
                        "     first_not_null(a7) a7," +
                        "     first_not_null(a8) a8," +
                        "     first_not_null(a9) a9, " +
                        "     first_not_null(a10) a10, " +
                        "     first_not_null(a11) a11, " +
                        "     first_not_null(a12) a12, " +
                        "     first_not_null(a13) a13, " +
                        "     first_not_null(a14) a14 " +
                        "from tab",
                "create table tab as ( " +
                        "select cast(null as char) a0," +
                        "       cast(null as date) a1," +
                        "       cast(null as double) a2," +
                        "       cast(null as float) a3," +
                        "       cast(null as int) a4," +
                        "       cast(null as long) a5," +
                        "       cast(null as symbol) a6," +
                        "       cast(null as timestamp) a7," +
                        "       cast(null as uuid) a8," +
                        "       cast(null as string) a9," +
                        "       cast(null as geohash(5b)) a10, " +
                        "       cast(null as geohash(10b)) a11, " +
                        "       cast(null as geohash(25b)) a12, " +
                        "       cast(null as geohash(35b)) a13, " +
                        "       cast(null as ipv4) a14 " +
                        "from long_sequence(3))",
                null,
                false,
                true
        );
    }

    @Test
    public void testFirstNotNull() throws Exception {
        UUID firstUuid = UUID.randomUUID();

        execute("create table tab (a0 char," +
                "a1 date," +
                "a2 double," +
                "a3 float," +
                "a4 int," +
                "a5 long," +
                "a6 symbol," +
                "a7 timestamp," +
                "a8 uuid," +
                "a9 string," +
                "a10 geohash(5b)," +
                "a11 geohash(10b)," +
                "a12 geohash(25b)," +
                "a13 geohash(35b)," +
                "a14 ipv4 " +
                ")");

        execute("insert into tab (a1) values (null)"); // other columns default to null

        execute("insert into tab values(" +
                "'a', " +
                "to_date('2023-10-23','yyyy-MM-dd')," +
                "2.2," +
                "3.3," +
                "4," +
                "5," +
                "'a_symbol'," +
                "to_timestamp('2023-10-23T12:34:59.000000','yyyy-MM-ddTHH:mm:ss.SSSUUU')," +
                "'" + firstUuid + "'," +
                "'a_string'," +
                " #u," +
                " #uu," +
                " #uuuuu," +
                " #uuuuuuu, " +
                " '1.0.0.0'" +
                ")");

        execute("insert into tab values(" +
                "'b'," +
                "to_date('2023-10-22','yyyy-MM-dd')," +
                "22.2," +
                "33.3," +
                "44," +
                "55," +
                "'b_symbol'," +
                "to_timestamp('2023-10-22T01:02:03.000000','yyyy-MM-ddTHH:mm:ss.SSSUUU')," +
                "rnd_uuid4()," +
                "'b_string'," +
                " #v," +
                " #vv," +
                " #vvvvv," +
                " #vvvvvvv, " +
                " '2.0.0.0'" +
                ")");

        assertSql(
                "a0\ta1\ta2\ta3\ta4\ta5\ta6\ta7\ta8\ta9\ta10\ta11\ta12\ta13\ta14\n" +
                        "a\t2023-10-23T00:00:00.000Z\t2.2\t3.3\t4\t5\ta_symbol\t2023-10-23T12:34:59.000000Z\t" + firstUuid + "\ta_string\tu\tuu\tuuuuu\tuuuuuuu\t1.0.0.0\n",
                "select first_not_null(a0) a0," +
                        "     first_not_null(a1) a1," +
                        "     first_not_null(a2) a2," +
                        "     first_not_null(a3) a3," +
                        "     first_not_null(a4) a4," +
                        "     first_not_null(a5) a5," +
                        "     first_not_null(a6) a6," +
                        "     first_not_null(a7) a7," +
                        "     first_not_null(a8) a8," +
                        "     first_not_null(a9) a9, " +
                        "     first_not_null(a10) a10, " +
                        "     first_not_null(a11) a11, " +
                        "     first_not_null(a12) a12, " +
                        "     first_not_null(a13) a13, " +
                        "     first_not_null(a14) a14 " +
                        "from tab"
        );
    }

    @Test
    public void testMergeWithNullDestValue() throws Exception {
        // Regression test for a bug in FirstNotNull*GroupByFunction.merge().
        // When parallel GROUP BY processes partitions across workers:
        //   - Worker A processes partition 1 (earlier rows, all nulls for a key):
        //     computeFirst() stores a valid rowId with null value
        //   - Worker B processes partition 2 (later rows, non-null values):
        //     stores a higher rowId with a valid value
        //   - Merge B into A: srcRowId > destRowId and destRowId != LONG_NULL,
        //     so merge() incorrectly discards the non-null value.
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        execute(
                                compiler,
                                """
                                        CREATE TABLE tab (
                                            key SYMBOL,
                                            a_char CHAR,
                                            a_date DATE,
                                            a_double DOUBLE,
                                            a_float FLOAT,
                                            a_int INT,
                                            a_long LONG,
                                            a_sym SYMBOL,
                                            a_ts TIMESTAMP,
                                            a_uuid UUID,
                                            a_str STRING,
                                            a_varchar VARCHAR,
                                            ts TIMESTAMP
                                        ) TIMESTAMP(ts) PARTITION BY DAY""",
                                sqlExecutionContext
                        );

                        // Partition 1 (day 1): 1000 rows with key='k1', all value columns null.
                        // computeFirst() stores a valid rowId with null values.
                        execute(
                                compiler,
                                """
                                        INSERT INTO tab
                                        SELECT 'k1',
                                            NULL::CHAR,
                                            NULL::DATE,
                                            NULL::DOUBLE,
                                            NULL::FLOAT,
                                            NULL::INT,
                                            NULL::LONG,
                                            NULL::SYMBOL,
                                            NULL::TIMESTAMP,
                                            NULL::UUID,
                                            NULL::STRING,
                                            NULL::VARCHAR,
                                            timestamp_sequence('1970-01-01', 1_000_000) ts
                                        FROM long_sequence(1000)""",
                                sqlExecutionContext
                        );

                        // Partition 2 (day 2): 1000 rows with key='k1', non-null values.
                        execute(
                                compiler,
                                """
                                        INSERT INTO tab
                                        SELECT 'k1',
                                            'A',
                                            to_date('2024-01-01', 'yyyy-MM-dd'),
                                            3.14,
                                            2.72::FLOAT,
                                            42,
                                            123456789,
                                            'sym_val',
                                            to_timestamp('2024-01-01T12:00:00.000000', 'yyyy-MM-ddTHH:mm:ss.SSSUUU'),
                                            'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
                                            'hello',
                                            'world'::VARCHAR,
                                            timestamp_sequence('1970-01-02', 1_000_000) ts
                                        FROM long_sequence(1000)""",
                                sqlExecutionContext
                        );

                        String query = """
                                SELECT key,
                                    first_not_null(a_char) a_char,
                                    first_not_null(a_date) a_date,
                                    first_not_null(a_double) a_double,
                                    first_not_null(a_float) a_float,
                                    first_not_null(a_int) a_int,
                                    first_not_null(a_long) a_long,
                                    first_not_null(a_sym) a_sym,
                                    first_not_null(a_ts) a_ts,
                                    first_not_null(a_uuid) a_uuid,
                                    first_not_null(a_str) a_str,
                                    first_not_null(a_varchar) a_varchar
                                FROM tab""";

                        // Run with single-threaded GROUP BY (correct baseline).
                        sqlExecutionContext.setParallelGroupByEnabled(false);
                        try {
                            TestUtils.printSql(engine, sqlExecutionContext, query, sink);
                        } finally {
                            sqlExecutionContext.setParallelGroupByEnabled(true);
                        }

                        // Run with parallel GROUP BY (exercises merge).
                        sqlExecutionContext.setParallelGroupByEnabled(true);
                        StringSink parallelSink = new StringSink();
                        TestUtils.printSql(engine, sqlExecutionContext, query, parallelSink);

                        TestUtils.assertEquals(sink, parallelSink);
                    },
                    configuration,
                    LOG
            );
        });
    }
}
