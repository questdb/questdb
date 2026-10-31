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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class FirstSymbolGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testFirstAndLastResolveIndependently() throws Exception {
        assertMemoryLeak(() -> {
            for (String cacheFlag : new String[]{"CACHE", "NOCACHE"}) {
                execute("CREATE TABLE tab (k SYMBOL, s SYMBOL " + cacheFlag + ", ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
                execute("""
                        INSERT INTO tab VALUES
                        ('k1', 'abc', '2024-01-01T00:00:00.000000Z'),
                        ('k1', 'b', '2024-01-01T01:00:00.000000Z'),
                        ('k2', 'c', '2024-01-01T02:00:00.000000Z'),
                        ('k2', 'c', '2024-01-01T03:00:00.000000Z')
                        """);
                assertQuery("""
                        SELECT k,
                            nullif(first(s), last(s)) n,
                            starts_with(first(s), last(s)) sw,
                            strpos(first(s), last(s)) p,
                            replace(first(s), last(s), 'Z') r,
                            split_part(first(s), last(s), 1) sp,
                            lpad(first(s), 5, last(s)) lp,
                            rpad(first(s), 5, last(s)) rp,
                            nullif(first_not_null(s), last_not_null(s)) nn
                        FROM tab
                        ORDER BY k
                        """)
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                k\tn\tsw\tp\tr\tsp\tlp\trp\tnn
                                k1\tabc\tfalse\t2\taZc\ta\tbbabc\tabcbb\tabc
                                k2\t\ttrue\t1\tZ\t\tccccc\tccccc\t
                                """);
                assertQuery("SELECT nullif(first(s), last(s)) n FROM tab")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                n
                                abc
                                """);
                assertQuery("SELECT ts, nullif(first(s), last(s)) n FROM tab SAMPLE BY 1d")
                        .noLeakCheck()
                        .timestamp("ts")
                        .expectSize()
                        .returns("""
                                ts\tn
                                2024-01-01T00:00:00.000000Z\tabc
                                """);
                execute("DROP TABLE tab");
            }
        });
    }

    @Test
    public void testKeyed() throws Exception {
        assertMemoryLeak(() -> assertQuery("select a, first(sym) sym from tab order by a")
                .ddl("create table tab as (select rnd_int() % 2 a, rnd_symbol('aa', 'bb', 'cc') sym from long_sequence(10))")
                .expectSize()
                .returns("""
                        a\tsym
                        -1\tbb
                        0\taa
                        1\tcc
                        """));
    }

    @Test
    public void testNotKeyed() throws Exception {
        assertMemoryLeak(() -> assertQuery("select first(sym) sym from tab")
                .ddl("create table tab as (select rnd_int() % 2 a, rnd_symbol('aa', 'bb', 'cc') sym from long_sequence(10))")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        sym
                        aa
                        """));
    }

    @Test
    public void testNotKeyedConstantOverEmpty() throws Exception {
        // first(constant) over a WHERE-folded empty table must return NULL: setEmpty
        // stores VALUE_IS_NULL on the group-by state, and SymbolConstant.valueOf must
        // honour that key. Before the fix the constant was returned verbatim.
        assertQuery("select first(('0.83055')::symbol) a0 from tab where 1 = 0")
                .ddl("create table tab as (select rnd_int() a from long_sequence(10))")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        a0
                        
                        """);
    }

    @Test
    public void testSampleByFillFirstLastResolveIndependently() throws Exception {
        assertMemoryLeak(() -> {
            for (String cacheFlag : new String[]{"CACHE", "NOCACHE"}) {
                execute("CREATE TABLE tab (ts TIMESTAMP, s SYMBOL " + cacheFlag + ") TIMESTAMP(ts) PARTITION BY DAY");
                execute("""
                        INSERT INTO tab VALUES
                        ('2024-01-01T00:00:00.000000Z', 'alpha'),
                        ('2024-01-01T00:30:00.000000Z', 'omega'),
                        ('2024-01-01T02:00:00.000000Z', NULL),
                        ('2024-01-01T02:30:00.000000Z', 'bravo')
                        """);
                assertSymbolRecords(
                        "SELECT first(s) f, last(s) l FROM tab SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                        """
                                alpha\tomega\talpha\tomega
                                alpha\tomega\talpha\tomega
                                \tbravo\t\tbravo
                                """
                );
                execute("DROP TABLE tab");
            }
        });
    }

    @Test
    public void testSampleByFirstLastResolveIndependently() throws Exception {
        assertMemoryLeak(() -> {
            for (String cacheFlag : new String[]{"CACHE", "NOCACHE"}) {
                execute("CREATE TABLE tab (ts TIMESTAMP, k SYMBOL INDEX, s SYMBOL " + cacheFlag + ") TIMESTAMP(ts) PARTITION BY DAY");
                execute("""
                        INSERT INTO tab VALUES
                        ('2024-01-01T00:00:00.000000Z', 'K', 'A'),
                        ('2024-01-01T00:00:00.000001Z', 'K', 'B'),
                        ('2024-01-01T01:00:00.000000Z', 'K', 'C'),
                        ('2024-01-01T01:00:00.000001Z', 'K', 'D'),
                        ('2024-01-01T02:00:00.000000Z', 'K', 'E'),
                        ('2024-01-01T02:00:00.000001Z', 'K', 'F'),
                        ('2024-01-01T03:00:00.000000Z', 'K', NULL),
                        ('2024-01-01T03:00:00.000001Z', 'K', 'G')
                        """);
                assertQuery("""
                        SELECT * FROM (
                            SELECT first(s) f, last(s) l
                            FROM tab
                            WHERE k = 'K'
                            SAMPLE BY 1h ALIGN TO FIRST OBSERVATION
                        )
                        WHERE nullif(f, nullif(l, 'X')) IS NOT NULL
                        """)
                        .noLeakCheck()
                        .noRandomAccess()
                        .withPlanContaining("SampleByFirstLast")
                        .returns("""
                                f\tl
                                A\tB
                                C\tD
                                E\tF
                                """);
                assertSymbolRecords(
                        "SELECT first(s) f, last(s) l FROM tab WHERE k = 'K' SAMPLE BY 1h ALIGN TO FIRST OBSERVATION",
                        """
                                A\tB\tA\tB
                                C\tD\tC\tD
                                E\tF\tE\tF
                                \tG\t\tG
                                """
                );
                execute("DROP TABLE tab");
            }
        });
    }

    @Test
    public void testSampleFill() throws Exception {
        assertQuery("select b, first(a) a, k from x sample by 3h align to first observation")
                .ddl("create table x as " +
                        "(" +
                        "select" +
                        " rnd_symbol('l1', 'kl2', 'ss4') a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(172800000000, 360000000) k" +
                        " from" +
                        " long_sequence(100)" +
                        ") timestamp(k) partition by NONE")
                .mutateWith("insert into x select * from (" +
                        "select" +
                        " rnd_symbol('zz8', 'zz9') a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(277200000000, 360000000) k" +
                        " from" +
                        " long_sequence(35)" +
                        ") timestamp(k)")
                .timestamp("k")
                .noRandomAccess()
                .returns("""
                        b\ta\tk
                        \tkl2\t1970-01-03T00:00:00.000000Z
                        VTJW\tl1\t1970-01-03T00:00:00.000000Z
                        RXGZ\tkl2\t1970-01-03T00:00:00.000000Z
                        PEHN\tss4\t1970-01-03T00:00:00.000000Z
                        CPSW\tkl2\t1970-01-03T00:00:00.000000Z
                        HYRX\tss4\t1970-01-03T00:00:00.000000Z
                        RXGZ\tss4\t1970-01-03T03:00:00.000000Z
                        \tl1\t1970-01-03T03:00:00.000000Z
                        PEHN\tss4\t1970-01-03T03:00:00.000000Z
                        CPSW\tl1\t1970-01-03T03:00:00.000000Z
                        HYRX\tss4\t1970-01-03T03:00:00.000000Z
                        VTJW\tss4\t1970-01-03T03:00:00.000000Z
                        CPSW\tkl2\t1970-01-03T06:00:00.000000Z
                        \tkl2\t1970-01-03T06:00:00.000000Z
                        HYRX\tss4\t1970-01-03T06:00:00.000000Z
                        VTJW\tl1\t1970-01-03T06:00:00.000000Z
                        PEHN\tss4\t1970-01-03T06:00:00.000000Z
                        RXGZ\tkl2\t1970-01-03T06:00:00.000000Z
                        CPSW\tss4\t1970-01-03T09:00:00.000000Z
                        \tl1\t1970-01-03T09:00:00.000000Z
                        PEHN\tss4\t1970-01-03T09:00:00.000000Z
                        VTJW\tss4\t1970-01-03T09:00:00.000000Z
                        """, """
                        b\ta\tk
                        \tkl2\t1970-01-03T00:00:00.000000Z
                        VTJW\tl1\t1970-01-03T00:00:00.000000Z
                        RXGZ\tkl2\t1970-01-03T00:00:00.000000Z
                        PEHN\tss4\t1970-01-03T00:00:00.000000Z
                        CPSW\tkl2\t1970-01-03T00:00:00.000000Z
                        HYRX\tss4\t1970-01-03T00:00:00.000000Z
                        RXGZ\tss4\t1970-01-03T03:00:00.000000Z
                        \tl1\t1970-01-03T03:00:00.000000Z
                        PEHN\tss4\t1970-01-03T03:00:00.000000Z
                        CPSW\tl1\t1970-01-03T03:00:00.000000Z
                        HYRX\tss4\t1970-01-03T03:00:00.000000Z
                        VTJW\tss4\t1970-01-03T03:00:00.000000Z
                        CPSW\tkl2\t1970-01-03T06:00:00.000000Z
                        \tkl2\t1970-01-03T06:00:00.000000Z
                        HYRX\tss4\t1970-01-03T06:00:00.000000Z
                        VTJW\tl1\t1970-01-03T06:00:00.000000Z
                        PEHN\tss4\t1970-01-03T06:00:00.000000Z
                        RXGZ\tkl2\t1970-01-03T06:00:00.000000Z
                        CPSW\tss4\t1970-01-03T09:00:00.000000Z
                        \tl1\t1970-01-03T09:00:00.000000Z
                        PEHN\tss4\t1970-01-03T09:00:00.000000Z
                        VTJW\tss4\t1970-01-03T09:00:00.000000Z
                        \tzz9\t1970-01-04T03:00:00.000000Z
                        ZMZV\tzz8\t1970-01-04T03:00:00.000000Z
                        QLDG\tzz9\t1970-01-04T06:00:00.000000Z
                        LOGI\tzz9\t1970-01-04T06:00:00.000000Z
                        QEBN\tzz8\t1970-01-04T06:00:00.000000Z
                        \tzz8\t1970-01-04T06:00:00.000000Z
                        FOUS\tzz9\t1970-01-04T06:00:00.000000Z
                        """);
    }

    private static void assertSymbolRecords(String sql, String expected) throws SqlException {
        try (
                RecordCursorFactory factory = select(sql);
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            final Record record = cursor.getRecord();
            for (int pass = 0; pass < 2; pass++) {
                sink.clear();
                while (cursor.hasNext()) {
                    final CharSequence firstA = record.getSymA(0);
                    final CharSequence lastA = record.getSymA(1);
                    final CharSequence firstB = record.getSymB(0);
                    final CharSequence lastB = record.getSymB(1);
                    sink.put(firstA).put('\t').put(lastA).put('\t').put(firstB).put('\t').put(lastB).put('\n');
                }
                TestUtils.assertEquals(expected, sink);
                cursor.toTop();
            }
        }
    }
}
