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

package io.questdb.test.griffin.engine.window;

import io.questdb.PropertyKey;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.engine.window.CachedWindowLightRecordCursorFactory;
import io.questdb.griffin.engine.window.CachedWindowMapGroups;
import io.questdb.griffin.engine.window.CachedWindowRecordCursorFactory;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class PercentileWindowFunctionTest extends AbstractCairoTest {

    @Test
    public void testMultiPercentileBeforeVarSizeColumns() throws Exception {
        // An array window output takes a slot in the record chain's var-size header next to
        // the VARCHAR, STRING and DOUBLE[] columns the chain copies after it. Every one of
        // them must still read its own value, including a PARTITION BY key that the SELECT
        // list does not name, under each of the three RecordSink implementations.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x DOUBLE, a VARCHAR, s STRING, arr DOUBLE[], b VARCHAR)");
            execute("""
                    INSERT INTO t VALUES
                    (1.0, 'g1', 's1', ARRAY[1.0], 'h1'),
                    (2.0, 'g1', 's2', ARRAY[2.0, 2.5], 'h2'),
                    (10.0, 'g2', null, null, 'h1'),
                    (20.0, 'g2', 's4', ARRAY[4.0], null)
                    """);
            // 0 picks the sink by schema size; 1, 2 and 3 force single-method, chunked and looping
            for (int copierType = 0; copierType < 4; copierType++) {
                setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, copierType);
                assertQuery("SELECT percentile_disc(x, ARRAY[0.5]) OVER () p, * FROM t")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                p\tx\ta\ts\tarr\tb
                                [2.0]\t1.0\tg1\ts1\t[1.0]\th1
                                [2.0]\t2.0\tg1\ts2\t[2.0,2.5]\th2
                                [2.0]\t10.0\tg2\t\tnull\th1
                                [2.0]\t20.0\tg2\ts4\t[4.0]\t
                                """);
                assertQuery("SELECT x, percentile_disc(x, ARRAY[0.5]) OVER (PARTITION BY a) p FROM t")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                x\tp
                                1.0\t[1.0]
                                2.0\t[1.0]
                                10.0\t[10.0]
                                20.0\t[10.0]
                                """);
                assertQuery("SELECT percentile_disc(x, ARRAY[0.5]) OVER (PARTITION BY s) p, a FROM t")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                p\ta
                                [1.0]\tg1
                                [2.0]\tg1
                                [10.0]\tg2
                                [20.0]\tg2
                                """);
                assertQuery("""
                        SELECT percentile_cont(x, ARRAY[0.5]) OVER (PARTITION BY a) pc,
                        quantile_disc(x, ARRAY[0.5, 1.0]) OVER (PARTITION BY b) qd,
                        x, s, arr, b
                        FROM t
                        """)
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                pc\tqd\tx\ts\tarr\tb
                                [1.5]\t[1.0,10.0]\t1.0\ts1\t[1.0]\th1
                                [1.5]\t[2.0,2.0]\t2.0\ts2\t[2.0,2.5]\th2
                                [15.0]\t[1.0,10.0]\t10.0\t\tnull\th1
                                [15.0]\t[20.0,20.0]\t20.0\ts4\t[4.0]\t
                                """);
            }
        });
    }

    @Test
    public void testMultiPercentileBeforeVarSizeColumnsEdgeValues() throws Exception {
        // NULLs, empty values, multi-byte UTF-8, a single row, no rows and zero-length array
        // outputs, all behind an array output. NULL and '' stay different partition keys and
        // every var-size column reads its own value.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE e (x DOUBLE, a VARCHAR, s STRING, arr DOUBLE[], b VARCHAR)");
            execute("""
                    INSERT INTO e VALUES
                    (1.0, null, null, null, null),
                    (2.0, '', '', ARRAY[]::DOUBLE[], ''),
                    (2.5, '', '', '{}'::DOUBLE[], ''),
                    (3.0, '\u00e4\u20ac\ud83d\ude00', '\u00df\u6f22\u5b57', ARRAY[3.0], 'ok'),
                    (null, '\u00e4\u20ac\ud83d\ude00', 'x', ARRAY[4.0, 5.0], null)
                    """);
            for (int copierType = 0; copierType < 4; copierType++) {
                setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, copierType);
                assertQuery("SELECT percentile_disc(x, ARRAY[0.5]) OVER () p, * FROM e")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                p\tx\ta\ts\tarr\tb
                                [2.0]\t1.0\t\t\tnull\t
                                [2.0]\t2.0\t\t\t[]\t
                                [2.0]\t2.5\t\t\t[]\t
                                [2.0]\t3.0\t\u00e4\u20ac\ud83d\ude00\t\u00df\u6f22\u5b57\t[3.0]\tok
                                [2.0]\tnull\t\u00e4\u20ac\ud83d\ude00\tx\t[4.0,5.0]\t
                                """);
                // NULL and '' print alike, the partitions they form do not
                assertQuery("SELECT percentile_disc(x, ARRAY[0.5, 1.0]) OVER (PARTITION BY a) p, a, b FROM e")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                p\ta\tb
                                [1.0,1.0]\t\t
                                [2.0,2.5]\t\t
                                [2.0,2.5]\t\t
                                [3.0,3.0]\t\u00e4\u20ac\ud83d\ude00\tok
                                [3.0,3.0]\t\u00e4\u20ac\ud83d\ude00\t
                                """);
                assertQuery("SELECT percentile_cont(x, ARRAY[0.5]) OVER (PARTITION BY s) p, s, a FROM e")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                p\ts\ta
                                [1.0]\t\t
                                [2.25]\t\t
                                [2.25]\t\t
                                [3.0]\t\u00df\u6f22\u5b57\t\u00e4\u20ac\ud83d\ude00
                                null\tx\t\u00e4\u20ac\ud83d\ude00
                                """);
                assertQuery("SELECT percentile_disc(x, ARRAY[0.5]) OVER () p, a, s, b FROM e WHERE x = 3.0")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                p\ta\ts\tb
                                [3.0]\t\u00e4\u20ac\ud83d\ude00\t\u00df\u6f22\u5b57\tok
                                """);
                assertQuery("SELECT percentile_disc(x, ARRAY[0.5]) OVER () p, a, s, b FROM e WHERE x > 100")
                        .noLeakCheck()
                        .expectSize()
                        .returns("p\ta\ts\tb\n");
                // an empty percentile list writes a zero-length array into every output slot
                assertQuery("SELECT percentile_disc(x, ARRAY[]::DOUBLE[]) OVER () p, a FROM e")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                p\ta
                                []\t
                                []\t
                                []\t
                                []\t\u00e4\u20ac\ud83d\ude00
                                []\t\u00e4\u20ac\ud83d\ude00
                                """);
            }
        });
    }

    @Test
    public void testMultiPercentileBeforeVarSizeColumnsLargeValues() throws Exception {
        // Values of several kilobytes and 99-element outputs make every row, and every output
        // pass 2 appends behind the rows, grow the chain's memory. The scalar forms on the
        // light path, which builds no RecordSink, give the expected answer.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE big AS (
                        SELECT
                            x id,
                            rnd_double() v,
                            rnd_varchar('k1', 'k2', 'k3') k,
                            rnd_varchar(2_000, 8_000, 5) u,
                            rnd_str(1_000, 4_000, 5) s,
                            rnd_double_array(1, 0, 0, 2_000) arr
                        FROM long_sequence(300)
                    )
                    """);
            final StringSink percentiles = new StringSink();
            percentiles.put("ARRAY[");
            for (int i = 1; i <= 99; i++) {
                if (i > 1) {
                    percentiles.put(", ");
                }
                percentiles.put(i / 100.0);
            }
            percentiles.put(']');
            final String scalarForm = """
                    SELECT
                        percentile_disc(v, 0.01) OVER (PARTITION BY k) p01,
                        percentile_disc(v, 0.99) OVER (PARTITION BY k) p99,
                        percentile_cont(v, 0.5) OVER (PARTITION BY k) c50,
                        id, k, u, s, arr
                    FROM big
                    """;
            final String arrayForm = "SELECT d[1] p01, d[99] p99, c[1] c50, id, k, u, s, arr FROM ("
                    + "SELECT percentile_disc(v, " + percentiles + ") OVER (PARTITION BY k) d, "
                    + "percentile_cont(v, ARRAY[0.5]) OVER (PARTITION BY k) c, id, k, u, s, arr FROM big)";
            assertWindowFactory(scalarForm, CachedWindowLightRecordCursorFactory.class, "p01,p99,c50,id,k,u,s,arr");
            assertWindowFactory(arrayForm, CachedWindowRecordCursorFactory.class, "d,c,id,k,u,s,arr");
            for (int copierType = 0; copierType < 4; copierType++) {
                setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, copierType);
                assertSqlCursors(scalarForm, arrayForm);
            }
        });
    }

    @Test
    public void testMultiPercentileBeforeVarSizeColumnsMatchesScalarForm() throws Exception {
        // A randomized cross-check over enough rows to grow the record chain many times. The
        // array forms read every var-size column through the chain, behind their own output.
        // The scalar forms take the light path, which reads the base cursor and builds no
        // RecordSink, so the two agree only when every slot is in its own place.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t AS (
                        SELECT
                            x id,
                            CASE WHEN x % 11 = 0 THEN NULL ELSE rnd_double() * 100 END v,
                            CASE WHEN x % 7 = 0 THEN NULL ELSE rnd_varchar('a', 'bb', 'ccc', 'dddd', 'eeeee') END k,
                            rnd_str(1, 24, 3) s,
                            rnd_varchar(1, 40, 3) u,
                            rnd_bin(1, 32, 3) bin,
                            rnd_double_array(1, 3, 8) arr
                        FROM long_sequence(10_000)
                    )
                    """);
            final String scalarForm = """
                    SELECT
                        percentile_disc(v, 0.5) OVER (PARTITION BY k) d50,
                        percentile_cont(v, 0.9) OVER (PARTITION BY k) c90,
                        id, k, s, u, bin, arr
                    FROM t
                    """;
            // The optimizer lays the window model out in the order the outer projection
            // names its columns, so the array outputs lead the chain only if d[1] and c[1]
            // come first out here.
            final String arrayForm = """
                    SELECT d[1] d50, c[1] c90, id, k, s, u, bin, arr
                    FROM (
                        SELECT
                            percentile_disc(v, ARRAY[0.5]) OVER (PARTITION BY k) d,
                            percentile_cont(v, ARRAY[0.9]) OVER (PARTITION BY k) c,
                            id, k, s, u, bin, arr
                        FROM t
                    )
                    """;
            assertWindowFactory(scalarForm, CachedWindowLightRecordCursorFactory.class, "d50,c90,id,k,s,u,bin,arr");
            assertWindowFactory(arrayForm, CachedWindowRecordCursorFactory.class, "d,c,id,k,s,u,bin,arr");
            for (int copierType = 0; copierType < 4; copierType++) {
                setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, copierType);
                assertSqlCursors(scalarForm, arrayForm);
            }
        });
    }

    @Test
    public void testMultiPercentileBeforeVarSizeColumnsReadDownstream() throws Exception {
        // The operators above the window read its chain record too: an outer sort and LIMIT,
        // a negative one through random access, GROUP BY and JOIN keys, and CREATE TABLE AS
        // SELECT, which persists whatever the record holds.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x DOUBLE, a VARCHAR, s STRING, b VARCHAR)");
            execute("""
                    INSERT INTO t VALUES
                    (1.0, 'g1', 's1', 'h1'),
                    (2.0, 'g1', 's2', 'h2'),
                    (10.0, 'g2', null, 'h1'),
                    (20.0, 'g2', 's4', null)
                    """);
            execute("CREATE TABLE u (a VARCHAR, y INT)");
            execute("INSERT INTO u VALUES ('g1', 100), ('g2', 200)");
            final String window = "SELECT percentile_disc(x, ARRAY[0.5]) OVER (PARTITION BY a) p, a, b FROM t";
            for (int copierType = 0; copierType < 4; copierType++) {
                setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, copierType);
                assertQuery("SELECT p, a, b FROM (" + window + ") ORDER BY b DESC, a")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                p\ta\tb
                                [1.0]\tg1\th2
                                [1.0]\tg1\th1
                                [10.0]\tg2\th1
                                [10.0]\tg2\t
                                """);
                assertQuery(window + " LIMIT 2")
                        .noLeakCheck()
                        .returns("""
                                p\ta\tb
                                [1.0]\tg1\th1
                                [1.0]\tg1\th2
                                """);
                assertQuery(window + " LIMIT -2")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                p\ta\tb
                                [10.0]\tg2\th1
                                [10.0]\tg2\t
                                """);
                assertQuery("SELECT sum(p[1]) sp, b FROM (" + window + ") GROUP BY b ORDER BY b")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                sp\tb
                                10.0\t
                                11.0\th1
                                1.0\th2
                                """);
                assertQuery("""
                        SELECT w.p, w.a, u.y
                        FROM (SELECT percentile_disc(x, ARRAY[0.5]) OVER (PARTITION BY b) p, a FROM t) w
                        JOIN u ON (a)
                        """)
                        .noLeakCheck()
                        .noRandomAccess()
                        .returns("""
                                p\ta\ty
                                [1.0]\tg1\t100
                                [2.0]\tg1\t100
                                [1.0]\tg2\t200
                                [20.0]\tg2\t200
                                """);
                execute("CREATE TABLE snap AS (" + window + ")");
                assertQuery("SELECT * FROM snap")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                p\ta\tb
                                [1.0]\tg1\th1
                                [1.0]\tg1\th2
                                [10.0]\tg2\th1
                                [10.0]\tg2\t
                                """);
                execute("DROP TABLE snap");
            }
        });
    }

    @Test
    public void testMultiPercentileBeforeVarSizeColumnsWideTable() throws Exception {
        // Over 600 columns the chain's RecordSink no longer fits one method, so the default
        // configuration generates it in chunks. The array output must take its var-size slot
        // whichever chunk it lands in: first, ahead of VARCHARs in a later chunk, or last,
        // where the window path's skew index has no entry for it.
        assertMemoryLeak(() -> {
            final int doubleCount = 600;
            final StringBuilder ddl = new StringBuilder("CREATE TABLE wide AS (SELECT x::DOUBLE x, ('a' || x)::VARCHAR a");
            final StringSink header = new StringSink();
            header.put("x\ta");
            for (int i = 1; i <= doubleCount; i++) {
                ddl.append(", (x * ").append(i).append(")::DOUBLE d").append(i);
                header.put("\td").put(i);
            }
            ddl.append(", ('b' || x)::VARCHAR b FROM long_sequence(3))");
            header.put("\tb");
            execute(ddl);

            final StringSink windowFirst = new StringSink();
            final StringSink windowLast = new StringSink();
            windowFirst.put("p\t").put(header).put('\n');
            windowLast.put(header).put("\tp\n");
            for (int x = 1; x <= 3; x++) {
                final StringSink row = new StringSink();
                row.put(x).put(".0\ta").put(x);
                for (int i = 1; i <= doubleCount; i++) {
                    row.put('\t').put(x * i).put(".0");
                }
                row.put("\tb").put(x);
                windowFirst.put("[2.0]\t").put(row).put('\n');
                windowLast.put(row).put("\t[2.0]\n");
            }

            for (int copierType = 0; copierType < 4; copierType++) {
                setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, copierType);
                assertQuery("SELECT percentile_disc(x, ARRAY[0.5]) OVER () p, * FROM wide")
                        .noLeakCheck()
                        .expectSize()
                        .returns(windowFirst);
                assertQuery("SELECT *, percentile_disc(x, ARRAY[0.5]) OVER () p FROM wide")
                        .noLeakCheck()
                        .expectSize()
                        .returns(windowLast);
            }
        });
    }

    @Test
    public void testMultiPercentileBeforeVarSizeColumnsWindowCacheCap() throws Exception {
        // The window cache cap can stop the query in pass 1, while the rows are copied, or in
        // pass 2, while the array outputs are appended behind them, after the copy has stepped
        // past their slots. Either way the query fails cleanly, frees what it allocated, and
        // the next run with room enough returns the right answer.
        setProperty(PropertyKey.CAIRO_SQL_WINDOW_STORE_PAGE_SIZE, 4096);
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t AS (
                        SELECT x::DOUBLE x, ('g' || (x % 3))::VARCHAR a, ('h' || x)::VARCHAR b
                        FROM long_sequence(1_000)
                    )
                    """);
            final StringSink percentiles = new StringSink();
            percentiles.put("ARRAY[");
            for (int i = 1; i <= 99; i++) {
                if (i > 1) {
                    percentiles.put(", ");
                }
                percentiles.put(i / 100.0);
            }
            percentiles.put(']');
            // 99 percentiles make each output 808 bytes; one percentile makes it 24 bytes
            final String wideOutputs = "SELECT d[1] p01, d[99] p99, a, b FROM (SELECT percentile_disc(x, "
                    + percentiles + ") OVER (PARTITION BY a) d, a, b FROM t)";
            final String narrowOutputs = "SELECT d[1] p50, a, b FROM ("
                    + "SELECT percentile_disc(x, ARRAY[0.5]) OVER (PARTITION BY a) d, a, b FROM t)";
            final String breach = "breached in VirtualMemory (raise cairo.sql.window.cache.max.bytes)";

            // a single page cannot hold the rows themselves
            setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHE_MAX_BYTES, 4096);
            assertExceptionNoLeakCheck(wideOutputs, 0, breach);

            // 256 KiB holds the 1,000 rows and 24-byte outputs, so with 808-byte outputs the
            // cap can only be breached in pass 2
            setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHE_MAX_BYTES, 256 * 1024);
            assertSqlCursors(
                    "SELECT percentile_disc(x, 0.5) OVER (PARTITION BY a) p50, a, b FROM t",
                    narrowOutputs
            );
            assertExceptionNoLeakCheck(wideOutputs, 0, breach);

            setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHE_MAX_BYTES, 16 * 1024 * 1024);
            assertSqlCursors(
                    """
                            SELECT
                                percentile_disc(x, 0.01) OVER (PARTITION BY a) p01,
                                percentile_disc(x, 0.99) OVER (PARTITION BY a) p99,
                                a, b
                            FROM t
                            """,
                    wideOutputs
            );
        });
    }

    @Test
    public void testMultiPercentileBeforeVarSizeWindowInputs() throws Exception {
        // The record chain feeds more than the projection: a window function's argument, the
        // sort key of an ordered window function and the key of a fused Map group all read
        // chain columns. Behind an array output whose slot the RecordSink did not step past,
        // each of them read a neighbour's slot, NULL or a dangling pointer.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x DOUBLE, a VARCHAR, c VARCHAR, s STRING, b VARCHAR)");
            execute("""
                    INSERT INTO t VALUES
                    (1.0, 'g1', '1.5', 's1', 'h1'),
                    (2.0, 'g1', '2.5', 's2', 'h2'),
                    (10.0, 'g2', '10.5', null, 'h1'),
                    (20.0, 'g2', null, 's4', null)
                    """);
            final String fused = """
                    SELECT percentile_disc(x, ARRAY[0.5]) OVER () p,
                    sum(x) OVER (PARTITION BY a) sx, avg(x) OVER (PARTITION BY a) ax, count(x) OVER (PARTITION BY a) cx,
                    a
                    FROM t
                    """;
            // sum, avg and count share one Map group, which reads its key a from the chain
            assertMapGroupCount(fused, 1);
            for (int copierType = 0; copierType < 4; copierType++) {
                setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, copierType);
                assertQuery("SELECT percentile_disc(c::DOUBLE, ARRAY[0.5]) OVER (PARTITION BY a) p, c FROM t")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                p\tc
                                [1.5]\t1.5
                                [1.5]\t2.5
                                [10.5]\t10.5
                                [10.5]\t
                                """);
                // fixed-size window outputs on either side of the array output
                assertQuery("""
                        SELECT sum(x) OVER () s0, percentile_disc(x, ARRAY[0.5]) OVER (PARTITION BY b) p,
                        max(x) OVER (PARTITION BY a) m, a, b
                        FROM t
                        """)
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                s0\tp\tm\ta\tb
                                33.0\t[1.0]\t2.0\tg1\th1
                                33.0\t[2.0]\t2.0\tg1\th2
                                33.0\t[1.0]\t20.0\tg2\th1
                                33.0\t[20.0]\t20.0\tg2\t
                                """);
                assertQuery(fused)
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                p\tsx\tax\tcx\ta
                                [2.0]\t3.0\t1.5\t2\tg1
                                [2.0]\t3.0\t1.5\t2\tg1
                                [2.0]\t30.0\t15.0\t2\tg2
                                [2.0]\t30.0\t15.0\t2\tg2
                                """);
                // the sort key of an ordered window function, through the encoded sort buffer
                // and, with sorting off, through the comparator-driven one
                for (int sortEnabled = 0; sortEnabled < 2; sortEnabled++) {
                    setProperty(PropertyKey.CAIRO_SQL_ORDER_BY_SORT_ENABLED, sortEnabled == 1 ? "true" : "false");
                    assertQuery("SELECT percentile_disc(x, ARRAY[0.5]) OVER () p, row_number() OVER (ORDER BY b, x) rn, b, x FROM t")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    p\trn\tb\tx
                                    [2.0]\t2\th1\t1.0
                                    [2.0]\t4\th2\t2.0
                                    [2.0]\t3\th1\t10.0
                                    [2.0]\t1\t\t20.0
                                    """);
                    assertQuery("SELECT percentile_disc(x, ARRAY[0.5]) OVER () p, row_number() OVER (ORDER BY s DESC, x) rn, s, x FROM t")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    p\trn\ts\tx
                                    [2.0]\t3\ts1\t1.0
                                    [2.0]\t2\ts2\t2.0
                                    [2.0]\t4\t\t10.0
                                    [2.0]\t1\ts4\t20.0
                                    """);
                }
            }
        });
    }

    @Test
    public void testMultiPercentileContInterpolationBetweenExtremes() throws Exception {
        // hi - lo overflows for these two values, but every interpolated value lies between them
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (x DOUBLE, k INT)");
            execute("INSERT INTO test VALUES (1.7976931348623157E308, 0), (-1.7976931348623157E308, 0)");
            assertQuery("SELECT percentile_cont(x, ARRAY[0.5, 0.25]) OVER () w, percentile_cont(x, ARRAY[0.5, 0.25]) OVER (PARTITION BY k) p FROM test")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            w\tp
                            [0.0,-8.988465674311579E307]\t[0.0,-8.988465674311579E307]
                            [0.0,-8.988465674311579E307]\t[0.0,-8.988465674311579E307]
                            """);
        });
    }

    @Test
    public void testMultiPercentileContOverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (" +
                    "select x % 2 as category, cast(x as double) as value from long_sequence(10)" +
                    ")");
            // cat=0 (even): values 2, 4, 6, 8, 10
            //   position(0.25) = 0.25*(5-1) = 1.0 → value at index 1 = 4.0
            //   position(0.50) = 0.50*(5-1) = 2.0 → value at index 2 = 6.0
            //   position(0.75) = 0.75*(5-1) = 3.0 → value at index 3 = 8.0
            // cat=1 (odd): values 1, 3, 5, 7, 9
            //   position(0.25) = 0.25*(5-1) = 1.0 → value at index 1 = 3.0
            //   position(0.50) = 0.50*(5-1) = 2.0 → value at index 2 = 5.0
            //   position(0.75) = 0.75*(5-1) = 3.0 → value at index 3 = 7.0
            assertQuery("select category, value, percentile_cont(value, ARRAY[0.25, 0.5, 0.75]) over (partition by category) from test")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            category\tvalue\tpercentile_cont
                            1\t1.0\t[3.0,5.0,7.0]
                            0\t2.0\t[4.0,6.0,8.0]
                            1\t3.0\t[3.0,5.0,7.0]
                            0\t4.0\t[4.0,6.0,8.0]
                            1\t5.0\t[3.0,5.0,7.0]
                            0\t6.0\t[4.0,6.0,8.0]
                            1\t7.0\t[3.0,5.0,7.0]
                            0\t8.0\t[4.0,6.0,8.0]
                            1\t9.0\t[3.0,5.0,7.0]
                            0\t10.0\t[4.0,6.0,8.0]
                            """);
        });
    }

    @Test
    public void testMultiPercentileContOverPartitionReopen() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (" +
                    "SELECT x % 2 AS category, CAST(x AS DOUBLE) AS value FROM long_sequence(10)" +
                    ")");
            assertQuery("SELECT category, value, percentile_cont(value, ARRAY[0.25, 0.5, 0.75]) OVER (PARTITION BY category) FROM test")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            category\tvalue\tpercentile_cont
                            1\t1.0\t[3.0,5.0,7.0]
                            0\t2.0\t[4.0,6.0,8.0]
                            1\t3.0\t[3.0,5.0,7.0]
                            0\t4.0\t[4.0,6.0,8.0]
                            1\t5.0\t[3.0,5.0,7.0]
                            0\t6.0\t[4.0,6.0,8.0]
                            1\t7.0\t[3.0,5.0,7.0]
                            0\t8.0\t[4.0,6.0,8.0]
                            1\t9.0\t[3.0,5.0,7.0]
                            0\t10.0\t[4.0,6.0,8.0]
                            """);
        });
    }

    @Test
    public void testMultiPercentileContOverPartitionWithAllNullPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (k SYMBOL, v DOUBLE)");
            // the rows of the all-NULL partition 'b' sit between the rows of partition 'a', so the
            // result array alternates between the NULL array and a sized array
            execute("INSERT INTO t VALUES ('a', 1.0), ('b', null), ('a', 2.0), ('b', null), ('a', 4.0)");
            assertQuery("SELECT k, v, percentile_cont(v, ARRAY[0.5, 0.75]) OVER (PARTITION BY k) p FROM t")
                    .noLeakCheck()
                    .expectSize()
                    .withPlan("""
                            CachedWindow
                              unorderedFunctions: [percentile_cont(v, ARRAY[0.5,0.75]) over (partition by [k])]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """)
                    .returns("""
                            k\tv\tp
                            a\t1.0\t[2.0,3.0]
                            b\tnull\tnull
                            a\t2.0\t[2.0,3.0]
                            b\tnull\tnull
                            a\t4.0\t[2.0,3.0]
                            """);
        });
    }

    @Test
    public void testMultiPercentileContOverWholeResultSet() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) value from long_sequence(10))");
            assertQuery("select value, percentile_cont(value, ARRAY[0.25, 0.5, 0.75]) over () from test")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            value\tpercentile_cont
                            1.0\t[3.25,5.5,7.75]
                            2.0\t[3.25,5.5,7.75]
                            3.0\t[3.25,5.5,7.75]
                            4.0\t[3.25,5.5,7.75]
                            5.0\t[3.25,5.5,7.75]
                            6.0\t[3.25,5.5,7.75]
                            7.0\t[3.25,5.5,7.75]
                            8.0\t[3.25,5.5,7.75]
                            9.0\t[3.25,5.5,7.75]
                            10.0\t[3.25,5.5,7.75]
                            """);
        });
    }

    @Test
    public void testMultiPercentileContOverWholeResultSetAllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (v DOUBLE)");
            execute("INSERT INTO t VALUES (null), (null)");
            assertQuery("SELECT v, percentile_cont(v, ARRAY[0.5, 0.75]) OVER () p FROM t")
                    .noLeakCheck()
                    .expectSize()
                    .withPlan("""
                            CachedWindow
                              unorderedFunctions: [percentile_cont(v, ARRAY[0.5,0.75]) over ()]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """)
                    .returns("""
                            v\tp
                            null\tnull
                            null\tnull
                            """);
        });
    }

    @Test
    public void testMultiPercentileContOverWholeResultSetReopen() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT CAST(x AS DOUBLE) AS value FROM long_sequence(10))");
            assertQuery("SELECT value, percentile_cont(value, ARRAY[0.25, 0.5, 0.75]) OVER () FROM test")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            value\tpercentile_cont
                            1.0\t[3.25,5.5,7.75]
                            2.0\t[3.25,5.5,7.75]
                            3.0\t[3.25,5.5,7.75]
                            4.0\t[3.25,5.5,7.75]
                            5.0\t[3.25,5.5,7.75]
                            6.0\t[3.25,5.5,7.75]
                            7.0\t[3.25,5.5,7.75]
                            8.0\t[3.25,5.5,7.75]
                            9.0\t[3.25,5.5,7.75]
                            10.0\t[3.25,5.5,7.75]
                            """);
        });
    }

    @Test
    public void testMultiPercentileContRejectsNonConstantPercentiles() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (v DOUBLE, ps DOUBLE[])");
            final String sql = "SELECT percentile_cont(v, ps) OVER () FROM t";
            assertExceptionNoLeakCheck(sql, sql.indexOf("ps)"), "percentile argument must be a constant");
        });
    }

    @Test
    public void testMultiPercentileContRejectsOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT CAST(x AS DOUBLE) AS value FROM long_sequence(10))");
            assertException(
                    "SELECT percentile_cont(value, ARRAY[0.25, 0.5]) OVER (ORDER BY value) FROM test",
                    7,
                    "percentile_cont window function only supports whole partition frames"
            );
        });
    }

    @Test
    public void testMultiPercentileContRejectsRowsFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT CAST(x AS DOUBLE) AS value, x % 2 AS cat FROM long_sequence(10))");
            assertException(
                    "SELECT percentile_cont(value, ARRAY[0.25, 0.5]) OVER (PARTITION BY cat ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM test",
                    7,
                    "percentile_cont window function only supports whole partition frames"
            );
        });
    }

    @Test
    public void testMultiPercentileContWithExtremePercentiles() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) value from long_sequence(10))");
            // Test with 0th, 50th, and 100th percentiles
            // 0th: position = 0 * 9 = 0 → index 0 → 1.0
            // 50th: position = 0.5 * 9 = 4.5 → interpolate between 4 and 5 → 5.5
            // 100th: position = 1.0 * 9 = 9 → index 9 → 10.0
            assertQuery("select value, percentile_cont(value, ARRAY[0.0, 0.5, 1.0]) over () from test")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            value\tpercentile_cont
                            1.0\t[1.0,5.5,10.0]
                            2.0\t[1.0,5.5,10.0]
                            3.0\t[1.0,5.5,10.0]
                            4.0\t[1.0,5.5,10.0]
                            5.0\t[1.0,5.5,10.0]
                            6.0\t[1.0,5.5,10.0]
                            7.0\t[1.0,5.5,10.0]
                            8.0\t[1.0,5.5,10.0]
                            9.0\t[1.0,5.5,10.0]
                            10.0\t[1.0,5.5,10.0]
                            """);
        });
    }

    @Test
    public void testMultiPercentileContWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (" +
                    "select x, " +
                    "case when x % 3 = 0 then null else cast(x as double) end as value " +
                    "from long_sequence(10)" +
                    ")");
            // Non-null values: 1, 2, 4, 5, 7, 8, 10 (7 values), sorted: [1, 2, 4, 5, 7, 8, 10], n-1 = 6
            // 25th percentile: position = 0.25 * 6 = 1.5 → interpolate between index 1 and 2 → 2 + 0.5*(4-2) = 3.0
            // 50th percentile: position = 0.50 * 6 = 3.0 → index 3 → 5.0
            // 75th percentile: position = 0.75 * 6 = 4.5 → interpolate between index 4 and 5 → 7 + 0.5*(8-7) = 7.5
            assertQuery("select x, value, percentile_cont(value, ARRAY[0.25, 0.5, 0.75]) over () from test")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x\tvalue\tpercentile_cont
                            1\t1.0\t[3.0,5.0,7.5]
                            2\t2.0\t[3.0,5.0,7.5]
                            3\tnull\t[3.0,5.0,7.5]
                            4\t4.0\t[3.0,5.0,7.5]
                            5\t5.0\t[3.0,5.0,7.5]
                            6\tnull\t[3.0,5.0,7.5]
                            7\t7.0\t[3.0,5.0,7.5]
                            8\t8.0\t[3.0,5.0,7.5]
                            9\tnull\t[3.0,5.0,7.5]
                            10\t10.0\t[3.0,5.0,7.5]
                            """);
        });
    }

    @Test
    public void testMultiPercentileContWithNullsInPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (" +
                    "select x % 2 as category, " +
                    "case when x % 4 = 0 then null else cast(x as double) end as value " +
                    "from long_sequence(10)" +
                    ")");
            // cat=0: values 2, null, 6, null, 10 → non-null: 2, 6, 10 (3 values)
            //   Sorted: [2, 6, 10], n-1 = 2
            //   25th: 0.25 * 2 = 0.5 → interpolate between index 0 and 1 → 2 + 0.5*(6-2) = 4.0
            //   50th: 0.50 * 2 = 1.0 → index 1 → 6.0
            //   75th: 0.75 * 2 = 1.5 → interpolate between index 1 and 2 → 6 + 0.5*(10-6) = 8.0
            // cat=1: values 1, 3, 5, 7, 9 (5 values)
            //   25th: 0.25 * 4 = 1.0 → index 1 → 3.0
            //   50th: 0.50 * 4 = 2.0 → index 2 → 5.0
            //   75th: 0.75 * 4 = 3.0 → index 3 → 7.0
            assertQuery("select category, value, percentile_cont(value, ARRAY[0.25, 0.5, 0.75]) over (partition by category) from test")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            category\tvalue\tpercentile_cont
                            1\t1.0\t[3.0,5.0,7.0]
                            0\t2.0\t[4.0,6.0,8.0]
                            1\t3.0\t[3.0,5.0,7.0]
                            0\tnull\t[4.0,6.0,8.0]
                            1\t5.0\t[3.0,5.0,7.0]
                            0\t6.0\t[4.0,6.0,8.0]
                            1\t7.0\t[3.0,5.0,7.0]
                            0\tnull\t[4.0,6.0,8.0]
                            1\t9.0\t[3.0,5.0,7.0]
                            0\t10.0\t[4.0,6.0,8.0]
                            """);
        });
    }

    @Test
    public void testMultiPercentileContWithSingleValueInPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (" +
                    "select x, " +
                    "case when x = 5 then 1 else 2 end as category, " +
                    "cast(x as double) as value " +
                    "from long_sequence(10)" +
                    ")");
            // cat=1: only value 5 → all percentiles should be 5.0
            // cat=2: values 1,2,3,4,6,7,8,9,10 (9 values), n-1 = 8
            //   25th: 0.25 * 8 = 2.0 → index 2 → 3.0
            //   50th: 0.50 * 8 = 4.0 → index 4 → 6.0
            //   75th: 0.75 * 8 = 6.0 → index 6 → 8.0
            assertQuery("select x, category, value, percentile_cont(value, ARRAY[0.25, 0.5, 0.75]) over (partition by category) from test")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x\tcategory\tvalue\tpercentile_cont
                            1\t2\t1.0\t[3.0,6.0,8.0]
                            2\t2\t2.0\t[3.0,6.0,8.0]
                            3\t2\t3.0\t[3.0,6.0,8.0]
                            4\t2\t4.0\t[3.0,6.0,8.0]
                            5\t1\t5.0\t[5.0,5.0,5.0]
                            6\t2\t6.0\t[3.0,6.0,8.0]
                            7\t2\t7.0\t[3.0,6.0,8.0]
                            8\t2\t8.0\t[3.0,6.0,8.0]
                            9\t2\t9.0\t[3.0,6.0,8.0]
                            10\t2\t10.0\t[3.0,6.0,8.0]
                            """);
        });
    }

    @Test
    public void testMultiPercentileContWithTwoValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) value from long_sequence(2))");
            // With only 2 values [1.0, 2.0]:
            // 25th: position = 0.25 * 1 = 0.25 → 0.25 * (2.0 - 1.0) + 1.0 = 1.25
            // 50th: position = 0.50 * 1 = 0.50 → 0.50 * (2.0 - 1.0) + 1.0 = 1.5
            // 75th: position = 0.75 * 1 = 0.75 → 0.75 * (2.0 - 1.0) + 1.0 = 1.75
            assertQuery("select value, percentile_cont(value, ARRAY[0.25, 0.5, 0.75]) over () from test")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            value\tpercentile_cont
                            1.0\t[1.25,1.5,1.75]
                            2.0\t[1.25,1.5,1.75]
                            """);
        });
    }

    @Test
    public void testMultiPercentileContSlicedPercentiles() throws Exception {
        // a slice or sub-array shares the backing buffer of its parent array, starting at an
        // offset and possibly striding over it
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT x::DOUBLE v, (x % 2)::INT k FROM long_sequence(4))");
            for (String percentiles : new String[]{
                    "ARRAY[0.25, 0.5]",
                    "ARRAY[0.1, 0.25, 0.5, 0.99][2:4]",
                    "ARRAY[[0.1, 0.25], [0.2, 0.5]][1:, 2]",
                    // the slice leaves out the invalid percentile
                    "ARRAY[-1.0, 0.25, 0.5][2:4]"
            }) {
                assertQuery("SELECT v, percentile_cont(v, " + percentiles + ") OVER () p, quantile_cont(v, " + percentiles + ") OVER (PARTITION BY k) q FROM test")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                v\tp\tq
                                1.0\t[1.75,2.5]\t[1.5,2.0]
                                2.0\t[1.75,2.5]\t[2.5,3.0]
                                3.0\t[1.75,2.5]\t[1.5,2.0]
                                4.0\t[1.75,2.5]\t[2.5,3.0]
                                """);
            }
        });
    }

    @Test
    public void testMultiPercentileRejectsMultiDimensionalPercentiles() throws Exception {
        // the result is always one-dimensional, so a deeper percentile list would lose its shape
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (v DOUBLE, k INT)");
            for (String function : new String[]{"percentile_disc", "quantile_disc", "percentile_cont", "quantile_cont"}) {
                String sql = "SELECT " + function + "(v, ARRAY[[0.1, 0.5], [0.9, 0.99]]) OVER () FROM t";
                assertExceptionNoLeakCheck(sql, sql.indexOf('['), "array is not one-dimensional");
                sql = "SELECT " + function + "(v, transpose(ARRAY[[0.1, 0.5], [0.9, 0.99]])) OVER (PARTITION BY k) FROM t";
                assertExceptionNoLeakCheck(sql, sql.indexOf("transpose"), "array is not one-dimensional");
            }
        });
    }

    @Test
    public void testMultiPercentileDiscSlicedPercentiles() throws Exception {
        // a slice or sub-array shares the backing buffer of its parent array, starting at an
        // offset and possibly striding over it
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT x::DOUBLE v, (x % 2)::INT k FROM long_sequence(4))");
            for (String percentiles : new String[]{
                    "ARRAY[0.5, 0.9]",
                    "ARRAY[0.1, 0.5, 0.9, 0.99][2:4]",
                    "ARRAY[[0.1, 0.5], [0.2, 0.9]][1:, 2]",
                    // the slice leaves out the invalid percentile
                    "ARRAY[-1.0, 0.5, 0.9][2:4]"
            }) {
                assertQuery("SELECT v, percentile_disc(v, " + percentiles + ") OVER () p, quantile_disc(v, " + percentiles + ") OVER (PARTITION BY k) q FROM test")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                v\tp\tq
                                1.0\t[2.0,4.0]\t[1.0,3.0]
                                2.0\t[2.0,4.0]\t[2.0,4.0]
                                3.0\t[2.0,4.0]\t[1.0,3.0]
                                4.0\t[2.0,4.0]\t[2.0,4.0]
                                """);
            }
        });
    }

    @Test
    public void testMultiPercentileDiscOverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (" +
                    "select x % 2 as category, cast(x as double) as value from long_sequence(10)" +
                    ")");
            // cat=0 (even): values 2, 4, 6, 8, 10
            //   25th percentile index = ceil(5*0.25)-1 = 1 → 4.0
            //   50th percentile index = ceil(5*0.50)-1 = 2 → 6.0
            //   75th percentile index = ceil(5*0.75)-1 = 3 → 8.0
            // cat=1 (odd): values 1, 3, 5, 7, 9
            //   25th percentile index = ceil(5*0.25)-1 = 1 → 3.0
            //   50th percentile index = ceil(5*0.50)-1 = 2 → 5.0
            //   75th percentile index = ceil(5*0.75)-1 = 3 → 7.0
            assertQuery("select category, value, percentile_disc(value, ARRAY[0.25, 0.5, 0.75]) over (partition by category) from test")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            category\tvalue\tpercentile_disc
                            1\t1.0\t[3.0,5.0,7.0]
                            0\t2.0\t[4.0,6.0,8.0]
                            1\t3.0\t[3.0,5.0,7.0]
                            0\t4.0\t[4.0,6.0,8.0]
                            1\t5.0\t[3.0,5.0,7.0]
                            0\t6.0\t[4.0,6.0,8.0]
                            1\t7.0\t[3.0,5.0,7.0]
                            0\t8.0\t[4.0,6.0,8.0]
                            1\t9.0\t[3.0,5.0,7.0]
                            0\t10.0\t[4.0,6.0,8.0]
                            """);
        });
    }

    @Test
    public void testMultiPercentileDiscOverPartitionReopen() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (" +
                    "SELECT x % 2 AS category, CAST(x AS DOUBLE) AS value FROM long_sequence(10)" +
                    ")");
            assertQuery("SELECT category, value, percentile_disc(value, ARRAY[0.25, 0.5, 0.75]) OVER (PARTITION BY category) FROM test")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            category\tvalue\tpercentile_disc
                            1\t1.0\t[3.0,5.0,7.0]
                            0\t2.0\t[4.0,6.0,8.0]
                            1\t3.0\t[3.0,5.0,7.0]
                            0\t4.0\t[4.0,6.0,8.0]
                            1\t5.0\t[3.0,5.0,7.0]
                            0\t6.0\t[4.0,6.0,8.0]
                            1\t7.0\t[3.0,5.0,7.0]
                            0\t8.0\t[4.0,6.0,8.0]
                            1\t9.0\t[3.0,5.0,7.0]
                            0\t10.0\t[4.0,6.0,8.0]
                            """);
        });
    }

    @Test
    public void testMultiPercentileDiscOverPartitionWithAllNullPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (k SYMBOL, v DOUBLE)");
            // the rows of the all-NULL partition 'b' sit between the rows of partition 'a', so the
            // result array alternates between the NULL array and a sized array
            execute("INSERT INTO t VALUES ('a', 1.0), ('b', null), ('a', 2.0), ('b', null), ('a', 4.0)");
            assertQuery("SELECT k, v, percentile_disc(v, ARRAY[0.5, 0.75]) OVER (PARTITION BY k) p FROM t")
                    .noLeakCheck()
                    .expectSize()
                    .withPlan("""
                            CachedWindow
                              unorderedFunctions: [percentile_disc(v, ARRAY[0.5,0.75]) over (partition by [k])]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """)
                    .returns("""
                            k\tv\tp
                            a\t1.0\t[2.0,4.0]
                            b\tnull\tnull
                            a\t2.0\t[2.0,4.0]
                            b\tnull\tnull
                            a\t4.0\t[2.0,4.0]
                            """);
        });
    }

    @Test
    public void testMultiPercentileDiscOverWholeResultSet() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) value from long_sequence(10))");
            assertQuery("select value, percentile_disc(value, ARRAY[0.25, 0.5, 0.75]) over () from test")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            value\tpercentile_disc
                            1.0\t[3.0,5.0,8.0]
                            2.0\t[3.0,5.0,8.0]
                            3.0\t[3.0,5.0,8.0]
                            4.0\t[3.0,5.0,8.0]
                            5.0\t[3.0,5.0,8.0]
                            6.0\t[3.0,5.0,8.0]
                            7.0\t[3.0,5.0,8.0]
                            8.0\t[3.0,5.0,8.0]
                            9.0\t[3.0,5.0,8.0]
                            10.0\t[3.0,5.0,8.0]
                            """);
        });
    }

    @Test
    public void testMultiPercentileDiscOverWholeResultSetAllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (v DOUBLE)");
            execute("INSERT INTO t VALUES (null), (null)");
            assertQuery("SELECT v, percentile_disc(v, ARRAY[0.5, 0.75]) OVER () p FROM t")
                    .noLeakCheck()
                    .expectSize()
                    .withPlan("""
                            CachedWindow
                              unorderedFunctions: [percentile_disc(v, ARRAY[0.5,0.75]) over ()]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """)
                    .returns("""
                            v\tp
                            null\tnull
                            null\tnull
                            """);
        });
    }

    @Test
    public void testMultiPercentileDiscOverWholeResultSetReopen() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT CAST(x AS DOUBLE) AS value FROM long_sequence(10))");
            assertQuery("SELECT value, percentile_disc(value, ARRAY[0.25, 0.5, 0.75]) OVER () FROM test")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            value\tpercentile_disc
                            1.0\t[3.0,5.0,8.0]
                            2.0\t[3.0,5.0,8.0]
                            3.0\t[3.0,5.0,8.0]
                            4.0\t[3.0,5.0,8.0]
                            5.0\t[3.0,5.0,8.0]
                            6.0\t[3.0,5.0,8.0]
                            7.0\t[3.0,5.0,8.0]
                            8.0\t[3.0,5.0,8.0]
                            9.0\t[3.0,5.0,8.0]
                            10.0\t[3.0,5.0,8.0]
                            """);
        });
    }

    @Test
    public void testMultiPercentileDiscRejectsNonConstantPercentiles() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (v DOUBLE, ps DOUBLE[])");
            final String sql = "SELECT percentile_disc(v, ps) OVER () FROM t";
            assertExceptionNoLeakCheck(sql, sql.indexOf("ps)"), "percentile argument must be a constant");
        });
    }

    @Test
    public void testMultiPercentileDiscRejectsOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT CAST(x AS DOUBLE) AS value FROM long_sequence(10))");
            assertException(
                    "SELECT percentile_disc(value, ARRAY[0.25, 0.5]) OVER (ORDER BY value) FROM test",
                    7,
                    "percentile_disc window function only supports whole partition frames"
            );
        });
    }

    @Test
    public void testMultiPercentileDiscRejectsRowsFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT CAST(x AS DOUBLE) AS value, x % 2 AS cat FROM long_sequence(10))");
            assertException(
                    "SELECT percentile_disc(value, ARRAY[0.25, 0.5]) OVER (PARTITION BY cat ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM test",
                    7,
                    "percentile_disc window function only supports whole partition frames"
            );
        });
    }

    @Test
    public void testMultiPercentileDiscWithAllSameValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select 5.0 as value from long_sequence(10))");
            // All values are 5.0, so all percentiles should return 5.0
            assertQuery("select value, percentile_disc(value, ARRAY[0.25, 0.5, 0.75]) over () from test")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            value\tpercentile_disc
                            5.0\t[5.0,5.0,5.0]
                            5.0\t[5.0,5.0,5.0]
                            5.0\t[5.0,5.0,5.0]
                            5.0\t[5.0,5.0,5.0]
                            5.0\t[5.0,5.0,5.0]
                            5.0\t[5.0,5.0,5.0]
                            5.0\t[5.0,5.0,5.0]
                            5.0\t[5.0,5.0,5.0]
                            5.0\t[5.0,5.0,5.0]
                            5.0\t[5.0,5.0,5.0]
                            """);
        });
    }

    @Test
    public void testMultiPercentileDiscWithDescendingValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(11 - x as double) value from long_sequence(10))");
            // Values will be 10, 9, 8, 7, 6, 5, 4, 3, 2, 1 (in that order)
            // After sorting: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
            // Should give same results as ascending test
            assertQuery("select value, percentile_disc(value, ARRAY[0.25, 0.5, 0.75]) over () from test")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            value\tpercentile_disc
                            10.0\t[3.0,5.0,8.0]
                            9.0\t[3.0,5.0,8.0]
                            8.0\t[3.0,5.0,8.0]
                            7.0\t[3.0,5.0,8.0]
                            6.0\t[3.0,5.0,8.0]
                            5.0\t[3.0,5.0,8.0]
                            4.0\t[3.0,5.0,8.0]
                            3.0\t[3.0,5.0,8.0]
                            2.0\t[3.0,5.0,8.0]
                            1.0\t[3.0,5.0,8.0]
                            """);
        });
    }

    @Test
    public void testMultiPercentileDiscWithExtremePercentiles() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) value from long_sequence(10))");
            // Test with 0th, 50th, and 100th percentiles
            // 0th: max(0, ceil(10 * 0.0) - 1) = 0 → index 0 → 1.0
            // 50th: ceil(10 * 0.5) - 1 = 5 - 1 = 4 → index 4 → 5.0
            // 100th: ceil(10 * 1.0) - 1 = 10 - 1 = 9 → index 9 → 10.0
            assertQuery("select value, percentile_disc(value, ARRAY[0.0, 0.5, 1.0]) over () from test")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            value\tpercentile_disc
                            1.0\t[1.0,5.0,10.0]
                            2.0\t[1.0,5.0,10.0]
                            3.0\t[1.0,5.0,10.0]
                            4.0\t[1.0,5.0,10.0]
                            5.0\t[1.0,5.0,10.0]
                            6.0\t[1.0,5.0,10.0]
                            7.0\t[1.0,5.0,10.0]
                            8.0\t[1.0,5.0,10.0]
                            9.0\t[1.0,5.0,10.0]
                            10.0\t[1.0,5.0,10.0]
                            """);
        });
    }

    @Test
    public void testMultiPercentileDiscWithManyPercentiles() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) value from long_sequence(10))");
            // Test with 10 different percentiles
            assertQuery("select value, percentile_disc(value, ARRAY[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]) over () from test")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            value\tpercentile_disc
                            1.0\t[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0]
                            2.0\t[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0]
                            3.0\t[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0]
                            4.0\t[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0]
                            5.0\t[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0]
                            6.0\t[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0]
                            7.0\t[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0]
                            8.0\t[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0]
                            9.0\t[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0]
                            10.0\t[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0]
                            """);
        });
    }

    @Test
    public void testMultiPercentileDiscWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (" +
                    "select x, " +
                    "case when x % 3 = 0 then null else cast(x as double) end as value " +
                    "from long_sequence(10)" +
                    ")");
            // Non-null values: 1, 2, 4, 5, 7, 8, 10 (7 values)
            // 25th percentile: ceil(7 * 0.25) - 1 = 2 - 1 = 1 → index 1 → 2.0
            // 50th percentile: ceil(7 * 0.50) - 1 = 4 - 1 = 3 → index 3 → 5.0
            // 75th percentile: ceil(7 * 0.75) - 1 = 6 - 1 = 5 → index 5 → 8.0
            assertQuery("select x, value, percentile_disc(value, ARRAY[0.25, 0.5, 0.75]) over () from test")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x\tvalue\tpercentile_disc
                            1\t1.0\t[2.0,5.0,8.0]
                            2\t2.0\t[2.0,5.0,8.0]
                            3\tnull\t[2.0,5.0,8.0]
                            4\t4.0\t[2.0,5.0,8.0]
                            5\t5.0\t[2.0,5.0,8.0]
                            6\tnull\t[2.0,5.0,8.0]
                            7\t7.0\t[2.0,5.0,8.0]
                            8\t8.0\t[2.0,5.0,8.0]
                            9\tnull\t[2.0,5.0,8.0]
                            10\t10.0\t[2.0,5.0,8.0]
                            """);
        });
    }

    @Test
    public void testMultiPercentileDiscWithSingleValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) value from long_sequence(10))");
            assertQuery("select value, percentile_disc(value, ARRAY[0.5]) over () from test")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            value\tpercentile_disc
                            1.0\t[5.0]
                            2.0\t[5.0]
                            3.0\t[5.0]
                            4.0\t[5.0]
                            5.0\t[5.0]
                            6.0\t[5.0]
                            7.0\t[5.0]
                            8.0\t[5.0]
                            9.0\t[5.0]
                            10.0\t[5.0]
                            """);
        });
    }

    @Test
    public void testMultiPercentileInterleavedWithVarSizeColumns() throws Exception {
        // Array outputs between base VARCHAR, STRING, SYMBOL and DOUBLE[] columns, and over a
        // base that computes its VARCHAR partition key: each column keeps its own chain slot.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x DOUBLE, a VARCHAR, b VARCHAR, s STRING, sym SYMBOL, arr DOUBLE[])");
            execute("""
                    INSERT INTO t VALUES
                    (1.0, 'g1', 'h1', 's1', 'A', ARRAY[1.0]),
                    (2.0, 'g1', 'h2', 's2', 'B', ARRAY[2.0]),
                    (10.0, 'g2', 'h1', 's3', 'A', ARRAY[3.0]),
                    (20.0, 'g2', 'h2', 's4', 'B', ARRAY[4.0])
                    """);
            for (int copierType = 0; copierType < 4; copierType++) {
                setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, copierType);
                assertQuery("""
                        SELECT percentile_disc(x, ARRAY[0.5]) OVER () p1, a,
                        percentile_cont(x, ARRAY[0.5]) OVER (PARTITION BY b) p2, sym, b,
                        quantile_disc(x, ARRAY[0.25, 0.75]) OVER (PARTITION BY a) p3, s, arr
                        FROM t
                        """)
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                p1\ta\tp2\tsym\tb\tp3\ts\tarr
                                [2.0]\tg1\t[5.5]\tA\th1\t[1.0,2.0]\ts1\t[1.0]
                                [2.0]\tg1\t[11.0]\tB\th2\t[1.0,2.0]\ts2\t[2.0]
                                [2.0]\tg2\t[5.5]\tA\th1\t[10.0,20.0]\ts3\t[3.0]
                                [2.0]\tg2\t[11.0]\tB\th2\t[10.0,20.0]\ts4\t[4.0]
                                """);
                assertQuery("""
                        SELECT percentile_disc(x, ARRAY[0.5]) OVER (PARTITION BY ab) p, ab, sym
                        FROM (SELECT x, a || '-' || b ab, sym FROM t)
                        """)
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                p\tab\tsym
                                [1.0]\tg1-h1\tA
                                [2.0]\tg1-h2\tB
                                [10.0]\tg2-h1\tA
                                [20.0]\tg2-h2\tB
                                """);
            }
        });
    }

    @Test
    public void testPercentileContEmptyFrame() throws Exception {
        // a frame that ends before it starts holds no rows, so the percentile is NULL
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (k SYMBOL, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                    ('a', 1.0, '2024-01-01T00:00:00.000000Z'),
                    ('a', 2.0, '2024-01-01T00:01:00.000000Z'),
                    ('b', 3.0, '2024-01-01T00:02:00.000000Z')
                    """);
            assertQuery("""
                    SELECT v,
                        percentile_cont(v, 0.5) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) p,
                        percentile_cont(v, 0.5) OVER (PARTITION BY k ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) pk
                    FROM t
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            v\tp\tpk
                            1.0\tnull\tnull
                            2.0\tnull\tnull
                            3.0\tnull\tnull
                            """);
        });
    }

    @Test
    public void testPercentileContOverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (" +
                    "select x % 2 as category, cast(x as double) as value from long_sequence(10)" +
                    ")");
            assertQuery("select category, value, percentile_cont(value, 0.5) over (partition by category) from test")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            category\tvalue\tpercentile_cont
                            1\t1.0\t5.0
                            0\t2.0\t6.0
                            1\t3.0\t5.0
                            0\t4.0\t6.0
                            1\t5.0\t5.0
                            0\t6.0\t6.0
                            1\t7.0\t5.0
                            0\t8.0\t6.0
                            1\t9.0\t5.0
                            0\t10.0\t6.0
                            """);
        });
    }

    @Test
    public void testPercentileContOverPartitionWithAllNullPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (k SYMBOL, v DOUBLE)");
            execute("INSERT INTO t VALUES ('a', 1.0), ('b', null), ('a', 2.0), ('b', null), ('a', 4.0)");
            assertQuery("SELECT k, v, percentile_cont(v, 0.75) OVER (PARTITION BY k) p FROM t")
                    .noLeakCheck()
                    .expectSize()
                    .withPlan("""
                            CachedWindowLight
                              unorderedFunctions: [percentile_cont(v, 0.75) over (partition by [k])]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """)
                    .returns("""
                            k\tv\tp
                            a\t1.0\t3.0
                            b\tnull\tnull
                            a\t2.0\t3.0
                            b\tnull\tnull
                            a\t4.0\t3.0
                            """);
        });
    }

    @Test
    public void testPercentileContOverWholeResultSet() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) value from long_sequence(10))");
            assertQuery("select value, percentile_cont(value, 0.5) over () from test")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            value\tpercentile_cont
                            1.0\t5.5
                            2.0\t5.5
                            3.0\t5.5
                            4.0\t5.5
                            5.0\t5.5
                            6.0\t5.5
                            7.0\t5.5
                            8.0\t5.5
                            9.0\t5.5
                            10.0\t5.5
                            """);
        });
    }

    @Test
    public void testPercentileContInterpolationBetweenExtremes() throws Exception {
        // hi - lo overflows for these two values, but every interpolated value lies between them
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (x DOUBLE, k INT)");
            execute("INSERT INTO test VALUES (1.7976931348623157E308, 0), (-1.7976931348623157E308, 0)");
            assertQuery("SELECT percentile_cont(x, 0.5) OVER () w50, percentile_cont(x, 0.25) OVER (PARTITION BY k) p25 FROM test")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            w50\tp25
                            0.0\t-8.988465674311579E307
                            0.0\t-8.988465674311579E307
                            """);
        });
    }

    @Test
    public void testPercentileContRejectsNegativePercentile() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT x::DOUBLE x, x % 2 k FROM long_sequence(10))");
            String sql = "SELECT percentile_cont(x, -0.5) OVER () FROM test";
            assertExceptionNoLeakCheck(sql, sql.indexOf('-'), "invalid percentile [expected=range(0.0, 1.0), actual=-0.5]");
            sql = "SELECT percentile_cont(x, -0.5) OVER (PARTITION BY k) FROM test";
            assertExceptionNoLeakCheck(sql, sql.indexOf('-'), "invalid percentile [expected=range(0.0, 1.0), actual=-0.5]");
            sql = "SELECT percentile_cont(x, ARRAY[0.5, -0.25]) OVER () FROM test";
            assertExceptionNoLeakCheck(sql, sql.indexOf('['), "invalid percentile [expected=range(0.0, 1.0), actual=-0.25]");
            sql = "SELECT percentile_cont(x, ARRAY[0.5, -0.25]) OVER (PARTITION BY k) FROM test";
            assertExceptionNoLeakCheck(sql, sql.indexOf('['), "invalid percentile [expected=range(0.0, 1.0), actual=-0.25]");
        });
    }

    @Test
    public void testPercentileContRejectsNonConstantPercentile() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (v DOUBLE, p DOUBLE)");
            final String sql = "SELECT percentile_cont(v, p) OVER () FROM t";
            assertExceptionNoLeakCheck(sql, sql.indexOf("p)"), "percentile argument must be a constant");
        });
    }

    @Test
    public void testPercentileContRejectsNonWholePartitionFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) value, x % 2 as category from long_sequence(10))");
            assertException(
                    "select percentile_cont(value, 0.5) over (partition by category rows between 1 preceding and current row) from test",
                    7,
                    "percentile_cont window function only supports whole partition frames"
            );
        });
    }

    @Test
    public void testPercentileContRejectsOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) value from long_sequence(10))");
            assertException(
                    "select percentile_cont(value, 0.5) over (order by value) from test",
                    7,
                    "percentile_cont window function only supports whole partition frames"
            );
        });
    }

    @Test
    public void testPercentileContWith75thPercentile() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) value from long_sequence(10))");
            assertQuery("select percentile_cont(value, 0.75) over () from test")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            percentile_cont
                            7.75
                            7.75
                            7.75
                            7.75
                            7.75
                            7.75
                            7.75
                            7.75
                            7.75
                            7.75
                            """);
        });
    }

    @Test
    public void testPercentileContWithInterpolation() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) value from long_sequence(5))");
            // For 5 values (1,2,3,4,5), 25th percentile at position 0.25*(5-1) = 1.0
            // This is exactly at index 1, so result should be the value at index 1 after sorting = 2.0
            assertQuery("select percentile_cont(value, 0.25) over () from test")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            percentile_cont
                            2.0
                            2.0
                            2.0
                            2.0
                            2.0
                            """);
        });
    }

    @Test
    public void testPercentileDisc0thPercentile() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) value from long_sequence(10))");
            assertQuery("select percentile_disc(value, 0) over () from test")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            percentile_disc
                            1.0
                            1.0
                            1.0
                            1.0
                            1.0
                            1.0
                            1.0
                            1.0
                            1.0
                            1.0
                            """);
        });
    }

    @Test
    public void testPercentileDisc100thPercentile() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) value from long_sequence(10))");
            assertQuery("select percentile_disc(value, 1.0) over () from test")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            percentile_disc
                            10.0
                            10.0
                            10.0
                            10.0
                            10.0
                            10.0
                            10.0
                            10.0
                            10.0
                            10.0
                            """);
        });
    }

    @Test
    public void testPercentileDiscEmptyFrame() throws Exception {
        // a frame that ends before it starts holds no rows, so the percentile is NULL
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (k SYMBOL, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                    ('a', 1.0, '2024-01-01T00:00:00.000000Z'),
                    ('a', 2.0, '2024-01-01T00:01:00.000000Z'),
                    ('b', 3.0, '2024-01-01T00:02:00.000000Z')
                    """);
            assertQuery("""
                    SELECT v,
                        percentile_disc(v, 0.5) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) p,
                        percentile_disc(v, 0.5) OVER (PARTITION BY k ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) pk
                    FROM t
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            v\tp\tpk
                            1.0\tnull\tnull
                            2.0\tnull\tnull
                            3.0\tnull\tnull
                            """);
        });
    }

    @Test
    public void testPercentileDiscMultiplePartitions() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (" +
                    "select x % 2 as cat1, cast(x as double) as value " +
                    "from long_sequence(10)" +
                    ")");
            // cat1=0 (even): values 2, 4, 6, 8, 10 → 50th percentile index = ceil(5*0.5)-1 = 2 → value 6.0
            // cat1=1 (odd):  values 1, 3, 5, 7, 9  → 50th percentile index = ceil(5*0.5)-1 = 2 → value 5.0
            assertQuery("select cat1, value, percentile_disc(value, 0.5) over (partition by cat1) from test")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            cat1\tvalue\tpercentile_disc
                            1\t1.0\t5.0
                            0\t2.0\t6.0
                            1\t3.0\t5.0
                            0\t4.0\t6.0
                            1\t5.0\t5.0
                            0\t6.0\t6.0
                            1\t7.0\t5.0
                            0\t8.0\t6.0
                            1\t9.0\t5.0
                            0\t10.0\t6.0
                            """);
        });
    }

    @Test
    public void testPercentileDiscOverPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (" +
                    "select x % 2 as category, cast(x as double) as value from long_sequence(10)" +
                    ")");
            assertQuery("select category, value, percentile_disc(value, 0.5) over (partition by category) from test")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            category\tvalue\tpercentile_disc
                            1\t1.0\t5.0
                            0\t2.0\t6.0
                            1\t3.0\t5.0
                            0\t4.0\t6.0
                            1\t5.0\t5.0
                            0\t6.0\t6.0
                            1\t7.0\t5.0
                            0\t8.0\t6.0
                            1\t9.0\t5.0
                            0\t10.0\t6.0
                            """);
        });
    }

    @Test
    public void testPercentileDiscOverPartitionWithAllNullPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (k SYMBOL, v DOUBLE)");
            execute("INSERT INTO t VALUES ('a', 1.0), ('b', null), ('a', 2.0), ('b', null), ('a', 4.0)");
            assertQuery("SELECT k, v, percentile_disc(v, 0.75) OVER (PARTITION BY k) p FROM t")
                    .noLeakCheck()
                    .expectSize()
                    .withPlan("""
                            CachedWindowLight
                              unorderedFunctions: [percentile_disc(v, 0.75) over (partition by [k])]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """)
                    .returns("""
                            k\tv\tp
                            a\t1.0\t4.0
                            b\tnull\tnull
                            a\t2.0\t4.0
                            b\tnull\tnull
                            a\t4.0\t4.0
                            """);
        });
    }

    @Test
    public void testPercentileDiscOverWholeResultSet() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) value from long_sequence(10))");
            assertQuery("select value, percentile_disc(value, 0.5) over () from test")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            value\tpercentile_disc
                            1.0\t5.0
                            2.0\t5.0
                            3.0\t5.0
                            4.0\t5.0
                            5.0\t5.0
                            6.0\t5.0
                            7.0\t5.0
                            8.0\t5.0
                            9.0\t5.0
                            10.0\t5.0
                            """);
        });
    }

    @Test
    public void testPercentileDiscAtWholeNumberProduct() throws Exception {
        // Each partition holds 100 values. 0.07 * 100 evaluates to 7.000000000000001; the 7th
        // value's cumulative distribution, 7 / 100, already reaches 0.07, so the result is the
        // 7th value, not the 8th. The same holds for 0.14, 0.28, 0.55 and 0.56.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT x::DOUBLE x, x % 2 k FROM long_sequence(200))");
            assertQuery(
                    "SELECT x, d, a, w FROM (" +
                            "SELECT x, percentile_disc(x, 0.07) OVER (PARTITION BY k) d, " +
                            "percentile_disc(x, ARRAY[0.07, 0.14, 0.28, 0.55, 0.56]) OVER (PARTITION BY k) a, " +
                            "percentile_disc(x, ARRAY[0.07, 0.14, 0.28, 0.55, 0.56]) OVER () w " +
                            "FROM test) WHERE x <= 2 ORDER BY x"
            )
                    .noLeakCheck()
                    .returns("""
                            x\td\ta\tw
                            1.0\t13.0\t[13.0,27.0,55.0,109.0,111.0]\t[14.0,28.0,56.0,110.0,112.0]
                            2.0\t14.0\t[14.0,28.0,56.0,110.0,112.0]\t[14.0,28.0,56.0,110.0,112.0]
                            """);
            assertQuery("SELECT percentile_disc(x, 0.07) OVER () FROM test LIMIT 1")
                    .noLeakCheck()
                    .returns("""
                            percentile_disc
                            14.0
                            """);
        });
    }

    @Test
    public void testPercentileDiscRejectsNegativePercentile() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT x::DOUBLE x, x % 2 k FROM long_sequence(10))");
            String sql = "SELECT percentile_disc(x, -0.5) OVER () FROM test";
            assertExceptionNoLeakCheck(sql, sql.indexOf('-'), "invalid percentile [expected=range(0.0, 1.0), actual=-0.5]");
            sql = "SELECT percentile_disc(x, -0.5) OVER (PARTITION BY k) FROM test";
            assertExceptionNoLeakCheck(sql, sql.indexOf('-'), "invalid percentile [expected=range(0.0, 1.0), actual=-0.5]");
            sql = "SELECT percentile_disc(x, ARRAY[0.5, -0.25]) OVER () FROM test";
            assertExceptionNoLeakCheck(sql, sql.indexOf('['), "invalid percentile [expected=range(0.0, 1.0), actual=-0.25]");
            sql = "SELECT percentile_disc(x, ARRAY[0.5, -0.25]) OVER (PARTITION BY k) FROM test";
            assertExceptionNoLeakCheck(sql, sql.indexOf('['), "invalid percentile [expected=range(0.0, 1.0), actual=-0.25]");
        });
    }

    @Test
    public void testPercentileDiscRejectsNonConstantPercentile() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (v DOUBLE, p DOUBLE)");
            final String sql = "SELECT percentile_disc(v, p) OVER () FROM t";
            assertExceptionNoLeakCheck(sql, sql.indexOf("p)"), "percentile argument must be a constant");
        });
    }

    @Test
    public void testPercentileDiscRejectsNonWholePartitionFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) value, x % 2 as category from long_sequence(10))");
            assertException(
                    "select percentile_disc(value, 0.5) over (partition by category rows between 1 preceding and current row) from test",
                    7,
                    "percentile_disc window function only supports whole partition frames"
            );
        });
    }

    @Test
    public void testPercentileDiscRejectsOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) value from long_sequence(10))");
            assertException(
                    "select percentile_disc(value, 0.5) over (order by value) from test",
                    7,
                    "percentile_disc window function only supports whole partition frames"
            );
        });
    }

    @Test
    public void testPercentileDiscWith75thPercentile() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) value from long_sequence(10))");
            assertQuery("select percentile_disc(value, 0.75) over () from test")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            percentile_disc
                            8.0
                            8.0
                            8.0
                            8.0
                            8.0
                            8.0
                            8.0
                            8.0
                            8.0
                            8.0
                            """);
        });
    }

    @Test
    public void testPercentileDiscWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (value double)");
            execute("insert into test values (1.0), (2.0), (null), (4.0), (5.0)");
            // For 4 non-null values: [1.0, 2.0, 4.0, 5.0], 50th percentile index = ceil(4 * 0.5) - 1 = 1
            // After sorting: [1.0, 2.0, 4.0, 5.0], index 1 = 2.0
            assertQuery("select percentile_disc(value, 0.5) over () from test")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            percentile_disc
                            2.0
                            2.0
                            2.0
                            2.0
                            2.0
                            """);
        });
    }

    @Test
    public void testPercentileLargePartitionCapacityGrowth() throws Exception {
        // Exercises capacity growth path in partitioned window percentile:
        // INITIAL_CAPACITY=16 doubles through 16 -> 32 -> 64 -> 128 for 100-row partition.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE large_part AS (" +
                    "SELECT x % 2 AS g, CAST(x AS DOUBLE) AS value FROM long_sequence(100)" +
                    ")");

            // g=0 (even): 50 values [2, 4, 6, ..., 100]
            //   percentile_disc(0.5): ceil(50 * 0.5) - 1 = 24 -> sorted[24] = 50.0
            // g=1 (odd): 50 values [1, 3, 5, ..., 99]
            //   percentile_disc(0.5): ceil(50 * 0.5) - 1 = 24 -> sorted[24] = 49.0
            assertQuery("SELECT DISTINCT g, percentile_disc(value, 0.5) OVER (PARTITION BY g) FROM large_part ORDER BY g")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            g\tpercentile_disc
                            0\t50.0
                            1\t49.0
                            """);

            // percentile_cont(0.5): position = 0.5 * (50 - 1) = 24.5
            //   g=0 even: lerp(sorted[24], sorted[25]) = lerp(50.0, 52.0) = 51.0
            //   g=1 odd: lerp(sorted[24], sorted[25]) = lerp(49.0, 51.0) = 50.0
            assertQuery("SELECT DISTINCT g, percentile_cont(value, 0.5) OVER (PARTITION BY g) FROM large_part ORDER BY g")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            g\tpercentile_cont
                            0\t51.0
                            1\t50.0
                            """);

            // Multi-percentile variants
            assertQuery("SELECT DISTINCT g, percentile_disc(value, ARRAY[0.25, 0.5, 0.75]) OVER (PARTITION BY g) FROM large_part ORDER BY g")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            g\tpercentile_disc
                            0\t[26.0,50.0,76.0]
                            1\t[25.0,49.0,75.0]
                            """);

            assertQuery("SELECT DISTINCT g, percentile_cont(value, ARRAY[0.25, 0.5, 0.75]) OVER (PARTITION BY g) FROM large_part ORDER BY g")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            g\tpercentile_cont
                            0\t[26.5,51.0,75.5]
                            1\t[25.5,50.0,74.5]
                            """);
        });
    }

    @Test
    public void testQuantileAliasesRejectFramesUnderOwnName() throws Exception {
        // The aliases reject the same frames as percentile_disc and percentile_cont, and the error names
        // the function the query called.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, k SYMBOL, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            assertExceptionNoLeakCheck(
                    "SELECT quantile_disc(v, 0.5) OVER (PARTITION BY k ORDER BY ts) FROM t",
                    7,
                    "quantile_disc window function only supports whole partition frames"
            );
            assertExceptionNoLeakCheck(
                    "SELECT quantile_cont(v, 0.5) OVER (ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t",
                    7,
                    "quantile_cont window function only supports whole partition frames"
            );
            assertExceptionNoLeakCheck(
                    "SELECT quantile_disc(v, ARRAY[0.25, 0.75]) OVER (ORDER BY ts) FROM t",
                    7,
                    "quantile_disc window function only supports whole partition frames"
            );
            assertExceptionNoLeakCheck(
                    "SELECT quantile_cont(v, ARRAY[0.25, 0.75]) OVER (PARTITION BY k ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t",
                    7,
                    "quantile_cont window function only supports whole partition frames"
            );
        });
    }

    @Test
    public void testQuantileAliasesShowOwnNameInPlan() throws Exception {
        // EXPLAIN names the alias the query called rather than the percentile function behind it, and so
        // does the null function an empty frame compiles to.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (k SYMBOL, v DOUBLE)");
            execute("INSERT INTO t VALUES ('a', 1.0), ('b', 2.0), ('a', 3.0)");
            assertQuery("""
                    SELECT k, v, quantile_disc(v, 0.5) OVER (PARTITION BY k) qd, quantile_cont(v, 0.5) OVER () qc,
                        quantile_disc(v, ARRAY[0.5]) OVER () qda, quantile_cont(v, ARRAY[0.5]) OVER (PARTITION BY k) qca
                    FROM t
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .withPlan("""
                            CachedWindow
                              unorderedFunctions: [quantile_disc(v, 0.5) over (partition by [k]),quantile_cont(v, 0.5) over (),quantile_disc(v, ARRAY[0.5]) over (),quantile_cont(v, ARRAY[0.5]) over (partition by [k])]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """)
                    .returns("""
                            k\tv\tqd\tqc\tqda\tqca
                            a\t1.0\t1.0\t2.0\t[2.0]\t[2.0]
                            b\t2.0\t2.0\t2.0\t[2.0]\t[2.0]
                            a\t3.0\t1.0\t2.0\t[2.0]\t[2.0]
                            """);
            assertQuery("SELECT v, quantile_disc(v, 0.5) OVER (ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) qd FROM t")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .withPlan("""
                            Window
                              functions: [quantile_disc(v) over ( rows between 1 preceding and 2 preceding)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """)
                    .returns("""
                            v\tqd
                            1.0\tnull
                            2.0\tnull
                            3.0\tnull
                            """);
        });
    }

    @Test
    public void testQuantileContAliasEquivalence() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT CAST(x AS DOUBLE) AS value FROM long_sequence(10))");
            // quantile_cont must produce the same result as percentile_cont
            assertQuery("SELECT percentile_cont(value, 0.5) OVER () AS pc, quantile_cont(value, 0.5) OVER () AS qc FROM test")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            pc\tqc
                            5.5\t5.5
                            5.5\t5.5
                            5.5\t5.5
                            5.5\t5.5
                            5.5\t5.5
                            5.5\t5.5
                            5.5\t5.5
                            5.5\t5.5
                            5.5\t5.5
                            5.5\t5.5
                            """);
        });
    }

    @Test
    public void testQuantileDiscAliasEquivalence() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT CAST(x AS DOUBLE) AS value FROM long_sequence(10))");
            // quantile_disc must produce the same result as percentile_disc
            assertQuery("SELECT percentile_disc(value, 0.5) OVER () AS pd, quantile_disc(value, 0.5) OVER () AS qd FROM test")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            pd\tqd
                            5.0\t5.0
                            5.0\t5.0
                            5.0\t5.0
                            5.0\t5.0
                            5.0\t5.0
                            5.0\t5.0
                            5.0\t5.0
                            5.0\t5.0
                            5.0\t5.0
                            5.0\t5.0
                            """);
        });
    }

    private static void assertMapGroupCount(String sql, int expected) throws Exception {
        try (
                SqlCompiler compiler = engine.getSqlCompiler();
                RecordCursorFactory factory = select(compiler, sql, sqlExecutionContext)
        ) {
            final CachedWindowMapGroups groups = windowFactoryOf(factory) instanceof CachedWindowRecordCursorFactory cached
                    ? cached.getWindowMapGroups()
                    : null;
            Assert.assertNotNull("no window Map group was compiled", groups);
            Assert.assertEquals(expected, groups.getStates().size());
        }
    }

    private static void assertWindowFactory(
            String sql,
            Class<? extends RecordCursorFactory> expectedClass,
            String expectedColumns
    ) throws Exception {
        try (
                SqlCompiler compiler = engine.getSqlCompiler();
                RecordCursorFactory factory = select(compiler, sql, sqlExecutionContext)
        ) {
            final RecordCursorFactory windowFactory = windowFactoryOf(factory);
            Assert.assertEquals(expectedClass, windowFactory.getClass());
            final RecordMetadata metadata = windowFactory.getMetadata();
            final StringSink columns = new StringSink();
            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                if (i > 0) {
                    columns.put(',');
                }
                columns.put(metadata.getColumnName(i));
            }
            Assert.assertEquals(expectedColumns, columns.toString());
        }
    }

    private static RecordCursorFactory windowFactoryOf(RecordCursorFactory factory) {
        // a projection wrapper can sit above the window factory
        RecordCursorFactory root = factory;
        while (root != null
                && !(root instanceof CachedWindowRecordCursorFactory)
                && !(root instanceof CachedWindowLightRecordCursorFactory)) {
            root = root.getBaseFactory();
        }
        Assert.assertNotNull("no cached window factory in the tree", root);
        return root;
    }
}
