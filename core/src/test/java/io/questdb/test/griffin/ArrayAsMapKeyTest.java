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

import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.mp.WorkerPool;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class ArrayAsMapKeyTest extends AbstractCairoTest {

    @Test
    public void testArrayAsGroupByKey() throws Exception {
        execute("create table array_test(k symbol, ob_buy double[][], ob_sell double[][], ts timestamp) timestamp(ts) partition by day ;");
        execute(
                """
                        insert into array_test values
                           ('vod', ARRAY[[9., 1000], [10., 10000]], ARRAY[[12., 1000], [11., 10000]], 123),
                           ('vod2', ARRAY[[4., 1000], [10., 10000]], ARRAY[[12., 1000], [11., 10000]], 123),
                           ('vod3', ARRAY[[3., 1000], [10., 10000]], ARRAY[[12., 1000], [11., 10000]], 123)
                           ;
                        """
        );
        assertQuery(
                """
                        []\tk\tcount
                        [[9.0,1000.0],[10.0,10000.0]]\tvod\t1
                        [[4.0,1000.0],[10.0,10000.0]]\tvod2\t1
                        [[3.0,1000.0],[10.0,10000.0]]\tvod3\t1
                        """,
                "select ob_buy[1:], k, count() from array_test;",
                true,
                true
        );

        assertQuery(
                """
                        ob_buy\tk\tcount
                        [[9.0,1000.0],[10.0,10000.0]]\tvod\t1
                        [[4.0,1000.0],[10.0,10000.0]]\tvod2\t1
                        [[3.0,1000.0],[10.0,10000.0]]\tvod3\t1
                        """,
                "select ob_buy, k, count() from array_test;",
                true,
                true
        );
    }

    @Test
    public void testArrayAsOrderByColumn() throws Exception {
        execute("create table array_test(k symbol, ob_buy double[][], ob_sell double[][], ts timestamp) timestamp(ts) partition by day ;");
        execute(
                """
                        insert into array_test values
                           ('vod', ARRAY[[9., 1000], [10., 10000]], ARRAY[[12., 1000], [11., 10000]], 123),
                           ('vod2', ARRAY[[4., 1000], [10., 10000]], ARRAY[[12., 1000], [11., 10000]], 123),
                           ('vod3', ARRAY[[3., 1000], [10., 10000]], ARRAY[[12., 1000], [11., 10000]], 123)
                           ;
                        """
        );

        assertException(
                "select ob_buy[1:] c, k from array_test order by c;",
                48,
                "DOUBLE[][] is not a supported type in ORDER BY clause"
        );

        assertException(
                "select ob_buy, k from array_test order by ob_buy;",
                42,
                "DOUBLE[][] is not a supported type in ORDER BY clause"
        );

        assertException(
                "select k, ob_buy from array_test order by 2;",
                42,
                "DOUBLE[][] is not a supported type in ORDER BY clause"
        );
    }

    @Test
    public void testDistinct2dArrayPlusColumnKeyDoesNotLeak() throws Exception {
        // A non-thread-safe 2D ARRAY key (ArrayCreateFunctionFactory output, which owns a
        // DirectArray) together with a non-thread-safe SYMBOL column key exercises the
        // per-worker copy path: the per-worker copy of the 2D array key must be extracted and
        // freed, and the column key must not be copied at all.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO tab SELECT rnd_symbol('a','b','c'), (x*1_000_000)::timestamp FROM long_sequence(100)");
            assertQueryNoLeakCheck(
                    """
                            k\tsym
                            [[0.1,0.2],[0.3,0.4]]\ta
                            [[0.1,0.2],[0.3,0.4]]\tb
                            [[0.1,0.2],[0.3,0.4]]\tc
                            """,
                    "SELECT DISTINCT ARRAY[ARRAY[0.1,0.2],ARRAY[0.3,0.4]] k, sym FROM tab ORDER BY sym",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testDistinctConstArrayPlusColumnKeyDoesNotLeak() throws Exception {
        // DISTINCT over a thread-safe constant ARRAY key together with a non-thread-safe
        // SYMBOL column key drives the parallel/async GROUP BY per-worker key-function path.
        // The constant array folds to a thread-safe ArrayConstant; its per-worker DirectArray
        // copy must still be owned and freed (regression for a NATIVE_ND_ARRAY leak).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO tab SELECT rnd_symbol('a','b','c'), (x*1_000_000)::timestamp FROM long_sequence(100)");
            assertQueryNoLeakCheck(
                    """
                            arr\tsym
                            [0.5]\ta
                            [0.5]\tb
                            [0.5]\tc
                            """,
                    "SELECT DISTINCT ARRAY[0.5] arr, sym FROM tab ORDER BY sym",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testDistinctOverTwoArrayKeysWithNullValue() throws Exception {
        // A null value in the first array key column lays out as just an 8-byte
        // NULL_LEN marker, so addressOfKeyColumn must advance by 8 before reading
        // the next var-size key. A prior off-by-one in getPlainValueSize(long)
        // returned 8 + NULL_LEN = 7, corrupting the offset of the following array
        // column and tripping the "typeTag of encodedType is not ARRAY" assert.
        assertMemoryLeak(() -> {
            final int workerCount = 4;
            final WorkerPool pool = new WorkerPool(() -> workerCount);
            TestUtils.setupWorkerPool(pool, engine);
            pool.start(LOG);
            try (
                    SqlExecutionContext parallelCtx = new SqlExecutionContextImpl(engine, workerCount)
                            .with(securityContext, bindVariableService, null, -1, circuitBreaker)
            ) {
                parallelCtx.initNow();
                execute("CREATE TABLE t (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
                // The null v's become a null DOUBLE[] when cast; that's the slot that triggers the off-by-one.
                execute(
                        "INSERT INTO t SELECT rnd_long(-1_000_000, 1_000_000, 4) v, " +
                                "timestamp_sequence(to_timestamp('2024-01-01','yyyy-MM-dd'), 1_800_000_000L) ts " +
                                "FROM long_sequence(200)"
                );
                final StringSink sink = new StringSink();
                // The print path is what trips the assert — it iterates the cursor and reads each
                // array column from the ordered-map record, exercising addressOfKeyColumn.
                TestUtils.printSql(
                        engine,
                        parallelCtx,
                        "SELECT DISTINCT v::DOUBLE[] a0, ARRAY[0.1, 0.2] a1 FROM t",
                        sink
                );
            } finally {
                pool.halt();
            }
        });
    }

    @Test
    public void testGroupByConstArrayKeyWithNonThreadSafeAggDoesNotLeak() throws Exception {
        // A thread-safe constant ARRAY key alongside a non-thread-safe aggregate
        // (count_distinct) makes the async GROUP BY create per-worker copies (the aggregate
        // forces it), while the array key itself stays thread-safe. The per-worker
        // ArrayConstant copy must still be extracted and freed: the extraction must not skip
        // it just because the key functions are thread-safe (regression for a NATIVE_ND_ARRAY
        // leak distinct from the DISTINCT-plus-column case above).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO tab SELECT rnd_symbol('a','b','c'), (x*1_000_000)::timestamp FROM long_sequence(100)");
            assertQueryNoLeakCheck(
                    """
                            k\tc
                            [0.5]\t3
                            """,
                    "SELECT ARRAY[0.5] k, count_distinct(sym) c FROM tab",
                    null,
                    true,
                    true
            );
        });
    }
}
