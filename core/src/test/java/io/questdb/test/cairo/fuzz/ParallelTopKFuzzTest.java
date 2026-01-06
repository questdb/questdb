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

package io.questdb.test.cairo.fuzz;

import io.questdb.PropertyKey;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static io.questdb.test.cairo.fuzz.ParallelGroupByFuzzTest.assertQueries;

// Tests ORDER BY + LIMIT (top K) parallel execution.
// This is not a fuzz test in traditional sense, but it's multithreaded, and we want to run it
// in CI frequently along with other fuzz tests.
@RunWith(Parameterized.class)
public class ParallelTopKFuzzTest extends AbstractCairoTest {
    private static final int PAGE_FRAME_COUNT = 4; // also used to set queue size, so must be a power of 2
    private static final int PAGE_FRAME_MAX_ROWS = 100;
    private static final int ROW_COUNT = 10 * PAGE_FRAME_COUNT * PAGE_FRAME_MAX_ROWS;
    private final boolean convertToParquet;
    private final boolean enableJitCompiler;
    private final boolean enableParallelTopK;

    public ParallelTopKFuzzTest(boolean enableParallelTopK, boolean enableJitCompiler, boolean convertToParquet) {
        this.enableParallelTopK = enableParallelTopK;
        this.enableJitCompiler = enableJitCompiler;
        this.convertToParquet = convertToParquet;
    }

    @Parameterized.Parameters(name = "parallel={0} JIT={1} parquet={2}")
    public static Collection<Object[]> data() {
        // only run a single combination per CI run
        final Rnd rnd = TestUtils.generateRandom(LOG);
        // make sure to have a run with all equal flags occasionally
        if (rnd.nextInt(100) >= 90) {
            boolean flag = rnd.nextBoolean();
            return Arrays.asList(new Object[][]{{flag, flag, flag}});
        }
        return Arrays.asList(new Object[][]{{rnd.nextBoolean(), rnd.nextBoolean(), rnd.nextBoolean()}});
        // uncomment to run all combinations
//        return Arrays.asList(new Object[][]{
//                {true, true, true},
//                {true, true, false},
//                {true, false, true},
//                {true, false, false},
//                {false, true, true},
//                {false, true, false},
//                {false, false, true},
//                {false, false, false},
//        });
    }

    @Override
    @Before
    public void setUp() {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, PAGE_FRAME_MAX_ROWS);
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, PAGE_FRAME_MAX_ROWS);
        // We intentionally use small values for shard count and reduce
        // queue capacity to exhibit various edge cases.
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_SHARD_COUNT, 2);
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, PAGE_FRAME_COUNT);
        // Set the sharding threshold to a small value to test sharding.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, 2);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WORK_STEALING_THRESHOLD, 1);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_TOP_K_ENABLED, String.valueOf(enableParallelTopK));
        super.setUp();
    }

    @Test
    public void testParallelTopK() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        testParallelTopK(
                "SELECT * FROM tab ORDER BY key DESC, price ASC LIMIT 3;",
                "ts\tkey\tprice\tquantity\tcolTop\n" +
                        "1970-01-01T00:57:36.000000Z\tk4\t4.0\t4\tnull\n" +
                        "1970-01-01T02:09:36.000000Z\tk4\t9.0\t9\tnull\n" +
                        "1970-01-01T03:21:36.000000Z\tk4\t14.0\t14\tnull\n"
        );
    }

    @Test
    public void testParallelTopKFilter() throws Exception {
        testParallelTopK(
                "SELECT * FROM tab WHERE key = 'k0' ORDER BY price DESC LIMIT 1;",
                "ts\tkey\tprice\tquantity\tcolTop\n" +
                        "1970-02-10T12:00:00.000000Z\tk0\t4050.0\t4050\t4050.0\n"
        );
    }

    @Test
    public void testParallelTopKIntrinsicsFilter() throws Exception {
        testParallelTopK(
                "SELECT * FROM tab WHERE ts in '1970-02' ORDER BY colTop LIMIT 3;",
                "ts\tkey\tprice\tquantity\tcolTop\n" +
                        "1970-02-01T00:00:00.000000Z\tk0\t3100.0\t3100\t3100.0\n" +
                        "1970-02-01T00:14:24.000000Z\tk1\t3101.0\t3101\t3101.0\n" +
                        "1970-02-01T00:28:48.000000Z\tk2\t3102.0\t3102\t3102.0\n"
        );
    }

    @Test
    public void testParallelTopKThreadUnsafeOrderByExpression() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        // The query won't use the parallel factory due to virtual base factory.
        testParallelTopK(
                "SELECT * FROM tab ORDER BY concat(key, 'foobar'), ts DESC LIMIT 3;",
                "ts\tkey\tprice\tquantity\tcolTop\n" +
                        "1970-02-10T12:00:00.000000Z\tk0\t4050.0\t4050\t4050.0\n" +
                        "1970-02-10T10:48:00.000000Z\tk0\t4045.0\t4045\t4045.0\n" +
                        "1970-02-10T09:36:00.000000Z\tk0\t4040.0\t4040\t4040.0\n"
        );
    }

    @Test
    public void testParallelTopKWithBindVariablesInFilter() throws Exception {
        testParallelTopK(
                (sqlExecutionContext) -> {
                    BindVariableService bindVariableService = sqlExecutionContext.getBindVariableService();
                    bindVariableService.clear();
                    bindVariableService.setStr("asym", "k0");
                },
                "SELECT * FROM tab WHERE key = :asym ORDER BY price DESC LIMIT 1;",
                "ts\tkey\tprice\tquantity\tcolTop\n" +
                        "1970-02-10T12:00:00.000000Z\tk0\t4050.0\t4050\t4050.0\n"
        );
    }

    private void testParallelTopK(String... queriesAndExpectedResults) throws Exception {
        testParallelTopK(null, queriesAndExpectedResults);
    }

    private void testParallelTopK(BindVariablesInitializer initializer, String... queriesAndExpectedResults) throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        sqlExecutionContext.setJitMode(enableJitCompiler ? SqlJitMode.JIT_MODE_ENABLED : SqlJitMode.JIT_MODE_DISABLED);
                        if (initializer != null) {
                            initializer.init(sqlExecutionContext);
                        }

                        engine.execute(
                                "CREATE TABLE tab (" +
                                        "  ts TIMESTAMP," +
                                        "  key SYMBOL," +
                                        "  price DOUBLE," +
                                        "  quantity LONG) timestamp (ts) PARTITION BY DAY",
                                sqlExecutionContext
                        );
                        engine.execute(
                                "insert into tab select (x * 864000000)::timestamp, 'k' || (x % 5), x, x from long_sequence(" + ROW_COUNT + ")",
                                sqlExecutionContext
                        );
                        engine.execute("ALTER TABLE tab ADD COLUMN colTop DOUBLE", sqlExecutionContext);
                        engine.execute(
                                "insert into tab " +
                                        "select ((50 + x) * 864000000)::timestamp, " +
                                        "  'k' || ((50 + x) % 5), 50 + x, 50 + x, 50 + x " +
                                        "from long_sequence(" + ROW_COUNT + ")",
                                sqlExecutionContext
                        );
                        if (convertToParquet) {
                            execute(compiler, "alter table tab convert partition to parquet where ts >= 0", sqlExecutionContext);
                        }
                        assertQueries(engine, sqlExecutionContext, queriesAndExpectedResults);
                    },
                    configuration,
                    LOG
            );
        });
    }

    private interface BindVariablesInitializer {
        void init(SqlExecutionContext sqlExecutionContext) throws SqlException;
    }
}
