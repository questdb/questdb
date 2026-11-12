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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.engine.table.AsyncHorizonJoinRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncMultiHorizonJoinRecordCursorFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class SqlGroupByGenerationOwnershipTest extends AbstractCairoTest {

    @Test
    public void testKeyedHorizonJoinConstructorFailureClosesKeyFunctionsOnce() throws Exception {
        assertKeyedHorizonJoinOomSweep(
                "SELECT owned_long(t.qty::LONG) k, avg(p.price) FROM trades t "
                        + "HORIZON JOIN prices p ON (t.sym = p.sym) RANGE FROM -2s TO 2s STEP 1s AS h",
                AsyncHorizonJoinRecordCursorFactory.class
        );
    }

    @Test
    public void testKeyedMultiHorizonJoinConstructorFailureClosesKeyFunctionsOnce() throws Exception {
        assertKeyedHorizonJoinOomSweep(
                "SELECT owned_long(t.qty::LONG) k, avg(p.price), count(q.price) FROM trades t "
                        + "HORIZON JOIN prices p ON (t.sym = p.sym) HORIZON JOIN prices q "
                        + "RANGE FROM -2s TO 2s STEP 1s AS h",
                AsyncMultiHorizonJoinRecordCursorFactory.class
        );
    }

    @Test
    public void testNotKeyedConstructorFailureClosesInputOnce() throws Exception {
        assertMemoryLeak(() -> {
            try (OwnershipFixture fixture = new OwnershipFixture(engine);
                 SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                fixture.table("v", ColumnType.LONG).closeFailure = new RuntimeException("input close");
                fixture.assertCompileOomSweep(compiler, sqlExecutionContext,
                        "SELECT sum(v) s, min(v) lo, max(v) hi, count(v) c FROM owned_table(0)");
            }
        });
    }

    private static void assertKeyedHorizonJoinOomSweep(String sql, Class<? extends RecordCursorFactory> factoryClass) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HORIZON_JOIN_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, "true");
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(2, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(
                    pool,
                    (engine, compiler, context) -> {
                        engine.execute("CREATE TABLE trades (ts TIMESTAMP, sym SYMBOL, qty DOUBLE) TIMESTAMP(ts) PARTITION BY DAY", context);
                        engine.execute("CREATE TABLE prices (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY", context);
                        engine.execute("INSERT INTO trades VALUES ('2024-01-01T00:00:01Z', 'a', 1.0), ('2024-01-01T00:00:02Z', 'b', 2.0)", context);
                        engine.execute("INSERT INTO prices VALUES ('2024-01-01T00:00:00Z', 'a', 10.0), ('2024-01-01T00:00:01Z', 'b', 20.0)", context);
                        try (OwnershipFixture fixture = new OwnershipFixture(engine)) {
                            try (RecordCursorFactory factory = compiler.compile(sql, context).getRecordCursorFactory()) {
                                TestUtils.assertFactoryInTree(factory, factoryClass);
                            }
                            fixture.assertCompileOomSweep(compiler, context, sql);
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }
}
