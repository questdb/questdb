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

package io.questdb.test.griffin.engine.functions.lt;

import io.questdb.PropertyKey;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.WorkerPool;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;

/**
 * Shared harness of the numeric cursor-comparison factory tests: enables the parallel group by /
 * async filter paths and the {@code test_timestamp_counter()} instrumentation, and runs test
 * bodies against a four-worker pool.
 */
abstract class AbstractCursorFunctionFactoryTest extends AbstractCairoTest {

    @Override
    @Before
    public void setUp() {
        // exercise the parallel group by / async filter paths
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, 1);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WORK_STEALING_THRESHOLD, 1);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1000);
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_SHARD_COUNT, 4);
        // enables the test_timestamp_counter() function used to count sub-query executions
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        super.setUp();
    }

    /**
     * Asserts that a bare {@code null} literal on the right of every comparison operator compiles
     * to a scalar null-comparison (never a cursor comparison) and matches no rows. The generic
     * behavior is shared by every numeric left-operand type; type-specific rationale stays with
     * the concrete test.
     */
    protected final void assertBareNullBehavior(String columnType, String columnName) throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x::" + columnType + " " + columnName + " from long_sequence(10))");
            final String empty = columnName + "\n";
            // null comparison matches no rows for every operator, and must not throw at compile time
            assertQuery("select " + columnName + " from t where " + columnName + " <= null")
                    .noLeakCheck()
                    .returns(empty);
            assertQuery("select " + columnName + " from t where " + columnName + " >= null")
                    .noLeakCheck()
                    .returns(empty);
            assertQuery("select " + columnName + " from t where " + columnName + " > null")
                    .noLeakCheck()
                    .returns(empty);
            assertQuery("select " + columnName + " from t where " + columnName + " < null")
                    .noLeakCheck()
                    .returns(empty);
        });
    }

    /**
     * Asserts that a null cursor scalar (bare or typed) and an empty scalar sub-query match no
     * rows for the strict operators and their negated forms alike. The generic behavior is shared
     * by every numeric left-operand type.
     */
    protected final void assertNullAndEmptyCursorBehavior(String columnType, String columnName) throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x::" + columnType + " " + columnName + " from long_sequence(10))");
            final String empty = columnName + "\n";
            assertQuery("select " + columnName + " from t where " + columnName + " < (select null)")
                    .noLeakCheck()
                    .returns(empty);
            assertQuery("select " + columnName + " from t where " + columnName + " > (select null::long)")
                    .noLeakCheck()
                    .returns(empty);
            assertQuery("select " + columnName + " from t where " + columnName + " < (select max(" + columnName + ") from t where 1 <> 1)")
                    .noLeakCheck()
                    .returns(empty);
            // negated operators over a null / empty cursor must also match no rows
            assertQuery("select " + columnName + " from t where " + columnName + " >= (select null)")
                    .noLeakCheck()
                    .returns(empty);
            assertQuery("select " + columnName + " from t where " + columnName + " <= (select null::long)")
                    .noLeakCheck()
                    .returns(empty);
            assertQuery("select " + columnName + " from t where " + columnName + " >= (select max(" + columnName + ") from t where 1 <> 1)")
                    .noLeakCheck()
                    .returns(empty);
        });
    }

    /**
     * Asserts the null LEFT-column contract: long_sequence never yields null cells, so the table
     * carries an explicit null. A null left value must never match a non-null cursor scalar (any
     * operator), and must follow QuestDB's null == null convention against a null cursor: >= and
     * <= match, strict > / < do not. The generic behavior is shared by every numeric left-operand
     * type.
     */
    protected final void assertNullLeftColumnBehavior(String columnType, String columnName) throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (id int, " + columnName + " " + columnType + ")");
            execute("insert into t values (1, null), (2, 5), (3, 8)");
            // null-left (id 1) is excluded for every operator against a non-null cursor
            assertQuery("select id from t where " + columnName + " > (select min(" + columnName + ") from t)") // > 5
                    .noLeakCheck()
                    .returns("id\n3\n");
            assertQuery("select id from t where " + columnName + " < (select max(" + columnName + ") from t)") // < 8
                    .noLeakCheck()
                    .returns("id\n2\n");
            assertQuery("select id from t where " + columnName + " >= (select max(" + columnName + ") from t)") // >= 8
                    .noLeakCheck()
                    .returns("id\n3\n");
            assertQuery("select id from t where " + columnName + " <= (select min(" + columnName + ") from t)") // <= 5
                    .noLeakCheck()
                    .returns("id\n2\n");
            // null == null: a null left value matches a null cursor for >= and <= only
            assertQuery("select id from t where " + columnName + " >= (select null)")
                    .noLeakCheck()
                    .returns("id\n1\n");
            assertQuery("select id from t where " + columnName + " <= (select null)")
                    .noLeakCheck()
                    .returns("id\n1\n");
            assertQuery("select id from t where " + columnName + " > (select null)")
                    .noLeakCheck()
                    .returns("id\n");
            assertQuery("select id from t where " + columnName + " < (select null)")
                    .noLeakCheck()
                    .returns("id\n");
        });
    }

    protected final void runWithPool(PoolRunnable body) throws Exception {
        assertMemoryLeak(() -> {
            try (WorkerPool pool = new WorkerPool(() -> 4)) {
                TestUtils.execute(pool, (_, compiler, sqlExecutionContext) ->
                        body.run(compiler, sqlExecutionContext), configuration, LOG);
            }
        });
    }

    @FunctionalInterface
    protected interface PoolRunnable {
        void run(SqlCompiler compiler, SqlExecutionContext ctx) throws Exception;
    }
}
