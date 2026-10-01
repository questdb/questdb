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

package io.questdb.test.griffin.engine;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableReader;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

// Verifies the query-progress logging/metric behavior in validation-only mode:
// validation failures must not be counted as server-side query errors, while
// reader leaks must still be counted even during validation.
public class QueryProgressValidationTest extends AbstractCairoTest {

    @Test
    public void testNonValidationFailureIncrementsQueryErrorMetric() throws Exception {
        assertMemoryLeak(() -> {
            final SqlExecutionContextImpl ctx = (SqlExecutionContextImpl) sqlExecutionContext;
            final long before = engine.getMetrics().healthMetrics().queryErrorCounter();

            ctx.setValidationOnly(false);
            QueryProgress.logError(
                    CairoException.nonCritical().put("boom"),
                    -1,
                    "select 1",
                    ctx,
                    0
            );

            Assert.assertEquals(before + 1, engine.getMetrics().healthMetrics().queryErrorCounter());
        });
    }

    @Test
    public void testShouldLogSqlReflectsValidationOnly() {
        final SqlExecutionContextImpl ctx = (SqlExecutionContextImpl) sqlExecutionContext;
        try {
            ctx.setValidationOnly(true);
            Assert.assertFalse(ctx.shouldLogSql());
            ctx.setValidationOnly(false);
            Assert.assertTrue(ctx.shouldLogSql());
        } finally {
            ctx.setValidationOnly(false);
        }
    }

    @Test
    public void testValidationFailureDoesNotIncrementQueryErrorMetric() throws Exception {
        assertMemoryLeak(() -> {
            final SqlExecutionContextImpl ctx = (SqlExecutionContextImpl) sqlExecutionContext;
            final long before = engine.getMetrics().healthMetrics().queryErrorCounter();

            ctx.setValidationOnly(true);
            try {
                QueryProgress.logError(
                        CairoException.nonCritical().put("boom"),
                        -1,
                        "select 1",
                        ctx,
                        0
                );
            } finally {
                ctx.setValidationOnly(false);
            }

            Assert.assertEquals(before, engine.getMetrics().healthMetrics().queryErrorCounter());
        });
    }

    @Test
    public void testValidationReaderLeakStillCounted() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, v INT)");

            final SqlExecutionContextImpl ctx = (SqlExecutionContextImpl) sqlExecutionContext;
            final long before = engine.getMetrics().healthMetrics().readerLeakCounter();

            ctx.setValidationOnly(true);
            try (TableReader reader = engine.getReader("x")) {
                final ObjList<TableReader> leaked = new ObjList<>();
                leaked.add(reader);
                QueryProgress.logEnd(-1, "select * from x", ctx, 0, leaked, null);
            } finally {
                ctx.setValidationOnly(false);
            }

            Assert.assertEquals(before + 1, engine.getMetrics().healthMetrics().readerLeakCounter());
        });
    }
}
