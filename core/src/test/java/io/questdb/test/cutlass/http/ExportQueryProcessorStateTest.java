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

package io.questdb.test.cutlass.http;

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cutlass.http.processors.ExportQueryProcessorState;
import io.questdb.cutlass.parquet.CopyExportRequestTask;
import io.questdb.griffin.QueryRegistry;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

public class ExportQueryProcessorStateTest extends AbstractCairoTest {

    @Test
    public void testClearClosesAndUnregistersCursorAfterTaskCleanupFailure() throws Exception {
        assertTaskCleanupFailureDoesNotStrandCursor(false);
    }

    @Test
    public void testCloseClosesAndUnregistersCursorAfterTaskCleanupFailure() throws Exception {
        assertTaskCleanupFailureDoesNotStrandCursor(true);
    }

    private void assertTaskCleanupFailureDoesNotStrandCursor(boolean isClose) throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = isClose ? "state_close_failure" : "state_clear_failure";
            execute("CREATE TABLE " + tableName + " AS (SELECT x FROM long_sequence(1))");
            final QueryRegistry registry = engine.getQueryRegistry();
            final AtomicLong queryId = new AtomicLong(-1);
            registry.setListener((query, id, context) -> {
                if (tableName.contentEquals(query)) {
                    queryId.set(id);
                }
            });

            final RuntimeException cleanupFailure = new RuntimeException("expected task cleanup failure");
            final ExportQueryProcessorState state = new ExportQueryProcessorState(null, null);
            try (RecordCursorFactory factory = select(tableName)) {
                Assert.assertTrue(factory instanceof QueryProgress);
                final RecordCursor cursor = factory.getCursor(sqlExecutionContext);
                Assert.assertTrue(queryId.get() >= 0);
                Assert.assertNotNull(registry.getEntry(queryId.get()));
                state.setTaskAndCursorForTest(
                        new CopyExportRequestTask() {
                            private boolean isFailurePending = true;

                            @Override
                            public void clear() {
                                super.clear();
                                if (!isClose && isFailurePending) {
                                    isFailurePending = false;
                                    Assert.assertNotNull("task must clear before query cursor closes", registry.getEntry(queryId.get()));
                                    throw cleanupFailure;
                                }
                            }

                            @Override
                            public void close() {
                                super.close();
                                if (isClose && isFailurePending) {
                                    isFailurePending = false;
                                    Assert.assertNotNull("task must close before query cursor closes", registry.getEntry(queryId.get()));
                                    throw cleanupFailure;
                                }
                            }
                        },
                        cursor
                );

                try {
                    if (isClose) {
                        state.close();
                    } else {
                        state.clear();
                    }
                    Assert.fail("expected cleanup failure");
                } catch (RuntimeException e) {
                    Assert.assertSame(cleanupFailure, e);
                }

                Assert.assertNull("cleanup failure must not strand query registration", registry.getEntry(queryId.get()));
                Assert.assertEquals("cleanup failure must not strand raw table reader", 0, engine.getBusyReaderCount());
            } finally {
                registry.setListener(null);
                state.close();
            }
        });
    }
}
