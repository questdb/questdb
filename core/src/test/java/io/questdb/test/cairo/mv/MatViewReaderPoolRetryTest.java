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

package io.questdb.test.cairo.mv;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.mv.MatViewTimerJob;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class MatViewReaderPoolRetryTest extends AbstractCairoTest {
    @BeforeClass
    public static void setUpStatic() throws Exception {
        // Two readers per table make real pool exhaustion cheap and deterministic.
        setProperty(PropertyKey.CAIRO_READER_POOL_MAX_SEGMENTS, 1);
        setProperty(PropertyKey.DEBUG_CAIRO_POOL_SEGMENT_SIZE, 2);
        AbstractCairoTest.setUpStatic();
    }

    @Test
    public void testBaseReaderPoolExhaustion() throws Exception {
        assertReaderPoolExhaustionRetries("base_price", true);
    }

    @Test
    public void testJoinReaderPoolExhaustionDuringCompilation() throws Exception {
        assertReaderPoolExhaustionRetries("symbols", false);
    }

    @Test
    public void testJoinReaderPoolExhaustionDuringExecution() throws Exception {
        assertReaderPoolExhaustionRetries("symbols", true);
    }

    @Test
    public void testSqlCompilationPreservesPoolExhaustionError() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE symbols (sym SYMBOL)");
            final TableToken token = engine.verifyTableName("symbols");
            try (
                    TableReader ignored1 = engine.getReader(token);
                    TableReader ignored2 = engine.getReader(token);
                    SqlCompiler compiler = engine.getSqlCompiler()
            ) {
                try (RecordCursorFactory ignored = compiler.compile("SELECT * FROM symbols", sqlExecutionContext).getRecordCursorFactory()) {
                    Assert.fail("expected reader pool exhaustion");
                } catch (SqlException e) {
                    // Ordinary SQL callers must still see the same exception, position, code and text.
                    Assert.assertEquals(SqlException.class, e.getClass());
                    Assert.assertEquals(14, e.getPosition());
                    Assert.assertEquals(0, e.getErrorCode());
                    TestUtils.assertEquals("[-1]: table busy [reason=pool size exceeded]", e.getFlyweightMessage());
                    Assert.assertTrue(e.isTableBusy());
                }
            }
            assertQuery("SELECT * FROM symbols").noLeakCheck().expectSize().returns("sym\n");
        });
    }

    private void assertReaderPoolExhaustionRetries(String busyTableName, boolean isFactoryCached) throws Exception {
        setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_BUSY_RETRY_TIMEOUT, 1000);
        assertMemoryLeak(() -> {
            final long now = 1_700_000_000_000_000L;
            setCurrentMicros(now);
            try {
                execute("CREATE TABLE base_price (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("CREATE TABLE symbols (sym SYMBOL)");
                execute("INSERT INTO symbols VALUES ('a')");
                execute("INSERT INTO base_price VALUES ('a', 1.0, '2020-01-01T00:00:00Z')");
                drainWalQueue();
                execute("""
                        CREATE MATERIALIZED VIEW price_1h WITH BASE base_price AS (
                            SELECT b.ts, avg(b.price) AS avg_price
                            FROM base_price b JOIN symbols s ON (sym)
                            SAMPLE BY 1h
                        ) PARTITION BY DAY
                        """);

                if (isFactoryCached) {
                    drainWalAndMatViewQueues();
                    execute("INSERT INTO base_price VALUES ('a', 2.0, '2020-01-01T01:00:00Z')");
                    drainWalQueue();
                }

                final TableToken viewToken = engine.verifyTableName("price_1h");
                final MatViewState viewState = engine.getMatViewStateStore().getViewState(viewToken);
                final long lastRefreshBaseTxn = viewState.getLastRefreshBaseTxn();
                final TableToken busyToken = engine.verifyTableName(busyTableName);
                final MatViewTimerJob timerJob = new MatViewTimerJob(engine);
                try (
                        TableReader ignored1 = engine.getReader(busyToken);
                        TableReader ignored2 = engine.getReader(busyToken)
                ) {
                    drainWalAndMatViewQueues();
                    assertRetrying(viewState, lastRefreshBaseTxn, 1);

                    // Retry while the pool is still full: this must remain a transient failure.
                    setCurrentMicros(now + 1_000_000L);
                    drainMatViewTimerQueue(timerJob);
                    drainWalAndMatViewQueues();
                    assertRetrying(viewState, lastRefreshBaseTxn, 2);
                }

                // No new base commit or manual REFRESH: releasing the readers is enough to recover.
                setCurrentMicros(now + 2_000_000L);
                drainMatViewTimerQueue(timerJob);
                drainWalAndMatViewQueues();
                Assert.assertEquals(0, viewState.getRefreshRetryCount());
                assertQuery("SELECT view_status, invalidation_reason FROM materialized_views()")
                        .noLeakCheck()
                        .noRandomAccess()
                        .returns("view_status\tinvalidation_reason\nvalid\t\n");
                assertQuery("SELECT ts, avg_price FROM price_1h")
                        .noLeakCheck()
                        .timestamp("ts")
                        .expectSize()
                        .returns(isFactoryCached ? """
                                ts\tavg_price
                                2020-01-01T00:00:00.000000Z\t1.0
                                2020-01-01T01:00:00.000000Z\t2.0
                                """ : """
                                ts\tavg_price
                                2020-01-01T00:00:00.000000Z\t1.0
                                """);
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    private void assertRetrying(MatViewState viewState, long lastRefreshBaseTxn, int attempts) throws Exception {
        assertQuery("SELECT view_status, invalidation_reason FROM materialized_views()")
                .noLeakCheck()
                .noRandomAccess()
                .returns("view_status\tinvalidation_reason\nretrying\t\n");
        Assert.assertEquals(lastRefreshBaseTxn, viewState.getLastRefreshBaseTxn());
        Assert.assertEquals(attempts, viewState.getRefreshRetryCount());
    }
}
