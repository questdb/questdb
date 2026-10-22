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

import io.questdb.cairo.CairoEngine;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import org.junit.Assert;
import org.junit.Test;

public class MatViewRefreshTypeChangeTest extends AbstractCairoTest {
    @Test
    public void testSymbolConversionAfterEngineReopenAndMalformedRecovery() throws Exception {
        assertMemoryLeak(() -> {
            final var conf = new DefaultTestCairoConfiguration(temp.newFolder().getAbsolutePath()) {
                @Override
                public int getCommitMode() {
                    return configuration.getCommitMode();
                }
            };
            try (CairoEngine first = new CairoEngine(conf)) {
                first.load();
                first.execute("CREATE TABLE base_price (sym VARCHAR, at TIMESTAMP, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                first.execute("CREATE MATERIALIZED VIEW price_1h AS (SELECT sym, last(at) AS at, ts FROM base_price SAMPLE BY 1h) PARTITION BY DAY");
                first.execute("INSERT INTO base_price VALUES ('gbpusd', '2024-09-10T12:01:02.123456Z', '2024-09-10T12:01'), ('null', NULL, '2024-09-10T12:02')");
                drainWalAndMatViewQueues(first);
                assertRows(first);
                first.execute("ALTER TABLE base_price ALTER COLUMN at TYPE SYMBOL");
                drainWalAndMatViewQueues(first);
                assertStatus(first, "invalid");
            }
            try (CairoEngine second = new CairoEngine(conf)) {
                second.load();
                second.hydrateMatViewStateStore();
                Assert.assertNull(second.getMatViewStateStore().getViewState(second.verifyTableName("price_1h")).getRecordToRowCopier());
                for (int i = 0; i < 2; i++) {
                    second.execute("REFRESH MATERIALIZED VIEW price_1h FULL");
                    drainWalAndMatViewQueues(second);
                    assertStatus(second, "valid");
                    assertRows(second);
                }
                second.execute("INSERT INTO base_price VALUES ('gbpusd', 'not-a-timestamp', '2024-09-10T12:30')");
                drainWalAndMatViewQueues(second);
                assertStatus(second, "invalid");
                second.execute("REFRESH MATERIALIZED VIEW price_1h FULL");
                drainWalAndMatViewQueues(second);
                assertStatus(second, "invalid");
                assertMalformedSymbolInvalidation(second);
            }
            try (CairoEngine third = new CairoEngine(conf)) {
                third.load();
                third.hydrateMatViewStateStore();
                assertStatus(third, "invalid");
                assertMalformedSymbolInvalidation(third);
                third.execute("UPDATE base_price SET at = '2024-09-10T12:01:02.123456Z' WHERE sym = 'gbpusd'");
                drainWalAndMatViewQueues(third);
                third.execute("REFRESH MATERIALIZED VIEW price_1h FULL");
                drainWalAndMatViewQueues(third);
                assertStatus(third, "valid");
                assertRows(third);
            }
        });
    }

    private void assertMalformedSymbolInvalidation(CairoEngine reopened) throws Exception {
        try (SqlExecutionContextImpl context = new SqlExecutionContextImpl(reopened, 1).with(reopened.getConfiguration().getFactoryProvider().getSecurityContextFactory().getRootContext())) {
            assertQuery("SELECT invalidation_reason FROM materialized_views").withEngine(reopened).withContext(context)
                    .noRandomAccess().noLeakCheck().returns("""
                            invalidation_reason
                            inconvertible value: `not-a-timestamp` [SYMBOL -> TIMESTAMP]
                            """);
        }
    }

    private void assertRows(CairoEngine reopened) throws Exception {
        try (SqlExecutionContextImpl context = new SqlExecutionContextImpl(reopened, 1).with(reopened.getConfiguration().getFactoryProvider().getSecurityContextFactory().getRootContext())) {
            assertQuery("price_1h ORDER BY ts, sym").withEngine(reopened).withContext(context)
                    .timestamp("ts").expectSize().noLeakCheck().returns("""
                            sym\tat\tts
                            gbpusd\t2024-09-10T12:01:02.123456Z\t2024-09-10T12:00:00.000000Z
                            null\t\t2024-09-10T12:00:00.000000Z
                            """);
        }
    }

    private void assertStatus(CairoEngine reopened, String status) throws Exception {
        try (SqlExecutionContextImpl context = new SqlExecutionContextImpl(reopened, 1).with(reopened.getConfiguration().getFactoryProvider().getSecurityContextFactory().getRootContext())) {
            assertQuery("SELECT view_status FROM materialized_views").withEngine(reopened).withContext(context)
                    .noRandomAccess().noLeakCheck().returns("view_status\n" + status + "\n");
        }
    }
}
