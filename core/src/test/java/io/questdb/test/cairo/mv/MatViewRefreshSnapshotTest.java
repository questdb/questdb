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
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableReader;
import io.questdb.test.AbstractCairoTest;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A materialized view refresh pins the base table reader at the transaction it chose; compiling the view
 * query must not move that reader to a transaction committed while the refresh runs.
 */
public class MatViewRefreshSnapshotTest extends AbstractCairoTest {
    private static final AtomicBoolean isCommitOnPinArmed = new AtomicBoolean();

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractCairoTest.engineFactory = conf -> new CairoEngine(conf) {
            @Override
            public void detachReader(TableReader reader) {
                super.detachReader(reader);
                if (isCommitOnPinArmed.compareAndSet(true, false)) {
                    try {
                        execute("INSERT INTO base VALUES ('2026-01-01T00:01:00Z', 2), ('2026-01-01T02:00:00Z', 4)");
                        drainWalQueue();
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                }
            }
        };
        AbstractCairoTest.setUpStatic();
    }

    @Override
    public void setUp() {
        super.setUp();
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        isCommitOnPinArmed.set(false);
    }

    @Test
    public void testRefreshCompileKeepsPinnedBaseSnapshot() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base VALUES ('2026-01-01T00:00:00Z', 1)");
            drainWalQueue();
            execute("CREATE MATERIALIZED VIEW mv REFRESH MANUAL DEFERRED AS SELECT ts, sum(v) s FROM base SAMPLE BY 1h");
            drainWalAndMatViewQueues();

            isCommitOnPinArmed.set(true);
            execute("REFRESH MATERIALIZED VIEW mv FULL");
            drainWalAndMatViewQueues();

            assertQuery("SELECT * FROM mv")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\ts
                            2026-01-01T00:00:00.000000Z\t1
                            """);
            assertQuery("SELECT refresh_base_table_txn, base_table_txn FROM materialized_views")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            refresh_base_table_txn\tbase_table_txn
                            1\t2
                            """);

            execute("REFRESH MATERIALIZED VIEW mv INCREMENTAL");
            drainWalAndMatViewQueues();
            assertQuery("SELECT * FROM mv")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\ts
                            2026-01-01T00:00:00.000000Z\t3
                            2026-01-01T02:00:00.000000Z\t4
                            """);
        });
    }
}
