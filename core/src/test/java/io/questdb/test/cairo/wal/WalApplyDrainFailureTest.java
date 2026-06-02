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

package io.questdb.test.cairo.wal;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.DefaultLifecycleManager;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.test.AbstractCairoTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.cairo.TableUtils.WAL_2_TABLE_WRITE_REASON;

/**
 * Verifies that a failure while draining the async writer command queue after a WAL apply
 * (the {@code writer.tick()} call in {@link io.questdb.cairo.wal.ApplyWal2TableJob}) does not
 * fail the WAL apply or suspend the table, and instead marks the writer distressed so the pool
 * recreates it on the next acquisition.
 */
public class WalApplyDrainFailureTest extends AbstractCairoTest {

    private static final AtomicBoolean failTickEnabled = new AtomicBoolean(false);
    private static final AtomicInteger markDistressedCount = new AtomicInteger(0);
    private static final AtomicInteger tickFailureCount = new AtomicInteger(0);
    private static volatile String failTableName;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        // Inject a writer whose tick() throws when draining the async command queue, so the apply
        // job's drain catch block (which must not suspend the table) gets exercised end to end.
        AbstractCairoTest.engineFactory = conf -> new CairoEngine(conf) {
            @Override
            public TableWriter getWriterUnsafe(TableToken tableToken, @NotNull String lockReason) {
                if (failTickEnabled.get()
                        && WAL_2_TABLE_WRITE_REASON.equals(lockReason)
                        && tableToken.getTableName().equals(failTableName)) {
                    return new TableWriter(
                            getConfiguration(),
                            tableToken,
                            getMessageBus(),
                            null,
                            true,
                            DefaultLifecycleManager.INSTANCE,
                            getConfiguration().getDbRoot(),
                            getDdlListener(tableToken),
                            this
                    ) {
                        @Override
                        public void markDistressed() {
                            markDistressedCount.incrementAndGet();
                            super.markDistressed();
                        }

                        @Override
                        public void tick(boolean contextAllowsAnyStructureChanges, long deadlineMicros) {
                            tickFailureCount.incrementAndGet();
                            throw CairoException.critical(0).put("injected drain failure");
                        }
                    };
                }
                return super.getWriterUnsafe(tableToken, lockReason);
            }
        };
        AbstractCairoTest.setUpStatic();
    }

    @Test
    public void testDrainFailureDoesNotSuspendTable() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            failTableName = tableName;
            failTickEnabled.set(true);
            markDistressedCount.set(0);
            tickFailureCount.set(0);

            execute("create table " + tableName + " (x long, ts timestamp) timestamp(ts) partition by DAY WAL");
            execute("insert into " + tableName + " values (1, '2022-02-24T00:00:00'), (2, '2022-02-25T00:00:00')");

            final TableToken tableToken = engine.verifyTableName(tableName);
            drainWalQueue();

            // The drain failure must not suspend the table...
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
            // ...the WAL transactions must still be durably applied...
            assertQuery("select count() from " + tableName)
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n2\n");
            // ...the tick failure must have been hit...
            Assert.assertTrue("tick() drain was expected to fail", tickFailureCount.get() > 0);
            // ...and the writer must have been marked distressed so the pool recreates it.
            Assert.assertTrue("writer should be marked distressed on drain failure", markDistressedCount.get() > 0);
        });
    }
}
