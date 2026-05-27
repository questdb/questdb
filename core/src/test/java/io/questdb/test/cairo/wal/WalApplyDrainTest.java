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

package io.questdb.test.cairo.wal;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultLifecycleManager;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.wal.TableWriterPressureControl;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.engine.ops.AlterOperationBuilder;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.cairo.TableUtils.WAL_2_TABLE_WRITE_REASON;

/**
 * End-to-end happy-path coverage for the async writer command-queue drain on the WAL apply path
 * (the {@code writer.tick(false, drainDeadline)} call in {@link io.questdb.cairo.wal.ApplyWal2TableJob}).
 * <p>
 * The hand-replicated tests in {@code TableWriterTest} drive the drain by calling {@code tick()}
 * themselves, so they would still pass if the apply-job drain call-site were deleted. This test
 * drives the real {@code ApplyWal2TableJob} via {@code drainWalQueue()} instead. The catch is that
 * the pool-return tick ({@code returnToPool} calls {@code tick(true)} == {@code tick(true, MAX_VALUE)})
 * also drains the queue, so a command published during apply ends up applied either way -- a naive
 * "assert applied after drainWalQueue" cannot tell the two drains apart.
 * <p>
 * To genuinely guard the apply-job call-site, the injected writer distinguishes the apply-job drain
 * ({@code tick(false, <finite deadline>)}) from the pool-return tick ({@code tick(true, MAX_VALUE)})
 * and records that the queued command was applied by the apply-job drain specifically. If the
 * call-site is removed, {@code applyJobDrainCount} stays 0 and the test fails.
 */
public class WalApplyDrainTest extends AbstractCairoTest {

    private static final AtomicInteger applyJobDrainCount = new AtomicInteger(0);
    private static final AtomicBoolean commandAppliedByApplyJobDrain = new AtomicBoolean(false);
    private static final AtomicBoolean publishOnApply = new AtomicBoolean(false);
    private static volatile int expectedMaxUncommittedRows = -1;
    private static volatile String targetTableName;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        // Inject a writer that, while held by the apply loop, publishes a real non-structural
        // command (SET PARAM maxUncommittedRows) to its own async queue -- mimicking a storage
        // policy command published to a busy WAL writer. The overridden tick() records when the
        // apply-job drain (false, finite deadline) runs and whether it applied the queued command.
        AbstractCairoTest.engineFactory = conf -> new CairoEngine(conf) {
            @Override
            public TableWriter getWriterUnsafe(TableToken tableToken, @NotNull String lockReason) {
                if (publishOnApply.get()
                        && WAL_2_TABLE_WRITE_REASON.equals(lockReason)
                        && tableToken.getTableName().equals(targetTableName)) {
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
                        private boolean commandPublished;

                        @Override
                        public void commitWalInsertTransactions(
                                Path walPath,
                                long seqTxn,
                                TableWriterPressureControl pressureControl
                        ) {
                            super.commitWalInsertTransactions(walPath, seqTxn, pressureControl);
                            // Publish once, after a WAL transaction is committed, so the command sits
                            // on the async queue until the apply loop's end-of-batch drain runs. The
                            // apply loop never ticks mid-batch, so nothing else can drain it first.
                            if (!commandPublished) {
                                commandPublished = true;
                                final int target = getMetadata().getMaxUncommittedRows() + 12_345;
                                expectedMaxUncommittedRows = target;
                                final AlterOperationBuilder builder = new AlterOperationBuilder();
                                builder.ofSetParamUncommittedRows(0, getTableToken(), getTableToken().getTableId(), target);
                                final AlterOperation alterOp = builder.build();
                                alterOp.withSecurityContext(AllowAllSecurityContext.INSTANCE);
                                publishAsyncWriterCommand(alterOp);
                            }
                        }

                        @Override
                        public void tick(boolean contextAllowsAnyStructureChanges, long deadlineMicros) {
                            super.tick(contextAllowsAnyStructureChanges, deadlineMicros);
                            // ApplyWal2TableJob's end-of-batch drain calls tick(false, <finite deadline>);
                            // the pool-return tick calls tick(true, Long.MAX_VALUE). Only the former is the
                            // call-site under test.
                            if (!contextAllowsAnyStructureChanges && deadlineMicros != Long.MAX_VALUE) {
                                applyJobDrainCount.incrementAndGet();
                                if (getMetadata().getMaxUncommittedRows() == expectedMaxUncommittedRows) {
                                    commandAppliedByApplyJobDrain.set(true);
                                }
                            }
                        }
                    };
                }
                return super.getWriterUnsafe(tableToken, lockReason);
            }
        };
        AbstractCairoTest.setUpStatic();
    }

    @Test
    public void testApplyJobDrainsQueuedCommandAfterWalApply() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            targetTableName = tableName;
            applyJobDrainCount.set(0);
            commandAppliedByApplyJobDrain.set(false);
            expectedMaxUncommittedRows = -1;
            publishOnApply.set(true);

            execute("create table " + tableName + " (x long, ts timestamp) timestamp(ts) partition by DAY WAL");
            execute("insert into " + tableName + " values (1, '2022-02-24T00:00:00'), (2, '2022-02-25T00:00:00')");

            final TableToken tableToken = engine.verifyTableName(tableName);
            drainWalQueue();

            // The WAL transactions must be durably applied...
            assertSql("count\n2\n", "select count() from " + tableName);
            // ...the table must not be suspended...
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
            // ...the apply-job drain call-site must have run (fails if it is removed; the pool-return
            // tick does not match the (false, finite-deadline) signature)...
            Assert.assertTrue(
                    "ApplyWal2TableJob drain call-site (tick(false, deadline)) was expected to run",
                    applyJobDrainCount.get() > 0
            );
            // ...and the command queued during apply must have been applied by that drain, not later
            // on pool return.
            Assert.assertTrue(
                    "queued command must be applied by the apply-job drain",
                    commandAppliedByApplyJobDrain.get()
            );
        });
    }
}
