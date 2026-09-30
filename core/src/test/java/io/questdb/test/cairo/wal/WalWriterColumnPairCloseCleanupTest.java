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

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.engine.ops.AlterOperationBuilder;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

/**
 * Cleanup-completes bar for {@code WalWriter}'s column-pair free path.
 * <p>
 * Changing a column's type makes the WalWriter drop the old column pair. Under any non-NOSYNC commit mode
 * those closes carry a durability barrier — {@code MemoryPMARImpl.close} -> {@code releaseCurrentPage} ->
 * {@code msync} — so the close itself can fail, for a simulated crash or a genuine EIO.
 * <p>
 * The failure must remain FAIL-STOP (it is rethrown), but it must not escape the free path part-way.
 * {@code freeAndRemoveColumnPair} detaches BOTH slots from {@code columns} before closing either, so a
 * throw from the primary's close used to leave the secondary mapped AND unreachable — {@code freeColumns}
 * could no longer see it, and nothing else ever would. That stranded a mapping and its descriptor.
 * <p>
 * Discovered by the randomized adaptive crash-fuzz once its seed stopped being hard-coded: every seed that
 * emitted a structural column change leaked exactly one page here.
 */
public class WalWriterColumnPairCloseCleanupTest extends AbstractCairoTest {

    /**
     * Fails the next {@code msync} once armed, with a genuine data-sync failure, then behaves normally.
     */
    private static final class OneShotMsyncFailureFacade extends TestFilesFacadeImpl {
        int injected;
        volatile boolean failNext;

        String injectedAt;

        @Override
        public void msync(long addr, long len, boolean async) {
            if (failNext) {
                java.io.StringWriter sw = new java.io.StringWriter();
                new Throwable().printStackTrace(new java.io.PrintWriter(sw));
                // Only fail the msync that the column-pair free performs; any other msync during the ALTER
                // would make this test pass for the wrong reason (and its negative control pass with it).
                if (!sw.toString().contains("freeAndRemoveColumnPair")) {
                    super.msync(addr, len, async);
                    return;
                }
                failNext = false;
                injected++;
                injectedAt = sw.toString();
                throw CairoException.dataSyncFailure(5, "msync")
                        .put("simulated msync failure during column-pair free");
            }
            super.msync(addr, len, async);
        }
    }

    @Test
    public void testColumnPairFreeReleasesSiblingWhenPrimaryCloseFails() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, "1h");
        final OneShotMsyncFailureFacade ff = new OneShotMsyncFailureFacade();
        // assertMemoryLeak IS the cleanup bar: a stranded page shows up as a non-zero MMAP_TABLE_WRITER
        // balance, and a stranded descriptor trips the fd cache's paranoia check.
        assertMemoryLeak(ff, () -> {
            execute("create table wcp (ts timestamp, s string, v long) timestamp(ts) partition by day wal");
            execute("insert into wcp values ('2024-01-01T00:00:00.000000Z', 'abc', 1)");
            drainWalQueue();

            final TableToken tt = engine.verifyTableName("wcp");
            try (WalWriter w = engine.getWalWriter(tt)) {
                // Give the WalWriter live mapped columns, so the pair being dropped has a current page and
                // its release actually reaches an msync.
                w.newRow(1_704_067_200_000_000L).append();
                w.commit();

                final AlterOperationBuilder builder = new AlterOperationBuilder().ofColumnChangeType(
                        0, tt, w.getMetadata().getTableId());
                builder.addColumnToList("s", 0, ColumnType.VARCHAR, 0, false, (byte) 0, 0, false);
                final AlterOperation alterOp = builder.build();

                Throwable seen = null;
                try (SqlExecutionContextImpl context =
                             new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE)) {
                    alterOp.withContext(context);
                    ff.failNext = true;
                    try {
                        w.apply(alterOp, true);
                    } catch (Throwable t) {
                        seen = t;
                    }
                }

                Assert.assertEquals("the injected msync failure must actually have been reached", 1, ff.injected);
                Assert.assertNotNull("a data-sync failure on the column-pair free path must remain FAIL-STOP, "
                        + "not be swallowed", seen);
            } catch (CairoException | io.questdb.cairo.CairoError ignore) {
                // The writer is distressed by the injected fault; its close may resurface it. The bar under
                // test is what assertMemoryLeak checks after this block: everything was still released.
            } finally {
                engine.releaseAllWalWriters();
                engine.releaseAllWriters();
            }
        });
    }
}
