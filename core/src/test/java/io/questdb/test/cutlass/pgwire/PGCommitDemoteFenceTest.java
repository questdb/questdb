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

package io.questdb.test.cutlass.pgwire;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cutlass.pgwire.PGPipelineEntry;
import io.questdb.std.ObjObjHashMap;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Verifies that the pg-wire transaction-commit path holds the role-switch lock around the read-only
 * re-check so that a PRIMARY-to-REPLICA flip cannot race a client COMMIT onto the replica.
 * <p>
 * A BEGIN; INSERT parks a WalWriter in the connection's pendingWriters map. That writer is invisible
 * to the demote drain (drainWriterPool counts the non-WAL writer pool only, by design), so a demote
 * can settle while the transaction is still open. The flush -- explicit COMMIT, the implicit commit
 * on SYNC, or the implicit commit before a pipelined SELECT -- all funnel through
 * PGPipelineEntry.commit(pendingWriters), so the fence sits there.
 * <p>
 * RED state (before the fix): commit(pendingWriters) had no read-only check and no lock; a parked
 * writer was flushed unconditionally even on a read-only node, acknowledging an unreplicated write
 * to the client (the table's seqTxn advances). The test asserts writerAPI.commit() is NOT called and
 * an authorization error is thrown -- both fail before the fix.
 * <p>
 * GREEN state (after the fix): the early-out, the in-lock re-check, and the rollback-on-refusal all
 * refuse the flush; writerAPI.commit() is never called and the parked writers are rolled back.
 */
public class PGCommitDemoteFenceTest extends AbstractCairoTest {

    /**
     * The in-lock re-check catches the TOCTOU scenario: the engine is PRIMARY on the first
     * isReadOnlyMode() call (early-out passes) but REPLICA on the second call (in-lock re-check),
     * modelling a demote that flips the flag after the early-out but before the flush.
     * <p>
     * RED state: only the (absent) early-out existed, so the flip between the two reads went
     * undetected and the parked writer was flushed.
     * GREEN state: the in-lock re-check catches the flip and throws authorization without flushing.
     */
    @Test
    public void testCommitInLockReCheckCatchesFlipAfterEarlyOut() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger commitCalled = new AtomicInteger(0);
            AtomicInteger readOnlyCallCount = new AtomicInteger(0);
            try (CairoEngine flipEngine = buildFlipAfterFirstCallEngine(readOnlyCallCount)) {
                PGPipelineEntry entry = new PGPipelineEntry(flipEngine);
                ObjObjHashMap<TableToken, TableWriterAPI> pendingWriters = parkWriter(commitCalled);
                try {
                    entry.commit(pendingWriters);
                    Assert.fail("commit() must throw authorization when the in-lock re-check sees read-only");
                } catch (CairoException e) {
                    assertReadOnlyRefusal(e);
                }
                Assert.assertEquals("writerAPI.commit() must not be called on the flipped node", 0, commitCalled.get());
                Assert.assertEquals("parked writers must be rolled back and cleared", 0, pendingWriters.size());
                Assert.assertTrue(
                        "isReadOnlyMode() must be called at least twice to reach the in-lock re-check",
                        readOnlyCallCount.get() >= 2
                );
            }
        });
    }

    /**
     * On a PRIMARY (read-write) node the parked writer must be flushed exactly once and the map
     * cleared -- the fence must not refuse a legitimate commit.
     */
    @Test
    public void testCommitOnPrimaryFlushesParkedWriter() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger commitCalled = new AtomicInteger(0);
            try (CairoEngine primaryEngine = buildPrimaryEngine()) {
                PGPipelineEntry entry = new PGPipelineEntry(primaryEngine);
                ObjObjHashMap<TableToken, TableWriterAPI> pendingWriters = parkWriter(commitCalled);
                entry.commit(pendingWriters);
                Assert.assertEquals("writerAPI.commit() must be called once on a primary node", 1, commitCalled.get());
                Assert.assertEquals("pendingWriters must be cleared after a successful commit", 0, pendingWriters.size());
            }
        });
    }

    /**
     * The headline bypass: a parked writer plus a read-only engine. The flush must be refused with
     * the standard authorization error, the writer rolled back, and writerAPI.commit() never called.
     * This exercises the early-out path AND, via the always-read-only engine, the in-lock re-check.
     */
    @Test
    public void testCommitRefusesAndRollsBackOnReadOnlyReplica() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger commitCalled = new AtomicInteger(0);
            try (CairoEngine readOnlyEngine = buildReadOnlyEngine()) {
                PGPipelineEntry entry = new PGPipelineEntry(readOnlyEngine);
                ObjObjHashMap<TableToken, TableWriterAPI> pendingWriters = parkWriter(commitCalled);
                try {
                    entry.commit(pendingWriters);
                    Assert.fail("commit() must throw CairoException.authorization on a read-only node");
                } catch (CairoException e) {
                    assertReadOnlyRefusal(e);
                }
                Assert.assertEquals("writerAPI.commit() must not be called on a read-only node", 0, commitCalled.get());
                Assert.assertEquals("parked writers must be rolled back and cleared", 0, pendingWriters.size());
            }
        });
    }

    private static void assertReadOnlyRefusal(CairoException e) {
        Assert.assertTrue("exception must be an authorization error", e.isAuthorizationError());
        Assert.assertTrue(
                "message must be 'replica access is read-only'",
                e.getMessage().contains("replica access is read-only")
        );
    }

    /**
     * Builds an ObjObjHashMap holding one parked writer that records its commit() calls -- the shape
     * a BEGIN; INSERT leaves behind in a pg-wire connection's pendingWriters before the flush.
     */
    private static ObjObjHashMap<TableToken, TableWriterAPI> parkWriter(AtomicInteger commitCalled) {
        TableToken token = new TableToken("parked", "parked~1", null, 1, false, false, false);
        TableWriterAPI fakeWriter = (TableWriterAPI) Proxy.newProxyInstance(
                TableWriterAPI.class.getClassLoader(),
                new Class[]{TableWriterAPI.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "commit" -> {
                        commitCalled.incrementAndGet();
                        yield null;
                    }
                    case "getTableToken" -> token;
                    case "close", "rollback" -> null;
                    default -> throw new UnsupportedOperationException(method.getName() + " not stubbed");
                }
        );
        ObjObjHashMap<TableToken, TableWriterAPI> pendingWriters = new ObjObjHashMap<>();
        pendingWriters.put(token, fakeWriter);
        return pendingWriters;
    }

    /**
     * Builds a minimal CairoEngine whose isReadOnlyMode() returns false on the first call (early-out
     * passes) and true on every subsequent call (the flip happened inside the lock window).
     */
    private CairoEngine buildFlipAfterFirstCallEngine(AtomicInteger callCount) throws Exception {
        String dir = temp.newFolder().getAbsolutePath();
        CairoConfiguration cfg = new DefaultCairoConfiguration(dir);
        return new CairoEngine(cfg, false) {
            @Override
            public boolean isReadOnlyMode() {
                int n = callCount.incrementAndGet();
                return n >= 2;  // false on first call; true from second call onward
            }
        };
    }

    /**
     * Builds a minimal CairoEngine that always reports primary (read-write) mode.
     */
    private CairoEngine buildPrimaryEngine() throws Exception {
        String dir = temp.newFolder().getAbsolutePath();
        CairoConfiguration cfg = new DefaultCairoConfiguration(dir);
        return new CairoEngine(cfg, false) {
            @Override
            public boolean isReadOnlyMode() {
                return false;
            }
        };
    }

    /**
     * Builds a minimal CairoEngine that always reports read-only mode.
     */
    private CairoEngine buildReadOnlyEngine() throws Exception {
        String dir = temp.newFolder().getAbsolutePath();
        CairoConfiguration cfg = new DefaultCairoConfiguration(dir);
        return new CairoEngine(cfg, false) {
            @Override
            public boolean isReadOnlyMode() {
                return true;
            }
        };
    }
}
