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
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cutlass.pgwire.PGPipelineEntry;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.ops.Operation;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Verifies the pg-wire CTAS/CREATE/DROP demote write-fence (executeDdlFenced). These statements
 * execute outside the pendingWriters funnel that PGPipelineEntry.commit() fences, so the
 * pre-execution ReadOnlyStatementGate check is their only read-only guard -- and that check is
 * check-then-act: the operation can begin while the node is PRIMARY and externalize its effect (a
 * fresh table, a CTAS data commit, a drop) on a node that flips to REPLICA, acknowledging a change no
 * uploader replicates.
 * <p>
 * RED state (before the fix): the CTAS/CREATE/DROP arms called operation.execute() with no lock and no
 * re-check, so a flip after the pre-compile gate executed the operation anyway. The tests assert
 * operation.execute() is NOT called when the engine is read-only and an authorization error is thrown
 * -- both fail before the fix.
 * <p>
 * GREEN state (after the fix): executeDdlFenced re-checks isReadOnlyMode() under the role-switch lock
 * before executing, refusing with the standard authorization error.
 * <p>
 * The private executeDdlFenced(...) is reached via reflection (the same technique
 * TableUpdateDetailsCommitAtomicityTest uses for releaseWriter): the production callers wire it into
 * the CTAS/CREATE/DROP arms of msgExecute, which would otherwise require a full pg-wire pipeline to
 * drive.
 */
public class PGDdlDemoteFenceTest extends AbstractCairoTest {

    /**
     * CTAS arm (reportAffectedRows=true): the in-lock re-check catches a flip that lands after the
     * early-out passes but before the execute. operation.execute() must not be called.
     */
    @Test
    public void testCtasInLockReCheckCatchesFlip() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger executeCalled = new AtomicInteger(0);
            AtomicInteger readOnlyCallCount = new AtomicInteger(0);
            try (CairoEngine flipEngine = buildFlipAfterFirstCallEngine(readOnlyCallCount)) {
                PGPipelineEntry entry = new PGPipelineEntry(flipEngine);
                setOperation(entry, fakeOperation(executeCalled));
                SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(flipEngine);
                try {
                    callExecuteDdlFenced(entry, ctx, true);
                    Assert.fail("executeDdlFenced must throw authorization when the in-lock re-check sees read-only");
                } catch (CairoException e) {
                    assertReadOnlyRefusal(e);
                }
                Assert.assertEquals("operation.execute() must not be called on the flipped node", 0, executeCalled.get());
                Assert.assertTrue(
                        "isReadOnlyMode() must be called at least twice to reach the in-lock re-check",
                        readOnlyCallCount.get() >= 2
                );
            }
        });
    }

    /**
     * CTAS on a PRIMARY node must execute exactly once -- the fence must not refuse a legitimate DDL.
     */
    @Test
    public void testCtasOnPrimaryExecutes() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger executeCalled = new AtomicInteger(0);
            try (CairoEngine primaryEngine = buildPrimaryEngine()) {
                PGPipelineEntry entry = new PGPipelineEntry(primaryEngine);
                setOperation(entry, fakeOperation(executeCalled));
                SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(primaryEngine);
                callExecuteDdlFenced(entry, ctx, true);
                Assert.assertEquals("operation.execute() must be called once on a primary node", 1, executeCalled.get());
            }
        });
    }

    /**
     * CREATE/DROP arm (reportAffectedRows=false) on a read-only node: refuse with the standard
     * authorization error and never call operation.execute().
     */
    @Test
    public void testCreateDropRefusedOnReadOnlyReplica() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger executeCalled = new AtomicInteger(0);
            try (CairoEngine readOnlyEngine = buildReadOnlyEngine()) {
                PGPipelineEntry entry = new PGPipelineEntry(readOnlyEngine);
                setOperation(entry, fakeOperation(executeCalled));
                SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(readOnlyEngine);
                try {
                    callExecuteDdlFenced(entry, ctx, false);
                    Assert.fail("executeDdlFenced must throw CairoException.authorization on a read-only node");
                } catch (CairoException e) {
                    assertReadOnlyRefusal(e);
                }
                Assert.assertEquals("operation.execute() must not be called on a read-only node", 0, executeCalled.get());
            }
        });
    }

    // --- helpers ---

    private static void assertReadOnlyRefusal(CairoException e) {
        Assert.assertTrue("exception must be an authorization error", e.isAuthorizationError());
        Assert.assertTrue(
                "message must be 'replica access is read-only'",
                e.getMessage().contains("replica access is read-only")
        );
    }

    private static void callExecuteDdlFenced(
            PGPipelineEntry entry, SqlExecutionContext ctx, boolean reportAffectedRows
    ) throws Exception {
        Method m = PGPipelineEntry.class.getDeclaredMethod(
                "executeDdlFenced", SqlExecutionContext.class, io.questdb.mp.SCSequence.class, boolean.class
        );
        m.setAccessible(true);
        try {
            m.invoke(entry, ctx, null, reportAffectedRows);
        } catch (java.lang.reflect.InvocationTargetException ite) {
            Throwable cause = ite.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            throw ite;
        }
    }

    /**
     * An Operation proxy that records execute() calls and returns a no-op OperationFuture. On a
     * read-only/flipped node the fence must refuse before this is ever reached.
     */
    private static Operation fakeOperation(AtomicInteger executeCalled) {
        OperationFuture fut = (OperationFuture) Proxy.newProxyInstance(
                OperationFuture.class.getClassLoader(),
                new Class[]{OperationFuture.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "await" -> null;
                    case "getAffectedRowsCount" -> 0L;
                    case "close" -> null;
                    default -> throw new UnsupportedOperationException(method.getName() + " not stubbed");
                }
        );
        return (Operation) Proxy.newProxyInstance(
                Operation.class.getClassLoader(),
                new Class[]{Operation.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "execute" -> {
                        executeCalled.incrementAndGet();
                        yield fut;
                    }
                    case "close" -> null;
                    default -> throw new UnsupportedOperationException(method.getName() + " not stubbed");
                }
        );
    }

    private static void setOperation(PGPipelineEntry entry, Operation operation) throws Exception {
        Field f = PGPipelineEntry.class.getDeclaredField("operation");
        f.setAccessible(true);
        f.set(entry, operation);
    }

    private CairoEngine buildFlipAfterFirstCallEngine(AtomicInteger callCount) throws Exception {
        String dir = temp.newFolder().getAbsolutePath();
        CairoConfiguration cfg = new DefaultCairoConfiguration(dir);
        return new CairoEngine(cfg, false) {
            @Override
            public boolean isReadOnlyMode() {
                int n = callCount.incrementAndGet();
                return n >= 2;
            }
        };
    }

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
