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

package io.questdb.test.cutlass.http;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.OperationCodes;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.processors.JsonQueryProcessor;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.ops.GenericDropOperation;
import io.questdb.griffin.engine.ops.Operation;
import io.questdb.mp.SCSequence;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Verifies the HTTP /exec CTAS/CREATE/DROP/CREATE MAT VIEW/CREATE VIEW demote write-fence, the twin of
 * the pg-wire PGDdlDemoteFenceTest. These DDL statements execute through JsonQueryProcessor.executeDdl,
 * which submits op.execute() outside any writer funnel; the pre-compile gate at the top of
 * compileAndExecuteQuery is the only read-only guard -- and that read is check-then-act (TOCTOU):
 * isReadOnlyMode() is read once before the arbitrarily long CTAS SELECT pump, so a node that flips to
 * REPLICA mid-execute still externalizes its effect (a fresh table, a CTAS data commit, a drop) and
 * acknowledges with HTTP 200 a change no uploader replicates.
 * <p>
 * RED state (before the fence): executeDdl's operation-execution core ran op.execute() with no lock and
 * no in-lock re-check. A flip after the pre-compile gate executed the operation anyway. The flip-caught
 * and read-only-DROP legs below assert op.execute() is NOT called and an authorization error is thrown,
 * so both fail against the unfenced core.
 * <p>
 * GREEN state (after the fence): the extracted executeDdlFenced core re-checks the read-only state under
 * engine.getRoleSwitchReadLock() before and around op.execute(), refusing with the standard
 * authorization error if the gate refuses the statement on a read-only node.
 * <p>
 * WAL/non-WAL coverage argument: the fence wraps op.execute() wholesale. For a non-WAL CTAS the data
 * commit lives inside execute() (SqlCompilerImpl.copyTableData -> writer.commit(), around
 * SqlCompilerImpl.java:4061, with the writer built directly via new TableWriter at :4083, bypassing the
 * pooled acquire-gates); for a WAL CTAS the externalization is the WAL pump inside the same execute().
 * Refusing before execute() (or holding the role-switch read lock across it so the flip's write acquire
 * waits) therefore covers BOTH variants from one seam -- the fence is upstream of the divergence point
 * in either path.
 * <p>
 * Both fence checks consult the shared ReadOnlyStatementGate predicate, NOT a blanket isReadOnlyMode()
 * refusal: the gate carries the one DDL exemption a read-only replica must keep -- the admin's DROP of
 * the HTTP parquet exporter's leftover temp table. A blanket refusal regressed exactly that flow
 * (ReplicationAclTest, CI build 241196), so the read-only-DROP leg here drives a real GenericDropOperation
 * against an ordinary table name (refused) and the export-temp leg drives one against the export prefix
 * (still executes on a read-only node through the new fence).
 * <p>
 * The private operation-execution core is reached via reflection (the same technique PGDdlDemoteFenceTest
 * uses for the pg-wire twin): the production caller wires it into executeDdl, which would otherwise
 * require a full HTTP pipeline and a live JsonQueryProcessorState to drive.
 */
public class HttpExecDdlDemoteFenceTest extends AbstractCairoTest {

    /**
     * CTAS arm: the in-lock re-check catches a flip that lands after the early-out passes but before the
     * execute. operation.execute() must not be called and isReadOnlyMode() must be consulted at least
     * twice (the pre-execution check and the authoritative in-lock re-check).
     */
    @Test
    public void testCtasInLockReCheckCatchesFlip() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger executeCalled = new AtomicInteger(0);
            AtomicInteger readOnlyCallCount = new AtomicInteger(0);
            try (CairoEngine flipEngine = buildFlipAfterFirstCallEngine(readOnlyCallCount)) {
                JsonQueryProcessor processor = newProcessor(flipEngine);
                try {
                    SqlExecutionContextImpl ctx = TestUtils.createSqlExecutionCtx(flipEngine, 1);
                    try {
                        callExecuteDdlFenced(processor, ctx, CompiledQuery.CREATE_TABLE_AS_SELECT, fakeOperation(executeCalled));
                        Assert.fail("executeDdlFenced must throw authorization when the in-lock re-check sees read-only");
                    } catch (CairoException e) {
                        assertReadOnlyRefusal(e);
                    }
                    Assert.assertEquals("operation.execute() must not be called on the flipped node", 0, executeCalled.get());
                    Assert.assertTrue(
                            "isReadOnlyMode() must be called at least twice to reach the in-lock re-check",
                            readOnlyCallCount.get() >= 2
                    );
                } finally {
                    processor.close();
                }
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
                JsonQueryProcessor processor = newProcessor(primaryEngine);
                try {
                    SqlExecutionContextImpl ctx = TestUtils.createSqlExecutionCtx(primaryEngine, 1);
                    callExecuteDdlFenced(processor, ctx, CompiledQuery.CREATE_TABLE_AS_SELECT, fakeOperation(executeCalled));
                    Assert.assertEquals("operation.execute() must be called once on a primary node", 1, executeCalled.get());
                } finally {
                    processor.close();
                }
            }
        });
    }

    /**
     * CREATE/DROP arm on a read-only node: a genuine client DROP (NOT the exempted export-temp-table
     * drop) must refuse with the standard authorization error and never call operation.execute(). Drives
     * a real GenericDropOperation targeting an ordinary table name so the gate predicate's exemption
     * check runs and correctly does NOT exempt it.
     */
    @Test
    public void testCreateDropRefusedOnReadOnlyReplica() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger executeCalled = new AtomicInteger(0);
            try (CairoEngine readOnlyEngine = buildReadOnlyEngine()) {
                JsonQueryProcessor processor = newProcessor(readOnlyEngine);
                try {
                    SqlExecutionContextImpl ctx = TestUtils.createSqlExecutionCtx(readOnlyEngine, 1);
                    try {
                        callExecuteDdlFenced(processor, ctx, CompiledQuery.DROP, recordingDropOperation("ordinary_client_table", executeCalled));
                        Assert.fail("executeDdlFenced must throw CairoException.authorization on a read-only node");
                    } catch (CairoException e) {
                        assertReadOnlyRefusal(e);
                    }
                    Assert.assertEquals("operation.execute() must not be called on a read-only node", 0, executeCalled.get());
                } finally {
                    processor.close();
                }
            }
        });
    }

    /**
     * The export-temp-table DROP exemption must survive the fence: the admin's DROP of the HTTP parquet
     * exporter's leftover temp table is the ONE DROP a read-only replica permits (the pre-execution gate
     * lets it through via ReadOnlyStatementGate), so the fence must let it execute rather than
     * re-refusing it with a blanket read-only check.
     * <p>
     * RED state (the regression this pins, CI build 241196): a blanket isReadOnlyMode() re-check refused
     * the exempted DROP the pre-gate had just allowed, breaking the replica parquet-export cleanup.
     * GREEN state: both fence checks consult ReadOnlyStatementGate.isRefusedOnReadOnly, which exempts
     * this DROP; operation.execute() runs exactly once on the read-only node.
     */
    @Test
    public void testExportTempTableDropExecutesOnReadOnlyReplica() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger executeCalled = new AtomicInteger(0);
            try (CairoEngine readOnlyEngine = buildReadOnlyEngine()) {
                JsonQueryProcessor processor = newProcessor(readOnlyEngine);
                try {
                    String exportTempName = readOnlyEngine.getConfiguration().getParquetExportTableNamePrefix() + "1234";
                    SqlExecutionContextImpl ctx = TestUtils.createSqlExecutionCtx(readOnlyEngine, 1);
                    callExecuteDdlFenced(processor, ctx, CompiledQuery.DROP, recordingDropOperation(exportTempName, executeCalled));
                    Assert.assertEquals(
                            "the exempted export-temp-table DROP must execute on a read-only node",
                            1, executeCalled.get()
                    );
                } finally {
                    processor.close();
                }
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
            JsonQueryProcessor processor, SqlExecutionContextImpl ctx, short sqlType, Operation operation
    ) throws Exception {
        Method m = JsonQueryProcessor.class.getDeclaredMethod(
                "executeDdlFenced",
                SqlExecutionContextImpl.class,
                int.class,
                Operation.class,
                SCSequence.class,
                long.class
        );
        m.setAccessible(true);
        try {
            m.invoke(processor, ctx, (int) sqlType, operation, null, 30_000L);
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
     * An Operation proxy that records execute() calls and returns a no-op OperationFuture whose await
     * reports completion. On a read-only/flipped node the fence must refuse before this is ever reached.
     */
    private static Operation fakeOperation(AtomicInteger executeCalled) {
        OperationFuture fut = noopFuture();
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

    private static JsonQueryProcessor newProcessor(CairoEngine engine) {
        DefaultHttpServerConfiguration httpConfig = new HttpServerConfigurationBuilder()
                .withPort(0)
                .build(engine.getConfiguration());
        return new JsonQueryProcessor(httpConfig.getJsonQueryProcessorConfiguration(), engine, 1);
    }

    private static OperationFuture noopFuture() {
        return (OperationFuture) Proxy.newProxyInstance(
                OperationFuture.class.getClassLoader(),
                new Class[]{OperationFuture.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "await" -> OperationFuture.QUERY_COMPLETE;
                    case "getAffectedRowsCount" -> 0L;
                    case "close" -> null;
                    default -> throw new UnsupportedOperationException(method.getName() + " not stubbed");
                }
        );
    }

    /**
     * A real GenericDropOperation (a concrete class, so the gate predicate's instanceof + entity-name
     * exemption check runs against it) whose execute() only records the call. The entity name decides
     * the gate verdict: an export-prefixed name is the exempt drop, any other name a genuine client DROP.
     */
    private static Operation recordingDropOperation(String tableName, AtomicInteger executeCalled) {
        return new GenericDropOperation(OperationCodes.DROP_TABLE, null, tableName, 0, false) {
            @Override
            public OperationFuture execute(SqlExecutionContext sqlExecutionContext, @Nullable SCSequence eventSubSeq) {
                executeCalled.incrementAndGet();
                return noopFuture();
            }
        };
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
