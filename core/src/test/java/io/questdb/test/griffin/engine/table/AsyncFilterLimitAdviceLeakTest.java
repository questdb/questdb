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

package io.questdb.test.griffin.engine.table;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.jit.JitUtil;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.FaultInjectedException;
import io.questdb.test.tools.FaultInjectingConfiguration;
import io.questdb.test.tools.FaultInjectingConfiguration.FaultMethod;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

/**
 * Pins the generator-side ownership of the LIMIT advice function on the async filter paths.
 * <p>
 * {@code SqlCodeGenerator} creates it with {@code getLimitLoFunctionOnly()} and owns it until a
 * factory constructor returns holding it; neither constructor frees its inputs on its own failure.
 * The JIT branch and the Java fallback each build one and each can still throw afterwards - from
 * the per-worker filter compile, from {@code deepClone}, or from the constructor itself - so both
 * are covered here, along with the success path that hands ownership over.
 * <p>
 * A LIMIT bound is normally a heap-only constant, so the orphan is invisible to
 * {@code assertMemoryLeak}. {@code alloc_ts(...)::long} makes it observable: the cast is a runtime
 * constant of LONG type (so it satisfies the LIMIT type checks), and closing it closes the inner
 * {@code alloc_ts}, which holds a tracked 1 KiB native buffer.
 */
public class AsyncFilterLimitAdviceLeakTest extends AbstractCairoTest {

    @Test
    public void testJavaFilterConstructorFailureFreesLimitAdviceFunction() throws Exception {
        // The non-JIT twin. The Java fallback builds its own LIMIT advice function, and only the
        // method-level catch can free it; when the two branches kept separate locals that catch
        // could not see this one. getSqlParallelFilterPreTouchThreshold() is read inside
        // AsyncFilterAtom's constructor, i.e. after the function exists and before the factory
        // returns holding it.
        assertNoLeakOnFault(SqlJitMode.JIT_MODE_DISABLED, FaultMethod.SQL_PARALLEL_FILTER_PRE_TOUCH_THRESHOLD);
    }

    @Test
    public void testJitConstructorFailureFreesLimitAdviceFunction() throws Exception {
        Assume.assumeTrue(JitUtil.isJitSupported());
        // getSqlJitBindVarsMemoryPageSize() is read by AsyncJitFilteredRecordCursorFactory's
        // constructor to size the bind variable memory, i.e. after the generator has built the LIMIT
        // advice function and before the factory returns holding it.
        assertNoLeakOnFault(SqlJitMode.JIT_MODE_ENABLED, FaultMethod.SQL_JIT_BIND_VARS_MEMORY_PAGE_SIZE);
    }

    @Test
    public void testSuccessfulCompileFreesLimitAdviceFunctionOnFactoryClose() throws Exception {
        Assume.assumeTrue(JitUtil.isJitSupported());
        assertFactoryCloseFreesLimitAdviceFunction(SqlJitMode.JIT_MODE_ENABLED);
    }

    @Test
    public void testSuccessfulJavaFilterCompileFreesLimitAdviceFunctionOnFactoryClose() throws Exception {
        // The non-JIT twin. The two factories carry duplicated _close() ownership code, and the
        // test configuration defaults the JIT mode to enabled, so the query above always compiled
        // to AsyncJitFilteredRecordCursorFactory and left the free in
        // AsyncFilteredRecordCursorFactory._close() unpinned - reverting it kept the class green.
        assertFactoryCloseFreesLimitAdviceFunction(SqlJitMode.JIT_MODE_DISABLED);
    }

    // Control for the fault tests: on the path where a factory DOES adopt the LIMIT advice
    // function, closing that factory must release it. This pins which side owns the function, so
    // the fault-path fix cannot be "free it everywhere" and double-free here.
    private void assertFactoryCloseFreesLimitAdviceFunction(int jitMode) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (
                    CairoEngine engine = new CairoEngine(configuration);
                    SqlExecutionContext context = new SqlExecutionContextImpl(engine, 4)
                            .with(engine.getConfiguration().getFactoryProvider().getSecurityContextFactory().getRootContext(), null)
            ) {
                context.setJitMode(jitMode);
                engine.execute("CREATE TABLE x (i INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY;", context);
                engine.execute("INSERT INTO x VALUES (1, '2024-01-01T00:00:00.000000Z');", context);
                try (RecordCursorFactory factory = engine.select(
                        "SELECT * FROM x WHERE i > 0 " +
                                "LIMIT alloc_ts('2024-01-01T00:00:00.000000Z'::timestamp)::long",
                        context)) {
                    // Pin which twin actually ran: without this, a mode that silently declined the
                    // JIT would leave the other factory untested exactly as before.
                    Assert.assertEquals(jitMode == SqlJitMode.JIT_MODE_ENABLED, factory.usesCompiledFilter());
                    // closing the factory is the rest of the assertion
                }
            }
        });
    }

    private void assertNoLeakOnFault(int jitMode, FaultMethod faultMethod) throws Exception {
        final String query = "SELECT * FROM x WHERE i > 0 "
                + "LIMIT alloc_ts('2024-01-01T00:00:00.000000Z'::timestamp)::long";
        // The engine and the DDL below read configuration too, so the fault starts disarmed.
        final FaultInjectingConfiguration config =
                new FaultInjectingConfiguration(configuration, faultMethod, null, false);
        TestUtils.assertMemoryLeak(() -> {
            try (
                    CairoEngine faultEngine = new CairoEngine(config);
                    SqlExecutionContext context = new SqlExecutionContextImpl(faultEngine, 4)
                            .with(faultEngine.getConfiguration().getFactoryProvider().getSecurityContextFactory().getRootContext(), null)
            ) {
                context.setJitMode(jitMode);
                faultEngine.execute("CREATE TABLE x (i INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY;", context);
                faultEngine.execute("INSERT INTO x VALUES (1, '2024-01-01T00:00:00.000000Z');", context);

                // Armed only around select(): each fault getter is read once, inside the factory
                // constructor, which is after the generator has already built the LIMIT advice
                // function and before the factory returns holding it.
                config.setArmed(true);
                try (RecordCursorFactory ignored = faultEngine.select(query, context)) {
                    Assert.fail("expected the injected fault");
                } catch (FaultInjectedException expected) {
                    // The generator must have freed the LIMIT advice function on the way out.
                } finally {
                    config.setArmed(false);
                }
            }
        });
    }
}
