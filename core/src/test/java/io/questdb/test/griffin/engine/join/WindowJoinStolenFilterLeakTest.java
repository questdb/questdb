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

package io.questdb.test.griffin.engine.join;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoConfigurationWrapper;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

/**
 * A parallel WINDOW JOIN steals the master's filter: it takes the compiled JIT filter, the bind
 * variable memory, the bind variable functions and the filter function itself, then calls
 * {@code halfClose()}, which deliberately frees none of the four because the window-join factory is
 * about to adopt them. Everything between that steal and the constructor therefore runs with four
 * unowned native handles in hand, and the generator's enclosing catch frees only {@code master}.
 * <p>
 * These tests inject a failure into that window and let {@code assertMemoryLeak} decide: the
 * compiled filter holds an executable JIT page and the bind variable memory is native, so leaking
 * them fails the assertion. The fault point is
 * {@code CairoConfiguration#getSqlSmallPageFrameMinRows()}, which the generator reads on the very
 * next line after {@code halfClose()}.
 */
public class WindowJoinStolenFilterLeakTest extends AbstractCairoTest {

    @Test
    public void testStolenFilterFreedWhenFastFactoryNeverBuilt() throws Exception {
        assertNoLeakOnFault("""
                SELECT t.ts, t.price, sum(p.px)
                FROM (SELECT * FROM trades WHERE price > 10) t
                WINDOW JOIN prices p ON (t.sym = p.sym)
                RANGE BETWEEN 1 MINUTE PRECEDING AND 1 MINUTE FOLLOWING EXCLUDE PREVAILING
                """);
    }

    @Test
    public void testStolenFilterFreedWhenGeneralFactoryNeverBuilt() throws Exception {
        // No ON clause, so the generator takes the non-symbol branch and builds the general factory.
        assertNoLeakOnFault("""
                SELECT t.ts, t.price, sum(p.px)
                FROM (SELECT * FROM trades WHERE price > 10) t
                WINDOW JOIN prices p
                RANGE BETWEEN 1 MINUTE PRECEDING AND 1 MINUTE FOLLOWING EXCLUDE PREVAILING
                """);
    }

    private void assertNoLeakOnFault(String query) throws Exception {
        final FaultInjectingConfiguration config = new FaultInjectingConfiguration(configuration);
        TestUtils.assertMemoryLeak(() -> {
            try (
                    CairoEngine faultEngine = new CairoEngine(config);
                    SqlExecutionContext context = new SqlExecutionContextImpl(faultEngine, 4)
                            .with(faultEngine.getConfiguration().getFactoryProvider().getSecurityContextFactory().getRootContext(), null)
            ) {
                faultEngine.execute("CREATE TABLE trades (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY;", context);
                faultEngine.execute("CREATE TABLE prices (sym SYMBOL, px DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY;", context);
                faultEngine.execute("INSERT INTO trades VALUES ('a', 100.0, '2022-01-01T00:00:00.000000Z'), ('b', 200.0, '2022-01-01T00:01:00.000000Z');", context);
                faultEngine.execute("INSERT INTO prices VALUES ('a', 1.0, '2022-01-01T00:00:00.000000Z'), ('b', 2.0, '2022-01-01T00:01:00.000000Z');", context);

                config.armed = true;
                try (RecordCursorFactory ignored = faultEngine.select(query, context)) {
                    Assert.fail("expected the injected fault");
                } catch (FaultInjectedException expected) {
                    // The generator must have freed the four stolen handles on the way out.
                } finally {
                    config.armed = false;
                }
            }
        });
    }

    private static class FaultInjectedException extends RuntimeException {
        private FaultInjectedException() {
            super("injected", null, false, false);
        }
    }

    private static class FaultInjectingConfiguration extends CairoConfigurationWrapper {
        private volatile boolean armed;

        private FaultInjectingConfiguration(@NotNull CairoConfiguration delegate) {
            super(delegate);
        }

        @Override
        public int getSqlSmallPageFrameMinRows() {
            if (armed) {
                throw new FaultInjectedException();
            }
            return super.getSqlSmallPageFrameMinRows();
        }
    }
}
