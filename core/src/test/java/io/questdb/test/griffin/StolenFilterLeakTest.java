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

package io.questdb.test.griffin;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.FaultInjectedException;
import io.questdb.test.tools.FaultInjectingConfiguration;
import io.questdb.test.tools.FaultInjectingConfiguration.FaultMethod;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * A parallel consumer that steals a filter adopts the filter function, its worker clones, its compiled JIT filter
 * and bind variables once its constructor starts. These tests fail each consuming constructor after the adoption
 * and let {@code assertMemoryLeak} check that the constructor freed what it adopted.
 */
public class StolenFilterLeakTest extends AbstractCairoTest {

    @Test
    public void testAsOfJoinFreesStolenFilterOnConstructorFailure() throws Exception {
        assertNoLeakOnFault(
                FaultMethod.SQL_AS_OF_JOIN_LOOK_AHEAD,
                "Filtered AsOf Join Fast",
                "SELECT t.ts, t.price, p.px FROM trades t ASOF JOIN (SELECT * FROM prices WHERE px > 1 AND sym ~ 'b') p ON (sym)"
        );
    }

    @Test
    public void testHorizonJoinFreesStolenFilterOnConstructorFailure() throws Exception {
        assertNoLeakOnFault(
                FaultMethod.PAGE_FRAME_REDUCE_ROW_ID_LIST_CAPACITY,
                "Async Horizon Join",
                """
                        SELECT t.sym, avg(p.px) a
                        FROM trades AS t
                        HORIZON JOIN prices AS p ON (t.sym = p.sym)
                        RANGE FROM 0s TO 0s STEP 1s AS h
                        WHERE t.price > 150 AND t.sym ~ 'b'
                        """
        );
    }

    @Test
    public void testTopKFreesStolenFilterOnConstructorFailure() throws Exception {
        assertNoLeakOnFault(
                FaultMethod.PAGE_FRAME_REDUCE_ROW_ID_LIST_CAPACITY,
                "Async Top K",
                "SELECT * FROM trades WHERE price > 150 AND sym ~ 'b' ORDER BY price DESC LIMIT 1"
        );
    }

    private void assertNoLeakOnFault(FaultMethod faultMethod, String consumer, String query) throws Exception {
        final FaultInjectingConfiguration config = new FaultInjectingConfiguration(configuration, faultMethod, null, false);
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

                try (
                        RecordCursorFactory explain = faultEngine.select("EXPLAIN " + query, context);
                        RecordCursor cursor = explain.getCursor(context)
                ) {
                    sink.clear();
                    while (cursor.hasNext()) {
                        sink.put(cursor.getRecord().getStrA(0)).put('\n');
                    }
                }
                TestUtils.assertContains(sink, consumer);

                config.setArmed(true);
                try (RecordCursorFactory ignored = faultEngine.select(query, context)) {
                    Assert.fail("expected the injected fault");
                } catch (FaultInjectedException ignore) {
                } finally {
                    config.setArmed(false);
                }
            }
        });
    }
}
