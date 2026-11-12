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

package io.questdb.test.griffin.engine.functions.cast;

import io.questdb.cairo.SqlJitMode;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class CastSymbolToIntFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testSymbolArgumentOfIntParameterReadsItsValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (sym SYMBOL, i INT, d DECIMAL(10,2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('5', 5, 1.25m, 0), ('7', 8, 2.5m, 1), (null, 0, 3.75m, 2), ('-3', -4, 1.0m, 3)");
            assertQuery("SELECT sym + 1 a, abs(sym) b, sym * 2 c, -sym n FROM t")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a\tb\tc\tn
                            6\t5\t10\t-5
                            8\t7\t14\t-7
                            null\tnull\tnull\tnull
                            -2\t3\t-6\t3
                            """);
            assertSqlCursors(
                    "SELECT sym::int + 1 a, abs(sym::int) b, sym::int * 2 c, -sym::int n, round(1.2345, sym::int) r FROM t",
                    "SELECT sym + 1 a, abs(sym) b, sym * 2 c, -sym n, round(1.2345, sym) r FROM t"
            );
            assertQuery("SELECT sum(sym) s, avg(sym) a FROM t")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            s\ta
                            9\t3.0
                            """);
            assertQuery("SELECT avg(d, '1'::symbol) a, avg(d, 1) b FROM t")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            a\tb
                            2.1\t2.1
                            """);
            for (int jitMode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
                sqlExecutionContext.setJitMode(jitMode);
                assertQuery("SELECT sym FROM t WHERE sym > 1")
                        .noLeakCheck()
                        .returns("""
                                sym
                                5
                                7
                                """);
                assertQuery("SELECT sym, i FROM t WHERE sym >= i")
                        .noLeakCheck()
                        .returns("""
                                sym\ti
                                5\t5
                                -3\t-4
                                """);
            }
        });
    }
}
