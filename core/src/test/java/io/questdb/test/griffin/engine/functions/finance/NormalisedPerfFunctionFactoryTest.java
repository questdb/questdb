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

package io.questdb.test.griffin.engine.functions.finance;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

        public class NormalisedPerfFunctionFactoryTest extends AbstractCairoTest {

            @Test
            public void testNormalisedPerfBasic() throws Exception {
                assertMemoryLeak(() ->
                        assertQuery("SELECT normalised_perf(price, 100.0) FROM trades")
                                .ddl("CREATE TABLE trades AS (SELECT x * 10.0 AS price FROM long_sequence(5))")
                                .expectSize()
                                .returns(
                                        "normalised_perf\n" +
                                                "100.0\n200.0\n300.0\n400.0\n500.0\n"
                                )
                );
            }

            @Test
            public void testNormalisedPerfSingleRow() throws Exception {
                assertMemoryLeak(() ->
                        assertQuery("SELECT normalised_perf(price, 100.0) FROM trades")
                                .ddl("CREATE TABLE trades AS (SELECT 42.0 AS price FROM long_sequence(1))")
                                .expectSize()
                                .returns("normalised_perf\n100.0\n")
                );
            }

            @Test
            public void testNormalisedPerfFirstRowZero() throws Exception {
                assertMemoryLeak(() ->
                        assertQuery("SELECT normalised_perf(price, 100.0) FROM trades")
                                .ddl("CREATE TABLE trades AS (SELECT (x - 1) * 10.0 AS price FROM long_sequence(5))")
                                .expectSize()
                                .returns("normalised_perf\nnull\nnull\nnull\nnull\nnull\n")
                );
            }

            @Test
            public void testNormalisedPerfFirstRowNull() throws Exception {
                assertMemoryLeak(() -> {
                    execute("CREATE TABLE trades (price DOUBLE)");
                    execute("INSERT INTO trades VALUES (null), (10.0), (20.0)");
                    assertQuery("SELECT normalised_perf(price, 100.0) FROM trades")
                            .expectSize()
                            .returns("normalised_perf\nnull\nnull\nnull\n");
                });
            }

            @Test
            public void testNormalisedPerfNullInLaterRow() throws Exception {
                assertMemoryLeak(() -> {
                    execute("CREATE TABLE trades (price DOUBLE)");
                    execute("INSERT INTO trades VALUES (10.0), (null), (30.0)");
                    assertQuery("SELECT normalised_perf(price, 100.0) FROM trades")
                            .expectSize()
                            .returns("normalised_perf\n100.0\nnull\n300.0\n");
                });
            }

            @Test
            public void testNormalisedPerfNullBaseValue() throws Exception {
                assertMemoryLeak(() ->
                        assertQuery("SELECT normalised_perf(price, null::double) FROM trades")
                                .ddl("CREATE TABLE trades AS (SELECT x * 10.0 AS price FROM long_sequence(3))")
                                .expectSize()
                                .returns("normalised_perf\nnull\nnull\nnull\n")
                );
            }

            @Test
            public void testNormalisedPerfBaseValueColumn() throws Exception {
                assertMemoryLeak(() -> {
                    execute("CREATE TABLE trades (price DOUBLE, base DOUBLE)");
                    execute("INSERT INTO trades VALUES (10.0, 100.0), (20.0, 200.0), (30.0, 50.0)");
                    assertQuery("SELECT normalised_perf(price, base) FROM trades")
                            .expectSize()
                            .returns("normalised_perf\n100.0\n400.0\n150.0\n");
                });
            }

            @Test
            public void testNormalisedPerfCursorRewind() throws Exception {
                assertMemoryLeak(() ->
                        assertQuery("SELECT normalised_perf(price, 100.0) FROM trades")
                                .ddl("CREATE TABLE trades AS (SELECT x * 5.0 AS price FROM long_sequence(3))")
                                .expectSize()
                                .returns("normalised_perf\n100.0\n200.0\n300.0\n")
                );
            }
        }