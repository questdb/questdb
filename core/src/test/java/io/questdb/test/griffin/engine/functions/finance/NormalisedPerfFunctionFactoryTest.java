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
                        .ddl("CREATE TABLE trades AS " +
                                "(SELECT x * 10.0 AS price FROM long_sequence(5))")
                        .returns(
                                "normalised_perf\n" +
                                        "100.0\n" +
                                        "200.0\n" +
                                        "300.0\n" +
                                        "400.0\n" +
                                        "500.0\n"
                        )
        );
    }

    @Test
    public void testNormalisedPerfSingleRow() throws Exception {
        assertMemoryLeak(() ->
                assertQuery("SELECT normalised_perf(price, 100.0) FROM trades")
                        .ddl("CREATE TABLE trades AS " +
                                "(SELECT 42.0 AS price FROM long_sequence(1))")
                        .returns("normalised_perf\n100.0\n")
        );
    }

    @Test
    public void testNormalisedPerfFirstRowZero() throws Exception {
        assertMemoryLeak(() ->
                assertQuery("SELECT normalised_perf(price, 100.0) FROM trades")
                        .ddl("CREATE TABLE trades AS " +
                                "(SELECT (x - 1) * 10.0 AS price FROM long_sequence(5))")
                        .returns(
                                "normalised_perf\n" +
                                        "null\n" +
                                        "null\n" +
                                        "null\n" +
                                        "null\n" +
                                        "null\n"
                        )
        );
    }

    @Test
    public void testNormalisedPerfFirstRowNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE)");
            execute("INSERT INTO trades VALUES (null), (10.0), (20.0)");
            assertQuery("SELECT normalised_perf(price, 100.0) FROM trades")
                    .returns(
                            "normalised_perf\n" +
                                    "null\n" +
                                    "null\n" +
                                    "null\n"
                    );
        });
    }

    @Test
    public void testNormalisedPerfNullInLaterRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE)");
            execute("INSERT INTO trades VALUES (10.0), (null), (30.0)");
            assertQuery("SELECT normalised_perf(price, 100.0) FROM trades")
                    .returns(
                            "normalised_perf\n" +
                                    "100.0\n" +
                                    "null\n" +
                                    "300.0\n"
                    );
        });
    }

    @Test
    public void testNormalisedPerfNullBaseValue() throws Exception {
        assertMemoryLeak(() ->
                assertQuery("SELECT normalised_perf(price, null::double) FROM trades")
                        .ddl("CREATE TABLE trades AS " +
                                "(SELECT x * 10.0 AS price FROM long_sequence(3))")
                        .returns(
                                "normalised_perf\n" +
                                        "null\n" +
                                        "null\n" +
                                        "null\n"
                        )
        );
    }

    @Test
    public void testNormalisedPerfBaseValueColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, base DOUBLE)");
            execute("INSERT INTO trades VALUES (10.0, 100.0), (20.0, 200.0), (30.0, 50.0)");
            assertQuery("SELECT normalised_perf(price, base) FROM trades")
                    .returns(
                            "normalised_perf\n" +
                                    "100.0\n" +
                                    "400.0\n" +
                                    "150.0\n"
                    );
        });
    }

    @Test
    public void testNormalisedPerfCursorRewind() throws Exception {
        // Runs the query via .returns(), which internally rewinds and re-reads
        // the cursor a second time. If toTop() does not reset isFirstRow, the
        // second pass will skip capturing column[0] and produce wrong results.
        assertMemoryLeak(() ->
                assertQuery("SELECT normalised_perf(price, 100.0) FROM trades")
                        .ddl("CREATE TABLE trades AS " +
                                "(SELECT x * 5.0 AS price FROM long_sequence(3))")
                        .returns(
                                "normalised_perf\n" +
                                        "100.0\n" +
                                        "200.0\n" +
                                        "300.0\n"
                        )
        );
    }
}