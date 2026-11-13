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

package io.questdb.test;

import io.questdb.griffin.SqlCompiler;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;

/**
 * Tests for {@link QueryAssertion#fullFatJoins()} together with the plan assertions
 * {@link QueryAssertion#withPlan}, {@link QueryAssertion#withPlanContaining} and
 * {@link QueryAssertion#withPlanNotContaining}: the cursor battery asserts the full-fat factory, and
 * the EXPLAIN describes that factory. The fixture tells the two factories apart. The full-fat hash
 * join prints "Hash Join" and computes its size, the light hash join prints "Hash Join Light" and
 * reports size -1, which the battery checks: expectSize() demands a known size, its absence demands -1.
 */
public class QueryAssertionFullFatJoinsTest extends AbstractCairoTest {
    private static final String CREATE_M = "CREATE TABLE m AS (SELECT x::INT id, x::INT k FROM long_sequence(3))";
    private static final String CREATE_S = "CREATE TABLE s AS (SELECT (10 + x)::INT id, x::INT k FROM long_sequence(3))";
    // Each m row matches the one s row with its k, and both tables list k in ascending order, so either
    // hash join returns the rows in k order, whichever side it hashes.
    private static final String EXPECTED = """
            id\tsid
            1\t11
            2\t12
            3\t13
            """;
    private static final String QUERY = "SELECT m.id, s.id sid FROM m JOIN s ON m.k = s.k";

    @Test
    public void testFullFatJoinsWithPlan() throws Exception {
        // The plan has the shape that ExplainPlanTest.testFullFatHashJoin1 pins for a full-fat hash join.
        // The builder runs the DDL and its own leak check here.
        assertQuery(QUERY)
                .ddl(CREATE_M, CREATE_S)
                .fullFatJoins()
                .noRandomAccess()
                .expectSize()
                .withPlan("""
                        SelectedRecord
                            Hash Join
                              condition: s.k=m.k
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: m
                                Hash
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: s
                        """)
                .returns(EXPECTED);
    }

    @Test
    public void testFullFatJoinsWithPlanContaining() throws Exception {
        assertMemoryLeak(() -> {
            execute(CREATE_M);
            execute(CREATE_S);
            // The light hash join, for contrast: the same rows, its own plan, size -1.
            assertQuery(QUERY)
                    .noLeakCheck()
                    .noRandomAccess()
                    .withPlanContaining("Hash Join Light")
                    .returns(EXPECTED);
            assertQuery(QUERY)
                    .noLeakCheck()
                    .fullFatJoins()
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("Hash Join\n", "condition: s.k=m.k")
                    .withPlanNotContaining("Light")
                    .returns(EXPECTED);
            // The plan assertions fail on what the light plan has and the full-fat plan lacks, and the
            // other way around.
            final AssertionError missing = Assert.assertThrows(AssertionError.class, () -> assertQuery(QUERY)
                    .noLeakCheck()
                    .fullFatJoins()
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("Hash Join Light")
                    .returns(EXPECTED));
            TestUtils.assertContains(missing.getMessage(), "does not contain: Hash Join Light");
            final AssertionError present = Assert.assertThrows(AssertionError.class, () -> assertQuery(QUERY)
                    .noLeakCheck()
                    .fullFatJoins()
                    .noRandomAccess()
                    .expectSize()
                    .withPlanNotContaining("Hash Join\n")
                    .returns(EXPECTED));
            TestUtils.assertContains(present.getMessage(), "to NOT contain term <Hash Join\n> but it did.");
        });
    }

    @Test
    public void testUnsupportedCombinationsFail() throws Exception {
        assertMemoryLeak(() -> {
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                assertIllegalState(
                        "withPlan(...)/withPlanContaining(...)/withPlanNotContaining(...) cannot be combined with withCompiler()",
                        () -> assertQuery(QUERY).withCompiler(compiler).withPlanContaining("Hash Join").returns(EXPECTED)
                );
                assertIllegalState(
                        "fullFatJoins()/withCompiler() cannot be combined with mutateWith()/sizeMayVary()",
                        () -> assertQuery(QUERY).fullFatJoins().sizeMayVary().withPlanContaining("Hash Join").returns(EXPECTED)
                );
                assertIllegalState(
                        "fullFatJoins()/withCompiler() cannot be combined with mutateWith()/sizeMayVary()",
                        () -> assertQuery(QUERY).fullFatJoins().mutateWith("INSERT INTO s VALUES (14, 4)").returns(EXPECTED, EXPECTED)
                );
                assertIllegalState(
                        "fullFatJoins()/withCompiler() cannot be combined with mutateWith()/sizeMayVary()",
                        () -> assertQuery(QUERY).withCompiler(compiler).sizeMayVary().returns(EXPECTED)
                );
            }
        });
    }

    private static void assertIllegalState(String message, ThrowingRunnable code) {
        final IllegalStateException e = Assert.assertThrows(IllegalStateException.class, code);
        Assert.assertEquals(message, e.getMessage());
    }
}
