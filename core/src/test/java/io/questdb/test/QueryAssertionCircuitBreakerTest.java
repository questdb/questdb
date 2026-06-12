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

import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the {@link QueryAssertion} circuit breaker expectation (on by default, see
 * {@link QueryAssertion#expectCircuitBreakerChecks()}): the battery must observe at least one
 * circuit breaker check from a cursor that honors the breaker, fail by default when the cursor
 * never checks, honor {@link QueryAssertion#noCircuitBreakerCheck()}, and skip terminals that do
 * not run the full cursor battery.
 */
public class QueryAssertionCircuitBreakerTest extends AbstractCairoTest {

    @Test
    public void testDefaultFailsWhenCursorNeverChecksCircuitBreaker() throws Exception {
        // A constant projection over long_sequence never consults the circuit breaker, so the
        // default expectation must fail even though the result itself matches.
        try {
            assertQuery("SELECT 1")
                    .expectSize()
                    .returns("""
                            1
                            1
                            """);
            Assert.fail("expected the circuit breaker expectation to fail for a non-checking cursor");
        } catch (AssertionError e) {
            TestUtils.assertContains(e.getMessage(), "circuit breaker");
        }
    }

    @Test
    public void testFilteredScanChecksCircuitBreaker() throws Exception {
        assertQuery("SELECT v FROM x WHERE v <> 3")
                .ddl("CREATE TABLE x AS (SELECT x v FROM long_sequence(5))")
                .withPlanContaining("Async")
                .returns("""
                        v
                        1
                        2
                        4
                        5
                        """);
    }

    @Test
    public void testNoCircuitBreakerCheckAllowsNonCheckingCursor() throws Exception {
        assertQuery("SELECT 1")
                .noCircuitBreakerCheck()
                .expectSize()
                .returns("""
                        1
                        1
                        """);
    }

    @Test
    public void testOrderByChecksCircuitBreaker() throws Exception {
        // expectCircuitBreakerChecks() re-enables the default after an opt-out.
        assertQuery("SELECT v FROM x ORDER BY v")
                .ddl("CREATE TABLE x AS (SELECT (6 - x) v FROM long_sequence(5))")
                .noCircuitBreakerCheck()
                .expectCircuitBreakerChecks()
                .expectSize()
                .returns("""
                        v
                        1
                        2
                        3
                        4
                        5
                        """);
    }

    @Test
    public void testReturnsOnceSkipsCircuitBreakerCheck() throws Exception {
        // returnsOnce() performs a single print pass, not the full battery, so the default
        // expectation does not apply even though the cursor never checks the breaker.
        assertQuery("SELECT 1")
                .returnsOnce("""
                        1
                        1
                        """);
    }
}
