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

    // A genuinely non-checking cursor: pg_get_keywords() is an immutable constant list whose cursor
    // performs no per-row breaker checks (PgGetKeywordsFunctionFactoryTest opts it out via
    // noCircuitBreakerCheck()). Unlike "SELECT 1" - which runs through long_sequence(1) and now DOES
    // consult the breaker - this fixture keeps the consultation count at zero, so the default
    // expectation actually fires.
    private static final String NON_CHECKING_QUERY = "pg_get_keywords;";

    @Test
    public void testDefaultFailsWhenCursorNeverChecksCircuitBreaker() throws Exception {
        final String expected = captureOutput(NON_CHECKING_QUERY);
        // The result itself matches, so the cursor battery passes; only the breaker consultation is
        // missing. Capture the AssertionError with assertThrows (NOT a try/catch that would also catch
        // the test's own Assert.fail) and assert the UNIQUE enforcement sentence - not a generic
        // "circuit breaker" substring that the harness could emit for any reason. That is what proves
        // the enforcement actually fired.
        final AssertionError e = Assert.assertThrows(AssertionError.class, () ->
                assertQuery(NON_CHECKING_QUERY).noRandomAccess().expectSize().returns(expected));
        TestUtils.assertContains(e.getMessage(), "but it never did, so the query is not cancellable");
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
        // The SAME fixture that fails by default (see testDefaultFailsWhenCursorNeverChecksCircuitBreaker)
        // must pass once the check is opted out. Pairing both assertions on one genuine non-checker is
        // what makes this opt-out test non-vacuous: if noCircuitBreakerCheck() silently did nothing, the
        // default expectation would fire here too.
        final String expected = captureOutput(NON_CHECKING_QUERY);
        assertQuery(NON_CHECKING_QUERY)
                .noRandomAccess()
                .noCircuitBreakerCheck()
                .expectSize()
                .returns(expected);
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
        // returnsOnce() runs a single print pass, not the full battery, so enforcement does not apply -
        // even for the genuine non-checker that the full returns() battery rejects in
        // testDefaultFailsWhenCursorNeverChecksCircuitBreaker. That contrast is what makes this test
        // meaningful: it passes precisely because returnsOnce() skips the check.
        final String expected = captureOutput(NON_CHECKING_QUERY);
        assertQuery(NON_CHECKING_QUERY).returnsOnce(expected);
    }

    private static String captureOutput(CharSequence sql) throws Exception {
        sink.clear();
        printSql(sql);
        return sink.toString();
    }
}
