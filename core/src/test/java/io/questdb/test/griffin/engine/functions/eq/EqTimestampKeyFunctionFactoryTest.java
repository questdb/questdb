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

package io.questdb.test.griffin.engine.functions.eq;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

// __timestamp_key_eq(a, b) compares two timestamps as a join key does: next to a TIMESTAMP_NS, a TIMESTAMP
// widens to nanoseconds by a multiplication that wraps rather than fails, so 2^61 microseconds widens to 0
// nanoseconds and 9999-12-31 to a nanosecond of 1816. NULL equals NULL.
public class EqTimestampKeyFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testColumns() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertQuery("SELECT id, __timestamp_key_eq(m, n) mn, __timestamp_key_eq(n, m) nm, __timestamp_key_eq(m, m2) mm, __timestamp_key_eq(n, n2) nn FROM t")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tmn\tnm\tmm\tnn
                            1\ttrue\ttrue\ttrue\ttrue
                            2\tfalse\tfalse\tfalse\tfalse
                            3\ttrue\ttrue\ttrue\ttrue
                            4\tfalse\tfalse\tfalse\tfalse
                            5\tfalse\tfalse\tfalse\tfalse
                            6\ttrue\ttrue\ttrue\ttrue
                            7\tfalse\tfalse\ttrue\ttrue
                            8\ttrue\ttrue\ttrue\ttrue
                            9\ttrue\ttrue\tfalse\tfalse
                            """);
        });
    }

    @Test
    public void testConstantsAndBindVariables() throws Exception {
        assertMemoryLeak(() -> {
            // the same pairs as rows 1, 2, 3, 6 and 7 of testColumns(), folded to constants
            assertQuery("""
                    SELECT
                        __timestamp_key_eq(1_000_000::TIMESTAMP, 1_000_000_000::TIMESTAMP_NS) a,
                        __timestamp_key_eq(1_000_000::TIMESTAMP, 1_000_000_001::TIMESTAMP_NS) b,
                        __timestamp_key_eq(NULL::TIMESTAMP, NULL::TIMESTAMP_NS) c,
                        __timestamp_key_eq(2305843009213693952::TIMESTAMP, 0::TIMESTAMP_NS) d,
                        __timestamp_key_eq('9999-12-31T23:59:59.999999Z'::TIMESTAMP, '2262-04-11T23:47:16.854775807Z'::TIMESTAMP_NS) e
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a\tb\tc\td\te
                            true\tfalse\ttrue\ttrue\tfalse
                            """);
            createTable();
            bindVariableService.clear();
            bindVariableService.setTimestamp("micros", 2305843009213693952L);
            bindVariableService.setTimestampNano("nanos", 1_000_000_000L);
            assertQuery("SELECT id, __timestamp_key_eq(:micros, n) mn, __timestamp_key_eq(m, :nanos) nm FROM t")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tmn\tnm
                            1\tfalse\ttrue
                            2\tfalse\ttrue
                            3\tfalse\tfalse
                            4\ttrue\tfalse
                            5\tfalse\tfalse
                            6\ttrue\tfalse
                            7\tfalse\tfalse
                            8\tfalse\tfalse
                            9\tfalse\tfalse
                            """);
        });
    }

    @Test
    public void testFilter() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertQuery("SELECT id FROM t WHERE __timestamp_key_eq(m, n)")
                    .noLeakCheck()
                    .withPlanContaining("Async Filter workers: 1", "filter: __timestamp_key_eq(m,n)")
                    .returns("""
                            id
                            1
                            3
                            6
                            8
                            9
                            """);
        });
    }

    // Rows: equal instants; 1 microsecond or 1 nanosecond apart; NULL next to NULL; NULL next to 1970-01-01
    // and the reverse; 2^61 microseconds next to 0 nanoseconds; 9999-12-31 next to the last TIMESTAMP_NS
    // value; 1 microsecond before 1970 in both units; and 9999-12-31 next to -4852116231933723624
    // nanoseconds, the value that the join key widens it to.
    private static void createTable() throws Exception {
        execute("CREATE TABLE t (id INT, m TIMESTAMP, m2 TIMESTAMP, n TIMESTAMP_NS, n2 TIMESTAMP_NS)");
        execute("""
                INSERT INTO t VALUES
                    (1, 1_000_000::TIMESTAMP, 1_000_000::TIMESTAMP, 1_000_000_000::TIMESTAMP_NS, 1_000_000_000::TIMESTAMP_NS),
                    (2, 1_000_000::TIMESTAMP, 1_000_001::TIMESTAMP, 1_000_000_001::TIMESTAMP_NS, 1_000_000_000::TIMESTAMP_NS),
                    (3, NULL, NULL, NULL, NULL),
                    (4, NULL, 0::TIMESTAMP, 0::TIMESTAMP_NS, NULL),
                    (5, 0::TIMESTAMP, NULL, NULL, 0::TIMESTAMP_NS),
                    (6, 2305843009213693952::TIMESTAMP, 2305843009213693952::TIMESTAMP, 0::TIMESTAMP_NS, 0::TIMESTAMP_NS),
                    (7, '9999-12-31T23:59:59.999999Z', '9999-12-31T23:59:59.999999Z', '2262-04-11T23:47:16.854775807Z', '2262-04-11T23:47:16.854775807Z'),
                    (8, (-1)::TIMESTAMP, (-1)::TIMESTAMP, (-1000)::TIMESTAMP_NS, (-1000)::TIMESTAMP_NS),
                    (9, '9999-12-31T23:59:59.999999Z', 0::TIMESTAMP, (-4852116231933723624)::TIMESTAMP_NS, 1::TIMESTAMP_NS)
                """);
    }
}
