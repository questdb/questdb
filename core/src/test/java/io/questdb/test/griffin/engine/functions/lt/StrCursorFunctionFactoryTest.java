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

package io.questdb.test.griffin.engine.functions.lt;

import org.junit.Test;

/**
 * Tests for the {@code string (= | < | >) (sub-query)} operators, where the left operand is a SYMBOL,
 * STRING or VARCHAR and the right-hand side is a scalar sub-query that selects one text column.
 *
 * @see io.questdb.griffin.engine.functions.eq.EqStrCursorFunctionFactory
 * @see io.questdb.griffin.engine.functions.lt.LtStrCursorFunctionFactory
 * @see io.questdb.griffin.engine.functions.lt.GtStrCursorFunctionFactory
 */
public class StrCursorFunctionFactoryTest extends AbstractCursorFunctionFactoryTest {

    @Test
    public void testCharAndUnsupportedOperandTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (id INT, c CHAR, dt DATE, u UUID, ip IPv4, b BOOLEAN)");
            execute("""
                    INSERT INTO x VALUES
                    (1, 'a', '2024-01-02T00:00:00.000Z', '11111111-1111-1111-1111-111111111111', '1.1.1.1', true),
                    (2, 'b', '2024-01-03T00:00:00.000Z', null, null, false)
                    """);
            assertQuery("SELECT id FROM x WHERE c = (SELECT 'a')")
                    .noLeakCheck()
                    .returns("id\n1\n");
            assertQuery("SELECT id FROM x WHERE c > (SELECT c FROM x WHERE id = 1)")
                    .noLeakCheck()
                    .returns("id\n2\n");
            assertQuery("SELECT id FROM x WHERE dt = (SELECT max(dt) FROM x)")
                    .noLeakCheck()
                    .fails(23, "cannot compare DATE with a scalar sub-query");
            assertQuery("SELECT id FROM x WHERE dt > (SELECT max(dt) FROM x)")
                    .noLeakCheck()
                    .fails(23, "cannot compare DATE with a scalar sub-query");
            assertQuery("SELECT id FROM x WHERE u = (SELECT u FROM x WHERE id = 1)")
                    .noLeakCheck()
                    .fails(23, "cannot compare UUID with a scalar sub-query");
            assertQuery("SELECT id FROM x WHERE ip = (SELECT ip FROM x WHERE id = 1)")
                    .noLeakCheck()
                    .fails(23, "cannot compare IPv4 with a scalar sub-query");
            assertQuery("SELECT id FROM x WHERE ip < (SELECT ip FROM x WHERE id = 1)")
                    .noLeakCheck()
                    .fails(23, "cannot compare IPv4 with a scalar sub-query");
            assertQuery("SELECT id FROM x WHERE (SELECT ip FROM x WHERE id = 1) = ip")
                    .noLeakCheck()
                    .fails(57, "cannot compare IPv4 with a scalar sub-query");
            assertQuery("SELECT id FROM x WHERE b = (SELECT true)")
                    .noLeakCheck()
                    .returns("id\n1\n");
        });
    }

    @Test
    public void testErrors() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQuery("SELECT id FROM t WHERE sym > (SELECT sym FROM t)")
                    .noLeakCheck()
                    .fails(30, "scalar sub-query returned more than one row");
            assertQuery("SELECT id FROM t WHERE sym = (SELECT id FROM t WHERE id = 1)")
                    .noLeakCheck()
                    .fails(30, "cannot compare SYMBOL and INT");
            assertQuery("SELECT id FROM t WHERE s < (SELECT 1.5)")
                    .noLeakCheck()
                    .fails(28, "cannot compare STRING and DOUBLE");
            assertQuery("SELECT id FROM t WHERE sym = (SELECT sym, s FROM t WHERE id = 1)")
                    .noLeakCheck()
                    .fails(30, "select must provide exactly one column");
        });
    }

    @Test
    public void testNullAndEmptySubQuery() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQuery("SELECT id FROM t WHERE sym = (SELECT sym FROM t WHERE id = 4)")
                    .noLeakCheck()
                    .returns("id\n4\n");
            assertQuery("SELECT id FROM t WHERE sym = (SELECT sym FROM t WHERE id = 99)")
                    .noLeakCheck()
                    .returns("id\n4\n");
            assertQuery("SELECT id FROM t WHERE s = (SELECT null)")
                    .noLeakCheck()
                    .returns("id\n4\n");
            assertQuery("SELECT id FROM t WHERE vc != (SELECT vc FROM t WHERE id = 99)")
                    .noLeakCheck()
                    .returns("id\n1\n2\n3\n");
            assertQuery("SELECT id FROM t WHERE sym < (SELECT sym FROM t WHERE id = 99)")
                    .noLeakCheck()
                    .returns("id\n");
            assertQuery("SELECT id FROM t WHERE sym > (SELECT sym FROM t WHERE id = 4)")
                    .noLeakCheck()
                    .returns("id\n");
        });
    }

    @Test
    public void testStringAndVarchar() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQuery("SELECT id FROM t WHERE s > (SELECT s FROM t WHERE id = 1)")
                    .noLeakCheck()
                    .returns("id\n2\n3\n");
            assertQuery("SELECT id FROM t WHERE vc = (SELECT vc FROM t WHERE id = 3)")
                    .noLeakCheck()
                    .returns("id\n3\n");
            assertQuery("SELECT id FROM t WHERE s < (SELECT vc FROM t WHERE id = 3)")
                    .noLeakCheck()
                    .returns("id\n1\n2\n");
            assertQuery("SELECT id FROM t WHERE vc >= (SELECT sym FROM t WHERE id = 2)")
                    .noLeakCheck()
                    .returns("id\n2\n3\n");
            assertQuery("SELECT id FROM t WHERE vc <= (SELECT s FROM t WHERE id = 2)")
                    .noLeakCheck()
                    .returns("id\n1\n2\n");
        });
    }

    @Test
    public void testSymbolEqualityAndOrdering() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQuery("SELECT id FROM t WHERE sym = (SELECT sym FROM t WHERE id = 2)")
                    .noLeakCheck()
                    .returns("id\n2\n");
            assertQuery("SELECT id FROM t WHERE sym != (SELECT sym FROM t WHERE id = 2)")
                    .noLeakCheck()
                    .returns("id\n1\n3\n4\n");
            assertQuery("SELECT id FROM t WHERE sym > (SELECT sym FROM t WHERE id = 2)")
                    .noLeakCheck()
                    .returns("id\n3\n");
            assertQuery("SELECT id FROM t WHERE sym >= (SELECT sym FROM t WHERE id = 2)")
                    .noLeakCheck()
                    .returns("id\n2\n3\n");
            assertQuery("SELECT id FROM t WHERE sym < (SELECT sym FROM t WHERE id = 2)")
                    .noLeakCheck()
                    .returns("id\n1\n");
            assertQuery("SELECT id FROM t WHERE sym <= (SELECT sym FROM t WHERE id = 2)")
                    .noLeakCheck()
                    .returns("id\n1\n2\n");
            assertQuery("SELECT id FROM t WHERE (SELECT sym FROM t WHERE id = 2) < sym")
                    .noLeakCheck()
                    .returns("id\n3\n");
            assertQuery("SELECT id FROM t WHERE (SELECT sym FROM t WHERE id = 2) = sym")
                    .noLeakCheck()
                    .returns("id\n2\n");
        });
    }

    private void createRows() throws Exception {
        execute("CREATE TABLE t (id INT, sym SYMBOL, s STRING, vc VARCHAR)");
        execute("""
                INSERT INTO t VALUES
                (1, 'a', 'a', 'a'),
                (2, 'b', 'b', 'b'),
                (3, 'c', 'c', 'c'),
                (4, null, null, null)
                """);
    }
}
