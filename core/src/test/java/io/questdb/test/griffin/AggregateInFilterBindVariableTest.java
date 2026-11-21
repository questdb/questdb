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

package io.questdb.test.griffin;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * A bare aggregate used as a WHERE predicate reports itself runtime-constant when its only
 * argument is a bind variable, because {@code UnaryFunction.isRuntimeConstant()} forwards to
 * the argument. The predicate gate then evaluated the aggregate once with a null record and
 * crashed with a NullPointerException, while the same aggregate over a column argument runs on
 * the per-row path. A bind variable carries a runtime value, like a column and unlike a
 * literal (which the gate folds), so these tests pin each bind-variable form to its column
 * form.
 */
public class AggregateInFilterBindVariableTest extends AbstractCairoTest {

    @Test
    public void testBoolAndBindVariableMatchesColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE n (v LONG, b BOOLEAN, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO n VALUES (10, false, 0), (20, true, 1), (5, false, 2)");
            bindVariableService.clear();
            bindVariableService.setBoolean(0, true);
            assertSqlCursors("SELECT * FROM n WHERE bool_and(b)", "SELECT * FROM n WHERE bool_and($1)");
            assertSqlCursors("SELECT * FROM n WHERE bool_or(b)", "SELECT * FROM n WHERE bool_or($1)");
        });
    }

    @Test
    public void testIsOrderedCharBindVariableDoesNotCrash() throws Exception {
        // Exact repro from the issue: a CHAR bind variable passed to isOrdered() in a WHERE clause.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE n (c CHAR, b BOOLEAN, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO n VALUES (null, false, 0)");
            bindVariableService.clear();
            bindVariableService.setChar(0, 'a');
            assertSqlCursors("SELECT * FROM n WHERE isOrdered(c)", "SELECT * FROM n WHERE isOrdered($1)");
        });
    }

    @Test
    public void testIsOrderedLongBindVariableMatchesColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE n (v LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO n VALUES (10, 0), (20, 1), (5, 2)");
            bindVariableService.clear();
            bindVariableService.setLong(0, 7);
            assertSqlCursors("SELECT * FROM n WHERE isOrdered(v)", "SELECT * FROM n WHERE isOrdered($1)");
        });
    }

    @Test
    public void testIsOrderedBindVariableInJoinFilterMatchesColumn() throws Exception {
        // The post-join filter gate takes the same path, so a join also crashed.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE b (w LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO a VALUES (1, 0), (2, 1)");
            execute("INSERT INTO b VALUES (1, 0), (2, 1)");
            bindVariableService.clear();
            bindVariableService.setLong(0, 7);
            assertSqlCursors(
                    "SELECT * FROM a CROSS JOIN b WHERE isOrdered(a.v)",
                    "SELECT * FROM a CROSS JOIN b WHERE isOrdered($1)"
            );
        });
    }

    @Test
    public void testNestedAggregateStillRejected() throws Exception {
        // A nested aggregate stays rejected, bind variable or not.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE n (v LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO n VALUES (10, 0)");
            bindVariableService.clear();
            bindVariableService.setLong(0, 7);
            assertException("SELECT * FROM n WHERE isOrdered($1) > 0", 22, "Aggregate function cannot be passed as an argument");
        });
    }
}
