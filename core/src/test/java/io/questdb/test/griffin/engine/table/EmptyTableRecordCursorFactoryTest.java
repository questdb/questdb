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

package io.questdb.test.griffin.engine.table;

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.EmptyRecordMetadata;
import io.questdb.griffin.engine.EmptyTableRecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class EmptyTableRecordCursorFactoryTest extends AbstractCairoTest {

    @Test
    public void testCursorIsRandomAccessAndExposesNoRows() throws Exception {
        // Random access on an empty cursor is trivially correct: no row is ever fetched,
        // so recordAt never runs. Splice-style joins rely on this signal to admit an
        // empty side as the master factory rather than failing compilation.
        try (RecordCursorFactory factory = new EmptyTableRecordCursorFactory(EmptyRecordMetadata.INSTANCE)) {
            Assert.assertTrue(factory.recordCursorSupportsRandomAccess());
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Assert.assertFalse(cursor.hasNext());
                Assert.assertEquals(0, cursor.size());
                // recordAt is a no-op on the empty random cursor; it must not throw.
                cursor.recordAt(cursor.getRecord(), 0);
                cursor.toTop();
                Assert.assertFalse(cursor.hasNext());
            }
        }
    }

    @Test
    public void testFoldedFalseFilterAllowsSpliceJoin() throws Exception {
        // The literal filter ((c0 + null))::TIMESTAMP < '2024-...'::TIMESTAMP folds
        // through AddIntFunctionFactory's null short-circuit and the timestamp
        // comparator to constant FALSE. SqlCodeGenerator then substitutes the master
        // side of the splice join with EmptyTableRecordCursorFactory. The splice join
        // must compile because the empty cursor supports random access; the bind-form
        // equivalent compiles via a regular filter, and both forms must agree on the
        // row count produced.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (c0 SHORT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE t0 (c0 INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t1 VALUES (1::SHORT, '2024-01-01T00:00:00.000000Z'), " +
                    "(2::SHORT, '2024-01-02T00:00:00.000000Z'), " +
                    "(3::SHORT, '2024-01-03T00:00:00.000000Z')");
            execute("INSERT INTO t0 VALUES (10, '2024-01-01T00:00:00.000000Z'), " +
                    "(20, '2024-01-02T00:00:00.000000Z'), " +
                    "(30, '2024-01-03T00:00:00.000000Z')");
            drainWalQueue();

            String literalSql = "SELECT (a.c0)::STRING AS e0, true AS e1 " +
                    "FROM t1 a SPLICE JOIN t0 b " +
                    "WHERE ((a.c0 + null))::TIMESTAMP < '2024-03-06T20:54:00.000000Z'::TIMESTAMP";
            int literalRows = countRows(literalSql);

            bindVariableService.clear();
            bindVariableService.setStr("b1", "2024-03-06T20:54:00.000000Z");
            String bindSql = "SELECT (a.c0)::STRING AS e0, true AS e1 " +
                    "FROM t1 a SPLICE JOIN t0 b " +
                    "WHERE ((a.c0 + null))::TIMESTAMP < :b1::TIMESTAMP";
            int bindRows = countRows(bindSql);

            Assert.assertEquals("literal and bind splice forms must agree", bindRows, literalRows);
            // SPLICE JOIN with empty master pairs each slave row with a null master.
            Assert.assertEquals(3, literalRows);
        });
    }

    private int countRows(String sql) throws Exception {
        try (RecordCursorFactory factory = engine.select(sql, sqlExecutionContext)) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                int n = 0;
                while (cursor.hasNext()) {
                    n++;
                }
                return n;
            }
        }
    }
}
