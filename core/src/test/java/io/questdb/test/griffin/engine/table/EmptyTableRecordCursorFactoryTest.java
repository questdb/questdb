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
                // newSymbolTable must honor the same contract as getSymbolTable: an immutable
                // empty table that a parallel symbol-table consumer can clone without throwing.
                Assert.assertSame(cursor.getSymbolTable(0), cursor.newSymbolTable(0));
                Assert.assertNull(cursor.newSymbolTable(0).valueOf(0));
            }
        }
    }

    @Test
    public void testEmptyMasterFeedsSpliceJoin() throws Exception {
        // Integration counterpart to testCursorIsRandomAccessAndExposesNoRows: a SPLICE join
        // whose master sub-query carries a constant-FALSE WHERE folds the master to an
        // EmptyTableRecordCursorFactory (see the "Empty table" plan node), which remains the
        // splice master because it supports random access. SPLICE is a full outer temporal
        // join, so an empty master pairs every slave row with an all-NULL master (the splice
        // cursor calls recordAt on the empty random-access master, a no-op). This was formerly
        // reached via a master filter that folded to FALSE and got pushed into the master
        // sub-query; that predicate now stays post-join, so this constant-FALSE sub-query is
        // the remaining route that places an empty factory as a splice master. Projecting the
        // master SYMBOL column also drives the empty master's symbol-table API (newSymbolTable
        // and the keyOf null round-trip), which the assertion's symbol thread-safety pass checks.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m (sym SYMBOL, c1 INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO m VALUES ('s2', 100, 2), ('s2', 200, 4)");
            execute("CREATE TABLE s (sym SYMBOL, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO s VALUES ('s2', 10, 1), ('x', 50, 3)");

            // Master folds to Empty table; both slave rows survive with an all-NULL master.
            // ORDER BY wraps the splice output in a sort so the assertion's calculateSize()
            // cross-check has a supporting top factory; the Empty table remains the splice master.
            assertQuery("SELECT a.sym AS e0, a.c1 AS e1, b.v AS e2 " +
                    "FROM (SELECT * FROM m WHERE 1=2) a SPLICE JOIN s b ON (sym) ORDER BY e2")
                    .withPlanContaining("Empty table")
                    .returns("""
                            e0\te1\te2
                            \tnull\t10
                            \tnull\t50
                            """);
        });
    }

    @Test
    public void testEmptySymbolColumnInNullMatches() throws Exception {
        // Regression for EmptySymbolMapReader.keyOf(null) == VALUE_IS_NULL: the empty-master SPLICE
        // gives every row an all-NULL master SYMBOL backed by EmptySymbolMapReader, and "e0 IN (NULL)"
        // matches it via keyOf(null). With the old VALUE_NOT_FOUND key the filter dropped every row.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m (sym SYMBOL, c1 INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO m VALUES ('s2', 100, 2), ('s2', 200, 4)");
            execute("CREATE TABLE s (sym SYMBOL, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO s VALUES ('s2', 10, 1), ('x', 50, 3)");

            assertQuery("SELECT * FROM (" +
                    "SELECT a.sym AS e0, b.v AS e2 FROM (SELECT * FROM m WHERE 1=2) a SPLICE JOIN s b ON (sym)" +
                    ") WHERE e0 IN (NULL) ORDER BY e2")
                    .withPlanContaining("Filter filter: a.sym in [null]")
                    .returns("""
                            e0\te2
                            \t10
                            \t50
                            """);
        });
    }

}
