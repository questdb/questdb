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

package io.questdb.test.cairo.covering;

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.engine.table.CoveringIndexRecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * What a covering factory carrying a backup advertises as its scan direction.
 * <p>
 * The two delegates need not agree. The covering scan is row-id ordered, so on its own it is
 * ascending by designated timestamp; the WHERE IN-list backup is a
 * {@code FilterOnValuesRecordCursorFactory}, which under {@code ORDER_BY_INVARIANT} drains one
 * per-key cursor after another and so emits by key, not by row id. Codegen elides an ORDER BY on
 * the designated timestamp from whatever the factory answers here, at compile time, before either
 * delegate is chosen -- so answering FORWARD for the pair would silently return key-grouped rows
 * whenever the backup ran.
 */
public class CoveringIndexBackupScanDirectionTest extends AbstractCairoTest {

    @Test
    public void testBackupKeepingForwardOrderIsStillForward() throws Exception {
        // The control. ORDER BY ts leaves the backup on its heap cursor, which merges the per-key
        // streams into row-id order, so both delegates are FORWARD and the pair keeps it. Without
        // this case a getScanDirection() hard-wired to OTHER would pass the case below.
        assertMemoryLeak(() -> {
            createTopTable("t_sd_fwd");
            assertScanDirection(
                    "SELECT ts, sym, val FROM t_sd_fwd WHERE sym IN (null, 'A') ORDER BY ts",
                    RecordCursorFactory.SCAN_DIRECTION_FORWARD
            );
        });
    }

    @Test
    public void testBackupOrderingByKeyMakesThePairUnordered() throws Exception {
        // ORDER BY on the key column, not the timestamp: the IN-list backup takes its sequential
        // cursor and emits per key. The covering delegate would still be FORWARD, so the pair has
        // to answer OTHER.
        assertMemoryLeak(() -> {
            createTopTable("t_sd_other");
            assertScanDirection(
                    "SELECT ts, sym, val FROM t_sd_other WHERE sym IN (null, 'A') ORDER BY sym",
                    RecordCursorFactory.SCAN_DIRECTION_OTHER
            );
        });
    }

    /**
     * Compiles {@code sql} and asserts the scan direction of the {@link CoveringIndexRecordCursorFactory}
     * inside it. The factory is found by walking the base chain rather than cast from the top: an
     * ORDER BY puts a sort above it, and the plan shape is not what this test is pinning.
     */
    private static void assertScanDirection(String sql, int expectedScanDirection) throws Exception {
        try (
                SqlCompiler compiler = engine.getSqlCompiler();
                RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()
        ) {
            CoveringIndexRecordCursorFactory covering = null;
            for (RecordCursorFactory f = factory; f != null; f = f.getBaseFactory()) {
                if (f instanceof CoveringIndexRecordCursorFactory c) {
                    covering = c;
                    break;
                }
            }
            Assert.assertNotNull("no covering factory in the plan for: " + sql, covering);
            Assert.assertEquals(
                    "the covering factory and its backup disagree on row order, so the pair must"
                            + " advertise none; plan: " + sql,
                    expectedScanDirection,
                    covering.getScanDirection()
            );
        }
    }

    /**
     * Two rows written before {@code sym} exists carry a column top and match the NULL key
     * implicitly, so the IN-list below is null-capable and the factory is built with a backup.
     */
    private static void createTopTable(String name) throws Exception {
        execute("CREATE TABLE " + name + " (ts TIMESTAMP, val DOUBLE)"
                + " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        execute("""
                INSERT INTO %s VALUES
                ('2024-01-01T00:00:00', 10.0),
                ('2024-01-01T01:00:00', 20.0)
                """.formatted(name));
        execute("ALTER TABLE " + name + " ADD COLUMN sym SYMBOL");
        execute("INSERT INTO " + name + " VALUES ('2024-01-01T02:00:00', 30.0, 'A')");
        execute("ALTER TABLE " + name + " ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (ts, val)");
        engine.releaseAllWriters();
        engine.releaseAllReaders();
    }
}
