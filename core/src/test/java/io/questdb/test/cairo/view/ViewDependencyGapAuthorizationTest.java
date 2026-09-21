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

package io.questdb.test.cairo.view;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.view.ViewDefinition;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.ObjList;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * Exercises the fail-safe branch in {@code AbstractPartitionFrameCursorFactory.authorizeSelect}.
 * <p>
 * Reading a base table through a view looks the table up in the view's persisted dependency
 * map to learn which columns the view covers. The map can miss an entry for a base table the
 * scan actually touches - a residual dependency-collector gap, or a base table renamed after
 * the view was created. The lookup then returns {@code null}.
 * <p>
 * The guard must treat that {@code null} as "this base table is not covered by the view" and
 * fall back to requiring explicit per-column SELECT on the base table, rather than dereferencing
 * the null set and aborting the read with a {@link NullPointerException}. These tests simulate
 * the missing entry by removing it from the live dependency map after the view is created.
 */
public class ViewDependencyGapAuthorizationTest extends AbstractViewTest {

    @Test
    public void testMissingDependencyEntryDoesNotThrowUnderAllowAll() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            createView(VIEW1, "SELECT ts, k, v FROM " + TABLE1, TABLE1);
            removeDependencyEntry(VIEW1, TABLE1);

            // Allow-all caller: the fallback requires per-column SELECT on the base table, which is
            // granted, so the read completes. Before the guard, the null lookup tripped an NPE here.
            assertEquals(9, readViewRowCount(sqlExecutionContext));
        });
    }

    @Test
    public void testMissingDependencyEntryRequiresExplicitBaseTableSelect() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            createView(VIEW1, "SELECT ts, k, v FROM " + TABLE1, TABLE1);
            removeDependencyEntry(VIEW1, TABLE1);

            // View-level SELECT is granted but per-column base-table SELECT is denied. With the entry
            // missing, the fallback demands the base-table permission and the read is denied (not an NPE).
            try (SqlExecutionContext denyContext = denyBaseTableSelectContext()) {
                try (RecordCursorFactory factory = engine.select("SELECT * FROM " + VIEW1, denyContext)) {
                    try (RecordCursor cursor = factory.getCursor(denyContext)) {
                        //noinspection StatementWithEmptyBody
                        while (cursor.hasNext()) {
                            // drain
                        }
                        fail("expected the read to be denied");
                    }
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "permission denied");
                }
            }
        });
    }

    @Test
    public void testPresentDependencyEntrySuppressesBaseTableSelectCheck() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            createView(VIEW1, "SELECT ts, k, v FROM " + TABLE1, TABLE1);
            // No tampering: the dependency entry covers ts, k and v, so the scan never reaches the
            // per-column base-table check. The deny-base-table caller therefore still reads cleanly.

            try (SqlExecutionContext denyContext = denyBaseTableSelectContext()) {
                assertEquals(9, readViewRowCount(denyContext));
            }
        });
    }

    private static SqlExecutionContext denyBaseTableSelectContext() {
        return new SqlExecutionContextImpl(engine, 1).with(
                new DenyBaseTableSelectSecurityContext(),
                bindVariableService,
                null,
                -1,
                null
        );
    }

    private static long readViewRowCount(SqlExecutionContext context) throws SqlException {
        long count = 0;
        try (RecordCursorFactory factory = engine.select("SELECT * FROM " + VIEW1, context)) {
            try (RecordCursor cursor = factory.getCursor(context)) {
                while (cursor.hasNext()) {
                    count++;
                }
            }
        }
        return count;
    }

    private static void removeDependencyEntry(String viewName, String baseTableName) {
        final ViewDefinition viewDefinition = getViewDefinition(viewName);
        // guard the test itself: the entry must be present before we simulate the gap by removing it
        assertNotNull(viewDefinition.getDependencies().get(baseTableName));
        viewDefinition.getDependencies().remove(baseTableName);
    }

    /**
     * Grants everything except per-column SELECT on a base table, so the view-level authorization
     * still passes while the fallback's explicit per-column base-table check is rejected.
     */
    private static final class DenyBaseTableSelectSecurityContext extends AllowAllSecurityContext {
        @Override
        public void authorizeSelect(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames) {
            throw CairoException.nonCritical().put("permission denied [table=").put(tableToken.getTableName()).put(']');
        }
    }
}
