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
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.ObjList;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Pins the view-as-security-boundary contract on two paths:
 * <ul>
 * <li>CREATE VIEW records a {@code SELECT *} projection as a {@code "*"} wildcard entry in the
 * view's persisted dependency map (collected from the raw, pre-optimisation model - optimise()
 * expands the wildcard into the concrete column list). The wildcard is what keeps the view
 * covering columns added to the base table after the view was created: a caller holding SELECT
 * on the view but not on the base table can still read the new column through the view.</li>
 * <li>ALTER VIEW propagates an authorization failure from
 * {@link io.questdb.cairo.SecurityContext#authorizeAlterView} as a {@link CairoException} that
 * keeps {@link CairoException#isAuthorizationError()}, so the caller reports it as forbidden.
 * The retry loop's CairoException catch rewraps only genuine compile errors as
 * {@link SqlException}.</li>
 * </ul>
 * The default {@link AllowAllSecurityContext} grants everything, so both contracts are exercised
 * with narrow deny-one-permission contexts.
 */
public class ViewAuthorizationTest extends AbstractViewTest {

    @Test
    public void testAlterViewAuthorizationErrorKeepsIdentity() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            createView(VIEW1, "SELECT ts, k, v FROM " + TABLE1, TABLE1);
            final String sqlBefore = getView1DefinitionSql();

            try (SqlExecutionContext denyContext = denyAlterViewContext()) {
                try {
                    execute("ALTER VIEW " + VIEW1 + " AS (SELECT ts, v FROM " + TABLE1 + ")", denyContext);
                    fail("expected ALTER VIEW to be denied");
                } catch (SqlException e) {
                    fail("authorization error was downgraded to SqlException: " + e.getFlyweightMessage());
                } catch (CairoException e) {
                    assertTrue("expected the CairoException to keep isAuthorizationError()", e.isAuthorizationError());
                    TestUtils.assertContains(e.getFlyweightMessage(), "permission denied [view=" + VIEW1 + ']');
                }
            }

            // the denial happened before the definition was replaced
            assertEquals(sqlBefore, getView1DefinitionSql());
        });
    }

    @Test
    public void testSelectStarViewCoversColumnAddedAfterCreation() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            createView(VIEW1, "select * from " + TABLE1, TABLE1);

            final ViewDefinition viewDefinition = getViewDefinition(VIEW1);
            assertNotNull(viewDefinition);
            final LowerCaseCharSequenceHashSet depCols = viewDefinition.getDependencies().get(TABLE1);
            assertNotNull(depCols);
            assertTrue("SELECT * view must record the \"*\" wildcard dependency, got: " + depCols, depCols.contains("*"));

            execute("ALTER TABLE " + TABLE1 + " ADD COLUMN w INT");
            drainWalQueue();

            // The view's wildcard covers the column added after view creation, so a caller holding
            // SELECT on the view but not on the base table reads the new column through the view.
            try (SqlExecutionContext denyContext = denyBaseTableSelectContext()) {
                assertEquals(9, readViewRowCount(denyContext));
            }
        });
    }

    private static SqlExecutionContext denyAlterViewContext() {
        return new SqlExecutionContextImpl(engine, 1).with(
                new DenyAlterViewSecurityContext(),
                bindVariableService,
                null,
                -1,
                null
        );
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

    /**
     * Grants everything except ALTER VIEW, denied with an authorization CairoException.
     */
    private static final class DenyAlterViewSecurityContext extends AllowAllSecurityContext {
        @Override
        public void authorizeAlterView(TableToken tableToken) {
            throw CairoException.authorization().put("permission denied [view=").put(tableToken.getTableName()).put(']');
        }
    }

    /**
     * Grants everything except per-column SELECT on a base table, so view-level authorization
     * passes while any fallback per-column base-table check is rejected.
     */
    private static final class DenyBaseTableSelectSecurityContext extends AllowAllSecurityContext {
        @Override
        public void authorizeSelect(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames) {
            throw CairoException.nonCritical().put("permission denied [table=").put(tableToken.getTableName()).put(']');
        }
    }
}
