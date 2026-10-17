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

package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.TextPlanSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class SqlSortGenerationOwnershipTest extends AbstractCairoTest {
    @Test
    public void testBoundedSortCloseFailureStillReleasesAdviceOnce() throws Exception {
        assertMemoryLeak(() -> {
            setLimits();
            for (int mode = 0; mode < 2; mode++) {
                node1.setProperty(PropertyKey.CAIRO_SQL_ORDER_BY_SORT_ENABLED, mode == 0);
                try (OwnershipFixture fixture = new OwnershipFixture(engine);
                     SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                    final OwnershipFixture.TableSpec spec = fixture.table("id", ColumnType.INT);
                    spec.isRandomAccess = true;
                    final RuntimeException baseFailure = new RuntimeException("sort input close");
                    final RuntimeException limitFailure = new RuntimeException("sort limit close");
                    spec.closeFailure = baseFailure;
                    fixture.failLongClose(limitFailure);
                    final RecordCursorFactory factory = compiler.compile(
                            "SELECT * FROM owned_table(0) ORDER BY id LIMIT owned_long($1), owned_long($2)", sqlExecutionContext
                    ).getRecordCursorFactory();
                    Assert.assertTrue(factory.implementsLimit());
                    final RuntimeException actual = Assert.assertThrows(RuntimeException.class, factory::close);
                    Assert.assertSame(baseFailure, actual);
                    Assert.assertTrue(OwnershipFixture.hasSuppressed(actual, limitFailure));
                    factory.close();
                    fixture.assertAllClosedOnce();
                }
            }
        });
    }

    @Test
    public void testComparatorConstructorFailurePreservesPrimaryWhenInputCloseFails() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_ORDER_BY_SORT_ENABLED, false);
        assertConstructorFailures(true);
    }

    @Test
    public void testEncodedAndComparatorConstructorFailuresCloseInputOnce() throws Exception {
        for (int mode = 0; mode < 2; mode++) {
            node1.setProperty(PropertyKey.CAIRO_SQL_ORDER_BY_SORT_ENABLED, mode == 0);
            assertConstructorFailures(false);
        }
    }

    @Test
    public void testSortOwnsLimitFunctions() throws Exception {
        assertMemoryLeak(() -> {
            setLimits();
            for (int mode = 0; mode < 6; mode++) {
                node1.setProperty(PropertyKey.CAIRO_SQL_ORDER_BY_SORT_ENABLED, mode < 3);
                final boolean isLimited = mode % 3 == 2;
                try (OwnershipFixture fixture = new OwnershipFixture(engine);
                     SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                    fixture.table("id", ColumnType.INT).isRandomAccess = mode % 3 != 1;
                    final String sql = "SELECT * FROM owned_table(0) ORDER BY id" + (isLimited ? " LIMIT owned_long($1), owned_long($2)" : "");
                    try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                        final TextPlanSink plan = new TextPlanSink();
                        plan.of(factory, sqlExecutionContext);
                        TestUtils.assertContains(plan.getSink(), "keys: [id]");
                        Assert.assertEquals(isLimited, factory.implementsLimit());
                        Assert.assertEquals(isLimited ? 2 : 0, fixture.longCount());
                        fixture.assertNoneClosed();
                    }
                    fixture.assertAllClosedOnce();
                }
            }
        });
    }

    private static void setLimits() throws SqlException {
        bindVariableService.clear();
        bindVariableService.setLong(0, 1);
        bindVariableService.setLong(1, 3);
    }

    private static void assertConstructorFailures(boolean isInputCloseFailing) throws Exception {
        assertMemoryLeak(() -> {
            try (OwnershipFixture fixture = new OwnershipFixture(engine);
                 SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                for (int i = 0; i < 2; i++) {
                    final OwnershipFixture.TableSpec spec = fixture.table("id", ColumnType.INT);
                    spec.isRandomAccess = i == 0;
                    if (isInputCloseFailing) {
                        spec.closeFailure = new RuntimeException("sort input close");
                    }
                    fixture.assertMetadataFaultSweep(compiler, sqlExecutionContext, "SELECT * FROM owned_table(" + i + ") ORDER BY id");
                }
            }
        });
    }
}
