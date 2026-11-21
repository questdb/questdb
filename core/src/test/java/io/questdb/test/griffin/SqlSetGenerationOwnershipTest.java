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
import io.questdb.griffin.TextPlanSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class SqlSetGenerationOwnershipTest extends AbstractCairoTest {
    private static final String[] HASH_OPERATIONS = {"UNION", "INTERSECT", "INTERSECT ALL", "EXCEPT", "EXCEPT ALL"};

    @Test
    public void testHashConstructorFailuresCloseEveryAdoptedInput() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_SMALL_MAP_PAGE_SIZE, 64);
        assertMemoryLeak(() -> {
            try (OwnershipFixture fixture = new OwnershipFixture(engine);
                 SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                fixture.table("a", ColumnType.LONG256, "b", ColumnType.LONG256, "c", ColumnType.LONG256);
                fixture.table("a", ColumnType.LONG256, "b", ColumnType.LONG256, "c", ColumnType.LONG256);
                for (String operation : HASH_OPERATIONS) {
                    fixture.assertCompileFails(compiler, sqlExecutionContext,
                            "owned_table(0) " + operation + " owned_table(1)", "page size is too small to fit a single key");
                    Assert.assertEquals(2, fixture.tableCount());
                }
            }
        });
    }

    @Test
    public void testMergeAbsorptionRetainsLeafOwnersAfterCompilerCloses() throws Exception {
        assertMemoryLeak(() -> {
            try (OwnershipFixture fixture = new OwnershipFixture(engine)) {
                for (int i = 0; i < 3; i++) {
                    fixture.table("ts", ColumnType.TIMESTAMP_MICRO, "k", ColumnType.INT).timestamp(0);
                }
                final RecordCursorFactory retained;
                try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                    retained = compiler.compile(
                            "SELECT k FROM (owned_table(0) UNION ALL owned_table(1) UNION ALL owned_table(2)) ORDER BY ts",
                            sqlExecutionContext
                    ).getRecordCursorFactory();
                }
                try (retained) {
                    final TextPlanSink sink = new TextPlanSink();
                    sink.of(retained, sqlExecutionContext);
                    TestUtils.assertContains(sink.getSink(), "Union All Merge");
                    TestUtils.assertContains(sink.getSink(), "branches: 3");
                    Assert.assertEquals(3, fixture.tableCount());
                    fixture.assertNoneClosed();
                }
                fixture.assertAllClosedOnce();
            }
        });
    }

    @Test
    public void testMetadataFailureClosesAllInputsWithoutMaskingPrimary() throws Exception {
        assertMemoryLeak(() -> {
            try (OwnershipFixture fixture = new OwnershipFixture(engine);
                 SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                fixture.table("k", ColumnType.INT).closeFailure = new RuntimeException("input close");
                fixture.table("k", ColumnType.INT);
                for (String operation : HASH_OPERATIONS) {
                    fixture.assertMetadataFaultSweep(compiler, sqlExecutionContext, "owned_table(0) " + operation + " owned_table(1)");
                }
            }
        });
    }

    @Test
    public void testRetainedFactoriesKeepIndependentSymbolAndNumericLayouts() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE set_owner_a (k INT, s SYMBOL)");
            execute("CREATE TABLE set_owner_b (k INT, s SYMBOL)");
            execute("INSERT INTO set_owner_a VALUES (1,'b'),(2,'a'),(2,'a'),(3,null),(4,'b')");
            execute("INSERT INTO set_owner_b VALUES (2,'a'),(4,'c')");
            final RecordCursorFactory symbols;
            final RecordCursorFactory numbers;
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                symbols = compiler.compile("SELECT k,s FROM set_owner_a EXCEPT ALL SELECT k,s FROM set_owner_b", sqlExecutionContext)
                        .getRecordCursorFactory();
                try {
                    numbers = compiler.compile("SELECT k FROM set_owner_a UNION SELECT k FROM set_owner_b", sqlExecutionContext)
                            .getRecordCursorFactory();
                } catch (Throwable th) {
                    symbols.close();
                    throw th;
                }
            }
            try (symbols; numbers) {
                assertFactory(symbols).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess().sizeMayVary()
                        .returns("k\ts\n1\tb\n3\t\n4\tb\n");
                assertFactory(numbers).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess().sizeMayVary()
                        .returns("k\n1\n2\n3\n4\n");
            }
        });
    }
}
