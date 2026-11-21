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
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class SqlFillGenerationOwnershipTest extends AbstractCairoTest {
    private static final String QUERY = "SELECT ts, k, sum(v) s, first(p) p FROM owned_table(0) SAMPLE BY 1h FILL(42, PREV)";
    private static final String[] SORT_FACTORIES = {
            "EncodedSortLightRecordCursorFactory",
            "EncodedSortRecordCursorFactory",
            "SortedLightRecordCursorFactory",
            "SortedRecordCursorFactory"
    };
    private static final String[] SORT_STRATEGIES = {"light_encoded", "full_encoded", "light_recordchain", "full_recordchain"};

    @Test
    public void testMetadataFailureKeepsSingleOwnerAndPrimaryExceptionForEverySortStrategy() throws Exception {
        for (String strategy : SORT_STRATEGIES) {
            node1.setProperty(PropertyKey.CAIRO_SQL_SAMPLEBY_FILL_SORT_STRATEGY, strategy);
            assertFill((fixture, compiler) -> {
                createTable(fixture, true);
                fixture.assertMetadataFaultSweep(compiler, sqlExecutionContext, QUERY);
            });
        }
    }

    @Test
    public void testNeverOpenedFactoryOwnsInputsForEverySortStrategy() throws Exception {
        for (int strategy = 0; strategy < SORT_STRATEGIES.length; strategy++) {
            node1.setProperty(PropertyKey.CAIRO_SQL_SAMPLEBY_FILL_SORT_STRATEGY, SORT_STRATEGIES[strategy]);
            final String expectedSort = SORT_FACTORIES[strategy];
            assertFill((fixture, compiler) -> {
                createTable(fixture, false);
                try (RecordCursorFactory factory = compiler.compile(QUERY, sqlExecutionContext).getRecordCursorFactory()) {
                    fixture.assertNoneClosed();
                    Assert.assertEquals(0, factory.getMetadata().getTimestampIndex());
                    Assert.assertEquals(RecordCursorFactory.SCAN_DIRECTION_FORWARD, factory.getScanDirection());
                    RecordCursorFactory sort = factory;
                    while (sort != null && !expectedSort.equals(sort.getClass().getSimpleName())) {
                        sort = sort.getBaseFactory();
                    }
                    Assert.assertNotNull(expectedSort, sort);
                }
                fixture.assertAllClosedOnce();
            });
        }
    }

    @Test
    public void testTypedValidationClosesInputBeforePhysicalTransfer() throws Exception {
        assertFill((fixture, compiler) -> {
            fixture.table("ts", ColumnType.TIMESTAMP_MICRO, "b", ColumnType.BOOLEAN).timestamp(0)
                    .closeFailure = new RuntimeException("input close");
            final Throwable thrown = fixture.assertCompileFails(compiler, sqlExecutionContext,
                    "SELECT ts, last(b) b FROM owned_table(0) SAMPLE BY 1h FILL(NULL)",
                    "fill value of type NULL cannot fill column of type BOOLEAN");
            Assert.assertTrue(thrown.getSuppressed().length > 0);
        });
    }

    private static void assertFill(FillAssertion assertion) throws Exception {
        assertMemoryLeak(() -> {
            try (OwnershipFixture fixture = new OwnershipFixture(engine);
                 SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                assertion.run(fixture, compiler);
            }
        });
    }

    private static void createTable(OwnershipFixture fixture, boolean isInputCloseFailing) {
        final OwnershipFixture.TableSpec spec = fixture.table(
                "ts", ColumnType.TIMESTAMP_MICRO, "k", ColumnType.INT, "v", ColumnType.LONG, "p", ColumnType.LONG
        ).timestamp(0);
        if (isInputCloseFailing) {
            spec.closeFailure = new RuntimeException("input close");
        }
    }

    @FunctionalInterface
    private interface FillAssertion {
        void run(OwnershipFixture fixture, SqlCompilerImpl compiler) throws Exception;
    }
}
