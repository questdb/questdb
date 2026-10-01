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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.columns.LongColumn;
import io.questdb.griffin.engine.functions.groupby.InterpolationGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.SumLongGroupByFunction;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class SqlSampleByGenerationOwnershipTest extends AbstractCairoTest {
    @Test
    public void testConstructorFailureConsumesInputsAndPreservesPrimary() throws Exception {
        assertSampleBy((fixture, compiler) -> {
            fixture.table("ts", ColumnType.TIMESTAMP_MICRO, "k", ColumnType.INT, "v", ColumnType.LONG)
                    .timestamp(0).closeFailure = new RuntimeException("input close");
            fixture.assertMetadataFaultSweep(compiler, sqlExecutionContext,
                    "SELECT ts, sum(v) FROM owned_table(0) SAMPLE BY 1h FILL(PREV) ALIGN TO FIRST OBSERVATION");
            fixture.assertMetadataFaultSweep(compiler, sqlExecutionContext,
                    "SELECT ts, k, sum(v) FROM owned_table(0) SAMPLE BY 1h FILL(LINEAR) ALIGN TO FIRST OBSERVATION");
        });
    }

    @Test
    public void testInterpolationForwardsAggregateLifecycle() throws Exception {
        assertMemoryLeak(() -> {
            final TrackingAggregate aggregate = new TrackingAggregate();
            try (InterpolationGroupByFunction interpolation = InterpolationGroupByFunction.newInstance(aggregate)) {
                interpolation.setAllocator(null);
                for (int i = 0; i < 2; i++) {
                    interpolation.init(null, sqlExecutionContext);
                    interpolation.toTop();
                    interpolation.cursorClosed();
                    interpolation.clear();
                }
            }
            Assert.assertEquals(1, aggregate.allocatorCount);
            Assert.assertEquals(2, aggregate.initCount);
            Assert.assertEquals(2, aggregate.toTopCount);
            Assert.assertEquals(2, aggregate.cursorClosedCount);
            Assert.assertEquals(2, aggregate.clearCount);
            Assert.assertEquals(1, aggregate.closeCount);
        });
    }

    @Test
    public void testKeyedRangeValidationClosesAliasedOwners() throws Exception {
        assertSampleBy((fixture, compiler) -> {
            fixture.table("ts", ColumnType.TIMESTAMP_MICRO, "k", ColumnType.INT, "v", ColumnType.LONG)
                    .timestamp(0).closeFailure = new RuntimeException("input close");
            fixture.assertCompileFails(compiler, sqlExecutionContext,
                    "SELECT ts, k, sum(v) FROM owned_table(0) SAMPLE BY (1+0) h FROM '2024-01-01' TO '2024-01-02' FILL(PREV)",
                    "FROM-TO intervals are not supported for keyed SAMPLE BY queries");
        });
    }

    @Test
    public void testMixedLinearFillOwnsWrappedAggregateOnSuccessAndFailure() throws Exception {
        assertSampleBy((fixture, compiler) -> {
            fixture.table("ts", ColumnType.TIMESTAMP_MICRO, "v", ColumnType.LONG).timestamp(0);
            try (RecordCursorFactory ignored = compiler.compile(
                    "SELECT ts, sum(v) a, max(v) b FROM owned_table(0) SAMPLE BY 1h FILL(LINEAR, PREV) ALIGN TO FIRST OBSERVATION",
                    sqlExecutionContext
            ).getRecordCursorFactory()) {
                fixture.assertNoneClosed();
            }
            fixture.assertAllClosedOnce();
            fixture.assertCompileFails(compiler, sqlExecutionContext,
                    "SELECT ts, sum(v) a, max(v) b FROM owned_table(0) SAMPLE BY 1h FILL(LINEAR, invalid) ALIGN TO FIRST OBSERVATION",
                    "invalid fill value: invalid");
        });
    }

    @Test
    public void testTypedEntryRequiresActualTimestampAndClosesFailedInput() throws Exception {
        assertSampleBy((fixture, compiler) -> {
            final OwnershipFixture.TableSpec backward = fixture.table("ts", ColumnType.TIMESTAMP_MICRO, "v", ColumnType.LONG).timestamp(0);
            backward.scanDirection = RecordCursorFactory.SCAN_DIRECTION_BACKWARD;
            backward.closeFailure = new RuntimeException("input close");
            fixture.assertCompileFails(compiler, sqlExecutionContext,
                    "SELECT ts, sum(v) FROM owned_table(0) SAMPLE BY 1h FILL(PREV) ALIGN TO FIRST OBSERVATION",
                    "base query does not provide ASC order over designated TIMESTAMP column");
        });
    }

    private static void assertSampleBy(SampleByAssertion assertion) throws Exception {
        assertMemoryLeak(() -> {
            try (OwnershipFixture fixture = new OwnershipFixture(engine);
                 SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                assertion.run(fixture, compiler);
            }
        });
    }

    @FunctionalInterface
    private interface SampleByAssertion {
        void run(OwnershipFixture fixture, SqlCompilerImpl compiler) throws Exception;
    }

    private static final class TrackingAggregate extends SumLongGroupByFunction {
        private int allocatorCount;
        private int clearCount;
        private int closeCount;
        private int cursorClosedCount;
        private int initCount;
        private int toTopCount;

        private TrackingAggregate() {
            super(LongColumn.newInstance(1));
        }

        @Override
        public void clear() {
            clearCount++;
        }

        @Override
        public void close() {
            Assert.assertEquals(1, ++closeCount);
        }

        @Override
        public void cursorClosed() {
            cursorClosedCount++;
        }

        @Override
        public void init(SymbolTableSource source, SqlExecutionContext context) {
            initCount++;
        }

        @Override
        public void setAllocator(GroupByAllocator allocator) {
            allocatorCount++;
        }

        @Override
        public void toTop() {
            toTopCount++;
        }
    }
}
