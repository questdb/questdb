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

package io.questdb.test.griffin.engine;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.VirtualRecordNoRowid;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.LimitRecordCursorFactory;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.griffin.engine.functions.constants.LongConstant;
import io.questdb.griffin.engine.join.AsOfJoinNoKeyFastRecordCursorFactory;
import io.questdb.griffin.engine.join.CrossJoinRecordCursorFactory;
import io.questdb.griffin.engine.join.UnnestRecordCursorFactory;
import io.questdb.griffin.engine.table.FilteredRecordCursorFactory;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * A LIMIT that reads the first rows of its base calls {@link RecordCursor#expectLimitedIteration()}
 * on it, and the cursors between the LIMIT and an async filter pass the call on, so that the filter
 * caps the page frame tasks it dispatches ahead of the reader. A join passes it to its master, which
 * it reads as it returns rows. The call changes neither the rows nor the plan, so the tests count it
 * on a base cursor.
 */
public class ExpectLimitedIterationTest extends AbstractCairoTest {

    @Test
    public void testAsOfJoinPassesCallToMaster() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE q (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            final GenericRecordMetadata masterMetadata = new GenericRecordMetadata();
            masterMetadata.add(new TableColumnMetadata("ts", ColumnType.TIMESTAMP));
            masterMetadata.setTimestampIndex(0);
            final CountingFactory master = new CountingFactory(masterMetadata);
            final RecordCursorFactory slave = select("SELECT * FROM q");
            final GenericRecordMetadata joinMetadata = GenericRecordMetadata.copyOf(masterMetadata);
            joinMetadata.add(new TableColumnMetadata("v", ColumnType.INT));
            joinMetadata.add(new TableColumnMetadata("ts1", ColumnType.TIMESTAMP));
            try (RecordCursorFactory join = new AsOfJoinNoKeyFastRecordCursorFactory(configuration, joinMetadata, master, slave, 1, Numbers.LONG_NULL)) {
                try (RecordCursor cursor = join.getCursor(sqlExecutionContext)) {
                    cursor.expectLimitedIteration();
                    Assert.assertEquals(1, master.cursor.callCount);
                }
            }
        });
    }

    @Test
    public void testCrossJoinPassesCallToMasterOnly() throws Exception {
        // the CROSS join reads its slave once per master row, so the slave gets no call
        assertMemoryLeak(() -> {
            final CountingFactory master = new CountingFactory(new GenericRecordMetadata());
            final CountingFactory slave = new CountingFactory(new GenericRecordMetadata());
            try (RecordCursorFactory join = new CrossJoinRecordCursorFactory(new GenericRecordMetadata(), master, slave, 0)) {
                try (RecordCursor cursor = join.getCursor(sqlExecutionContext)) {
                    cursor.expectLimitedIteration();
                    Assert.assertEquals(1, master.cursor.callCount);
                    Assert.assertEquals(0, slave.cursor.callCount);
                }
            }
        });
    }

    @Test
    public void testFilterPassesCallToBase() throws Exception {
        assertMemoryLeak(() -> {
            final CountingFactory base = new CountingFactory(new GenericRecordMetadata());
            try (RecordCursorFactory filter = new FilteredRecordCursorFactory(base, BooleanConstant.TRUE)) {
                try (RecordCursor cursor = filter.getCursor(sqlExecutionContext)) {
                    cursor.expectLimitedIteration();
                    Assert.assertEquals(1, base.cursor.callCount);
                }
            }
        });
    }

    @Test
    public void testLimitCallsForFirstRowsOnly() throws Exception {
        assertMemoryLeak(() -> {
            // LIMIT n and LIMIT lo, hi with 0 <= lo < hi read the first rows of the base
            assertLimitCalls(LongConstant.newInstance(3), null, 1);
            assertLimitCalls(LongConstant.newInstance(Long.MAX_VALUE), null, 1);
            assertLimitCalls(LongConstant.newInstance(1), LongConstant.newInstance(4), 1);
            assertLimitCalls(LongConstant.newInstance(0), LongConstant.newInstance(4), 1);
            // LIMIT 4, 1 reads rows 1 to 3, as LIMIT 1, 4 does
            assertLimitCalls(LongConstant.newInstance(4), LongConstant.newInstance(1), 1);
            // these read no row, or read the base to its end to find the last rows
            assertLimitCalls(LongConstant.newInstance(0), null, 0);
            assertLimitCalls(LongConstant.newInstance(2), LongConstant.newInstance(2), 0);
            assertLimitCalls(LongConstant.newInstance(-3), null, 0);
            assertLimitCalls(LongConstant.newInstance(1), LongConstant.newInstance(-1), 0);
            assertLimitCalls(LongConstant.newInstance(-3), LongConstant.newInstance(-1), 0);
            assertLimitCalls(LongConstant.NULL, null, 0);
        });
    }

    @Test
    public void testLimitCallsOnEveryExecution() throws Exception {
        // the same factory runs with LIMIT 3, LIMIT -3 and LIMIT 3 again, as a prepared statement does
        assertMemoryLeak(() -> {
            final CountingFactory base = new CountingFactory(new GenericRecordMetadata());
            final MutableLimit lo = new MutableLimit();
            try (RecordCursorFactory limit = new LimitRecordCursorFactory(base, lo, null, 0)) {
                lo.value = 3;
                limit.getCursor(sqlExecutionContext).close();
                Assert.assertEquals(1, base.cursor.callCount);
                lo.value = -3;
                limit.getCursor(sqlExecutionContext).close();
                Assert.assertEquals(1, base.cursor.callCount);
                lo.value = 3;
                limit.getCursor(sqlExecutionContext).close();
                Assert.assertEquals(2, base.cursor.callCount);
            }
        });
    }

    @Test
    public void testUnnestPassesCallToBase() throws Exception {
        // UNNEST reads its base as it returns rows, as a join reads its master
        assertMemoryLeak(() -> {
            final CountingFactory base = new CountingFactory(new GenericRecordMetadata());
            final RecordCursorFactory unnest = new UnnestRecordCursorFactory(
                    new GenericRecordMetadata(),
                    base,
                    new ObjList<>(),
                    new ObjList<>(),
                    0,
                    false,
                    new ObjList<>()
            );
            // the LIMIT owns the UNNEST factory
            try (RecordCursorFactory limit = new LimitRecordCursorFactory(unnest, LongConstant.newInstance(3), null, 0)) {
                try (RecordCursor cursor = unnest.getCursor(sqlExecutionContext)) {
                    cursor.expectLimitedIteration();
                    Assert.assertEquals(1, base.cursor.callCount);
                }
                // a LIMIT that reads the first rows announces them through UNNEST
                try (RecordCursor ignore = limit.getCursor(sqlExecutionContext)) {
                    Assert.assertEquals(2, base.cursor.callCount);
                }
            }
        });
    }

    private static void assertLimitCalls(Function lo, Function hi, int expectedCallCount) throws Exception {
        final CountingFactory base = new CountingFactory(new GenericRecordMetadata());
        try (RecordCursorFactory limit = new LimitRecordCursorFactory(base, lo, hi, 0)) {
            try (RecordCursor ignore = limit.getCursor(sqlExecutionContext)) {
                Assert.assertEquals(expectedCallCount, base.cursor.callCount);
            }
        }
    }

    // An empty cursor that counts expectLimitedIteration() calls
    private static class CountingCursor implements NoRandomAccessRecordCursor {
        private final Record record = new VirtualRecordNoRowid(new ObjList<>());
        private int callCount;

        @Override
        public void close() {
        }

        @Override
        public void expectLimitedIteration() {
            callCount++;
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public long size() {
            return -1;
        }

        @Override
        public void toTop() {
        }
    }

    private static class CountingFactory extends AbstractRecordCursorFactory {
        private final CountingCursor cursor = new CountingCursor();

        private CountingFactory(RecordMetadata metadata) {
            super(metadata);
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) {
            return cursor;
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return false;
        }
    }

    // a LIMIT bound whose value changes between executions, as a bind variable does
    private static class MutableLimit extends LongFunction {
        private long value;

        @Override
        public long getLong(Record rec) {
            return value;
        }
    }
}
