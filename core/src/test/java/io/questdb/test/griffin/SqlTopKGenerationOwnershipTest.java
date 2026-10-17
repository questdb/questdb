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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.orderby.RecordComparatorCompiler;
import io.questdb.griffin.engine.table.AsyncTopKRecordCursorFactory;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class SqlTopKGenerationOwnershipTest extends AbstractCairoTest {
    @Test
    public void testConstructorFailureConsumesInputsBeforeAndAfterFilterContext() throws Exception {
        assertMemoryLeak(() -> {
            for (int stage = 0; stage < 4; stage++) {
                assertConstructorFailure(stage, false);
            }
        });
    }

    @Test
    public void testConstructorFailurePreservesPrimaryWhenOwnersFailToClose() throws Exception {
        assertMemoryLeak(() -> {
            for (int stage = 0; stage < 4; stage++) {
                assertConstructorFailure(stage, true);
            }
        });
    }

    private void assertConstructorFailure(int stage, boolean isCloseFailure) throws Exception {
        final RuntimeException failure = new RuntimeException("top-K construction " + stage);
        final RuntimeException inputCloseFailure = new RuntimeException("top-K input close");
        final RuntimeException filterCloseFailure = new RuntimeException("top-K filter close");
        final CairoConfiguration configuration = new DefaultCairoConfiguration(root) {
            @Override
            public int getPageFrameReduceRowIdListCapacity() {
                if (stage == 1) {
                    throw failure;
                }
                return super.getPageFrameReduceRowIdListCapacity();
            }

            @Override
            public long getSqlParallelWorkStealingSpinTimeout() {
                if (stage == 3) {
                    throw failure;
                }
                return super.getSqlParallelWorkStealingSpinTimeout();
            }

            @Override
            public long getSqlParquetCacheMemorySize() {
                if (stage == 0) {
                    throw failure;
                }
                return super.getSqlParquetCacheMemorySize();
            }

            @Override
            public boolean isSqlOrderBySortEnabled() {
                if (stage == 2) {
                    throw failure;
                }
                return true;
            }
        };
        final TrackingFactory base = new TrackingFactory();
        final TrackingFunction owner = new TrackingFunction();
        final TrackingFunction worker = new TrackingFunction();
        final TrackingFunction bindVariable = new TrackingFunction();
        final TrackingCompiledFilter compiled = new TrackingCompiledFilter();
        final ObjList<Function> workers = new ObjList<>();
        workers.add(worker);
        final ObjList<Function> bindVariables = new ObjList<>();
        bindVariables.add(bindVariable);
        final ListColumnFilter keys = new ListColumnFilter();
        keys.add(1);
        base.closeFailure = isCloseFailure ? inputCloseFailure : null;
        owner.closeFailure = isCloseFailure ? filterCloseFailure : null;
        try {
            final RuntimeException actual = Assert.assertThrows(RuntimeException.class, () ->
                    new AsyncTopKRecordCursorFactory(engine, configuration, engine.getMessageBus(),
                            base.getMetadata(), base, owner, null, workers, compiled, null, bindVariables,
                            new RecordComparatorCompiler(new BytecodeAssembler()), keys, base.getMetadata(), 3, 1));
            Assert.assertSame(failure, actual);
            Assert.assertEquals(1, base.closeCount);
            Assert.assertEquals(1, owner.closeCount);
            Assert.assertEquals(1, worker.closeCount);
            Assert.assertEquals(1, bindVariable.closeCount);
            Assert.assertEquals(1, compiled.closeCount);
            if (isCloseFailure) {
                Assert.assertEquals(2, actual.getSuppressed().length);
                Assert.assertSame(filterCloseFailure, actual.getSuppressed()[0]);
                Assert.assertSame(inputCloseFailure, actual.getSuppressed()[1]);
            }
        } finally {
            owner.closeFailure = null;
            if (owner.closeCount == 0) {
                owner.close();
            }
            if (worker.closeCount == 0) {
                worker.close();
            }
            if (bindVariable.closeCount == 0) {
                bindVariable.close();
            }
        }
    }

    private static class TrackingCompiledFilter extends CompiledFilter {
        private int closeCount;

        @Override
        public void close() {
            Assert.assertEquals(1, ++closeCount);
            super.close();
        }
    }

    private static class TrackingFactory implements RecordCursorFactory {
        private final GenericRecordMetadata metadata = new GenericRecordMetadata();
        private int closeCount;
        private RuntimeException closeFailure;

        private TrackingFactory() {
            metadata.add(new TableColumnMetadata("id", ColumnType.INT));
        }

        @Override
        public void close() {
            Assert.assertEquals(1, ++closeCount);
            if (closeFailure != null) {
                throw closeFailure;
            }
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) {
            throw new AssertionError("construction must not open input");
        }

        @Override
        public RecordMetadata getMetadata() {
            return metadata;
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return true;
        }
    }

    private static class TrackingFunction extends BooleanFunction {
        private final DirectLongList memory = new DirectLongList(8, MemoryTag.NATIVE_DEFAULT);
        private int closeCount;
        private RuntimeException closeFailure;

        @Override
        public void close() {
            Assert.assertEquals(1, ++closeCount);
            memory.close();
            if (closeFailure != null) {
                throw closeFailure;
            }
        }

        @Override
        public boolean getBool(Record rec) {
            throw new AssertionError("construction must not evaluate filter");
        }
    }
}
