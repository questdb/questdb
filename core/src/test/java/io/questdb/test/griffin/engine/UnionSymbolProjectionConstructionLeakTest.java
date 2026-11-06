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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.codegen.SqlCodeGenerator;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.union.UnionSymbolCastRecordCursorFactory;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class UnionSymbolProjectionConstructionLeakTest extends AbstractCairoTest {

    @Test
    public void testConstructionFailureAfterFunctionRegistrationFreesUnion() throws Exception {
        assertMemoryLeak(() -> {
            final RuntimeException failure = new RuntimeException("union metadata failure");
            final TrackingUnionFactory union = new TrackingUnionFactory(new FailingMetadata(failure), false);
            try {
                SqlCodeGenerator.resymboliseUnion(union, symbolColumns(0, 1));
                Assert.fail("expected union symbol projection failure");
            } catch (RuntimeException e) {
                Assert.assertSame(failure, e);
            }
            Assert.assertEquals(1, union.closeCount);
        });
    }

    @Test
    public void testConstructionSucceedsAndOwnsUnion() throws Exception {
        assertMemoryLeak(() -> {
            final TrackingUnionFactory union = new TrackingUnionFactory(metadata(), false);
            try (RecordCursorFactory factory = SqlCodeGenerator.resymboliseUnion(union, symbolColumns(0, 1))) {
                Assert.assertTrue(factory instanceof UnionSymbolCastRecordCursorFactory);
                Assert.assertEquals(ColumnType.SYMBOL, factory.getMetadata().getColumnType(0));
                Assert.assertEquals(ColumnType.SYMBOL, factory.getMetadata().getColumnType(1));
                Assert.assertEquals(0, union.closeCount);
            }
            Assert.assertEquals(1, union.closeCount);
        });
    }

    @Test
    public void testParallelBaseFailureFreesUnion() throws Exception {
        assertMemoryLeak(() -> {
            final TrackingUnionFactory union = new TrackingUnionFactory(metadata(), true);
            try {
                SqlCodeGenerator.resymboliseUnion(union, symbolColumns(0, 1));
                Assert.fail("expected union symbol projection failure");
            } catch (AssertionError e) {
                TestUtils.assertContains(e.getMessage(), "union symbol projection requires a serial base cursor");
            }
            Assert.assertEquals(1, union.closeCount);
        });
    }

    private static GenericRecordMetadata addColumns(GenericRecordMetadata metadata) {
        metadata.add(new TableColumnMetadata("a", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("b", ColumnType.STRING));
        return metadata;
    }

    private static GenericRecordMetadata metadata() {
        return addColumns(new GenericRecordMetadata());
    }

    private static IntList symbolColumns(int first, int second) {
        final IntList columns = new IntList();
        columns.add(first);
        columns.add(second);
        return columns;
    }

    private static class FailingMetadata extends GenericRecordMetadata {
        private final RuntimeException failure;

        private FailingMetadata(RuntimeException failure) {
            this.failure = failure;
            addColumns(this);
        }

        @Override
        public String getColumnName(int columnIndex) {
            if (columnIndex == 1) {
                throw failure;
            }
            return super.getColumnName(columnIndex);
        }
    }

    private static class TrackingUnionFactory implements RecordCursorFactory {
        private static final long ALLOC_SIZE = 64;
        private final boolean isParallel;
        private final RecordMetadata metadata;
        private long address = Unsafe.malloc(ALLOC_SIZE, MemoryTag.NATIVE_DEFAULT);
        private int closeCount;

        private TrackingUnionFactory(RecordMetadata metadata, boolean isParallel) {
            this.metadata = metadata;
            this.isParallel = isParallel;
        }

        @Override
        public void close() {
            closeCount++;
            if (address != 0) {
                address = Unsafe.free(address, ALLOC_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) {
            throw new UnsupportedOperationException();
        }

        @Override
        public RecordMetadata getMetadata() {
            return metadata;
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return false;
        }

        @Override
        public boolean supportsPageFrameCursor() {
            return isParallel;
        }
    }
}
