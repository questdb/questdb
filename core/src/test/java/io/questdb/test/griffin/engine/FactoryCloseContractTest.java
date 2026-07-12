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
import io.questdb.cairo.sql.ColumnMapping;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.EmptyTableRecordCursor;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.join.AsOfJoinLightNoKeyRecordCursorFactory;
import io.questdb.griffin.engine.table.FilteredRecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

public class FactoryCloseContractTest extends AbstractCairoTest {

    @Test
    public void testFilteredFactoryCloseContinuesAfterBaseFailure() throws Exception {
        assertMemoryLeak(() -> {
            final RuntimeException baseFailure = new RuntimeException("base close");
            final RuntimeException filterFailure = new RuntimeException("filter close");
            final CloseTrackingFactory base = new CloseTrackingFactory(new GenericRecordMetadata(), baseFailure);
            final CloseTrackingBooleanFunction filter = new CloseTrackingBooleanFunction(filterFailure);
            final FilteredRecordCursorFactory factory = new FilteredRecordCursorFactory(base, filter);

            try {
                factory.close();
                Assert.fail();
            } catch (RuntimeException e) {
                Assert.assertSame(baseFailure, e);
                Assert.assertArrayEquals(new Throwable[]{filterFailure}, e.getSuppressed());
            }
            factory.close();
            Assert.assertEquals(1, base.getCloseCount());
            Assert.assertEquals(1, filter.closeCount);
        });
    }

    @Test
    public void testJoinFactoryCloseContinuesAfterMasterFailure() throws Exception {
        assertMemoryLeak(() -> {
            final RuntimeException masterFailure = new RuntimeException("master close");
            final RuntimeException slaveFailure = new RuntimeException("slave close");
            final CloseTrackingFactory master = new CloseTrackingFactory(timestampMetadata("master_ts"), masterFailure);
            final CloseTrackingFactory slave = new CloseTrackingFactory(timestampMetadata("slave_ts"), slaveFailure);
            final AsOfJoinLightNoKeyRecordCursorFactory factory = new AsOfJoinLightNoKeyRecordCursorFactory(
                    joinMetadata(),
                    master,
                    slave,
                    1,
                    Long.MAX_VALUE
            );

            try {
                factory.close();
                Assert.fail();
            } catch (RuntimeException e) {
                Assert.assertSame(masterFailure, e);
                Assert.assertArrayEquals(new Throwable[]{slaveFailure}, e.getSuppressed());
            }
            factory.close();
            Assert.assertEquals(1, master.getCloseCount());
            Assert.assertEquals(1, slave.getCloseCount());
        });
    }

    @Test
    public void testQueryProgressFactoryCloseContinuesAfterBaseFailure() throws Exception {
        assertMemoryLeak(() -> {
            final RuntimeException baseFailure = new RuntimeException("base close");
            final CloseTrackingPageFrameCursor pageFrameCursor = new CloseTrackingPageFrameCursor();
            final PageFrameFactory base = new PageFrameFactory(new GenericRecordMetadata(), baseFailure, pageFrameCursor);
            final QueryProgress factory = new QueryProgress(engine.getQueryRegistry(), "SELECT 1", base);
            Assert.assertNotNull(factory.getPageFrameCursor(sqlExecutionContext, RecordCursorFactory.SCAN_DIRECTION_FORWARD));

            try {
                factory.close();
                Assert.fail();
            } catch (RuntimeException e) {
                Assert.assertSame(baseFailure, e);
            }
            factory.close();
            Assert.assertEquals(1, base.getCloseCount());
            Assert.assertEquals(1, pageFrameCursor.closeCount);
        });
    }

    private static GenericRecordMetadata joinMetadata() {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("master_ts", ColumnType.TIMESTAMP));
        metadata.add(new TableColumnMetadata("slave_ts", ColumnType.TIMESTAMP));
        metadata.setTimestampIndex(0);
        return metadata;
    }

    private static GenericRecordMetadata timestampMetadata(String name) {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata(name, ColumnType.TIMESTAMP));
        metadata.setTimestampIndex(0);
        return metadata;
    }

    private static class CloseTrackingBooleanFunction extends BooleanFunction {
        private final RuntimeException closeFailure;
        private int closeCount;

        private CloseTrackingBooleanFunction(RuntimeException closeFailure) {
            this.closeFailure = closeFailure;
        }

        @Override
        public void close() {
            closeCount++;
            if (closeFailure != null) {
                throw closeFailure;
            }
        }

        @Override
        public boolean getBool(Record rec) {
            return true;
        }
    }

    private static class CloseTrackingFactory extends AbstractRecordCursorFactory {
        private final RuntimeException closeFailure;
        private int closeCount;

        private CloseTrackingFactory(RecordMetadata metadata, RuntimeException closeFailure) {
            super(metadata);
            this.closeFailure = closeFailure;
        }

        protected int getCloseCount() {
            return closeCount;
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) {
            return EmptyTableRecordCursor.INSTANCE;
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return false;
        }

        @Override
        protected void _close() {
            closeCount++;
            if (closeFailure != null) {
                throw closeFailure;
            }
        }
    }

    private static class CloseTrackingPageFrameCursor implements PageFrameCursor {
        private int closeCount;

        @Override
        public void calculateSize(RecordCursor.Counter counter) {
        }

        @Override
        public void close() {
            closeCount++;
        }

        @Override
        public ColumnMapping getColumnMapping() {
            return null;
        }

        @Override
        public long getRemainingRowsInInterval() {
            return 0;
        }

        @Override
        public StaticSymbolTable getSymbolTable(int columnIndex) {
            return null;
        }

        @Override
        public boolean isExternal() {
            return false;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return null;
        }

        @Override
        public @Nullable PageFrame next(long skipTarget) {
            return null;
        }

        @Override
        public long size() {
            return 0;
        }

        @Override
        public boolean supportsSizeCalculation() {
            return true;
        }

        @Override
        public void toTop() {
        }
    }

    private static class PageFrameFactory extends CloseTrackingFactory {
        private final PageFrameCursor pageFrameCursor;

        private PageFrameFactory(
                RecordMetadata metadata,
                RuntimeException closeFailure,
                PageFrameCursor pageFrameCursor
        ) {
            super(metadata, closeFailure);
            this.pageFrameCursor = pageFrameCursor;
        }

        @Override
        public PageFrameCursor getPageFrameCursor(SqlExecutionContext executionContext, int order) {
            return pageFrameCursor;
        }

        @Override
        public boolean supportsPageFrameCursor() {
            return true;
        }
    }
}
