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

package io.questdb.test.cutlass.parquet;

import io.questdb.cairo.sql.ColumnMapping;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cutlass.parquet.CopyExportRequestTask;
import io.questdb.cutlass.parquet.HTTPSerialParquetExporter;
import io.questdb.cutlass.parquet.ParquetExportMode;
import io.questdb.test.AbstractCairoTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

public class HTTPSerialParquetExporterTest extends AbstractCairoTest {

    @Test
    public void testTimerForwardingUsesOnlyLiveExportModeOwner() throws Exception {
        assertMemoryLeak(() -> {
            final ProbeTask task = new ProbeTask();
            final TestExporter exporter = new TestExporter();
            final TimerProbePageFrameCursor pageFrameBackedCursor = new TimerProbePageFrameCursor();
            final TimerProbePageFrameCursor tempTableCursor = new TimerProbePageFrameCursor();
            try {
                task.pageFrameCursor = pageFrameBackedCursor;
                exporter.setTask(task);
                exporter.setExportMode(ParquetExportMode.PAGE_FRAME_BACKED);
                exporter.setupPageFrameBackedExport(pageFrameBackedCursor, null, null);
                exporter.clearExportResources();
                Assert.assertEquals(1, pageFrameBackedCursor.closeCount);

                exporter.suspendCursorTimer();
                exporter.resumeCursorTimer();
                Assert.assertEquals(
                        "cleared PAGE_FRAME_BACKED task alias must not receive timer calls",
                        0,
                        pageFrameBackedCursor.staleTimerCallCount
                );

                task.pageFrameCursor = tempTableCursor;
                exporter.setExportMode(ParquetExportMode.TEMP_TABLE);
                exporter.suspendCursorTimer();
                exporter.resumeCursorTimer();
                Assert.assertEquals(1, tempTableCursor.suspendCount);
                Assert.assertEquals(1, tempTableCursor.resumeCount);

                exporter.clearExportResources();
                exporter.suspendCursorTimer();
                exporter.resumeCursorTimer();
                Assert.assertEquals("cleared exporter must stop forwarding", 1, tempTableCursor.suspendCount);
                Assert.assertEquals("cleared exporter must stop forwarding", 1, tempTableCursor.resumeCount);
            } finally {
                exporter.clearExportResources();
                pageFrameBackedCursor.close();
                tempTableCursor.close();
                task.close();
            }
        });
    }

    private static final class ProbeTask extends CopyExportRequestTask {
        private PageFrameCursor pageFrameCursor;

        @Override
        public @Nullable PageFrameCursor getPageFrameCursor() {
            return pageFrameCursor;
        }
    }

    private static final class TestExporter extends HTTPSerialParquetExporter {
        private TestExporter() {
            super(engine);
        }

        private void setTask(CopyExportRequestTask task) {
            this.task = task;
        }
    }

    private static final class TimerProbePageFrameCursor implements PageFrameCursor {
        private int closeCount;
        private boolean isClosed;
        private int resumeCount;
        private int staleTimerCallCount;
        private int suspendCount;

        @Override
        public void calculateSize(RecordCursor.Counter counter) {
        }

        @Override
        public void close() {
            if (!isClosed) {
                closeCount++;
                isClosed = true;
            }
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
        public void resumeTimer() {
            resumeCount++;
            if (isClosed) {
                staleTimerCallCount++;
            }
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
        public void suspendTimer() {
            suspendCount++;
            if (isClosed) {
                staleTimerCallCount++;
            }
        }

        @Override
        public void toTop() {
        }
    }
}
