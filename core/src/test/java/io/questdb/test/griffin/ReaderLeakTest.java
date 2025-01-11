/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.cairo.DataUnavailableException;
import io.questdb.cairo.FullFwdPartitionFrameCursorFactory;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.DelegatingRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.griffin.engine.table.FwdPageFrameRowCursorFactory;
import io.questdb.griffin.engine.table.PageFrameRecordCursorFactory;
import io.questdb.std.IntList;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class ReaderLeakTest extends AbstractCairoTest {

    @Test
    public void testLeakPrevention() throws Exception {
        assertMemoryLeak(() -> {
            staticOverrides.freeLeakedReaders(true);
            // assemble factory
            engine.execute("create table x as (select rnd_int() a from long_sequence(20))");
            TableToken token = engine.verifyTableName("x");
            try (TableMetadata metadata = engine.getTableMetadata(token)) {

                // select all columns
                IntList columnIndexes = new IntList();
                IntList columnSizes = new IntList();
                for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                    columnIndexes.add(i);
                    columnSizes.add(ColumnType.pow2SizeOf(metadata.getColumnType(i)));
                }

                try (
                        RecordCursorFactory factory = new QueryProgress(
                                engine.getQueryRegistry(),
                                "select * from x",
                                new LeakingRecordCursorFactory(
                                        new PageFrameRecordCursorFactory(
                                                engine.getConfiguration(),
                                                metadata,
                                                new FullFwdPartitionFrameCursorFactory(
                                                        token,
                                                        TableUtils.ANY_TABLE_VERSION,
                                                        metadata
                                                ),
                                                new FwdPageFrameRowCursorFactory(),
                                                false,
                                                null,
                                                true,
                                                columnIndexes,
                                                columnSizes,
                                                true
                                        )
                                )
                        )
                ) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        assertCursor(
                                "a\n" +
                                        "-1148479920\n" +
                                        "315515118\n" +
                                        "1548800833\n" +
                                        "-727724771\n" +
                                        "73575701\n" +
                                        "-948263339\n" +
                                        "1326447242\n" +
                                        "592859671\n" +
                                        "1868723706\n" +
                                        "-847531048\n" +
                                        "-1191262516\n" +
                                        "-2041844972\n" +
                                        "-1436881714\n" +
                                        "-1575378703\n" +
                                        "806715481\n" +
                                        "1545253512\n" +
                                        "1569490116\n" +
                                        "1573662097\n" +
                                        "-409854405\n" +
                                        "339631474\n",
                                cursor,
                                factory.getMetadata(),
                                true
                        );
                    }
                    Assert.assertEquals(0, engine.getBusyReaderCount());
                    Assert.assertEquals(1, engine.getMetrics().healthMetrics().readerLeakCounter());
                }
            }
        });
    }

    @Test
    public void testSQLErrorsAreRecordedInMetrics() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table abc(a int, b int, c int)");
            Rnd rnd = TestUtils.generateRandom(LOG);
            int n = rnd.nextInt(10);
            for (int i = 0; i < n; i++) {
                assertException("select x from abc", 7, "Invalid column: x");
            }
            Assert.assertEquals(n, engine.getMetrics().healthMetrics().queryErrorCounter());
        });
    }

    private static class LeakingRecordCursor implements DelegatingRecordCursor {
        private RecordCursor base;

        @Override
        public void close() {
            // don't
        }

        @Override
        public Record getRecord() {
            return base.getRecord();
        }

        @Override
        public Record getRecordB() {
            return base.getRecordB();
        }

        @Override
        public boolean hasNext() throws DataUnavailableException {
            return base.hasNext();
        }

        @Override
        public void of(RecordCursor base, SqlExecutionContext executionContext) {
            this.base = base;
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            base.recordAt(record, atRowId);
        }

        @Override
        public long size() throws DataUnavailableException {
            return base.size();
        }

        @Override
        public void toTop() {
            base.toTop();
        }
    }

    private static class LeakingRecordCursorFactory implements RecordCursorFactory {
        private final RecordCursorFactory baseFactory;
        private final LeakingRecordCursor cursor = new LeakingRecordCursor();

        public LeakingRecordCursorFactory(RecordCursorFactory baseFactory) {
            this.baseFactory = baseFactory;
        }

        @Override
        public void close() {
            this.baseFactory.close();
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
            cursor.of(baseFactory.getCursor(sqlExecutionContext), sqlExecutionContext);
            return cursor;
        }

        @Override
        public RecordMetadata getMetadata() {
            return baseFactory.getMetadata();
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return baseFactory.recordCursorSupportsRandomAccess();
        }
    }
}
