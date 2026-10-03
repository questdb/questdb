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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.RecordComparator;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.window.CachedWindowLightRecordCursorFactory;
import io.questdb.griffin.engine.window.CachedWindowRecordCursorFactory;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class SqlWindowGenerationOwnershipTest extends AbstractCairoTest {
    @Test
    public void testCachedConstructionFailureClosesEveryFunctionAndPreservesFailure() throws Exception {
        assertMemoryLeak(() -> {
            for (boolean isLight : new boolean[]{false, true}) {
                final RuntimeException failure = new RuntimeException("window allocation");
                final RuntimeException closeFailure = new RuntimeException("window close");
                final TrackingFactory base = new TrackingFactory();
                final TrackingWindow ordered = new TrackingWindow(closeFailure);
                final TrackingWindow natural = new TrackingWindow(null);
                final DefaultCairoConfiguration configuration = new DefaultCairoConfiguration(root) {
                    @Override
                    public int getSqlWindowStorePageSize() {
                        throw failure;
                    }
                };
                final RuntimeException actual = Assert.assertThrows(RuntimeException.class,
                        () -> create(configuration, base, ordered, natural, isLight));
                Assert.assertSame(failure, actual);
                Assert.assertEquals(1, actual.getSuppressed().length);
                Assert.assertSame(closeFailure, actual.getSuppressed()[0]);
                Assert.assertEquals(1, base.closeCount);
                Assert.assertEquals(1, ordered.closeCount);
                Assert.assertEquals(1, natural.closeCount);
            }
        });
    }

    @Test
    public void testNeverOpenedCachedFactoryOwnsAllFunctions() throws Exception {
        assertMemoryLeak(() -> {
            for (boolean isLight : new boolean[]{false, true}) {
                final TrackingFactory base = new TrackingFactory();
                final TrackingWindow ordered = new TrackingWindow(null);
                final TrackingWindow natural = new TrackingWindow(null);
                try (RecordCursorFactory factory = create(new DefaultCairoConfiguration(root), base, ordered, natural, isLight)) {
                    Assert.assertEquals(0, base.closeCount);
                    Assert.assertEquals(0, ordered.closeCount);
                    Assert.assertEquals(0, natural.closeCount);
                    factory.close();
                }
                Assert.assertEquals(1, base.closeCount);
                Assert.assertEquals(1, ordered.closeCount);
                Assert.assertEquals(1, natural.closeCount);
            }
        });
    }

    private static RecordCursorFactory create(DefaultCairoConfiguration configuration, TrackingFactory base,
                                              TrackingWindow ordered, TrackingWindow natural, boolean isLight) {
        final GenericRecordMetadata metadata = GenericRecordMetadata.copyOfNew(base.getMetadata());
        metadata.add(new TableColumnMetadata("w", ColumnType.LONG));
        final ArrayColumnTypes types = new ArrayColumnTypes();
        types.add(ColumnType.LONG);
        types.add(ColumnType.LONG);
        final IntList indexes = new IntList();
        indexes.add(0);
        indexes.add(-1);
        final IntList order = new IntList();
        order.add(1);
        final ObjList<IntList> keys = new ObjList<>();
        keys.add(order);
        final ObjList<WindowFunction> functions = new ObjList<>();
        functions.add(ordered);
        final ObjList<ObjList<WindowFunction>> groups = new ObjList<>();
        groups.add(functions);
        final ObjList<WindowFunction> naturalFunctions = new ObjList<>();
        naturalFunctions.add(natural);
        if (isLight) {
            final IntList sources = new IntList();
            sources.add(0);
            sources.add(-1);
            final ArrayColumnTypes narrowTypes = new ArrayColumnTypes();
            narrowTypes.add(ColumnType.LONG);
            return new CachedWindowLightRecordCursorFactory(configuration, base, metadata, narrowTypes,
                    groups, naturalFunctions, indexes, keys, metadata, sources, null, null);
        }
        final ObjList<RecordComparator> comparators = new ObjList<>();
        comparators.add(null);
        return new CachedWindowRecordCursorFactory(configuration, base, null, metadata, types,
                comparators, groups, naturalFunctions, indexes, keys, metadata, null, null);
    }

    private static final class TrackingFactory implements RecordCursorFactory {
        private final GenericRecordMetadata metadata = new GenericRecordMetadata();
        private int closeCount;

        private TrackingFactory() {
            metadata.add(new TableColumnMetadata("v", ColumnType.LONG));
        }

        @Override
        public void close() {
            Assert.assertEquals(1, ++closeCount);
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
            return true;
        }
    }

    private static final class TrackingWindow extends LongFunction implements WindowFunction {
        private final RuntimeException closeFailure;
        private int closeCount;

        private TrackingWindow(RuntimeException closeFailure) {
            this.closeFailure = closeFailure;
        }

        @Override
        public void close() {
            Assert.assertEquals(1, ++closeCount);
            if (closeFailure != null) {
                throw closeFailure;
            }
        }

        @Override
        public long getLong(Record record) {
            return 0;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
        }

        @Override
        public void reset() {
        }

        @Override
        public void setColumnIndex(int columnIndex) {
        }
    }
}
