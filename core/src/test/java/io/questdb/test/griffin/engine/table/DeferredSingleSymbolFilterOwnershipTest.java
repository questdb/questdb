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

package io.questdb.test.griffin.engine.table;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.FullPartitionFrameCursorFactory;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RowCursorFactory;
import io.questdb.cairo.sql.SingleSymbolFilter;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.table.DeferredSingleSymbolFilterPageFrameRecordCursorFactory;
import io.questdb.griffin.engine.table.DeferredSymbolIndexRowCursorFactory;
import io.questdb.griffin.engine.table.SymbolIndexRowCursorFactory;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;

public class DeferredSingleSymbolFilterOwnershipTest extends AbstractCairoTest {
    @Test
    public void testCloseFailuresPreservePrimaryAndReleaseKeyOnce() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            for (int mode = 0; mode < 2; mode++) {
                final RuntimeException frameFailure = new RuntimeException("frame close");
                final RuntimeException keyFailure = new RuntimeException("key close");
                final TrackingKey key = new TrackingKey(keyFailure);
                final DeferredSingleSymbolFilterPageFrameRecordCursorFactory factory = createFactory(key, mode == 1, frameFailure);
                try {
                    assertRows(factory);
                } finally {
                    try {
                        factory.close();
                        Assert.fail("expected frame close failure");
                    } catch (RuntimeException e) {
                        Assert.assertSame(frameFailure, e);
                        Assert.assertEquals(1, e.getSuppressed().length);
                        Assert.assertSame(keyFailure, e.getSuppressed()[0]);
                    }
                }
                Assert.assertEquals(1, key.closeCount);
                factory.close();
                Assert.assertEquals(1, key.closeCount);
            }
        });
    }

    @Test
    public void testDeferredKeySurvivesFrameConversionAndClosesOnce() throws Exception {
        assertMemoryLeak(() -> assertReadAndClose(true));
    }

    @Test
    public void testResolvedBorrowedKeySurvivesFrameConversionAndClosesOnce() throws Exception {
        assertMemoryLeak(() -> assertReadAndClose(false));
    }

    private void assertReadAndClose(boolean isDeferred) throws Exception {
        createTable();
        final TrackingKey key = new TrackingKey(null);
        try (DeferredSingleSymbolFilterPageFrameRecordCursorFactory factory = createFactory(key, isDeferred, null)) {
            assertRows(factory);
            final SingleSymbolFilter symbolFilter = factory.convertToSampleByIndexPageFrameCursorFactory();
            try (PageFrameCursor frames = factory.getPageFrameCursor(sqlExecutionContext, ORDER_ASC)) {
                Assert.assertNotNull(frames.next());
                Assert.assertEquals(1, symbolFilter.getColumnIndex());
                Assert.assertEquals(TableUtils.toIndexKey(0), symbolFilter.getSymbolFilterKey());
            } finally {
                factory.revertFromSampleByIndexPageFrameCursorFactory();
            }
            assertRows(factory);
            Assert.assertEquals(0, key.closeCount);
        }
        Assert.assertEquals(1, key.closeCount);
    }

    private void assertRows(DeferredSingleSymbolFilterPageFrameRecordCursorFactory factory) throws Exception {
        assertFactory(factory).withContext(sqlExecutionContext).inferRandomAccess().inferTimestamp().sizeMayVary()
                .returns("id\ts\n1\tA\n3\tA\n");
    }

    private DeferredSingleSymbolFilterPageFrameRecordCursorFactory createFactory(
            TrackingKey key,
            boolean isDeferred,
            RuntimeException frameFailure
    ) {
        try (TableReader reader = engine.getReader("key_ownership")) {
            final GenericRecordMetadata metadata = GenericRecordMetadata.copyOf(reader.getMetadata());
            final IntList columnIndexes = new IntList();
            final IntList columnSizes = new IntList();
            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                columnIndexes.add(i);
                columnSizes.add(ColumnType.pow2SizeOf(metadata.getColumnType(i)));
            }
            final FullPartitionFrameCursorFactory frames = new FullPartitionFrameCursorFactory(
                    reader.getTableToken(), TableUtils.ANY_TABLE_VERSION, metadata, ORDER_ASC, null, 0, false
            ) {
                @Override
                public void close() {
                    super.close();
                    if (frameFailure != null) {
                        throw frameFailure;
                    }
                }
            };
            final RowCursorFactory rows = isDeferred
                    ? new DeferredSymbolIndexRowCursorFactory(1, key, IndexReader.DIR_FORWARD)
                    : new SymbolIndexRowCursorFactory(1, reader.getSymbolMapReader(1).keyOf("A"), IndexReader.DIR_FORWARD, key);
            return new DeferredSingleSymbolFilterPageFrameRecordCursorFactory(
                    configuration, 1, key, rows, metadata, frames, false, columnIndexes, columnSizes, true
            );
        }
    }

    private void createTable() throws Exception {
        execute("CREATE TABLE key_ownership(id INT,s SYMBOL INDEX)");
        execute("INSERT INTO key_ownership VALUES(1,'A'),(2,'B'),(3,'A')");
    }

    private static final class TrackingKey extends StrFunction {
        private final RuntimeException closeFailure;
        private final DirectLongList memory = new DirectLongList(1, MemoryTag.NATIVE_DEFAULT);
        private int closeCount;

        private TrackingKey(RuntimeException closeFailure) {
            this.closeFailure = closeFailure;
            memory.add(1);
        }

        @Override
        public void close() {
            closeCount++;
            memory.close();
            if (closeFailure != null) {
                throw closeFailure;
            }
        }

        @Override
        public CharSequence getStrA(Record rec) {
            Assert.assertEquals(0, closeCount);
            Assert.assertEquals(1, memory.get(0));
            return "A";
        }

        @Override
        public CharSequence getStrB(Record rec) {
            return getStrA(rec);
        }

        @Override
        public boolean isRuntimeConstant() {
            return true;
        }
    }
}
