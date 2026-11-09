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

package io.questdb.test.cairo.sql.async;

import io.questdb.DefaultFactoryProvider;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cairo.sql.async.AdaptiveWorkStealingStrategy;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.cairo.sql.async.WorkStealingStrategy;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.AsyncFilteredRecordCursorFactory;
import io.questdb.mp.SCSequence;
import io.questdb.std.FlyweightMessageContainer;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;
import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_DESC;

public class PageFrameSequenceTest extends AbstractCairoTest {

    @Test
    public void testCursorOverloadClosesTheCursorOnFailure() throws Exception {
        assertMemoryLeak(() -> {
            createFramesTable();
            // The atom fails to initialize: as a stale table reference, which the sequence passes on
            // as is, and as any other failure, which it logs first.
            final AtomicReference<Supplier<RuntimeException>> initFailure = new AtomicReference<>();
            final StatefulAtom atom = new StatefulAtom() {
                @Override
                public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
                    final Supplier<RuntimeException> failure = initFailure.get();
                    if (failure != null) {
                        throw failure.get();
                    }
                }
            };
            final SCSequence collectSubSeq = new SCSequence();
            try (
                    RecordCursorFactory factory = select("t");
                    PageFrameSequence<StatefulAtom> sequence = newFrameSequence(atom)
            ) {
                for (boolean isStaleReference : new boolean[]{true, false}) {
                    initFailure.set(isStaleReference
                            ? () -> TableReferenceOutOfDateException.of("t")
                            : () -> CairoException.nonCritical().put("injected init failure"));
                    final PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, ORDER_ASC);
                    Assert.assertEquals(1, engine.getBusyReaderCount());
                    final RuntimeException error = Assert.assertThrows(
                            RuntimeException.class,
                            () -> sequence.of(cursor, factory.getMetadata(), sqlExecutionContext, collectSubSeq)
                    );
                    Assert.assertEquals(isStaleReference, error instanceof TableReferenceOutOfDateException);
                    TestUtils.assertEquals(
                            isStaleReference
                                    ? "cached query plan cannot be used because table schema has changed [table=t]"
                                    : "injected init failure",
                            ((FlyweightMessageContainer) error).getFlyweightMessage()
                    );
                    // The sequence adopted the cursor, so the failure closed it and released its reader.
                    Assert.assertNull(sequence.getSymbolTableSource());
                    Assert.assertEquals(0, engine.getBusyReaderCount());
                    sequence.reset();
                }

                // The sequence binds to the next cursor it is handed.
                initFailure.set(null);
                final StringSink frames = new StringSink();
                final StringSink rows = new StringSink();
                readFrames(sequence, factory, ORDER_ASC, true, collectSubSeq, null, frames, rows);
                Assert.assertEquals(0, engine.getBusyReaderCount());
                TestUtils.assertEquals(queryRows("t"), rows);
            }
        });
    }

    @Test
    public void testCursorOverloadReadsTheFactoryOverloadFrames() throws Exception {
        assertMemoryLeak(() -> {
            createFramesTable();
            // Frames of seven rows at most, so that the native partitions split into many frames.
            sqlExecutionContext.changePageFrameSizes(7, 7);
            try (RecordCursorFactory factory = select("t")) {
                for (int order : new int[]{ORDER_ASC, ORDER_DESC}) {
                    final AtomicReference<SymbolTableSource> initSource = new AtomicReference<>();
                    final StatefulAtom atom = new StatefulAtom() {
                        @Override
                        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
                            initSource.set(symbolTableSource);
                        }
                    };
                    final SCSequence collectSubSeq = new SCSequence();
                    try (PageFrameSequence<StatefulAtom> sequence = newFrameSequence(atom)) {
                        final StringSink factoryFrames = new StringSink();
                        final StringSink factoryRows = new StringSink();
                        readFrames(sequence, factory, order, false, collectSubSeq, initSource, factoryFrames, factoryRows);
                        final StringSink cursorFrames = new StringSink();
                        final StringSink cursorRows = new StringSink();
                        readFrames(sequence, factory, order, true, collectSubSeq, initSource, cursorFrames, cursorRows);
                        // Both overloads dispatch the same frames, with the same rows, in the same order.
                        TestUtils.assertEquals(factoryFrames, cursorFrames);
                        TestUtils.assertEquals(factoryRows, cursorRows);
                        // Their rows, in scan order, are the table's rows in the scan's order.
                        TestUtils.assertEquals(queryRows(order == ORDER_ASC ? "t" : "t ORDER BY ts DESC"), cursorRows);
                        // Two Parquet partitions of one row group each, and three native ones of
                        // 60 rows, each split into several frames.
                        Assert.assertEquals(2, count(cursorFrames, "parquet=true"));
                        Assert.assertTrue(cursorFrames.toString(), count(cursorFrames, "parquet=false") > 3);
                    }
                }
            } finally {
                sqlExecutionContext.restoreToDefaultPageFrameSizes();
            }
        });
    }

    @Test
    public void testOwnerConsultsStrategyBeforeStealingOwnFrame() throws Exception {
        // No worker pool consumes the reduce queue, so the owner publishes each frame and then
        // steals it back in the gang-steal loop of dispatch. A strategy that gates the owner on a
        // worker, such as SlotGatedWorkStealingStrategy, can hold it back only if the loop calls
        // onBeforeDirectSteal() before each steal.
        final AtomicInteger directStealCount = new AtomicInteger();
        factoryProvider = new DefaultFactoryProvider() {
            @Override
            public @NotNull WorkStealingStrategy getWorkStealingStrategy(
                    @NotNull CairoConfiguration configuration,
                    int workerCount,
                    @NotNull StatefulAtom atom
            ) {
                return new AdaptiveWorkStealingStrategy(
                        configuration.getSqlParallelWorkStealingThreshold(),
                        configuration.getSqlParallelWorkStealingSpinTimeout()
                ) {
                    @Override
                    public void onBeforeDirectSteal() {
                        directStealCount.incrementAndGet();
                    }
                };
            }
        };
        assertMemoryLeak(() -> {
            // two daily partitions, so two page frames
            execute("""
                    CREATE TABLE t AS (
                        SELECT x, timestamp_sequence(0, 43_200_000_000) ts
                        FROM long_sequence(4)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            try (RecordCursorFactory factory = select("SELECT x FROM t WHERE x % 2 = 0")) {
                TestUtils.assertFactoryInTree(factory, AsyncFilteredRecordCursorFactory.class);
                for (int i = 0; i < 2; i++) {
                    directStealCount.set(0);
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        int rowCount = 0;
                        while (cursor.hasNext()) {
                            Assert.assertEquals(0, cursor.getRecord().getLong(0) % 2);
                            rowCount++;
                        }
                        Assert.assertEquals(2, rowCount);
                    }
                    Assert.assertEquals("execution " + i, 2, directStealCount.get());
                }
            }
        });
    }

    private static int count(CharSequence text, String term) {
        int count = 0;
        for (int i = text.toString().indexOf(term); i > -1; i = text.toString().indexOf(term, i + 1)) {
            count++;
        }
        return count;
    }

    private static void printRow(Record record, StringSink sink) {
        sink.put(record.getInt(0)).put(',')
                .put(record.getSymA(1)).put(',')
                .put(record.getLong(2)).put(',')
                .put(record.getTimestamp(3)).put('\n');
    }

    // 300 rows over five daily partitions, the first two of them Parquet.
    private void createFramesTable() throws SqlException {
        execute("""
                CREATE TABLE t AS (
                    SELECT (x % 11)::INT k, ('s' || (x % 5))::SYMBOL s, x v,
                        timestamp_sequence('2020-01-01', 1_440_000_000L) ts
                    FROM long_sequence(300)
                ) TIMESTAMP(ts) PARTITION BY DAY
                """);
        execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts < '2020-01-03'");
    }

    private PageFrameSequence<StatefulAtom> newFrameSequence(StatefulAtom atom) {
        return new PageFrameSequence<>(
                engine,
                configuration,
                engine.getMessageBus(),
                atom,
                (_, _, task, _, _) -> {
                    task.populateFrameMemory();
                    task.setFilteredRowCount(task.getFrameRowCount());
                },
                () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                1,
                PageFrameReduceTask.TYPE_HASH_JOIN
        );
    }

    private String queryRows(String sql) throws SqlException {
        final StringSink sink = new StringSink();
        try (
                RecordCursorFactory factory = select(sql);
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                printRow(record, sink);
            }
        }
        return sink.toString();
    }

    // Binds the sequence through one overload or the other and collects every frame: a header of
    // each frame into frames, and its rows in scan order, backwards within a frame of a backward
    // scan, into both frames and rows. When given initSource, which the atom's init() sets, checks
    // that the atom initialized over the bound cursor.
    private void readFrames(
            PageFrameSequence<StatefulAtom> sequence,
            RecordCursorFactory factory,
            int order,
            boolean isCursorOverload,
            SCSequence collectSubSeq,
            @Nullable AtomicReference<SymbolTableSource> initSource,
            StringSink frames,
            StringSink rows
    ) throws SqlException {
        if (isCursorOverload) {
            final PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, order);
            sequence.of(cursor, factory.getMetadata(), sqlExecutionContext, collectSubSeq);
            // The sequence adopted the cursor, which serves the frames' symbol tables.
            Assert.assertSame(cursor, sequence.getSymbolTableSource());
        } else {
            sequence.of(factory, sqlExecutionContext, collectSubSeq, order);
            Assert.assertNotNull(sequence.getSymbolTableSource());
        }
        if (initSource != null) {
            // The atom initialized over the cursor the sequence bound to.
            Assert.assertSame(sequence.getSymbolTableSource(), initSource.get());
        }
        try (PageFrameMemoryRecord record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER)) {
            sequence.prepareForDispatch();
            record.of(sequence.getSymbolTableSource());
            int frameIndex = 0;
            while (frameIndex < sequence.getFrameCount()) {
                final long cursor = sequence.next();
                if (cursor == -1) {
                    Os.pause();
                    continue;
                }
                Assert.assertTrue(cursor > -1);
                final PageFrameReduceTask task = sequence.getTask(cursor);
                Assert.assertEquals(PageFrameReduceTask.TYPE_HASH_JOIN, task.getTaskType());
                Assert.assertEquals(frameIndex, task.getFrameIndex());
                final long frameRows = task.getFrameRowCount();
                Assert.assertEquals(frameRows, task.getFilteredRowCount());
                frames.put('#').put(frameIndex).put(" rows=").put(frameRows)
                        .put(" parquet=").put(task.isParquetFrame()).put('\n');
                record.init(task.getFrameMemory());
                for (long i = 0; i < frameRows; i++) {
                    record.setRowIndex(order == ORDER_ASC ? i : frameRows - 1 - i);
                    printRow(record, frames);
                    printRow(record, rows);
                }
                sequence.collect(cursor, false);
                frameIndex++;
            }
        } finally {
            sequence.await();
            sequence.reset();
        }
    }
}
