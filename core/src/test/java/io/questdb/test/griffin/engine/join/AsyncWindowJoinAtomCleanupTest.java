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

package io.questdb.test.griffin.engine.join;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.TimeFrame;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.join.AsyncWindowJoinFastAtom;
import io.questdb.griffin.engine.table.ConcurrentTimeFrameCursor;
import io.questdb.griffin.engine.table.ConcurrentTimeFrameState;
import io.questdb.griffin.engine.table.TablePageFrameCursor;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Fault-injection coverage for the keyed WINDOW JOIN atom's cleanup ordering.
 * <p>
 * {@link AsyncWindowJoinFastAtom} owns five native structures the parent
 * {@code AsyncWindowJoinAtom} knows nothing about: the slave symbol lookup map, the owner
 * and per-slot slaveData maps and, for INCLUDE PREVAILING, the owner and per-slot prevailing
 * caches. All five charge the per-query memory tracker, so the query that allocated them must
 * also free them - a skipped free both leaks the block and leaves the tracker charged, which
 * trips {@code PerQueryMemoryTracker.init()}'s {@code used == 0} assert on the next query that
 * recycles that pooled tracker.
 * <p>
 * The parent's {@code close()} is best-effort by design and rethrows the accumulated failure at
 * the end, and its {@code clear()} frees a time-frame cursor whose own close can fail, so either
 * can throw at a subclass that runs its cleanup after {@code super}. These tests inject exactly
 * that failure and assert the keyed state is released regardless.
 * <p>
 * The atom is built directly rather than through a query, because the injection point - a slave
 * time-frame cursor whose {@code close()} fails - has no SQL-level seam.
 */
public class AsyncWindowJoinAtomCleanupTest extends AbstractCairoTest {

    private static final String INJECTED_FAILURE = "injected time-frame cursor close failure";
    private static final int WORKER_COUNT = 2;

    @Test
    public void testClearFreesKeyedStateWhenParentClearFails() throws Exception {
        assertMemoryLeak(() -> {
            final boolean[] failClose = {true};
            try (FaultySlaveFactory slaveFactory = new FaultySlaveFactory(failClose)) {
                final long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_UNORDERED_MAP);
                final AsyncWindowJoinFastAtom atom = newAtom(slaveFactory);
                try {
                    Assert.assertTrue(
                            "the atom must hold its keyed maps before the cleanup runs",
                            Unsafe.getMemUsedByTag(MemoryTag.NATIVE_UNORDERED_MAP) > baseline
                    );
                    try {
                        atom.clear();
                        Assert.fail("expected the injected time-frame cursor close failure");
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), INJECTED_FAILURE);
                    }
                    Assert.assertEquals(
                            "clear() must free the keyed maps even when the parent clear fails",
                            baseline,
                            Unsafe.getMemUsedByTag(MemoryTag.NATIVE_UNORDERED_MAP)
                    );
                } finally {
                    failClose[0] = false;
                    atom.close();
                }
            }
        });
    }

    @Test
    public void testClearReportsEveryParentFailure() throws Exception {
        // Every leg of the parent clear() has to be attempted, not just the ones before the first
        // failure: the owner cursor and each per-worker cursor own a reader apiece, and abandoning
        // the rest of the sequence strands them. The stub counts the close attempts, so a fail-fast
        // clear() shows up as a short count rather than only as a leak.
        assertMemoryLeak(() -> {
            final boolean[] failClose = {true};
            try (FaultySlaveFactory slaveFactory = new FaultySlaveFactory(failClose)) {
                final AsyncWindowJoinFastAtom atom = newAtom(slaveFactory);
                try {
                    final int cursorCount = slaveFactory.cursorCount;
                    Assert.assertTrue("expected owner and per-worker cursors", cursorCount > 1);
                    try {
                        atom.clear();
                        Assert.fail("expected the injected time-frame cursor close failure");
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), INJECTED_FAILURE);
                    }
                    Assert.assertEquals(
                            "clear() must attempt to close every slave time-frame cursor",
                            cursorCount,
                            slaveFactory.closeAttempts
                    );
                } finally {
                    failClose[0] = false;
                    atom.close();
                }
            }
        });
    }

    @Test
    public void testCloseFreesKeyedStateWhenParentCloseFails() throws Exception {
        // assertMemoryLeak is the assertion here: close() is the last chance to free the keyed maps,
        // so a subclass cleanup skipped by the parent's rethrow shows up as leaked native memory.
        assertMemoryLeak(() -> {
            final boolean[] failClose = {true};
            try (FaultySlaveFactory slaveFactory = new FaultySlaveFactory(failClose)) {
                final long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_UNORDERED_MAP);
                final AsyncWindowJoinFastAtom atom = newAtom(slaveFactory);
                Assert.assertTrue(
                        "the atom must hold its keyed maps before the cleanup runs",
                        Unsafe.getMemUsedByTag(MemoryTag.NATIVE_UNORDERED_MAP) > baseline
                );
                try {
                    atom.close();
                    Assert.fail("expected the injected time-frame cursor close failure");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), INJECTED_FAILURE);
                }
                Assert.assertEquals(
                        "close() must free the keyed maps even when the parent close fails",
                        baseline,
                        Unsafe.getMemUsedByTag(MemoryTag.NATIVE_UNORDERED_MAP)
                );
            }
        });
    }

    private static AsyncWindowJoinFastAtom newAtom(FaultySlaveFactory slaveFactory) {
        final ArrayColumnTypes valueTypes = new ArrayColumnTypes();
        valueTypes.add(ColumnType.LONG);
        return new AsyncWindowJoinFastAtom(
                new BytecodeAssembler(),
                configuration,
                slaveFactory,
                null,
                null,
                0,
                0,
                -1_000_000L,
                1_000_000L,
                true,
                2,
                0,
                valueTypes,
                new ObjList<GroupByFunction>(),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                false,
                1,
                1,
                WORKER_COUNT
        );
    }

    /**
     * A slave factory whose only job is to hand out time-frame cursors that fail to close while
     * {@code failClose} is set. Nothing else on the factory is reachable from the atom.
     */
    private static class FaultySlaveFactory extends AbstractRecordCursorFactory {
        private final boolean[] failClose;
        private int closeAttempts;
        private int cursorCount;

        private FaultySlaveFactory(boolean[] failClose) {
            super(newMetadata());
            this.failClose = failClose;
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ConcurrentTimeFrameCursor newTimeFrameCursor() {
            cursorCount++;
            return new FaultyTimeFrameCursor();
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return false;
        }

        private static GenericRecordMetadata newMetadata() {
            final GenericRecordMetadata metadata = new GenericRecordMetadata();
            metadata.add(new TableColumnMetadata("ts", ColumnType.TIMESTAMP));
            metadata.add(new TableColumnMetadata("price", ColumnType.DOUBLE));
            metadata.setTimestampIndex(0);
            return metadata;
        }

        private class FaultyTimeFrameCursor implements ConcurrentTimeFrameCursor {

            @Override
            public void close() {
                closeAttempts++;
                if (failClose[0]) {
                    throw CairoException.nonCritical().put(INJECTED_FAILURE);
                }
            }

            @Override
            public Record getRecord() {
                throw new UnsupportedOperationException();
            }

            @Override
            public StaticSymbolTable getSymbolTable(int columnIndex) {
                throw new UnsupportedOperationException();
            }

            @Override
            public TimeFrame getTimeFrame() {
                throw new UnsupportedOperationException();
            }

            @Override
            public int getTimestampIndex() {
                return 0;
            }

            @Override
            public void jumpTo(int frameIndex) {
                throw new UnsupportedOperationException();
            }

            @Override
            public SymbolTable newSymbolTable(int columnIndex) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean next() {
                throw new UnsupportedOperationException();
            }

            @Override
            public ConcurrentTimeFrameCursor of(
                    ConcurrentTimeFrameState sharedState,
                    TablePageFrameCursor frameCursor,
                    int timestampIndex
            ) {
                throw new UnsupportedOperationException();
            }

            @Override
            public long open() {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean prev() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void recordAt(Record record, long rowId) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void recordAt(Record record, int frameIndex, long rowIndex) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void recordAtRowIndex(Record record, long rowIndex) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void seekEstimate(long timestamp) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void toTop() {
                throw new UnsupportedOperationException();
            }
        }
    }
}
