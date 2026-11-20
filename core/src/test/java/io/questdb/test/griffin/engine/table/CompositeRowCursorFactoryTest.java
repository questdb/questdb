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

import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.sql.RowCursorFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.table.HeapRowCursorFactory;
import io.questdb.griffin.engine.table.SequentialRowCursorFactory;
import io.questdb.std.ObjList;
import org.junit.Assert;
import org.junit.Test;

public class CompositeRowCursorFactoryTest {

    @Test
    public void testHeapClosesChildrenWhenOpenFails() {
        final RuntimeException failure = new RuntimeException("open failed");
        final TestRowCursor firstCursor = new TestRowCursor(false, null);
        final ObjList<RowCursorFactory> childFactories = new ObjList<>();
        childFactories.add(new TestRowCursorFactory(firstCursor));
        childFactories.add(new TestRowCursorFactory(failure));

        try (HeapRowCursorFactory factory = new HeapRowCursorFactory(
                childFactories,
                new int[]{childFactories.size()}
        )) {
            Assert.assertSame(failure, Assert.assertThrows(
                    RuntimeException.class,
                    () -> factory.getCursor(null, null)
            ));
            Assert.assertTrue(firstCursor.isClosed());
        }
    }

    @Test
    public void testHeapClosesChildrenWhenWarmUpFails() {
        final RuntimeException failure = new RuntimeException("warm-up failed");
        final TestRowCursor firstCursor = new TestRowCursor(true, null);
        final TestRowCursor secondCursor = new TestRowCursor(false, failure);
        final ObjList<RowCursorFactory> childFactories = new ObjList<>();
        childFactories.add(new TestRowCursorFactory(firstCursor));
        childFactories.add(new TestRowCursorFactory(secondCursor));

        try (HeapRowCursorFactory factory = new HeapRowCursorFactory(
                childFactories,
                new int[]{childFactories.size()}
        )) {
            Assert.assertSame(failure, Assert.assertThrows(
                    RuntimeException.class,
                    () -> factory.getCursor(null, null)
            ));
            Assert.assertTrue(firstCursor.isClosed());
            Assert.assertTrue(secondCursor.isClosed());
        }
    }

    @Test
    public void testSequentialClosesChildrenWhenOpenFails() {
        final RuntimeException failure = new RuntimeException("open failed");
        final TestRowCursor firstCursor = new TestRowCursor(false, null);
        final ObjList<RowCursorFactory> childFactories = new ObjList<>();
        childFactories.add(new TestRowCursorFactory(firstCursor));
        childFactories.add(new TestRowCursorFactory(failure));

        try (SequentialRowCursorFactory factory = new SequentialRowCursorFactory(
                childFactories,
                new int[]{childFactories.size()}
        )) {
            Assert.assertSame(failure, Assert.assertThrows(
                    RuntimeException.class,
                    () -> factory.getCursor(null, null)
            ));
            Assert.assertTrue(firstCursor.isClosed());
        }
    }

    private static final class TestRowCursor implements RowCursor {
        private final RuntimeException failure;
        private final boolean hasNext;
        private boolean isClosed;

        private TestRowCursor(boolean hasNext, RuntimeException failure) {
            this.hasNext = hasNext;
            this.failure = failure;
        }

        @Override
        public void close() {
            isClosed = true;
        }

        @Override
        public boolean hasNext() {
            if (failure != null) {
                throw failure;
            }
            return hasNext;
        }

        private boolean isClosed() {
            return isClosed;
        }

        @Override
        public long next() {
            return 0;
        }
    }

    private static final class TestRowCursorFactory implements RowCursorFactory {
        private final RowCursor cursor;
        private final RuntimeException failure;

        private TestRowCursorFactory(RowCursor cursor) {
            this.cursor = cursor;
            this.failure = null;
        }

        private TestRowCursorFactory(RuntimeException failure) {
            this.cursor = null;
            this.failure = failure;
        }

        @Override
        public RowCursor getCursor(PageFrame pageFrame, PageFrameMemory pageFrameMemory) {
            if (failure != null) {
                throw failure;
            }
            return cursor;
        }

        @Override
        public boolean isEntity() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
        }
    }
}
