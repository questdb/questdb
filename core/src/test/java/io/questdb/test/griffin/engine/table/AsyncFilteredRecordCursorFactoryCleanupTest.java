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

import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.griffin.engine.table.AsyncFilteredRecordCursorFactory;
import io.questdb.std.Misc;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.Closeable;

public class AsyncFilteredRecordCursorFactoryCleanupTest extends AbstractCairoTest {

    @Test
    public void testHalfClosePreservesNegativeCursorFailure() throws Exception {
        assertMemoryLeak(() -> {
            final RuntimeException negativeFailure = new RuntimeException("negative cursor");
            final TrackingSequence sequence = new TrackingSequence(null);
            final TrackingRecordFreer cursor = new TrackingRecordFreer(null);
            final TrackingRecordFreer negativeLimitCursor = new TrackingRecordFreer(negativeFailure);

            assertFailure(negativeFailure, new Throwable[0], sequence, cursor, negativeLimitCursor);
            assertSingleAttempt(sequence, cursor, negativeLimitCursor);
            retryWithoutFailures(sequence, cursor, negativeLimitCursor);
        });
    }

    @Test
    public void testHalfClosePreservesNormalCursorFailureAndSuppressesNegativeCursorFailure() throws Exception {
        assertMemoryLeak(() -> {
            final RuntimeException cursorFailure = new RuntimeException("normal cursor");
            final RuntimeException negativeFailure = new RuntimeException("negative cursor");
            final TrackingSequence sequence = new TrackingSequence(null);
            final TrackingRecordFreer cursor = new TrackingRecordFreer(cursorFailure);
            final TrackingRecordFreer negativeLimitCursor = new TrackingRecordFreer(negativeFailure);

            assertFailure(cursorFailure, new Throwable[]{negativeFailure}, sequence, cursor, negativeLimitCursor);
            assertSingleAttempt(sequence, cursor, negativeLimitCursor);
            retryWithoutFailures(sequence, cursor, negativeLimitCursor);
        });
    }

    @Test
    public void testHalfClosePreservesSequenceFailureAndSuppressesBothCursorFailures() throws Exception {
        assertMemoryLeak(() -> {
            final RuntimeException sequenceFailure = new RuntimeException("sequence");
            final RuntimeException workerFailure = new RuntimeException("worker");
            sequenceFailure.addSuppressed(workerFailure);
            final RuntimeException cursorFailure = new RuntimeException("normal cursor");
            final RuntimeException negativeFailure = new RuntimeException("negative cursor");
            final TrackingSequence sequence = new TrackingSequence(sequenceFailure);
            final TrackingRecordFreer cursor = new TrackingRecordFreer(cursorFailure);
            final TrackingRecordFreer negativeLimitCursor = new TrackingRecordFreer(negativeFailure);

            assertFailure(
                    sequenceFailure,
                    new Throwable[]{workerFailure, cursorFailure, negativeFailure},
                    sequence,
                    cursor,
                    negativeLimitCursor
            );
            assertSingleAttempt(sequence, cursor, negativeLimitCursor);
            retryWithoutFailures(sequence, cursor, negativeLimitCursor);
        });
    }

    @Test
    public void testHalfCloseSharedFailureDoesNotSelfSuppress() throws Exception {
        assertMemoryLeak(() -> {
            final RuntimeException sharedFailure = new RuntimeException("shared");
            final TrackingSequence sequence = new TrackingSequence(sharedFailure);
            final TrackingRecordFreer cursor = new TrackingRecordFreer(sharedFailure);
            final TrackingRecordFreer negativeLimitCursor = new TrackingRecordFreer(sharedFailure);

            assertFailure(sharedFailure, new Throwable[0], sequence, cursor, negativeLimitCursor);
            assertSingleAttempt(sequence, cursor, negativeLimitCursor);
            retryWithoutFailures(sequence, cursor, negativeLimitCursor);
        });
    }

    private static void assertFailure(
            RuntimeException expected,
            Throwable[] suppressed,
            TrackingSequence sequence,
            TrackingRecordFreer cursor,
            TrackingRecordFreer negativeLimitCursor
    ) {
        try {
            AsyncFilteredRecordCursorFactory.halfCloseForTesting(sequence, cursor, negativeLimitCursor);
            Assert.fail();
        } catch (RuntimeException e) {
            Assert.assertSame(expected, e);
            Assert.assertArrayEquals(suppressed, e.getSuppressed());
        }
    }

    private static void assertSingleAttempt(
            TrackingSequence sequence,
            TrackingRecordFreer cursor,
            TrackingRecordFreer negativeLimitCursor
    ) {
        Assert.assertEquals(1, sequence.closeCount);
        Assert.assertEquals(1, cursor.freeRecordsCount);
        Assert.assertEquals(1, negativeLimitCursor.freeRecordsCount);
    }

    private static void retryWithoutFailures(
            TrackingSequence sequence,
            TrackingRecordFreer cursor,
            TrackingRecordFreer negativeLimitCursor
    ) {
        sequence.isFailureEnabled = false;
        cursor.isFailureEnabled = false;
        negativeLimitCursor.isFailureEnabled = false;
        AsyncFilteredRecordCursorFactory.halfCloseForTesting(sequence, cursor, negativeLimitCursor);
        Assert.assertEquals(2, sequence.closeCount);
        Assert.assertEquals(2, cursor.freeRecordsCount);
        Assert.assertEquals(2, negativeLimitCursor.freeRecordsCount);
    }

    private static class TrackingRecordFreer implements AsyncFilteredRecordCursorFactory.RecordFreer {
        private final RuntimeException failure;
        private final PageFrameMemoryPool frameMemoryPool;
        private final PageFrameMemoryRecord record;
        private final PageFrameMemoryRecord recordB;
        private int freeRecordsCount;
        private boolean isFailureEnabled = true;

        private TrackingRecordFreer(RuntimeException failure) {
            PageFrameMemoryRecord record = null;
            PageFrameMemoryRecord recordB = null;
            PageFrameMemoryPool frameMemoryPool;
            try {
                record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
                recordB = new PageFrameMemoryRecord(record, PageFrameMemoryRecord.RECORD_B_LETTER);
                frameMemoryPool = new PageFrameMemoryPool(configuration);
            } catch (Throwable th) {
                Misc.free(record);
                Misc.free(recordB);
                throw th;
            }
            this.failure = failure;
            this.frameMemoryPool = frameMemoryPool;
            this.record = record;
            this.recordB = recordB;
        }

        @Override
        public void freeRecords() {
            Misc.free(record);
            Misc.free(recordB);
            Misc.free(frameMemoryPool);
            freeRecordsCount++;
            if (isFailureEnabled && failure != null) {
                throw failure;
            }
        }
    }

    private static class TrackingSequence implements Closeable {
        private final RuntimeException failure;
        private int closeCount;
        private boolean isFailureEnabled = true;

        private TrackingSequence(RuntimeException failure) {
            this.failure = failure;
        }

        @Override
        public void close() {
            closeCount++;
            if (isFailureEnabled && failure != null) {
                throw failure;
            }
        }
    }
}
