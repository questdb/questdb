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

package io.questdb.test.cairo.wal;

import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.seq.TableMetadataChange;
import io.questdb.cairo.wal.seq.TableMetadataChangeLog;
import io.questdb.cairo.wal.seq.TableSequencerAPI;
import io.questdb.cairo.wal.seq.TableSequencerCursorPool;
import io.questdb.cairo.wal.seq.TransactionLogCursor;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.wal.WalUtils.WAL_SEQUENCER_FORMAT_VERSION_V1;
import static io.questdb.cairo.wal.WalUtils.WAL_SEQUENCER_FORMAT_VERSION_V2;

public class TableSequencerCursorPoolTest extends AbstractCairoTest {

    @Test
    public void testCloseAttemptsEveryResourceAndAggregatesFailures() throws Exception {
        assertMemoryLeak(() -> {
            final RuntimeException metadataFailure = new RuntimeException("metadata");
            final RuntimeException v1Failure = new RuntimeException("v1");
            final RuntimeException v2Failure = new RuntimeException("v2");
            final CloseCountingMetadataChangeLog metadataCursor =
                    new CloseCountingMetadataChangeLog(metadataFailure);
            final CloseCountingTransactionLogCursor v1Cursor =
                    new CloseCountingTransactionLogCursor(v1Failure);
            final CloseCountingTransactionLogCursor v2Cursor =
                    new CloseCountingTransactionLogCursor(v2Failure);

            final TableSequencerCursorPool pool = new TableSequencerCursorPool();
            TableSequencerCursorPoolTestSupport.setMetadataChangeLog(pool, metadataCursor);
            TableSequencerCursorPoolTestSupport.setTransactionLogCursor(
                    pool,
                    WAL_SEQUENCER_FORMAT_VERSION_V1,
                    v1Cursor
            );
            TableSequencerCursorPoolTestSupport.setTransactionLogCursor(
                    pool,
                    WAL_SEQUENCER_FORMAT_VERSION_V2,
                    v2Cursor
            );

            final RuntimeException thrown = Assert.assertThrows(RuntimeException.class, pool::close);
            Assert.assertSame(metadataFailure, thrown);
            Assert.assertArrayEquals(new Throwable[]{v1Failure, v2Failure}, thrown.getSuppressed());
            Assert.assertEquals(1, metadataCursor.closeCount);
            Assert.assertEquals(1, v1Cursor.closeCount);
            Assert.assertEquals(1, v2Cursor.closeCount);
        });
    }

    @Test
    public void testPoolsOwnIndependentReusableCursors() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE cursor_pool (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("ALTER TABLE cursor_pool ADD COLUMN value LONG");

            final TableToken tableToken = engine.verifyTableName("cursor_pool");
            final TableSequencerAPI sequencerAPI = engine.getTableSequencerAPI();
            try (
                    TableSequencerCursorPool poolA = new TableSequencerCursorPool();
                    TableSequencerCursorPool poolB = new TableSequencerCursorPool()
            ) {
                final TransactionLogCursor transactionCursorA = sequencerAPI.getCursor(tableToken, 0, poolA);
                final TransactionLogCursor transactionCursorB = sequencerAPI.getCursor(tableToken, 0, poolB);
                Assert.assertNotSame(transactionCursorA, transactionCursorB);
                Assert.assertTrue(transactionCursorA.hasNext());
                Assert.assertTrue(transactionCursorB.hasNext());
                transactionCursorA.close();
                transactionCursorB.close();

                try (
                        TransactionLogCursor reusedCursorA = sequencerAPI.getCursor(tableToken, 0, poolA);
                        TransactionLogCursor reusedCursorB = sequencerAPI.getCursor(tableToken, 0, poolB)
                ) {
                    Assert.assertSame(transactionCursorA, reusedCursorA);
                    Assert.assertSame(transactionCursorB, reusedCursorB);
                }

                final TableMetadataChangeLog metadataCursorA =
                        sequencerAPI.getMetadataChangeLogSlow(tableToken, 0, poolA);
                final TableMetadataChangeLog metadataCursorB =
                        sequencerAPI.getMetadataChangeLogSlow(tableToken, 0, poolB);
                Assert.assertNotSame(metadataCursorA, metadataCursorB);
                Assert.assertTrue(metadataCursorA.hasNext());
                Assert.assertTrue(metadataCursorB.hasNext());
                metadataCursorA.close();
                metadataCursorB.close();

                try (TableMetadataChangeLog reusedMetadataCursor =
                             sequencerAPI.getMetadataChangeLogSlow(tableToken, 0, poolA)) {
                    Assert.assertSame(metadataCursorA, reusedMetadataCursor);
                }
            }
        });
    }

    @Test
    public void testRegistrationFailureClosesCandidateForBothVersions() throws Exception {
        assertMemoryLeak(() -> {
            final CloseCountingTransactionLogCursor v1Owner = new CloseCountingTransactionLogCursor();
            final CloseCountingTransactionLogCursor v2Owner = new CloseCountingTransactionLogCursor();
            final CloseCountingTransactionLogCursor v1Candidate = new CloseCountingTransactionLogCursor();
            final CloseCountingTransactionLogCursor v2Candidate = new CloseCountingTransactionLogCursor();

            try (TableSequencerCursorPool pool = new TableSequencerCursorPool()) {
                TableSequencerCursorPoolTestSupport.setTransactionLogCursor(
                        pool,
                        WAL_SEQUENCER_FORMAT_VERSION_V1,
                        v1Owner
                );
                TableSequencerCursorPoolTestSupport.setTransactionLogCursor(
                        pool,
                        WAL_SEQUENCER_FORMAT_VERSION_V2,
                        v2Owner
                );

                final IllegalStateException v1Thrown = Assert.assertThrows(
                        IllegalStateException.class,
                        () -> TableSequencerCursorPoolTestSupport.registerTransactionLogCursor(
                                pool,
                                WAL_SEQUENCER_FORMAT_VERSION_V1,
                                v1Candidate
                        )
                );
                Assert.assertEquals("WAL sequencer V1 cursor is already configured", v1Thrown.getMessage());
                final IllegalStateException v2Thrown = Assert.assertThrows(
                        IllegalStateException.class,
                        () -> TableSequencerCursorPoolTestSupport.registerTransactionLogCursor(
                                pool,
                                WAL_SEQUENCER_FORMAT_VERSION_V2,
                                v2Candidate
                        )
                );
                Assert.assertEquals("WAL sequencer V2 cursor is already configured", v2Thrown.getMessage());

                Assert.assertEquals(1, v1Candidate.closeCount);
                Assert.assertEquals(1, v2Candidate.closeCount);
                Assert.assertEquals(0, v1Owner.closeCount);
                Assert.assertEquals(0, v2Owner.closeCount);
            }

            Assert.assertEquals(1, v1Owner.closeCount);
            Assert.assertEquals(1, v2Owner.closeCount);
        });
    }

    @Test
    public void testSettersRejectDistinctSecondOwner() throws Exception {
        assertMemoryLeak(() -> {
            final CloseCountingMetadataChangeLog metadataOwner = new CloseCountingMetadataChangeLog();
            final CloseCountingMetadataChangeLog metadataCandidate = new CloseCountingMetadataChangeLog();
            final CloseCountingTransactionLogCursor v1Owner = new CloseCountingTransactionLogCursor();
            final CloseCountingTransactionLogCursor v1Candidate = new CloseCountingTransactionLogCursor();
            final CloseCountingTransactionLogCursor v2Owner = new CloseCountingTransactionLogCursor();
            final CloseCountingTransactionLogCursor v2Candidate = new CloseCountingTransactionLogCursor();
            final CloseCountingTransactionLogCursor unsupportedCandidate = new CloseCountingTransactionLogCursor();

            try (TableSequencerCursorPool pool = new TableSequencerCursorPool()) {
                TableSequencerCursorPoolTestSupport.setMetadataChangeLog(pool, metadataOwner);
                TableSequencerCursorPoolTestSupport.setMetadataChangeLog(pool, metadataOwner);
                final IllegalStateException metadataThrown = Assert.assertThrows(
                        IllegalStateException.class,
                        () -> TableSequencerCursorPoolTestSupport.setMetadataChangeLog(pool, metadataCandidate)
                );
                Assert.assertEquals(
                        "table metadata change cursor is already configured",
                        metadataThrown.getMessage()
                );

                TableSequencerCursorPoolTestSupport.setTransactionLogCursor(
                        pool,
                        WAL_SEQUENCER_FORMAT_VERSION_V1,
                        v1Owner
                );
                TableSequencerCursorPoolTestSupport.setTransactionLogCursor(
                        pool,
                        WAL_SEQUENCER_FORMAT_VERSION_V1,
                        v1Owner
                );
                final IllegalStateException v1Thrown = Assert.assertThrows(
                        IllegalStateException.class,
                        () -> TableSequencerCursorPoolTestSupport.setTransactionLogCursor(
                                pool,
                                WAL_SEQUENCER_FORMAT_VERSION_V1,
                                v1Candidate
                        )
                );
                Assert.assertEquals("WAL sequencer V1 cursor is already configured", v1Thrown.getMessage());

                TableSequencerCursorPoolTestSupport.setTransactionLogCursor(
                        pool,
                        WAL_SEQUENCER_FORMAT_VERSION_V2,
                        v2Owner
                );
                TableSequencerCursorPoolTestSupport.setTransactionLogCursor(
                        pool,
                        WAL_SEQUENCER_FORMAT_VERSION_V2,
                        v2Owner
                );
                final IllegalStateException v2Thrown = Assert.assertThrows(
                        IllegalStateException.class,
                        () -> TableSequencerCursorPoolTestSupport.setTransactionLogCursor(
                                pool,
                                WAL_SEQUENCER_FORMAT_VERSION_V2,
                                v2Candidate
                        )
                );
                Assert.assertEquals("WAL sequencer V2 cursor is already configured", v2Thrown.getMessage());

                final IllegalArgumentException unsupportedThrown = Assert.assertThrows(
                        IllegalArgumentException.class,
                        () -> TableSequencerCursorPoolTestSupport.setTransactionLogCursor(
                                pool,
                                Integer.MAX_VALUE,
                                unsupportedCandidate
                        )
                );
                Assert.assertEquals(
                        "unsupported WAL sequencer format version [version=" + Integer.MAX_VALUE + ']',
                        unsupportedThrown.getMessage()
                );
            } finally {
                metadataCandidate.close();
                v1Candidate.close();
                v2Candidate.close();
                unsupportedCandidate.close();
            }

            Assert.assertEquals(1, metadataOwner.closeCount);
            Assert.assertEquals(1, metadataCandidate.closeCount);
            Assert.assertEquals(1, v1Owner.closeCount);
            Assert.assertEquals(1, v1Candidate.closeCount);
            Assert.assertEquals(1, v2Owner.closeCount);
            Assert.assertEquals(1, v2Candidate.closeCount);
            Assert.assertEquals(1, unsupportedCandidate.closeCount);
        });
    }

    private static class CloseCountingMetadataChangeLog implements TableMetadataChangeLog {
        private final RuntimeException closeFailure;
        private int closeCount;

        private CloseCountingMetadataChangeLog() {
            this(null);
        }

        private CloseCountingMetadataChangeLog(RuntimeException closeFailure) {
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
        public boolean hasNext() {
            throw new UnsupportedOperationException();
        }

        @Override
        public TableMetadataChange next() {
            throw new UnsupportedOperationException();
        }
    }

    private static class CloseCountingTransactionLogCursor implements TransactionLogCursor {
        private final RuntimeException closeFailure;
        private int closeCount;

        private CloseCountingTransactionLogCursor() {
            this(null);
        }

        private CloseCountingTransactionLogCursor(RuntimeException closeFailure) {
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
        public boolean extend() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getCommitTimestamp() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getMaxTxn() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getPartitionSize() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getSegmentId() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getSegmentTxn() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getStructureVersion() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getTxn() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getTxnMaxTimestamp() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getTxnMinTimestamp() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getTxnRowCount() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getVersion() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getWalId() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasNext() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setPosition(long txn) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void toMinTxn() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void toTop() {
            throw new UnsupportedOperationException();
        }
    }
}
