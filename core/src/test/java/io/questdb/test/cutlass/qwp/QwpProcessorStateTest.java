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

package io.questdb.test.cutlass.qwp;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.wal.DurableAckRegistry;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.processors.LineHttpProcessorConfiguration;
import io.questdb.cutlass.line.tcp.DefaultColumnTypes;
import io.questdb.cutlass.line.tcp.TableUpdateDetails;
import io.questdb.cutlass.line.tcp.WalTableUpdateDetails;
import io.questdb.cutlass.qwp.protocol.QwpArrayColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpColumnDef;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.protocol.QwpParseException;
import io.questdb.cutlass.qwp.protocol.QwpSchema;
import io.questdb.cutlass.qwp.protocol.QwpSchemaRegistry;
import io.questdb.cutlass.qwp.protocol.QwpTableBlockCursor;
import io.questdb.cutlass.qwp.server.QwpProcessorState;
import io.questdb.cutlass.qwp.server.QwpTudCache;
import io.questdb.std.LowerCaseUtf8SequenceObjHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.util.HashMap;

public class QwpProcessorStateTest extends AbstractCairoTest {

    @Test
    public void testAddDataIgnoresZeroLengthInput() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpProcessorState state = new QwpProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);

                // lo == hi → len=0 → early return
                long ptr = Unsafe.malloc(64, MemoryTag.NATIVE_HTTP_CONN);
                try {
                    state.addData(ptr, ptr);
                    Assert.assertTrue(state.isOk());
                } finally {
                    Unsafe.free(ptr, 64, MemoryTag.NATIVE_HTTP_CONN);
                }
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testAddDataRejectsWhenExceedingMaxBufferSize() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration) {
                        @Override
                        public long getMaxRecvBufferSize() {
                            return 256;
                        }
                    };
            QwpProcessorState state = new QwpProcessorState(64, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);

                // Add 200 bytes — should succeed (200 <= 256)
                long ptr = Unsafe.malloc(200, MemoryTag.NATIVE_HTTP_CONN);
                try {
                    state.addData(ptr, ptr + 200);
                    Assert.assertTrue("first addData should succeed", state.isOk());
                } finally {
                    Unsafe.free(ptr, 200, MemoryTag.NATIVE_HTTP_CONN);
                }

                // Add 100 more bytes — total 300 > 256, should reject
                ptr = Unsafe.malloc(100, MemoryTag.NATIVE_HTTP_CONN);
                try {
                    state.addData(ptr, ptr + 100);
                    Assert.assertFalse("should reject when exceeding max buffer size", state.isOk());
                } finally {
                    Unsafe.free(ptr, 100, MemoryTag.NATIVE_HTTP_CONN);
                }
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testAdvanceDurableWatermarkBlockedByEarliestTableInMultiTableCommit() throws Exception {
        // A single QWP message can commit to multiple tables. The consumer is
        // invoked once per table that committed. All segment entries from that
        // message share the same clientSeq. The watermark must not advance past
        // that clientSeq until ALL tables from that message are uploaded.
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpProcessorState state = new QwpProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);
                state.setDurableAckEnabled(true);
                FakeConsumerTudCache fake = installFakeTudCache(state, engine, lineConfig);

                // msg 0 writes to two tables in one commit.
                fake.queueCommit(
                        new String[]{"t1~1", "t2~1"},
                        new int[]{1, 2},
                        new int[]{0, 0},
                        new long[]{10L, 20L}
                );
                state.setHighestProcessedSequence(0);
                state.commit(0);

                // Only t1 is uploaded — t2 still pending.
                FakeDurableAckRegistry registry = new FakeDurableAckRegistry();
                registry.set("t1~1", 10L);
                state.advanceDurableWatermark(registry);
                // t2's segment has firstClientSeq=0, so watermark = 0 - 1 = -1.
                Assert.assertEquals(-1, state.getHighestDurableSequence());

                // t2 catches up — both tables from msg 0 are now durable.
                registry.set("t2~1", 20L);
                state.advanceDurableWatermark(registry);
                Assert.assertEquals(0, state.getHighestDurableSequence());
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testAdvanceDurableWatermarkCoalescesPerSegment() throws Exception {
        // Multiple commits to the same WAL segment should coalesce into a single
        // entry with the latest clientSeq. When that segment is uploaded, all
        // coalesced clientSeqs become durable.
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpProcessorState state = new QwpProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);
                state.setDurableAckEnabled(true);
                FakeConsumerTudCache fake = installFakeTudCache(state, engine, lineConfig);

                // Three commits all land in the same segment (walId=1, segmentId=0).
                fake.queueCommit(new String[]{"t~1"}, new int[]{1}, new int[]{0}, new long[]{10L});
                state.setHighestProcessedSequence(0);
                state.commit(0);
                fake.queueCommit(new String[]{"t~1"}, new int[]{1}, new int[]{0}, new long[]{11L});
                state.setHighestProcessedSequence(1);
                state.commit(1);
                fake.queueCommit(new String[]{"t~1"}, new int[]{1}, new int[]{0}, new long[]{12L});
                state.setHighestProcessedSequence(2);
                state.commit(2);

                // Upload catches up — the single segment entry covers all three msgs.
                FakeDurableAckRegistry registry = new FakeDurableAckRegistry();
                registry.set("t~1", 12L);
                state.advanceDurableWatermark(registry);
                Assert.assertEquals(2, state.getHighestDurableSequence());
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testAdvanceDurableWatermarkMultiTableMultiSegment() throws Exception {
        // With multiple tables, the watermark is blocked by the earliest
        // unuploaded segment's firstClientSeq.
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpProcessorState state = new QwpProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);
                state.setDurableAckEnabled(true);
                FakeConsumerTudCache fake = installFakeTudCache(state, engine, lineConfig);

                // msg 0: writes to T1 (walId=1, seg=0, seqTxn=10)
                fake.queueCommit(new String[]{"t1~1"}, new int[]{1}, new int[]{0}, new long[]{10L});
                state.setHighestProcessedSequence(0);
                state.commit(0);

                // msg 1: writes to T2 (walId=2, seg=0, seqTxn=5)
                fake.queueCommit(new String[]{"t2~1"}, new int[]{2}, new int[]{0}, new long[]{5L});
                state.setHighestProcessedSequence(1);
                state.commit(1);

                // T2 is uploaded but T1 is not.
                FakeDurableAckRegistry registry = new FakeDurableAckRegistry();
                registry.set("t2~1", 5L);
                state.advanceDurableWatermark(registry);
                // T1's segment has firstClientSeq=0, so watermark = 0 - 1 = -1.
                // T2's entry is removed (uploaded), but T1 blocks progress.
                Assert.assertEquals(-1, state.getHighestDurableSequence());

                // T1 catches up.
                registry.set("t1~1", 10L);
                state.advanceDurableWatermark(registry);
                // All segments uploaded → watermark = highestProcessedSequence = 1.
                Assert.assertEquals(1, state.getHighestDurableSequence());
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testAdvanceDurableWatermarkCatchesUpWhenQueueEmpty() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpProcessorState state = new QwpProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);
                state.setDurableAckEnabled(true);
                state.setHighestProcessedSequence(7);

                // Empty pending queue — watermark jumps straight to highestProcessedSequence
                // because every processed seq had its WAL already uploaded (or had no WAL).
                state.advanceDurableWatermark(new FakeDurableAckRegistry());
                Assert.assertEquals(7, state.getHighestDurableSequence());
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testAdvanceDurableWatermarkDrainsSatisfiedSegmentButStopsAtUnsatisfied() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpProcessorState state = new QwpProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);
                state.setDurableAckEnabled(true);
                FakeConsumerTudCache fake = installFakeTudCache(state, engine, lineConfig);

                // clientSeq=0 commits orders~1 in segment 0 at seqTxn 10
                fake.queueCommit(new String[]{"orders~1"}, new int[]{1}, new int[]{0}, new long[]{10L});
                state.setHighestProcessedSequence(0);
                state.commit(0);

                // clientSeq=1 commits orders~1 in segment 1 at seqTxn 20
                fake.queueCommit(new String[]{"orders~1"}, new int[]{1}, new int[]{1}, new long[]{20L});
                state.setHighestProcessedSequence(1);
                state.commit(1);

                // Uploads only up to seqTxn=15: segment 0 satisfied, segment 1 not.
                FakeDurableAckRegistry registry = new FakeDurableAckRegistry();
                registry.set("orders~1", 15L);

                state.advanceDurableWatermark(registry);
                Assert.assertEquals(0, state.getHighestDurableSequence());

                // Upload catches up → segment 1 becomes satisfied.
                registry.set("orders~1", 20L);
                state.advanceDurableWatermark(registry);
                Assert.assertEquals(1, state.getHighestDurableSequence());
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testAdvanceDurableWatermarkDrainsSatisfiedSegmentAcrossWalIdChange() throws Exception {
        // Writer eviction can assign a new walId to the same table mid-connection.
        // Each (table, walId, segmentId) triple is a distinct DurableSegmentEntry;
        // the watermark must track both independently.
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpProcessorState state = new QwpProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);
                state.setDurableAckEnabled(true);
                FakeConsumerTudCache fake = installFakeTudCache(state, engine, lineConfig);

                // msg 0: table t~1, walId=1, segmentId=0, seqTxn=10
                fake.queueCommit(new String[]{"t~1"}, new int[]{1}, new int[]{0}, new long[]{10L});
                state.setHighestProcessedSequence(0);
                state.commit(0);

                // msg 1: same table but new walId=2 (writer was evicted and re-opened)
                fake.queueCommit(new String[]{"t~1"}, new int[]{2}, new int[]{0}, new long[]{11L});
                state.setHighestProcessedSequence(1);
                state.commit(1);

                // Upload covers seqTxn=10 only — walId=1 segment satisfied, walId=2 not.
                FakeDurableAckRegistry registry = new FakeDurableAckRegistry();
                registry.set("t~1", 10L);
                state.advanceDurableWatermark(registry);
                Assert.assertEquals(0, state.getHighestDurableSequence());

                // Upload catches up to seqTxn=11 — both segments satisfied.
                registry.set("t~1", 11L);
                state.advanceDurableWatermark(registry);
                Assert.assertEquals(1, state.getHighestDurableSequence());
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testAdvanceDurableWatermarkDrainsWholeQueueThenCatchesUp() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpProcessorState state = new QwpProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);
                state.setDurableAckEnabled(true);
                FakeConsumerTudCache fake = installFakeTudCache(state, engine, lineConfig);

                fake.queueCommit(new String[]{"t~1"}, new int[]{1}, new int[]{0}, new long[]{10L});
                state.setHighestProcessedSequence(0);
                state.commit(0);
                fake.queueCommit(new String[]{"t~1"}, new int[]{1}, new int[]{0}, new long[]{20L});
                state.setHighestProcessedSequence(1);
                state.commit(1);

                // Two trailing empty messages — nothing added to pending.
                state.setHighestProcessedSequence(3);

                FakeDurableAckRegistry registry = new FakeDurableAckRegistry();
                registry.set("t~1", 25L);

                // Segment uploaded → catches up to processed=3.
                state.advanceDurableWatermark(registry);
                Assert.assertEquals(3, state.getHighestDurableSequence());
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testAdvanceDurableWatermarkDroppedTableUnblocks() throws Exception {
        // Regression test for C1: a dropped table must not permanently block
        // the durable watermark. The DurableUploadRegistry sets MAX_VALUE on
        // drop, which satisfies any pending DurableSegmentEntry.
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpProcessorState state = new QwpProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);
                state.setDurableAckEnabled(true);
                FakeConsumerTudCache fake = installFakeTudCache(state, engine, lineConfig);

                // msg 0 writes to t1 (seqTxn=10), msg 1 writes to t2 (seqTxn=5).
                fake.queueCommit(new String[]{"t1~1"}, new int[]{1}, new int[]{0}, new long[]{10L});
                state.setHighestProcessedSequence(0);
                state.commit(0);
                fake.queueCommit(new String[]{"t2~1"}, new int[]{2}, new int[]{0}, new long[]{5L});
                state.setHighestProcessedSequence(1);
                state.commit(1);

                // t1 is dropped (registry returns MAX_VALUE), t2 is uploaded normally.
                FakeDurableAckRegistry registry = new FakeDurableAckRegistry();
                registry.set("t1~1", Long.MAX_VALUE);
                registry.set("t2~1", 5L);

                state.advanceDurableWatermark(registry);
                Assert.assertEquals(1, state.getHighestDurableSequence());

                // Both entries satisfied — a durable ack is now pending delivery.
                Assert.assertTrue(state.hasPendingDurableAck());
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testAdvanceDurableWatermarkDroppedTableBeforeAnyUpload() throws Exception {
        // Edge case: table dropped before the uploader processes any segment.
        // The registry sentinel (MAX_VALUE) must still unblock the watermark.
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpProcessorState state = new QwpProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);
                state.setDurableAckEnabled(true);
                FakeConsumerTudCache fake = installFakeTudCache(state, engine, lineConfig);

                fake.queueCommit(new String[]{"dropped~1"}, new int[]{1}, new int[]{0}, new long[]{42L});
                state.setHighestProcessedSequence(0);
                state.commit(0);

                // No uploads happened, but the table was dropped → MAX_VALUE sentinel.
                FakeDurableAckRegistry registry = new FakeDurableAckRegistry();
                registry.set("dropped~1", Long.MAX_VALUE);

                state.advanceDurableWatermark(registry);
                Assert.assertEquals(0, state.getHighestDurableSequence());
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testAdvanceDurableWatermarkIsNoOpWhenDisabled() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpProcessorState state = new QwpProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);
                // durableAckEnabled stays false.
                state.setHighestProcessedSequence(5);

                state.advanceDurableWatermark(new FakeDurableAckRegistry());
                Assert.assertEquals(-1L, state.getHighestDurableSequence());
                Assert.assertFalse(state.hasPendingDurableAck());
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testAdvanceDurableWatermarkPartialCreditsEmptyGap() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpProcessorState state = new QwpProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);
                state.setDurableAckEnabled(true);
                FakeConsumerTudCache fake = installFakeTudCache(state, engine, lineConfig);

                // clientSeq=0 is empty (consumer not invoked), clientSeq=1 commits to t~1.
                state.setHighestProcessedSequence(0);
                fake.queueCommit(null, null, null, null);
                state.commit(0);
                state.setHighestProcessedSequence(1);
                fake.queueCommit(new String[]{"t~1"}, new int[]{1}, new int[]{0}, new long[]{42L});
                state.commit(1);

                // Nothing uploaded. The segment has firstClientSeq=1 (empty commits
                // produce no entries), so watermark = 1 - 1 = 0, crediting the empty msg.
                FakeDurableAckRegistry registry = new FakeDurableAckRegistry();
                state.advanceDurableWatermark(registry);
                Assert.assertEquals(0, state.getHighestDurableSequence());
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testAdvanceDurableWatermarkPingTriggeredDelivery() throws Exception {
        // Simulates the exact sequence that handlePing executes:
        // advanceDurableWatermark → hasPendingDurableAck → trySendDurableAck.
        // Verifies that a PING arriving after uploads complete (but with no new
        // binary messages) still produces a pending durable ack.
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpProcessorState state = new QwpProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);
                state.setDurableAckEnabled(true);
                FakeConsumerTudCache fake = installFakeTudCache(state, engine, lineConfig);

                // Client sends two messages that commit to different tables.
                fake.queueCommit(new String[]{"t1~1"}, new int[]{1}, new int[]{0}, new long[]{10L});
                state.setHighestProcessedSequence(0);
                state.commit(0);
                fake.queueCommit(new String[]{"t2~1"}, new int[]{2}, new int[]{0}, new long[]{5L});
                state.setHighestProcessedSequence(1);
                state.commit(1);

                // No uploads yet — no pending durable ack.
                FakeDurableAckRegistry registry = new FakeDurableAckRegistry();
                state.advanceDurableWatermark(registry);
                Assert.assertFalse(state.hasPendingDurableAck());

                // Uploads complete between messages (server-side, async).
                registry.set("t1~1", 10L);
                registry.set("t2~1", 5L);

                // PING arrives — the handler calls advanceDurableWatermark.
                state.advanceDurableWatermark(registry);
                Assert.assertEquals(1, state.getHighestDurableSequence());
                Assert.assertTrue(
                        "PING must trigger a pending durable ack after uploads complete",
                        state.hasPendingDurableAck()
                );

                // Simulate the trySendDurableAck success.
                state.onDurableAckSent(state.getHighestDurableSequence());
                Assert.assertFalse(state.hasPendingDurableAck());
                Assert.assertEquals(1, state.getLastDurablyAckedSequence());
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testCairoExceptionStatusReturnsInternalErrorForCriticalException() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpProcessorState state = new QwpProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);

                // Replace tudCache with one that throws a critical CairoException
                Field tudCacheField = QwpProcessorState.class.getDeclaredField("tudCache");
                tudCacheField.setAccessible(true);
                Misc.free((QwpTudCache) tudCacheField.get(state));
                DefaultColumnTypes defaultColumnTypes = new DefaultColumnTypes(lineConfig);
                tudCacheField.set(state, new QwpTudCache(engine, true, true, defaultColumnTypes, PartitionBy.DAY) {
                    @Override
                    public WalTableUpdateDetails getTableUpdateDetails(
                            SecurityContext secCtx, Utf8Sequence tableName,
                            ObjList<QwpColumnDef> schema, QwpTableBlockCursor cursor, int maxTables) {
                        throw CairoException.critical(0).put("simulated critical error");
                    }
                });

                // Send a minimal valid QWP message (0 columns, 0 rows)
                addNativeData(state, wrapQwpPayload(new byte[]{
                        4, 't', 'e', 's', 't',
                        0,    // rowCount=0
                        0,    // columnCount=0
                        0x00, // SCHEMA_MODE_FULL
                        0     // schemaId=0
                }));
                state.processMessage();
                Assert.assertEquals(QwpProcessorState.Status.INTERNAL_ERROR, state.getStatus());
                Assert.assertTrue(state.getErrorText().contains("simulated critical error"));
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testClearFreesResourcesWhenRollbackThrows() throws Exception {
        // When tud.rollback() throws during clear(), the cache enters the
        // distressed path: it frees all TUDs without rolling back and clears
        // the map. We trigger this by closing the TUD's WAL writer before
        // calling clear(), so rollback() hits a NullPointerException.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE clear_distress (ts TIMESTAMP, val INT) TIMESTAMP(ts) PARTITION BY DAY WAL");

            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            DefaultColumnTypes defaultColumnTypes = new DefaultColumnTypes(lineConfig);
            try (QwpTudCache cache = new QwpTudCache(
                    engine, true, true, defaultColumnTypes, PartitionBy.DAY)
            ) {
                WalTableUpdateDetails tud = cache.getTableUpdateDetails(
                        AllowAllSecurityContext.INSTANCE,
                        new Utf8String("clear_distress"),
                        null,
                        null,
                        1
                );
                Assert.assertNotNull(tud);

                // Close the TUD so its writerAPI becomes null.
                // This makes rollback() throw NullPointerException.
                tud.close();

                // clear() should catch the exception, enter the distressed
                // code path, free the TUD, and clear the map.
                cache.clear();
                Assert.assertEquals(0, getCacheSize(cache));
            }
        });
    }

    @Test
    public void testClearSkipsRollbackWhenDistressed() throws Exception {
        // When the cache is already distressed, clear() should skip
        // rollback and go straight to freeing all TUDs and clearing
        // the map.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE distressed_clear (ts TIMESTAMP, val INT) TIMESTAMP(ts) PARTITION BY DAY WAL");

            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            DefaultColumnTypes defaultColumnTypes = new DefaultColumnTypes(lineConfig);
            try (QwpTudCache cache = new QwpTudCache(
                    engine, true, true, defaultColumnTypes, PartitionBy.DAY)
            ) {
                WalTableUpdateDetails tud = cache.getTableUpdateDetails(
                        AllowAllSecurityContext.INSTANCE,
                        new Utf8String("distressed_clear"),
                        null,
                        null,
                        1
                );
                Assert.assertNotNull(tud);

                // Mark cache as distressed before calling clear().
                cache.setDistressed();
                cache.clear();
                Assert.assertEquals(0, getCacheSize(cache));
            }
        });
    }

    @Test
    public void testCloseAfterDisconnectFreesNativeMemory() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpProcessorState state = new QwpProcessorState(1024, 4096, engine, lineConfig);
            state.of(1, AllowAllSecurityContext.INSTANCE);
            // Simulate the fixed onConnectionClosed lifecycle:
            // onDisconnected() resets per-connection state (WAL writers, symbol caches),
            // close() frees native memory (bufferAddress, ddlMem, path, symbolCachePool).
            // Before the fix, only onDisconnected() was called, leaking native memory.
            state.onDisconnected();
            state.close();
        });
    }

    @Test
    public void testCommitAllBestEffortHandlesDroppedTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE be_drop (ts TIMESTAMP, val INT) TIMESTAMP(ts) PARTITION BY DAY WAL");

            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            DefaultColumnTypes defaultColumnTypes = new DefaultColumnTypes(lineConfig);
            try (QwpTudCache cache = new QwpTudCache(
                    engine, true, true, defaultColumnTypes, PartitionBy.DAY)
            ) {
                WalTableUpdateDetails tud = cache.getTableUpdateDetails(
                        AllowAllSecurityContext.INSTANCE,
                        new Utf8String("be_drop"),
                        null,
                        null,
                        1
                );
                Assert.assertNotNull(tud);

                replaceWriterWithFake(tud, true);
                Assert.assertEquals(1, getCacheSize(cache));

                // Should catch the table-dropped CommitFailedException,
                // mark the TUD as dropped, remove it, and free it.
                cache.commitAllBestEffort();
                Assert.assertEquals(0, getCacheSize(cache));
            }
        });
    }

    @Test
    public void testCommitAllBestEffortNonDropCommitFailure() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE be_fail (ts TIMESTAMP, val INT) TIMESTAMP(ts) PARTITION BY DAY WAL");

            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            DefaultColumnTypes defaultColumnTypes = new DefaultColumnTypes(lineConfig);
            try (QwpTudCache cache = new QwpTudCache(
                    engine, true, true, defaultColumnTypes, PartitionBy.DAY)
            ) {
                WalTableUpdateDetails tud = cache.getTableUpdateDetails(
                        AllowAllSecurityContext.INSTANCE,
                        new Utf8String("be_fail"),
                        null,
                        null,
                        1
                );
                Assert.assertNotNull(tud);

                replaceWriterWithFake(tud, false);

                // Should log the error and continue without throwing.
                cache.commitAllBestEffort();

                // TUD stays in the cache (not removed on non-drop failure)
                // and its writer is marked as being in error state.
                Assert.assertEquals(1, getCacheSize(cache));
                Assert.assertTrue(tud.isWriterInError());
            }
        });
    }

    @Test
    public void testCommitAllBestEffortSkipsAlreadyDroppedTud() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE be_skip_1 (ts TIMESTAMP, val INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE be_skip_2 (ts TIMESTAMP, val INT) TIMESTAMP(ts) PARTITION BY DAY WAL");

            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            DefaultColumnTypes defaultColumnTypes = new DefaultColumnTypes(lineConfig);
            try (QwpTudCache cache = new QwpTudCache(
                    engine, true, true, defaultColumnTypes, PartitionBy.DAY)
            ) {
                WalTableUpdateDetails tud1 = cache.getTableUpdateDetails(
                        AllowAllSecurityContext.INSTANCE,
                        new Utf8String("be_skip_1"),
                        null,
                        null,
                        2
                );
                Assert.assertNotNull(tud1);

                WalTableUpdateDetails tud2 = cache.getTableUpdateDetails(
                        AllowAllSecurityContext.INSTANCE,
                        new Utf8String("be_skip_2"),
                        null,
                        null,
                        2
                );
                Assert.assertNotNull(tud2);

                // Mark one TUD as already dropped before calling
                // commitAllBestEffort(). The loop should skip its
                // commit, remove it, and continue to the other TUD.
                Assert.assertEquals(2, getCacheSize(cache));
                tud1.setIsDropped();

                cache.commitAllBestEffort();

                // Only the non-dropped TUD remains in the cache.
                Assert.assertEquals(1, getCacheSize(cache));
                Assert.assertFalse(tud2.isDropped());
            }
        });
    }

    @Test
    public void testCommitAllInvokesConsumerWithDirName() throws Exception {
        // Regression test for the C1 bug: the consumer must receive the on-disk
        // directory name (e.g. "dir_vs_name~<tableId>"), not the client-facing
        // table name. The durable-upload registry is keyed by dir name because
        // that's what the Rust uploader uses as the key.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE dir_vs_name (ts TIMESTAMP, val INT) TIMESTAMP(ts) PARTITION BY DAY WAL");

            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            DefaultColumnTypes defaultColumnTypes = new DefaultColumnTypes(lineConfig);
            try (QwpTudCache cache = new QwpTudCache(
                    engine, true, true, defaultColumnTypes, PartitionBy.DAY)
            ) {
                WalTableUpdateDetails tud = cache.getTableUpdateDetails(
                        AllowAllSecurityContext.INSTANCE,
                        new Utf8String("dir_vs_name"),
                        null,
                        null,
                        1
                );
                Assert.assertNotNull(tud);
                replaceWriterWithNoRowFakeReportingSeqTxn(tud, 42L);

                ObjList<Utf8String> captured = new ObjList<>();
                long[] capturedSeq = new long[]{Long.MIN_VALUE};
                try {
                    cache.commitAll((tableDirName, walId, segmentId, seqTxn) -> {
                        captured.add(new Utf8String(tableDirName.toString()));
                        capturedSeq[0] = seqTxn;
                    });
                } catch (Exception e) {
                    throw e;
                } catch (Throwable t) {
                    throw new AssertionError("unexpected throwable", t);
                }

                Assert.assertEquals(1, captured.size());
                String dirName = tud.getTableToken().getDirName();
                String tableName = tud.getTableToken().getTableName();
                Assert.assertEquals("consumer must see dir name", dirName, captured.get(0).toString());
                Assert.assertNotEquals("dir name and table name must differ for WAL tables",
                        tableName, dirName);
                Assert.assertEquals(42L, capturedSeq[0]);
            }
        });
    }

    @Test
    public void testCommitAllRemovesDroppedTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE commit_drop (ts TIMESTAMP, val INT) TIMESTAMP(ts) PARTITION BY DAY WAL");

            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            DefaultColumnTypes defaultColumnTypes = new DefaultColumnTypes(lineConfig);
            try (QwpTudCache cache = new QwpTudCache(
                    engine, true, true, defaultColumnTypes, PartitionBy.DAY)
            ) {
                WalTableUpdateDetails tud = cache.getTableUpdateDetails(
                        AllowAllSecurityContext.INSTANCE,
                        new Utf8String("commit_drop"),
                        null,
                        null,
                        1
                );
                Assert.assertNotNull(tud);

                // Replace the real writer with a fake that simulates a
                // table-dropped commit failure. This exercises the
                // catch (CommitFailedException) branch where
                // e.isTableDropped() returns true, followed by the
                // if (tud.isDropped()) removal path.
                replaceWriterWithFake(tud, true);

                // commitAll() should catch the CommitFailedException, mark
                // the TUD as dropped, remove it from the cache, and free it.
                Assert.assertEquals(1, getCacheSize(cache));
                try {
                    cache.commitAll();
                } catch (Exception e) {
                    throw e;
                } catch (Throwable t) {
                    throw new AssertionError("unexpected throwable", t);
                }
                Assert.assertEquals(0, getCacheSize(cache));
            }
        });
    }

    @Test
    public void testCommitAllRethrowsNonDropCommitFailure() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE commit_fail (ts TIMESTAMP, val INT) TIMESTAMP(ts) PARTITION BY DAY WAL");

            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            DefaultColumnTypes defaultColumnTypes = new DefaultColumnTypes(lineConfig);
            try (QwpTudCache cache = new QwpTudCache(
                    engine, true, true, defaultColumnTypes, PartitionBy.DAY)
            ) {
                WalTableUpdateDetails tud = cache.getTableUpdateDetails(
                        AllowAllSecurityContext.INSTANCE,
                        new Utf8String("commit_fail"),
                        null,
                        null,
                        1
                );
                Assert.assertNotNull(tud);

                // Replace the real writer with a fake that simulates a
                // non-drop commit failure. This exercises the
                // catch (CommitFailedException) branch where
                // e.isTableDropped() returns false, causing commitAll()
                // to re-throw the original exception.
                replaceWriterWithFake(tud, false);

                try {
                    cache.commitAll();
                    Assert.fail("commitAll() should have re-thrown the commit failure");
                } catch (CairoException e) {
                    Assert.assertFalse(e.isTableDropped());
                } catch (Throwable t) {
                    throw new AssertionError("unexpected throwable type", t);
                }
            }
        });
    }

    @Test
    public void testCommitAllSkipsAlreadyDroppedTud() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE skip_1 (ts TIMESTAMP, val INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE skip_2 (ts TIMESTAMP, val INT) TIMESTAMP(ts) PARTITION BY DAY WAL");

            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            DefaultColumnTypes defaultColumnTypes = new DefaultColumnTypes(lineConfig);
            try (QwpTudCache cache = new QwpTudCache(
                    engine, true, true, defaultColumnTypes, PartitionBy.DAY)
            ) {
                WalTableUpdateDetails tud1 = cache.getTableUpdateDetails(
                        AllowAllSecurityContext.INSTANCE,
                        new Utf8String("skip_1"),
                        null,
                        null,
                        2
                );
                Assert.assertNotNull(tud1);

                WalTableUpdateDetails tud2 = cache.getTableUpdateDetails(
                        AllowAllSecurityContext.INSTANCE,
                        new Utf8String("skip_2"),
                        null,
                        null,
                        2
                );
                Assert.assertNotNull(tud2);

                // Mark one TUD as already dropped before calling
                // commitAll(). The loop should skip its commit,
                // remove it, and continue to the other TUD.
                Assert.assertEquals(2, getCacheSize(cache));
                tud1.setIsDropped();

                try {
                    cache.commitAll();
                } catch (Throwable t) {
                    throw new AssertionError("unexpected throwable", t);
                }

                // Only the non-dropped TUD remains in the cache.
                Assert.assertEquals(1, getCacheSize(cache));
                Assert.assertFalse(tud2.isDropped());
            }
        });
    }

    @Test
    public void testCommitAllSkipsConsumerWhenFirstRow() throws Exception {
        // When no uncommitted rows are pending, tud.isFirstRow() returns true and
        // the consumer must NOT be invoked — the commit is a no-op and wouldn't
        // advance the sequencer txn.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE first_row_skip (ts TIMESTAMP, val INT) TIMESTAMP(ts) PARTITION BY DAY WAL");

            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            DefaultColumnTypes defaultColumnTypes = new DefaultColumnTypes(lineConfig);
            try (QwpTudCache cache = new QwpTudCache(
                    engine, true, true, defaultColumnTypes, PartitionBy.DAY)
            ) {
                WalTableUpdateDetails tud = cache.getTableUpdateDetails(
                        AllowAllSecurityContext.INSTANCE,
                        new Utf8String("first_row_skip"),
                        null,
                        null,
                        1
                );
                Assert.assertNotNull(tud);
                // Fresh writer reports zero uncommitted rows → isFirstRow() is true.
                replaceWriterWithNoRowFakeReportingSeqTxn(tud, 99L);
                Field writerField = TableUpdateDetails.class.getDeclaredField("writerAPI");
                writerField.setAccessible(true);
                TableWriterAPI w = (TableWriterAPI) writerField.get(tud);
                Misc.free(w);
                writerField.set(tud, Proxy.newProxyInstance(
                        TableWriterAPI.class.getClassLoader(),
                        new Class[]{TableWriterAPI.class},
                        (proxy, method, args) -> switch (method.getName()) {
                            case "getUncommittedRowCount" -> 0L;
                            case "getLastSeqTxn" -> 99L;
                            case "getWalId" -> 1;
                            case "getSegmentId" -> 0;
                            case "commit", "close", "rollback" -> null;
                            default -> null;
                        }
                ));

                boolean[] invoked = new boolean[]{false};
                try {
                    cache.commitAll((tableDirName, walId, segmentId, seqTxn) -> invoked[0] = true);
                } catch (Exception e) {
                    throw e;
                } catch (Throwable t) {
                    throw new AssertionError("unexpected throwable", t);
                }
                Assert.assertFalse("consumer must be skipped when no rows to commit", invoked[0]);
            }
        });
    }

    @Test
    public void testCommitReleasesDurableEntryOnEmptyCommit() throws Exception {
        // A commit that advances nothing (consumer never called) must release the
        // durable entry back to the pool rather than queueing it — otherwise the
        // pending queue fills with empty entries and hasPendingDurableAck lies.
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpProcessorState state = new QwpProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);
                state.setDurableAckEnabled(true);
                FakeConsumerTudCache fake = installFakeTudCache(state, engine, lineConfig);

                fake.queueCommit(null, null, null, null);
                state.setHighestProcessedSequence(0);
                state.commit(0);

                // No segment entry should be created — advance sees empty and catches up.
                state.advanceDurableWatermark(new FakeDurableAckRegistry());
                Assert.assertEquals(0, state.getHighestDurableSequence());
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testCommitReleasesDurableEntryOnFailure() throws Exception {
        // When commitAll throws, the partially-built pending entry must be released
        // back to the pool — otherwise repeated commit failures leak entries.
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpProcessorState state = new QwpProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);
                state.setDurableAckEnabled(true);
                FakeConsumerTudCache fake = installFakeTudCache(state, engine, lineConfig);

                fake.queueCommitThrow(CairoException.nonCritical().put("simulated"));
                state.setHighestProcessedSequence(0);
                state.commit(0);

                // State rejected the commit; no durable entry should remain queued.
                Assert.assertFalse(state.isOk());
                state.advanceDurableWatermark(new FakeDurableAckRegistry());
                // Empty queue → watermark catches up to processed.
                Assert.assertEquals(0, state.getHighestDurableSequence());
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testCommitWithDisabledDurableAckIsNoOp() throws Exception {
        // When durable-ack is disabled, commit() must not acquire a durable entry,
        // and hasPendingDurableAck stays false regardless of processed sequence.
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpProcessorState state = new QwpProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);
                FakeConsumerTudCache fake = installFakeTudCache(state, engine, lineConfig);

                // Consumer is passed as null when disabled — even if the fake had
                // queued data, it would be ignored because the code path skips it.
                fake.queueCommit(new String[]{"t~1"}, new int[]{1}, new int[]{0}, new long[]{10L});
                state.setHighestProcessedSequence(5);
                state.commit(5);

                state.advanceDurableWatermark(new FakeDurableAckRegistry());
                Assert.assertEquals(-1L, state.getHighestDurableSequence());
                Assert.assertFalse(state.hasPendingDurableAck());
                Assert.assertFalse(state.isDurableAckEnabled());
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testDoubleCloseIsSafe() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpProcessorState state = new QwpProcessorState(1024, 4096, engine, lineConfig);
            state.of(1, AllowAllSecurityContext.INSTANCE);
            state.onDisconnected();
            // close() may be called twice: once explicitly and once via
            // LocalValueMap.set(key, null) which calls Misc.freeIfCloseable().
            state.close();
            state.close();
        });
    }

    @Test
    public void testGetTableUpdateDetailsAutoCreatesTableWithTimestampNanos() throws Exception {
        // Exercises the TYPE_TIMESTAMP_NANOS branch in the
        // QwpTableStructureAdapter constructor's designated-timestamp
        // detection loop.
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            DefaultColumnTypes defaultColumnTypes = new DefaultColumnTypes(lineConfig);
            try (QwpTudCache cache = new QwpTudCache(
                    engine, true, true, defaultColumnTypes, PartitionBy.DAY)
            ) {
                ObjList<QwpColumnDef> schema = new ObjList<>();
                schema.add(new QwpColumnDef("val", QwpConstants.TYPE_INT));
                schema.add(new QwpColumnDef("", QwpConstants.TYPE_TIMESTAMP_NANOS));

                WalTableUpdateDetails tud = cache.getTableUpdateDetails(
                        AllowAllSecurityContext.INSTANCE,
                        new Utf8String("ts_nanos_test"),
                        schema,
                        null,
                        1
                );
                Assert.assertNotNull(tud);

                // Verify the created table's designated timestamp column
                // is TIMESTAMP_NANO (not plain TIMESTAMP).
                try (TableReader reader = engine.getReader("ts_nanos_test")) {
                    int tsIndex = reader.getMetadata().getTimestampIndex();
                    Assert.assertTrue(tsIndex >= 0);
                    Assert.assertEquals(
                            ColumnType.TIMESTAMP_NANO,
                            reader.getMetadata().getColumnType(tsIndex)
                    );
                }
            }
        });
    }

    @Test
    public void testGetTableUpdateDetailsFreesWriterOnFailure() throws Exception {
        // Exercises the catch(Throwable) block in QwpTudCache.getTableUpdateDetails()
        // that frees the WAL writer when the try block fails after the writer
        // has been acquired. We inject a map subclass that throws from putAt(),
        // which fires after the WalTableUpdateDetails is successfully constructed
        // but before it is returned.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tud_fail (ts TIMESTAMP, val INT) TIMESTAMP(ts) PARTITION BY DAY WAL");

            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            DefaultColumnTypes defaultColumnTypes = new DefaultColumnTypes(lineConfig);
            try (QwpTudCache cache = new QwpTudCache(
                    engine, true, true, defaultColumnTypes, PartitionBy.DAY)
            ) {
                // Replace the internal map with one whose putAt() always throws.
                // keyIndex() still works (read-only), so the production code
                // reaches the try block, creates the TUD and WAL writer, then
                // crashes on putAt(). The catch block must free the TUD (and
                // its writer) to avoid a native memory leak.
                Field mapField = QwpTudCache.class.getDeclaredField("tableUpdateDetails");
                mapField.setAccessible(true);
                mapField.set(cache, new LowerCaseUtf8SequenceObjHashMap<WalTableUpdateDetails>() {
                    @Override
                    public boolean putAt(int index, Utf8String key, WalTableUpdateDetails value) {
                        throw new RuntimeException("simulated map failure");
                    }
                });

                try {
                    cache.getTableUpdateDetails(
                            AllowAllSecurityContext.INSTANCE,
                            new Utf8String("tud_fail"),
                            null,
                            null,
                            10
                    );
                    Assert.fail("should have thrown RuntimeException");
                } catch (RuntimeException e) {
                    Assert.assertEquals("simulated map failure", e.getMessage());
                }
            }
        });
    }

    @Test
    public void testGetTableUpdateDetailsRejectsInvalidDeferredArrayColumnName() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            DefaultColumnTypes defaultColumnTypes = new DefaultColumnTypes(lineConfig);
            final long addr = Unsafe.malloc(2, MemoryTag.NATIVE_DEFAULT);
            try (QwpTudCache cache = new QwpTudCache(
                    engine, true, true, defaultColumnTypes, PartitionBy.DAY)
            ) {
                Unsafe.getUnsafe().putByte(addr, (byte) 1);
                Unsafe.getUnsafe().putByte(addr + 1, (byte) 0x01);

                final QwpTableBlockCursor cursor = getQwpTableBlockCursor(addr);

                final String tableName = "invalid_deferred_array_col";
                final ObjList<QwpColumnDef> schema = new ObjList<>();
                schema.add(new QwpColumnDef("bad-name", QwpConstants.TYPE_DOUBLE_ARRAY));

                WalTableUpdateDetails tud = cache.getTableUpdateDetails(
                        AllowAllSecurityContext.INSTANCE,
                        new Utf8String(tableName),
                        schema,
                        cursor,
                        1
                );
                Assert.assertNull(tud);
                Assert.assertNull(engine.getTableTokenIfExists(tableName));
            } finally {
                Unsafe.free(addr, 2, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testGetTableUpdateDetailsReturnsNullForInvalidColumnName() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            DefaultColumnTypes defaultColumnTypes = new DefaultColumnTypes(lineConfig);
            try (QwpTudCache cache = new QwpTudCache(
                    engine, true, true, defaultColumnTypes, PartitionBy.DAY)
            ) {
                ObjList<QwpColumnDef> schema = new ObjList<>();
                schema.add(new QwpColumnDef("inv?lid", QwpConstants.TYPE_INT));
                schema.add(new QwpColumnDef("", QwpConstants.TYPE_TIMESTAMP));

                WalTableUpdateDetails tud = cache.getTableUpdateDetails(
                        AllowAllSecurityContext.INSTANCE,
                        new Utf8String("invalid_col_test"),
                        schema,
                        null,
                        1
                );
                Assert.assertNull(tud);
            }
        });
    }

    @Test
    public void testGetTableUpdateDetailsReturnsNullForInvalidTableName() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            DefaultColumnTypes defaultColumnTypes = new DefaultColumnTypes(lineConfig);
            try (QwpTudCache cache = new QwpTudCache(
                    engine, true, true, defaultColumnTypes, PartitionBy.DAY)
            ) {
                // ".." is an invalid table name (starts with a dot)
                WalTableUpdateDetails tud = cache.getTableUpdateDetails(
                        AllowAllSecurityContext.INSTANCE,
                        new Utf8String(".."),
                        null,
                        null,
                        1
                );
                Assert.assertNull(tud);
            }
        });
    }

    @Test
    public void testGetTableUpdateDetailsReturnsNullForMatView() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE mv_base (ts TIMESTAMP, val INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE MATERIALIZED VIEW mv_target AS (SELECT ts, count() cnt FROM mv_base SAMPLE BY 1h)");

            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            DefaultColumnTypes defaultColumnTypes = new DefaultColumnTypes(lineConfig);
            try (QwpTudCache cache = new QwpTudCache(
                    engine, true, true, defaultColumnTypes, PartitionBy.DAY)
            ) {
                WalTableUpdateDetails tud = cache.getTableUpdateDetails(
                        AllowAllSecurityContext.INSTANCE,
                        new Utf8String("mv_target"),
                        null,
                        null,
                        1
                );
                Assert.assertNull(tud);
            }
        });
    }

    @Test
    public void testGetTableUpdateDetailsReturnsNullWhenAutoCreateColumnsDisabled() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            DefaultColumnTypes defaultColumnTypes = new DefaultColumnTypes(lineConfig);
            // autoCreateNewColumns=false, autoCreateNewTables=true
            try (QwpTudCache cache = new QwpTudCache(
                    engine, false, true, defaultColumnTypes, PartitionBy.DAY)
            ) {
                WalTableUpdateDetails tud = cache.getTableUpdateDetails(
                        AllowAllSecurityContext.INSTANCE,
                        new Utf8String("nonexistent_table"),
                        null,
                        null,
                        1
                );
                Assert.assertNull(tud);
            }
        });
    }

    @Test
    public void testGetTableUpdateDetailsThrowsWhenMaxTablesExceeded() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE max_tbl (ts TIMESTAMP, val INT) TIMESTAMP(ts) PARTITION BY DAY WAL");

            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            DefaultColumnTypes defaultColumnTypes = new DefaultColumnTypes(lineConfig);
            try (QwpTudCache cache = new QwpTudCache(
                    engine, true, true, defaultColumnTypes, PartitionBy.DAY)
            ) {
                WalTableUpdateDetails tud = cache.getTableUpdateDetails(
                        AllowAllSecurityContext.INSTANCE,
                        new Utf8String("max_tbl"),
                        null,
                        null,
                        1
                );
                Assert.assertNotNull(tud);

                try {
                    cache.getTableUpdateDetails(
                            AllowAllSecurityContext.INSTANCE,
                            new Utf8String("another_table"),
                            null,
                            null,
                            1
                    );
                    Assert.fail("should have thrown CairoException");
                } catch (CairoException e) {
                    Assert.assertTrue(e.getMessage().contains("too many distinct tables"));
                }
            }
        });
    }

    @Test
    public void testOnErrorBlockedPreservesResumeDurableAck() throws Exception {
        // Regression for M1: if a durable-ack send is in flight and an error
        // needs to be deferred, the send state must transition into the compound
        // RESUME_DURABLE_ACK_THEN_ERROR (=5) so the in-flight frame isn't dropped.
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpProcessorState state = new QwpProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);

                // Durable ACK blocked → RESUME_DURABLE_ACK (=4)
                state.onDurableAckBlocked(3);
                Assert.assertEquals(4, state.getSendState());

                // Error blocked while in RESUME_DURABLE_ACK → RESUME_DURABLE_ACK_THEN_ERROR (=5)
                state.onErrorBlocked((byte) 1, 4, "boom");
                Assert.assertEquals(5, state.getSendState());

                // Another error while already in the compound state stays in the compound state.
                state.onErrorBlocked((byte) 1, 5, "boom2");
                Assert.assertEquals(5, state.getSendState());
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testOnResumeDurableAckThenErrorTransition() throws Exception {
        // Full lifecycle: durable-ack blocked → error blocked → resume durable
        // ack complete → deferred error still pending → resume error complete.
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpProcessorState state = new QwpProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);
                state.setDurableAckEnabled(true);

                // Durable ACK blocked → RESUME_DURABLE_ACK (=4)
                state.onDurableAckBlocked(7);
                Assert.assertEquals(4, state.getSendState());

                // Error arrives while the durable-ack frame is in flight →
                // RESUME_DURABLE_ACK_THEN_ERROR (=5)
                state.onErrorBlocked((byte) 6, 10, "write error");
                Assert.assertEquals(5, state.getSendState());

                // IO loop finishes the durable-ack send → should transition to
                // READY so the deferred error can be sent next.
                state.onResumeDurableAckComplete();
                Assert.assertEquals(0, state.getSendState());
                Assert.assertEquals(7, state.getLastDurablyAckedSequence());

                // Deferred error is still pending.
                Assert.assertEquals(10, state.getDeferredErrorSequence());
                Assert.assertEquals(6, state.getDeferredErrorStatus());
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testOnErrorBlockedTransitionsToAckThenError() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpProcessorState state = new QwpProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);

                // ACK blocked → RESUME_ACK (sendState=1)
                state.onAckBlocked(5);
                Assert.assertEquals(1, state.getSendState());

                // Error blocked while in RESUME_ACK → RESUME_ACK_THEN_ERROR (sendState=3)
                state.onErrorBlocked((byte) 1, 6, "test error");
                Assert.assertEquals(3, state.getSendState());
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testOnErrorBlockedWithNullMessage() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpProcessorState state = new QwpProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);

                state.onErrorBlocked((byte) 7, 42, null);

                // sendState = SEND_STATE_RESUME_ERROR (2)
                Assert.assertEquals(2, state.getSendState());
                Assert.assertEquals(7, state.getDeferredErrorStatus());
                Assert.assertEquals(42, state.getDeferredErrorSequence());
                Assert.assertEquals(0, state.getDeferredErrorMessage().length());
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testProcessMessageRejectsSchemaMismatch() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpProcessorState state = new QwpProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);

                // Pre-register schema 0 with 2 columns via reflection
                Field decoderField = QwpProcessorState.class.getDeclaredField("streamingDecoder");
                decoderField.setAccessible(true);
                Object decoder = decoderField.get(state);
                Field registryField = decoder.getClass().getDeclaredField("schemaRegistry");
                registryField.setAccessible(true);
                QwpSchemaRegistry registry = (QwpSchemaRegistry) registryField.get(decoder);
                registry.put(0, QwpSchema.create(new QwpColumnDef[]{
                        new QwpColumnDef("val", QwpConstants.TYPE_INT),
                        new QwpColumnDef("", QwpConstants.TYPE_TIMESTAMP)
                }));

                // Reference schema 0 but declare 3 columns (mismatch with 2)
                addNativeData(state, wrapQwpPayload(new byte[]{
                        7, 's', 'm', '_', 't', 'e', 's', 't',
                        0,                              // rowCount=0
                        3,                              // columnCount=3 (schema has 2)
                        0x01,                           // SCHEMA_MODE_REFERENCE
                        0                               // schemaId=0
                }));
                state.processMessage();
                Assert.assertEquals(QwpProcessorState.Status.SCHEMA_MISMATCH, state.getStatus());
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testProcessMessageReturnsEarlyWhenBufferEmpty() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpProcessorState state = new QwpProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);

                // No data added → bufferPosition==0 → early return
                state.processMessage();
                Assert.assertTrue(state.isOk());
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testProcessMessageReturnsEarlyWhenRejected() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpProcessorState state = new QwpProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);

                state.reject(QwpProcessorState.Status.PARSE_ERROR, "initial error", 1);
                Assert.assertFalse(state.isOk());

                // Add some data so bufferPosition > 0
                long ptr = Unsafe.malloc(64, MemoryTag.NATIVE_HTTP_CONN);
                try {
                    state.addData(ptr, ptr + 64);
                } finally {
                    Unsafe.free(ptr, 64, MemoryTag.NATIVE_HTTP_CONN);
                }

                // processMessage returns early because !isOk()
                state.processMessage();
                Assert.assertEquals("initial error", state.getErrorText());
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testRejectPreservesShortErrorMessage() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpProcessorState state = new QwpProcessorState(1024, 250, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);

                String shortError = "something went wrong";
                state.reject(QwpProcessorState.Status.PARSE_ERROR, shortError, 1);

                Assert.assertEquals(shortError, state.getErrorText());
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testRejectTruncatesLongErrorMessage() throws Exception {
        assertMemoryLeak(() -> {
            // maxResponseContentLength=250 → maxResponseErrorMessageLength = (250-100)/1.5 = 100
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpProcessorState state = new QwpProcessorState(1024, 250, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);

                // Build a 200-char error message, well above the 100-char limit
                String longError = "x".repeat(200);
                state.reject(QwpProcessorState.Status.INTERNAL_ERROR, longError, 1);

                String errorText = state.getErrorText();
                Assert.assertEquals(100, errorText.length());
                Assert.assertEquals(longError.substring(0, 100), errorText);
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testRejectWithNullErrorText() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpProcessorState state = new QwpProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);

                state.reject(QwpProcessorState.Status.INTERNAL_ERROR, null, 1);
                Assert.assertFalse(state.isOk());
                Assert.assertEquals("(no error message)", state.getErrorText());
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testShouldSendAckReturnsFalseWhenSending() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpProcessorState state = new QwpProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);

                // Set up sequences so the threshold IS met
                state.setHighestProcessedSequence(10);
                // lastAckedSequence defaults to -1, so gap=11 >= batchSize=1

                // Block ACK → sendState != READY
                state.onAckBlocked(5);
                Assert.assertFalse(state.shouldSendAck(1));
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    private static FakeConsumerTudCache installFakeTudCache(
            QwpProcessorState state, io.questdb.cairo.CairoEngine engine, LineHttpProcessorConfiguration lineConfig
    ) throws Exception {
        Field f = QwpProcessorState.class.getDeclaredField("tudCache");
        f.setAccessible(true);
        Misc.free((QwpTudCache) f.get(state));
        FakeConsumerTudCache fake = new FakeConsumerTudCache(engine, lineConfig);
        f.set(state, fake);
        return fake;
    }

    private static void replaceWriterWithNoRowFakeReportingSeqTxn(WalTableUpdateDetails tud, long seqTxn) throws Exception {
        Field writerField = TableUpdateDetails.class.getDeclaredField("writerAPI");
        writerField.setAccessible(true);
        Misc.free((TableWriterAPI) writerField.get(tud));
        writerField.set(tud, Proxy.newProxyInstance(
                TableWriterAPI.class.getClassLoader(),
                new Class[]{TableWriterAPI.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "getUncommittedRowCount" -> 1L;
                    case "getLastSeqTxn" -> seqTxn;
                    case "getWalId" -> 1;
                    case "getSegmentId" -> 0;
                    case "commit", "close", "rollback" -> null;
                    default -> null;
                }
        ));
    }

    private static void addNativeData(QwpProcessorState state, byte[] data) {
        long ptr = Unsafe.malloc(data.length, MemoryTag.NATIVE_HTTP_CONN);
        try {
            for (int i = 0; i < data.length; i++) {
                Unsafe.getUnsafe().putByte(ptr + i, data[i]);
            }
            state.addData(ptr, ptr + data.length);
        } finally {
            Unsafe.free(ptr, data.length, MemoryTag.NATIVE_HTTP_CONN);
        }
    }

    @SuppressWarnings("unchecked")
    private static int getCacheSize(QwpTudCache cache) throws Exception {
        Field field = QwpTudCache.class.getDeclaredField("tableUpdateDetails");
        field.setAccessible(true);
        return ((LowerCaseUtf8SequenceObjHashMap<WalTableUpdateDetails>) field.get(cache)).size();
    }

    private static @NotNull QwpTableBlockCursor getQwpTableBlockCursor(long addr) throws QwpParseException {
        final QwpArrayColumnCursor arrayCursor = new QwpArrayColumnCursor();
        arrayCursor.of(addr, 2, 1, QwpConstants.TYPE_DOUBLE_ARRAY);

        return new QwpTableBlockCursor() {
            @Override
            public QwpArrayColumnCursor getArrayColumn(int index) {
                return arrayCursor;
            }

            @Override
            public int getRowCount() {
                return 1;
            }
        };
    }

    private static void replaceWriterWithFake(WalTableUpdateDetails tud, boolean isTableDropped) throws Exception {
        TableToken tableToken = tud.getTableToken();
        Field writerField = TableUpdateDetails.class.getDeclaredField("writerAPI");
        writerField.setAccessible(true);

        // Free the real writer to avoid native memory leaks.
        Misc.free((TableWriterAPI) writerField.get(tud));

        writerField.set(tud, Proxy.newProxyInstance(
                TableWriterAPI.class.getClassLoader(),
                new Class[]{TableWriterAPI.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "getUncommittedRowCount" -> 1L;
                    case "getWalId" -> 1;
                    case "getSegmentId" -> 0;
                    case "commit" -> {
                        if (isTableDropped) {
                            throw CairoException.tableDropped(tableToken);
                        }
                        throw CairoException.nonCritical().put("simulated commit failure");
                    }
                    case "close", "rollback" -> null;
                    default -> throw new UnsupportedOperationException(method.getName());
                }
        ));
    }

    private static byte[] wrapQwpPayload(byte[] payload) {
        byte[] message = new byte[12 + payload.length];
        message[0] = 'Q';
        message[1] = 'W';
        message[2] = 'P';
        message[3] = '1';
        message[4] = 1; // version
        message[5] = 0; // flags
        message[6] = 1; // tableCount low byte
        message[7] = 0; // tableCount high byte
        message[8] = (byte) payload.length;
        message[9] = (byte) (payload.length >>> 8);
        message[10] = (byte) (payload.length >>> 16);
        message[11] = (byte) (payload.length >>> 24);
        System.arraycopy(payload, 0, message, 12, payload.length);
        return message;
    }

    private static final class FakeConsumerTudCache extends QwpTudCache {
        private CharSequence[] commitDirNames;
        private int[] commitSegmentIds;
        private long[] commitSeqTxns;
        private Throwable commitThrow;
        private int[] commitWalIds;

        FakeConsumerTudCache(io.questdb.cairo.CairoEngine engine, LineHttpProcessorConfiguration lineConfig) {
            super(engine, true, true, new DefaultColumnTypes(lineConfig), PartitionBy.DAY);
        }

        @Override
        public void commitAll(CommittedTxnConsumer consumer) throws Throwable {
            if (commitThrow != null) {
                Throwable t = commitThrow;
                commitThrow = null;
                throw t;
            }
            if (consumer != null && commitDirNames != null) {
                for (int i = 0; i < commitDirNames.length; i++) {
                    consumer.accept(commitDirNames[i], commitWalIds[i], commitSegmentIds[i], commitSeqTxns[i]);
                }
            }
            commitDirNames = null;
            commitWalIds = null;
            commitSegmentIds = null;
            commitSeqTxns = null;
        }

        void queueCommit(CharSequence[] dirNames, int[] walIds, int[] segmentIds, long[] seqTxns) {
            this.commitDirNames = dirNames;
            this.commitWalIds = walIds;
            this.commitSegmentIds = segmentIds;
            this.commitSeqTxns = seqTxns;
        }

        void queueCommitThrow(Throwable t) {
            this.commitThrow = t;
        }
    }

    private static final class FakeDurableAckRegistry implements DurableAckRegistry {
        private final HashMap<String, Long> watermarks = new HashMap<>();

        @Override
        public long getDurablyUploadedSeqTxn(CharSequence tableDirName) {
            Long v = watermarks.get(tableDirName.toString());
            return v == null ? -1L : v;
        }

        @Override
        public boolean isEnabled() {
            return true;
        }

        void set(String dirName, long seqTxn) {
            watermarks.put(dirName, seqTxn);
        }
    }
}
