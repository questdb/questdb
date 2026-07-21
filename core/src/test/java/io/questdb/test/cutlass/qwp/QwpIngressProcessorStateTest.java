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
import io.questdb.cutlass.qwp.protocol.QwpTableBlockCursor;
import io.questdb.cutlass.qwp.server.QwpIngressProcessorState;
import io.questdb.cutlass.qwp.server.QwpTudCache;
import io.questdb.std.CharSequenceLongHashMap;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.std.LowerCaseUtf8SequenceObjHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
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

public class QwpIngressProcessorStateTest extends AbstractCairoTest {

    @Test
    public void testAddDataIgnoresZeroLengthInput() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
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
            QwpIngressProcessorState state = new QwpIngressProcessorState(64, 4096, engine, lineConfig);
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
    public void testCommitReleasesDeferredWatermarkClamp() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);
                FakeConsumerTudCache fake = installFakeTudCache(state, engine, lineConfig);

                // Deferred rows buffered -> clamp armed.
                state.markUncommittedDeferredRows();
                Assert.assertTrue(state.hasUncommittedDeferredRows());

                // The group-closing commit (commitAll) releases the clamp and
                // the watermark may then cover the whole deferred group.
                fake.queueCommit(new String[]{"t"}, new String[]{"t~1"}, new long[]{10L});
                state.commit();
                Assert.assertTrue(state.isOk());
                Assert.assertFalse(state.hasUncommittedDeferredRows());

                state.setHighestProcessedSequence(7);
                Assert.assertEquals(7, state.getHighestProcessedSequence());
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testDeferredClampResetOnClearAndDisconnect() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);

                // clear() rolls the deferred rows back -> clamp released (the
                // frames were never acked; the client replays them).
                state.markUncommittedDeferredRows();
                state.clear();
                Assert.assertFalse(state.hasUncommittedDeferredRows());
                state.setHighestProcessedSequence(3);
                Assert.assertEquals(3, state.getHighestProcessedSequence());

                // clearMessageState() (between deferred frames of one group)
                // must NOT release the clamp -- the rows are still uncommitted.
                state.markUncommittedDeferredRows();
                state.clearMessageState();
                Assert.assertTrue(state.hasUncommittedDeferredRows());

                // onDisconnected() resets everything.
                state.onDisconnected();
                Assert.assertFalse(state.hasUncommittedDeferredRows());
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testWatermarkClampedWhileDeferredRowsUncommitted() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);

                // Committed traffic advances the watermark normally.
                state.setHighestProcessedSequence(2);
                Assert.assertEquals(2, state.getHighestProcessedSequence());

                // FLAG_DEFER_COMMIT rows buffered but uncommitted: the
                // cumulative-ack watermark must refuse to advance -- an OK ack
                // covering these frames would let a store-and-forward client
                // trim slots whose rows the server can still roll back (the
                // #7144 ack hole).
                state.markUncommittedDeferredRows();
                Assert.assertTrue(state.hasUncommittedDeferredRows());
                state.setHighestProcessedSequence(5);
                Assert.assertEquals("watermark must not advance over uncommitted deferred rows",
                        2, state.getHighestProcessedSequence());
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testCollectDurableProgressMultiTable() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);
                state.setDurableAckEnabled(true);
                FakeConsumerTudCache fake = installFakeTudCache(state, engine, lineConfig);

                fake.queueCommit(
                        new String[]{"t1", "t2"},
                        new String[]{"t1~1", "t2~1"},
                        new long[]{10L, 20L}
                );
                state.setHighestProcessedSequence(0);
                state.commit();

                FakeDurableAckRegistry registry = new FakeDurableAckRegistry();
                registry.set("t1~1", 10L);
                io.questdb.std.CharSequenceLongHashMap progress = state.collectDurableProgress(registry);
                Assert.assertEquals(1, progress.size());
                Assert.assertEquals(10L, progress.get("t1"));

                registry.set("t2~1", 20L);
                progress = state.collectDurableProgress(registry);
                Assert.assertEquals(2, progress.size());
                Assert.assertEquals(10L, progress.get("t1"));
                Assert.assertEquals(20L, progress.get("t2"));
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testCollectDurableProgressOnlyReportsNewProgress() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);
                state.setDurableAckEnabled(true);
                FakeConsumerTudCache fake = installFakeTudCache(state, engine, lineConfig);

                fake.queueCommit(new String[]{"t"}, new String[]{"t~1"}, new long[]{10L});
                state.setHighestProcessedSequence(0);
                state.commit();

                FakeDurableAckRegistry registry = new FakeDurableAckRegistry();
                registry.set("t~1", 10L);

                io.questdb.std.CharSequenceLongHashMap progress = state.collectDurableProgress(registry);
                Assert.assertEquals(1, progress.size());

                state.onDurableAckSent();

                progress = state.collectDurableProgress(registry);
                Assert.assertEquals(0, progress.size());

                // A new commit re-enters the table into the pending set.
                fake.queueCommit(new String[]{"t"}, new String[]{"t~1"}, new long[]{15L});
                state.setHighestProcessedSequence(1);
                state.commit();

                registry.set("t~1", 15L);
                progress = state.collectDurableProgress(registry);
                Assert.assertEquals(1, progress.size());
                Assert.assertEquals(15L, progress.get("t"));
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testCollectDurableProgressDroppedTableReportsMaxValue() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);
                state.setDurableAckEnabled(true);
                FakeConsumerTudCache fake = installFakeTudCache(state, engine, lineConfig);

                fake.queueCommit(new String[]{"dropped"}, new String[]{"dropped~1"}, new long[]{42L});
                state.setHighestProcessedSequence(0);
                state.commit();

                FakeDurableAckRegistry registry = new FakeDurableAckRegistry();
                registry.set("dropped~1", Long.MAX_VALUE);

                io.questdb.std.CharSequenceLongHashMap progress = state.collectDurableProgress(registry);
                Assert.assertEquals(1, progress.size());
                Assert.assertEquals(Long.MAX_VALUE, progress.get("dropped"));
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testOnDurableAckSentPrunesCaughtUpTables() throws Exception {
        // Regression: per-connection maps tableDirNames and lastDurableSeqTxns
        // (plus pendingDurableDirNames / pendingDurableSeqTxns) must not grow
        // one entry per unique table name for the connection's lifetime.
        // When the durable watermark catches up to the committed seqTxn for a
        // table, onDurableAckSent prunes ALL four maps for that table. A later
        // commit to the same table name re-populates via recordCommittedTable;
        // the drop-recreate check there treats an absent tableDirNames entry
        // the same as first-sight, which is correct behaviour.
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);
                state.setDurableAckEnabled(true);
                FakeConsumerTudCache fake = installFakeTudCache(state, engine, lineConfig);

                // Commit 500 distinct rotating tables. Each one catches up
                // immediately (durable watermark == committed seqTxn) so
                // onDurableAckSent should prune each one and leave the maps
                // empty. Without the fix, all 500 entries would accumulate.
                FakeDurableAckRegistry registry = new FakeDurableAckRegistry();
                for (int i = 0; i < 500; i++) {
                    String tableName = "t" + i;
                    String dirName = tableName + "~1";
                    long seqTxn = 10L + i;
                    fake.queueCommit(new String[]{tableName}, new String[]{dirName}, new long[]{seqTxn});
                    state.setHighestProcessedSequence(i);
                    state.commit();
                    registry.set(dirName, seqTxn);
                    io.questdb.std.CharSequenceLongHashMap progress = state.collectDurableProgress(registry);
                    Assert.assertEquals(1, progress.size());
                    state.onDurableAckSent();
                }

                Assert.assertEquals(
                        "pendingDurableDirNames must be empty after all tables caught up",
                        0, fieldSize(state, "pendingDurableDirNames")
                );
                Assert.assertEquals(
                        "pendingDurableSeqTxns must be empty after all tables caught up",
                        0, fieldSize(state, "pendingDurableSeqTxns")
                );
                Assert.assertEquals(
                        "tableDirNames must be pruned alongside pending entries",
                        0, fieldSize(state, "tableDirNames")
                );
                Assert.assertEquals(
                        "lastDurableSeqTxns must be pruned alongside pending entries",
                        0, fieldSize(state, "lastDurableSeqTxns")
                );

                // Sanity: a fresh commit to a previously-pruned table name
                // still produces a durable ack for positive progress.
                fake.queueCommit(new String[]{"t0"}, new String[]{"t0~1"}, new long[]{999L});
                state.setHighestProcessedSequence(500);
                state.commit();
                registry.set("t0~1", 999L);
                io.questdb.std.CharSequenceLongHashMap progress = state.collectDurableProgress(registry);
                Assert.assertEquals(1, progress.size());
                Assert.assertEquals(999L, progress.get("t0"));
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testRecordCommittedTableSkipsDurableMapsWhenDisabled() throws Exception {
        // Regression: connections that did not opt into durable-ack (no
        // X-QWP-Request-Durable-Ack header) must not pay the tracking cost.
        // recordCommittedTable used to populate tableDirNames on every commit
        // regardless of durableAckEnabled, leaking one entry per unique
        // table name for the connection's lifetime.
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);
                // durableAckEnabled is false by default
                FakeConsumerTudCache fake = installFakeTudCache(state, engine, lineConfig);

                for (int i = 0; i < 50; i++) {
                    String tableName = "t" + i;
                    String dirName = tableName + "~1";
                    fake.queueCommit(new String[]{tableName}, new String[]{dirName}, new long[]{10L + i});
                    state.setHighestProcessedSequence(i);
                    state.commit();
                }

                Assert.assertEquals(0, fieldSize(state, "tableDirNames"));
                Assert.assertEquals(0, fieldSize(state, "lastDurableSeqTxns"));
                Assert.assertEquals(0, fieldSize(state, "pendingDurableDirNames"));
                Assert.assertEquals(0, fieldSize(state, "pendingDurableSeqTxns"));
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testCollectDurableProgressDroppedTableThenRecreated() throws Exception {
        // Regression: when a table is dropped and re-created with the same name
        // on the same connection, lastDurableSeqTxns retains MAX_VALUE from the
        // drop. Without resetting it on dir name change, durable acks for the
        // re-created table would never be reported.
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);
                state.setDurableAckEnabled(true);
                FakeConsumerTudCache fake = installFakeTudCache(state, engine, lineConfig);

                // 1. Commit to "orders" (dir "orders~1")
                fake.queueCommit(new String[]{"orders"}, new String[]{"orders~1"}, new long[]{10L});
                state.setHighestProcessedSequence(0);
                state.commit();

                FakeDurableAckRegistry registry = new FakeDurableAckRegistry();
                registry.set("orders~1", 10L);
                io.questdb.std.CharSequenceLongHashMap progress = state.collectDurableProgress(registry);
                Assert.assertEquals(1, progress.size());
                state.onDurableAckSent();

                // 2. Table dropped — registry sets MAX_VALUE sentinel.
                // The table already left the pending set (durable caught up to
                // committed), so the sentinel is not reported to this connection.
                registry.set("orders~1", Long.MAX_VALUE);
                progress = state.collectDurableProgress(registry);
                Assert.assertEquals(0, progress.size());

                // 3. Table re-created with same name, new dir "orders~2"
                fake.queueCommit(new String[]{"orders"}, new String[]{"orders~2"}, new long[]{5L});
                state.setHighestProcessedSequence(1);
                state.commit();

                // 4. Upload completes for new incarnation
                registry.set("orders~2", 5L);
                progress = state.collectDurableProgress(registry);
                Assert.assertEquals(
                        "durable ack must be reported for re-created table",
                        1, progress.size()
                );
                Assert.assertEquals(5L, progress.get("orders"));
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testCollectDurableProgressIsEmptyWhenDisabled() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);
                state.setHighestProcessedSequence(5);

                io.questdb.std.CharSequenceLongHashMap progress = state.collectDurableProgress(new FakeDurableAckRegistry());
                Assert.assertEquals(0, progress.size());
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testHasPendingDurableAckDetectsProgress() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);
                state.setDurableAckEnabled(true);
                FakeConsumerTudCache fake = installFakeTudCache(state, engine, lineConfig);

                fake.queueCommit(new String[]{"t1", "t2"}, new String[]{"t1~1", "t2~1"}, new long[]{10L, 5L});
                state.setHighestProcessedSequence(0);
                state.commit();

                FakeDurableAckRegistry registry = new FakeDurableAckRegistry();
                Assert.assertEquals(0, state.collectDurableProgress(registry).size());

                registry.set("t1~1", 10L);
                registry.set("t2~1", 5L);
                Assert.assertEquals(2, state.collectDurableProgress(registry).size());

                state.collectDurableProgress(registry);
                state.onDurableAckSent();
                Assert.assertEquals(0, state.collectDurableProgress(registry).size());
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testPendingAckSeqTxnsPopulatedOnCommit() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);
                FakeConsumerTudCache fake = installFakeTudCache(state, engine, lineConfig);

                fake.queueCommit(new String[]{"t"}, new String[]{"t~1"}, new long[]{10L});
                state.setHighestProcessedSequence(0);
                state.commit();

                io.questdb.std.CharSequenceLongHashMap pending = state.getPendingAckSeqTxns();
                Assert.assertEquals(1, pending.size());
                Assert.assertEquals(10L, pending.get("t"));

                state.onAckSent(0);
                Assert.assertEquals(0, state.getPendingAckSeqTxns().size());
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testPendingAckSeqTxnsEmptyCommitProducesNoEntries() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);
                FakeConsumerTudCache fake = installFakeTudCache(state, engine, lineConfig);

                fake.queueCommit(null, null, null);
                state.setHighestProcessedSequence(0);
                state.commit();

                Assert.assertEquals(0, state.getPendingAckSeqTxns().size());
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testPingTriggeredDurableAckDelivery() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);
                state.setDurableAckEnabled(true);
                FakeConsumerTudCache fake = installFakeTudCache(state, engine, lineConfig);

                fake.queueCommit(new String[]{"t1"}, new String[]{"t1~1"}, new long[]{10L});
                state.setHighestProcessedSequence(0);
                state.commit();
                fake.queueCommit(new String[]{"t2"}, new String[]{"t2~1"}, new long[]{5L});
                state.setHighestProcessedSequence(1);
                state.commit();

                FakeDurableAckRegistry registry = new FakeDurableAckRegistry();
                Assert.assertEquals(0, state.collectDurableProgress(registry).size());

                registry.set("t1~1", 10L);
                registry.set("t2~1", 5L);

                io.questdb.std.CharSequenceLongHashMap progress = state.collectDurableProgress(registry);
                Assert.assertEquals(2, progress.size());
                state.onDurableAckSent();
                Assert.assertEquals(0, state.collectDurableProgress(registry).size());
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
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);

                // Replace tudCache with one that throws a critical CairoException
                Field tudCacheField = QwpIngressProcessorState.class.getDeclaredField("tudCache");
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
                        0     // columnCount=0
                }));
                state.processMessage();
                Assert.assertEquals(QwpIngressProcessorState.Status.INTERNAL_ERROR, state.getStatus());
                Assert.assertTrue(state.getErrorText().contains("simulated critical error"));
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testCairoExceptionStatusReturnsSchemaMismatchForSchemaMismatchException() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);

                Field tudCacheField = QwpIngressProcessorState.class.getDeclaredField("tudCache");
                tudCacheField.setAccessible(true);
                Misc.free((QwpTudCache) tudCacheField.get(state));
                DefaultColumnTypes defaultColumnTypes = new DefaultColumnTypes(lineConfig);
                tudCacheField.set(state, new QwpTudCache(engine, true, true, defaultColumnTypes, PartitionBy.DAY) {
                    @Override
                    public WalTableUpdateDetails getTableUpdateDetails(
                            SecurityContext secCtx, Utf8Sequence tableName,
                            ObjList<QwpColumnDef> schema, QwpTableBlockCursor cursor, int maxTables) {
                        throw CairoException.schemaMismatch().put("type coercion from VARCHAR to IPV4 is not supported");
                    }
                });

                addNativeData(state, wrapQwpPayload(new byte[]{
                        4, 't', 'e', 's', 't',
                        0,    // rowCount=0
                        0     // columnCount=0
                }));
                state.processMessage();
                Assert.assertEquals(QwpIngressProcessorState.Status.SCHEMA_MISMATCH, state.getStatus());
                Assert.assertTrue(state.getErrorText().contains("type coercion from VARCHAR to IPV4 is not supported"));
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testCairoExceptionStatusReturnsNotAcceptingWritesForUnmarkedNonCriticalException() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);

                Field tudCacheField = QwpIngressProcessorState.class.getDeclaredField("tudCache");
                tudCacheField.setAccessible(true);
                Misc.free((QwpTudCache) tudCacheField.get(state));
                DefaultColumnTypes defaultColumnTypes = new DefaultColumnTypes(lineConfig);
                tudCacheField.set(state, new QwpTudCache(engine, true, true, defaultColumnTypes, PartitionBy.DAY) {
                    @Override
                    public WalTableUpdateDetails getTableUpdateDetails(
                            SecurityContext secCtx, Utf8Sequence tableName,
                            ObjList<QwpColumnDef> schema, QwpTableBlockCursor cursor, int maxTables) {
                        throw CairoException.nonCritical().put("table is busy");
                    }
                });

                addNativeData(state, wrapQwpPayload(new byte[]{
                        4, 't', 'e', 's', 't',
                        0,    // rowCount=0
                        0     // columnCount=0
                }));
                state.processMessage();
                Assert.assertEquals(QwpIngressProcessorState.Status.NOT_ACCEPTING_WRITES, state.getStatus());
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
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
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
    public void testClearMessageStatePreservesWalState() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);
                FakeConsumerTudCache fake = installFakeTudCache(state, engine, lineConfig);

                // clearMessageState() resets parsing buffers but preserves tudCache
                state.clearMessageState();
                Assert.assertTrue(state.isOk());

                // tudCache survives: commit still works after clearMessageState()
                fake.queueCommit(new String[]{"t"}, new String[]{"t~1"}, new long[]{10L});
                state.setHighestProcessedSequence(0);
                state.commit();
                Assert.assertEquals(1, state.getPendingAckSeqTxns().size());
                Assert.assertEquals(10L, state.getPendingAckSeqTxns().get("t"));
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testClearMessageStateResetsBufferPosition() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);

                // Feed some data to advance bufferPosition
                addNativeData(state, wrapQwpPayload(new byte[]{
                        4, 't', 'e', 's', 't',
                        0, 0, 0x00
                }));

                // clearMessageState() resets buffer for the next message
                state.clearMessageState();
                Assert.assertTrue(state.isOk());

                // isDeferCommit() returns false when buffer is empty
                Assert.assertFalse(state.isDeferCommit());
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testDeferCommitFlagParsedFromHeader() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);

                // Message without FLAG_DEFER_COMMIT
                addNativeData(state, wrapQwpPayload(new byte[]{
                        4, 't', 'e', 's', 't',
                        0, 0, 0x00
                }, (byte) 0));
                Assert.assertFalse(state.isDeferCommit());
                state.clear();

                // Message with FLAG_DEFER_COMMIT
                addNativeData(state, wrapQwpPayload(new byte[]{
                        4, 't', 'e', 's', 't',
                        0, 0, 0x00
                }, QwpConstants.FLAG_DEFER_COMMIT));
                Assert.assertTrue(state.isDeferCommit());
                state.clear();

                // FLAG_DEFER_COMMIT combined with other flags
                addNativeData(state, wrapQwpPayload(new byte[]{
                        4, 't', 'e', 's', 't',
                        0, 0, 0x00
                }, (byte) (QwpConstants.FLAG_DEFER_COMMIT | QwpConstants.FLAG_DELTA_SYMBOL_DICT)));
                Assert.assertTrue(state.isDeferCommit());
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testDeferCommitReturnsFalseWhenBufferTooSmall() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);

                // No data at all: bufferPosition < HEADER_SIZE
                Assert.assertFalse(state.isDeferCommit());

                // Partial header (less than 12 bytes)
                long ptr = Unsafe.malloc(6, MemoryTag.NATIVE_HTTP_CONN);
                try {
                    Unsafe.putByte(ptr, (byte) 'Q');
                    Unsafe.putByte(ptr + 1, (byte) 'W');
                    Unsafe.putByte(ptr + 2, (byte) 'P');
                    Unsafe.putByte(ptr + 3, (byte) '1');
                    Unsafe.putByte(ptr + 4, (byte) 1);
                    Unsafe.putByte(ptr + 5, QwpConstants.FLAG_DEFER_COMMIT);
                    state.addData(ptr, ptr + 6);
                } finally {
                    Unsafe.free(ptr, 6, MemoryTag.NATIVE_HTTP_CONN);
                }
                // Only 6 bytes — less than HEADER_SIZE (12), so isDeferCommit returns false
                Assert.assertFalse(state.isDeferCommit());
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testDeferredCommitAccumulatesAcrossMessages() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);
                FakeConsumerTudCache fake = installFakeTudCache(state, engine, lineConfig);

                // Message 1: deferred — no commit, clearMessageState() preserves WAL state
                addNativeData(state, wrapQwpPayload(new byte[]{
                        4, 't', 'e', 's', 't',
                        0, 0, 0x00
                }, QwpConstants.FLAG_DEFER_COMMIT));
                Assert.assertTrue(state.isDeferCommit());
                Assert.assertTrue(state.isOk());
                state.clearMessageState();

                // Message 2: deferred — still no commit
                addNativeData(state, wrapQwpPayload(new byte[]{
                        4, 't', 'e', 's', 't',
                        0, 0, 0x00
                }, QwpConstants.FLAG_DEFER_COMMIT));
                Assert.assertTrue(state.isDeferCommit());
                Assert.assertTrue(state.isOk());
                state.clearMessageState();

                // Message 3: final message (no defer flag) — commit all accumulated rows
                addNativeData(state, wrapQwpPayload(new byte[]{
                        4, 't', 'e', 's', 't',
                        0, 0, 0x00
                }, (byte) 0));
                Assert.assertFalse(state.isDeferCommit());

                fake.queueCommit(new String[]{"test"}, new String[]{"test~1"}, new long[]{30L});
                state.setHighestProcessedSequence(2);
                state.commit();
                Assert.assertTrue(state.isOk());
                Assert.assertEquals(1, state.getPendingAckSeqTxns().size());
                Assert.assertEquals(30L, state.getPendingAckSeqTxns().get("test"));
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testDeferredCommitErrorCausesFullClear() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);
                FakeConsumerTudCache fake = installFakeTudCache(state, engine, lineConfig);

                // Message 1: deferred — clearMessageState preserves WAL
                addNativeData(state, wrapQwpPayload(new byte[]{
                        4, 't', 'e', 's', 't',
                        0, 0, 0x00
                }, QwpConstants.FLAG_DEFER_COMMIT));
                Assert.assertTrue(state.isOk());
                state.clearMessageState();

                // Now simulate an error on the next message by queuing a commit failure
                fake.queueCommitThrow(new RuntimeException("simulated commit failure"));
                state.setHighestProcessedSequence(1);
                state.commit();

                // After error, clear() rolls back all accumulated WAL state
                Assert.assertFalse(state.isOk());
                state.clear();
                Assert.assertTrue(state.isOk());
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testDeferredCommitOnDisconnectedRollsBackWalState() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);
                installFakeTudCache(state, engine, lineConfig);

                // Message 1: deferred — clearMessageState preserves WAL
                addNativeData(state, wrapQwpPayload(new byte[]{
                        4, 't', 'e', 's', 't',
                        0, 0, 0x00
                }, QwpConstants.FLAG_DEFER_COMMIT));
                Assert.assertTrue(state.isOk());
                state.clearMessageState();

                // Simulate connection drop mid-deferred-sequence
                // onDisconnected() calls clear() which rolls back all WAL state
                state.onDisconnected();

                // After disconnect + close, no leaks
            } finally {
                state.close();
            }
        });
    }

    @Test
    public void testDeferredCommitMaxUncommittedRowsTriggersCommit() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);
                FakeConsumerTudCache fake = installFakeTudCache(state, engine, lineConfig);

                // Deferred message — processMessage then commitIfMaxUncommittedRowsReached
                addNativeData(state, wrapQwpPayload(new byte[]{
                        4, 't', 'e', 's', 't',
                        0, 0, 0x00
                }, QwpConstants.FLAG_DEFER_COMMIT));
                Assert.assertTrue(state.isOk());

                // Simulate the limit being reached: queue a commit callback
                fake.queueMaxRowsCommit(new String[]{"test"}, new String[]{"test~1"}, new long[]{42L});
                state.commitIfMaxUncommittedRowsReached();
                Assert.assertTrue(state.isOk());

                // The forced commit should have recorded the seqTxn in pendingAckSeqTxns
                Assert.assertEquals(1, state.getPendingAckSeqTxns().size());
                Assert.assertEquals(42L, state.getPendingAckSeqTxns().get("test"));
                Assert.assertEquals(1, fake.getMaxRowsCommitCallCount());
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testDeferredCommitMaxUncommittedRowsNoOpBelowLimit() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);
                FakeConsumerTudCache fake = installFakeTudCache(state, engine, lineConfig);

                // Deferred message
                addNativeData(state, wrapQwpPayload(new byte[]{
                        4, 't', 'e', 's', 't',
                        0, 0, 0x00
                }, QwpConstants.FLAG_DEFER_COMMIT));
                Assert.assertTrue(state.isOk());

                // Don't queue any commit data — simulates rows below the limit
                state.commitIfMaxUncommittedRowsReached();
                Assert.assertTrue(state.isOk());

                // No seqTxns should be recorded
                Assert.assertEquals(0, state.getPendingAckSeqTxns().size());
                Assert.assertEquals(1, fake.getMaxRowsCommitCallCount());
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testDeferredCommitMaxUncommittedRowsErrorSetsDistressed() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);
                FakeConsumerTudCache fake = installFakeTudCache(state, engine, lineConfig);

                // Deferred message
                addNativeData(state, wrapQwpPayload(new byte[]{
                        4, 't', 'e', 's', 't',
                        0, 0, 0x00
                }, QwpConstants.FLAG_DEFER_COMMIT));
                Assert.assertTrue(state.isOk());

                // Simulate a commit failure during the max-rows check
                fake.queueMaxRowsCommitThrow(new RuntimeException("simulated WAL failure"));
                state.commitIfMaxUncommittedRowsReached();

                // Error should set the state to not-OK
                Assert.assertFalse(state.isOk());
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testDeferredCommitMaxUncommittedRowsUpdatesAckSeqTxns() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);
                FakeConsumerTudCache fake = installFakeTudCache(state, engine, lineConfig);

                // Message 1: deferred
                addNativeData(state, wrapQwpPayload(new byte[]{
                        4, 't', 'e', 's', 't',
                        0, 0, 0x00
                }, QwpConstants.FLAG_DEFER_COMMIT));
                Assert.assertTrue(state.isOk());

                // Max-rows triggers a mid-batch commit with seqTxn=10
                fake.queueMaxRowsCommit(new String[]{"test"}, new String[]{"test~1"}, new long[]{10L});
                state.commitIfMaxUncommittedRowsReached();
                Assert.assertTrue(state.isOk());
                Assert.assertEquals(10L, state.getPendingAckSeqTxns().get("test"));

                state.clearMessageState();

                // Message 2: deferred
                addNativeData(state, wrapQwpPayload(new byte[]{
                        4, 't', 'e', 's', 't',
                        0, 0, 0x00
                }, QwpConstants.FLAG_DEFER_COMMIT));
                Assert.assertTrue(state.isOk());

                // Max-rows triggers another mid-batch commit with seqTxn=20
                fake.queueMaxRowsCommit(new String[]{"test"}, new String[]{"test~1"}, new long[]{20L});
                state.commitIfMaxUncommittedRowsReached();
                Assert.assertTrue(state.isOk());

                // The cumulative ACK should carry the latest seqTxn (20, not 10)
                Assert.assertEquals(1, state.getPendingAckSeqTxns().size());
                Assert.assertEquals(20L, state.getPendingAckSeqTxns().get("test"));

                state.clearMessageState();

                // Message 3: final (non-deferred) — commit remaining
                addNativeData(state, wrapQwpPayload(new byte[]{
                        4, 't', 'e', 's', 't',
                        0, 0, 0x00
                }, (byte) 0));
                fake.queueCommit(new String[]{"test"}, new String[]{"test~1"}, new long[]{30L});
                state.commit();
                Assert.assertTrue(state.isOk());

                // Final commit updates to seqTxn=30
                Assert.assertEquals(30L, state.getPendingAckSeqTxns().get("test"));
            } finally {
                state.onDisconnected();
                state.close();
            }
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

                // Append a real row so isFirstRow() returns false and commitAll
                // actually advances the sequencer txn, invoking the consumer.
                tud.getWriter().newRow(0L).append();
                Assert.assertFalse(tud.isFirstRow());

                ObjList<Utf8String> captured = new ObjList<>();
                long[] capturedSeq = new long[]{Long.MIN_VALUE};
                try {
                    cache.commitAll((_, tableDirName, seqTxn) -> {
                        captured.add(new Utf8String(tableDirName));
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
                Assert.assertEquals(tud.getLastSeqTxn(), capturedSeq[0]);
                Assert.assertTrue("seqTxn must have advanced", capturedSeq[0] > 0);
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
                // Real writer, no rows ingested → getUncommittedRowCount() == 0 → isFirstRow() is true.
                Assert.assertTrue(tud.isFirstRow());

                boolean[] invoked = new boolean[]{false};
                try {
                    cache.commitAll((_, _, _) -> invoked[0] = true);
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
    public void testCommitEmptyProducesNoPendingSeqTxns() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);
                state.setDurableAckEnabled(true);
                FakeConsumerTudCache fake = installFakeTudCache(state, engine, lineConfig);

                fake.queueCommit(null, null, null);
                state.setHighestProcessedSequence(0);
                state.commit();

                Assert.assertEquals(0, state.getPendingAckSeqTxns().size());
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testCommitFailureRejectsState() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);
                state.setDurableAckEnabled(true);
                FakeConsumerTudCache fake = installFakeTudCache(state, engine, lineConfig);

                fake.queueCommitThrow(CairoException.nonCritical().put("simulated"));
                state.setHighestProcessedSequence(0);
                state.commit();

                Assert.assertFalse(state.isOk());
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testCommitAlwaysInvokesConsumer() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);
                FakeConsumerTudCache fake = installFakeTudCache(state, engine, lineConfig);

                fake.queueCommit(new String[]{"t"}, new String[]{"t~1"}, new long[]{10L});
                state.setHighestProcessedSequence(5);
                state.commit();

                Assert.assertEquals(1, state.getPendingAckSeqTxns().size());
                Assert.assertEquals(10L, state.getPendingAckSeqTxns().get("t"));
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testCommitConsumerNegativeSeqTxnIsIgnored() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);
                FakeConsumerTudCache fake = installFakeTudCache(state, engine, lineConfig);

                // The consumer receives a negative seqTxn (e.g., non-WAL writer).
                // recordCommittedTable must ignore it — no entry in pendingAckSeqTxns.
                fake.queueCommit(new String[]{"t"}, new String[]{"t~1"}, new long[]{-1L});
                state.setHighestProcessedSequence(0);
                state.commit();

                Assert.assertEquals(0, state.getPendingAckSeqTxns().size());
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testUnresolvedSequenceClampsAckWatermark() throws Exception {
        // Defense-in-depth for the cumulative-ack leapfrog: a sequence that was
        // consumed but neither committed nor error-responded (role-change close
        // paths) must never be covered by the cumulative-ack watermark, even if
        // the processor-level deferral gate regresses.
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);

                state.setHighestProcessedSequence(0);
                state.markSequenceUnresolved(1);

                // watermark must not reach the unresolved sequence...
                state.setHighestProcessedSequence(1);
                Assert.assertEquals(0, state.getHighestProcessedSequence());
                // ...nor leapfrog past it
                state.setHighestProcessedSequence(2);
                Assert.assertEquals(0, state.getHighestProcessedSequence());

                // marking keeps the minimum: a later, higher unresolved sequence
                // must not loosen the clamp
                state.markSequenceUnresolved(5);
                state.setHighestProcessedSequence(6);
                Assert.assertEquals(0, state.getHighestProcessedSequence());

                // advancing strictly below the unresolved sequence stays legal
                // (clamp boundary is firstUnresolved - 1; here that is 0)
                state.setHighestProcessedSequence(0);
                Assert.assertEquals(0, state.getHighestProcessedSequence());
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testUnresolvedSequenceResetOnDisconnect() throws Exception {
        // The unresolved marker is per-connection: after the reconnect-eligible
        // close the client replays from its acked watermark, so a recycled state
        // must accept fresh sequences without clamping.
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);
                state.markSequenceUnresolved(0);
                state.setHighestProcessedSequence(3);
                Assert.assertEquals(-1, state.getHighestProcessedSequence());

                state.onDisconnected();

                state.of(1, AllowAllSecurityContext.INSTANCE);
                state.setHighestProcessedSequence(3);
                Assert.assertEquals(3, state.getHighestProcessedSequence());
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testCommitConsumerThrowRejectsState() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);

                // Install a fake that invokes the consumer successfully, then throws.
                // This simulates a commitAll where some tables commit before one fails.
                FakeConsumerTudCache fake = installFakeTudCache(state, engine, lineConfig);
                fake.queueCommit(new String[]{"t"}, new String[]{"t~1"}, new long[]{10L});
                fake.queueCommitThrow(CairoException.nonCritical().put("consumer kaboom"));
                state.setHighestProcessedSequence(0);
                state.commit();

                Assert.assertFalse("state must be rejected after consumer throw", state.isOk());
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
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
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
                Unsafe.putByte(addr, (byte) 1);
                Unsafe.putByte(addr + 1, (byte) 0x01);

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
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);

                state.onDurableAckBlocked();
                Assert.assertEquals(4, state.getSendState());

                state.onErrorBlocked((byte) 1, 4, "boom");
                Assert.assertEquals(5, state.getSendState());

                state.onErrorBlocked((byte) 1, 5, "boom2");
                Assert.assertEquals(5, state.getSendState());
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testOnResumeDurableAckCompleteSimplePath() throws Exception {
        // Simple path: durable-ack blocked → resume complete (no error).
        // Verifies that lastDurableSeqTxns is updated so the next
        // collectDurableProgress only reports further advances.
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);
                state.setDurableAckEnabled(true);
                FakeConsumerTudCache fake = installFakeTudCache(state, engine, lineConfig);

                fake.queueCommit(new String[]{"t"}, new String[]{"t~1"}, new long[]{10L});
                state.setHighestProcessedSequence(0);
                state.commit();

                FakeDurableAckRegistry registry = new FakeDurableAckRegistry();
                registry.set("t~1", 10L);

                // Populate durableProgressSnapshot
                io.questdb.std.CharSequenceLongHashMap progress = state.collectDurableProgress(registry);
                Assert.assertEquals(1, progress.size());

                // Simulate blocked send
                state.onDurableAckBlocked();
                Assert.assertEquals(4, state.getSendState()); // SEND_STATE_RESUME_DURABLE_ACK

                // Resume completes
                state.onResumeDurableAckComplete();
                Assert.assertEquals(0, state.getSendState()); // SEND_STATE_READY

                // Same watermark should not be reported again
                progress = state.collectDurableProgress(registry);
                Assert.assertEquals(0, progress.size());

                // A new commit re-enters the table into the pending set.
                fake.queueCommit(new String[]{"t"}, new String[]{"t~1"}, new long[]{15L});
                state.setHighestProcessedSequence(1);
                state.commit();

                // Further advance is reported
                registry.set("t~1", 15L);
                progress = state.collectDurableProgress(registry);
                Assert.assertEquals(1, progress.size());
                Assert.assertEquals(15L, progress.get("t"));
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
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);
                state.setDurableAckEnabled(true);

                state.onDurableAckBlocked();
                Assert.assertEquals(4, state.getSendState());

                state.onErrorBlocked((byte) 6, 10, "write error");
                Assert.assertEquals(5, state.getSendState());

                state.onResumeDurableAckComplete();
                Assert.assertEquals(0, state.getSendState());

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
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
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
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
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
    public void testProcessMessageReturnsEarlyWhenBufferEmpty() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
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
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);

                state.reject(QwpIngressProcessorState.Status.PARSE_ERROR, "initial error", 1);
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
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 250, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);

                String shortError = "something went wrong";
                state.reject(QwpIngressProcessorState.Status.PARSE_ERROR, shortError, 1);

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
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 250, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);

                // Build a 200-char error message, well above the 100-char limit
                String longError = "x".repeat(200);
                state.reject(QwpIngressProcessorState.Status.INTERNAL_ERROR, longError, 1);

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
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);

                state.reject(QwpIngressProcessorState.Status.INTERNAL_ERROR, null, 1);
                Assert.assertFalse(state.isOk());
                Assert.assertEquals("(no error message)", state.getErrorText());
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testComputeAckPayloadSizeMatchesWrittenBytes() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);
                FakeConsumerTudCache fake = installFakeTudCache(state, engine, lineConfig);

                fake.queueCommit(
                        new String[]{"orders", "trades", "events"},
                        new String[]{"orders~1", "trades~1", "events~1"},
                        new long[]{10L, 20L, 30L}
                );
                state.setHighestProcessedSequence(0);
                state.commit();

                int payloadSize = state.computeAckPayloadSize();
                // Verify by writing: status(1) + sequence(8) + writeTableSeqTxnEntries
                long ptr = Unsafe.malloc(payloadSize, MemoryTag.NATIVE_DEFAULT);
                try {
                    Unsafe.putByte(ptr, (byte) 0x00); // STATUS_OK
                    Unsafe.putLong(ptr + 1, 0L);      // sequence
                    int tableBytes = QwpIngressProcessorState.writeTableSeqTxnEntries(ptr + 9, state.getPendingAckSeqTxns());
                    Assert.assertEquals(payloadSize, 9 + tableBytes);
                } finally {
                    Unsafe.free(ptr, payloadSize, MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testComputeDurableAckPayloadSizeMatchesWrittenBytes() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);
                state.setDurableAckEnabled(true);
                FakeConsumerTudCache fake = installFakeTudCache(state, engine, lineConfig);

                fake.queueCommit(
                        new String[]{"alpha", "beta"},
                        new String[]{"alpha~1", "beta~1"},
                        new long[]{5L, 15L}
                );
                state.setHighestProcessedSequence(0);
                state.commit();

                FakeDurableAckRegistry registry = new FakeDurableAckRegistry();
                registry.set("alpha~1", 5L);
                registry.set("beta~1", 15L);
                CharSequenceLongHashMap progress = state.collectDurableProgress(registry);
                Assert.assertEquals(2, progress.size());

                int payloadSize = state.computeDurableAckPayloadSize();
                // Verify by writing: status(1) + writeTableSeqTxnEntries
                long ptr = Unsafe.malloc(payloadSize, MemoryTag.NATIVE_DEFAULT);
                try {
                    Unsafe.putByte(ptr, (byte) 0x02); // STATUS_DURABLE_ACK
                    int tableBytes = QwpIngressProcessorState.writeTableSeqTxnEntries(ptr + 1, progress);
                    Assert.assertEquals(payloadSize, 1 + tableBytes);
                } finally {
                    Unsafe.free(ptr, payloadSize, MemoryTag.NATIVE_DEFAULT);
                }
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
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
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

    @Test
    public void testWriteTableSeqTxnEntriesEmpty() throws Exception {
        assertMemoryLeak(() -> {
            CharSequenceLongHashMap entries = new CharSequenceLongHashMap();
            long ptr = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
            try {
                int written = QwpIngressProcessorState.writeTableSeqTxnEntries(ptr, entries);
                // Empty map: just tableCount(2) = 0
                Assert.assertEquals(2, written);
                Assert.assertEquals(0, Unsafe.getShort(ptr) & 0xFFFF);
            } finally {
                Unsafe.free(ptr, 64, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testWriteTableSeqTxnEntriesMultipleEntries() throws Exception {
        assertMemoryLeak(() -> {
            CharSequenceLongHashMap entries = new CharSequenceLongHashMap();
            entries.put("t1", 10L);
            entries.put("t2", 20L);
            entries.put("abc", 30L);
            long ptr = Unsafe.malloc(256, MemoryTag.NATIVE_DEFAULT);
            try {
                int written = QwpIngressProcessorState.writeTableSeqTxnEntries(ptr, entries);
                // tableCount(2) + 3 * (nameLen(2) + name + seqTxn(8))
                // "t1"(2), "t2"(2), "abc"(3) -> 2 + (2+2+8) + (2+2+8) + (2+3+8) = 39
                Assert.assertEquals(39, written);
                int tableCount = Unsafe.getShort(ptr) & 0xFFFF;
                Assert.assertEquals(3, tableCount);

                // Verify all entries are readable by walking the wire format
                int offset = 2;
                for (int i = 0; i < tableCount; i++) {
                    int nameLen = Unsafe.getShort(ptr + offset) & 0xFFFF;
                    offset += 2;
                    StringBuilder sb = new StringBuilder();
                    for (int j = 0; j < nameLen; j++) {
                        sb.append((char) (Unsafe.getByte(ptr + offset + j) & 0xFF));
                    }
                    offset += nameLen;
                    long seqTxn = Unsafe.getLong(ptr + offset);
                    offset += 8;
                    Assert.assertEquals(entries.get(sb), seqTxn);
                }
                Assert.assertEquals(written, offset);
            } finally {
                Unsafe.free(ptr, 256, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testWriteTableSeqTxnEntriesNonAsciiTableName() throws Exception {
        assertMemoryLeak(() -> {
            CharSequenceLongHashMap entries = new CharSequenceLongHashMap();
            // "café" — the é encodes to two UTF-8 bytes (0xC3 0xA9),
            // so UTF-8 byte length (5) differs from char length (4).
            entries.put("café", 77L);
            long ptr = Unsafe.malloc(128, MemoryTag.NATIVE_DEFAULT);
            try {
                int written = QwpIngressProcessorState.writeTableSeqTxnEntries(ptr, entries);
                // tableCount(2) + nameLen(2) + "café" UTF-8 (5 bytes) + seqTxn(8) = 17
                Assert.assertEquals(17, written);
                int tableCount = Unsafe.getShort(ptr) & 0xFFFF;
                Assert.assertEquals(1, tableCount);
                int nameLen = Unsafe.getShort(ptr + 2) & 0xFFFF;
                Assert.assertEquals(5, nameLen);
                // Verify UTF-8 bytes: 'c'=0x63, 'a'=0x61, 'f'=0x66, é=0xC3 0xA9
                Assert.assertEquals((byte) 'c', Unsafe.getByte(ptr + 4));
                Assert.assertEquals((byte) 'a', Unsafe.getByte(ptr + 5));
                Assert.assertEquals((byte) 'f', Unsafe.getByte(ptr + 6));
                Assert.assertEquals((byte) 0xC3, Unsafe.getByte(ptr + 7));
                Assert.assertEquals((byte) 0xA9, Unsafe.getByte(ptr + 8));
                Assert.assertEquals(77L, Unsafe.getLong(ptr + 9));
            } finally {
                Unsafe.free(ptr, 128, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testWriteTableSeqTxnEntriesSingleEntry() throws Exception {
        assertMemoryLeak(() -> {
            CharSequenceLongHashMap entries = new CharSequenceLongHashMap();
            entries.put("trades", 42L);
            long ptr = Unsafe.malloc(128, MemoryTag.NATIVE_DEFAULT);
            try {
                int written = QwpIngressProcessorState.writeTableSeqTxnEntries(ptr, entries);
                // tableCount(2) + nameLen(2) + "trades"(6) + seqTxn(8) = 18
                Assert.assertEquals(18, written);
                Assert.assertEquals(1, Unsafe.getShort(ptr) & 0xFFFF);
                Assert.assertEquals(6, Unsafe.getShort(ptr + 2) & 0xFFFF);
                Assert.assertEquals((byte) 't', Unsafe.getByte(ptr + 4));
                Assert.assertEquals(42L, Unsafe.getLong(ptr + 10));
            } finally {
                Unsafe.free(ptr, 128, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testHasPendingAckTrueWhenSequenceAdvanced() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);

                Assert.assertFalse(state.hasPendingAck());

                state.setHighestProcessedSequence(5);
                // lastAckedSequence defaults to -1, gap = 6 > 0
                Assert.assertTrue(state.hasPendingAck());

                state.onAckSent(5);
                Assert.assertFalse(state.hasPendingAck());
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testOnResumeAckCompleteLifecycle() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);
                FakeConsumerTudCache fake = installFakeTudCache(state, engine, lineConfig);

                fake.queueCommit(new String[]{"t"}, new String[]{"t~1"}, new long[]{10L});
                state.setHighestProcessedSequence(5);
                state.commit();

                Assert.assertEquals(1, state.getPendingAckSeqTxns().size());

                // ACK blocked
                state.onAckBlocked(5);
                Assert.assertEquals(1, state.getSendState());
                // pendingAckSeqTxns cleared after snapshot
                Assert.assertEquals(0, state.getPendingAckSeqTxns().size());

                // Resume completes
                state.onResumeAckComplete();
                Assert.assertEquals(0, state.getSendState());
                Assert.assertEquals(5, state.getLastAckedSequence());
                Assert.assertFalse(state.hasPendingAck());
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testOnResumeAckThenErrorCompleteLifecycle() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);

                state.setHighestProcessedSequence(5);

                // ACK blocked
                state.onAckBlocked(5);
                Assert.assertEquals(1, state.getSendState());

                // Error blocked while ACK in flight
                state.onErrorBlocked((byte) 6, 10, "write error");
                Assert.assertEquals(3, state.getSendState()); // SEND_STATE_RESUME_ACK_THEN_ERROR

                // Resume ACK completes
                state.onResumeAckComplete();
                Assert.assertEquals(0, state.getSendState());
                Assert.assertEquals(5, state.getLastAckedSequence());

                // Deferred error is still pending
                Assert.assertEquals(10, state.getDeferredErrorSequence());
                Assert.assertEquals(6, state.getDeferredErrorStatus());

                // Error sent
                state.onErrorSent();
                Assert.assertEquals(0, state.getSendState());
                Assert.assertEquals(-1, state.getDeferredErrorSequence());
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testShouldSendAckReturnsTrueWhenThresholdMet() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);

                // lastAckedSequence defaults to -1
                state.setHighestProcessedSequence(9);
                // gap = 9 - (-1) = 10
                Assert.assertTrue(state.shouldSendAck(10));
                Assert.assertTrue(state.shouldSendAck(1));
                Assert.assertFalse(state.shouldSendAck(11));

                state.onAckSent(9);
                // gap = 9 - 9 = 0
                Assert.assertFalse(state.shouldSendAck(1));
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    private static int fieldSize(QwpIngressProcessorState state, String fieldName) throws Exception {
        Field f = QwpIngressProcessorState.class.getDeclaredField(fieldName);
        f.setAccessible(true);
        Object map = f.get(state);
        // Both CharSequenceLongHashMap and CharSequenceObjHashMap expose size().
        return (int) map.getClass().getMethod("size").invoke(map);
    }

    @Test
    public void testIsDurableWorkFullyUploaded() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);
                state.setDurableAckEnabled(true);
                FakeConsumerTudCache fake = installFakeTudCache(state, engine, lineConfig);
                FakeDurableAckRegistry registry = new FakeDurableAckRegistry();

                // Nothing pending -> trivially covered.
                Assert.assertTrue(state.isDurableWorkFullyUploaded(registry));

                fake.queueCommit(
                        new String[]{"t1", "t2"},
                        new String[]{"t1~1", "t2~1"},
                        new long[]{10L, 20L}
                );
                state.setHighestProcessedSequence(0);
                state.commit();

                // No uploads at all.
                Assert.assertFalse(state.isDurableWorkFullyUploaded(registry));

                // One table lagging behind its committed seqTxn.
                registry.set("t1~1", 10L);
                registry.set("t2~1", 19L);
                Assert.assertFalse(state.isDurableWorkFullyUploaded(registry));

                // Watermarks caught up on both tables.
                registry.set("t2~1", 20L);
                Assert.assertTrue(state.isDurableWorkFullyUploaded(registry));

                // Coverage survives the durable-ack prune...
                state.collectDurableProgress(registry);
                state.onDurableAckSent();
                Assert.assertTrue(state.isDurableWorkFullyUploaded(registry));

                // ...and a fresh commit re-opens the window until its upload lands.
                fake.queueCommit(new String[]{"t1"}, new String[]{"t1~1"}, new long[]{11L});
                state.setHighestProcessedSequence(1);
                state.commit();
                Assert.assertFalse(state.isDurableWorkFullyUploaded(registry));
                registry.set("t1~1", 11L);
                Assert.assertTrue(state.isDurableWorkFullyUploaded(registry));
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testRoleChangeCloseDeferralLifecycle() throws Exception {
        assertMemoryLeak(() -> {
            long[] nowMicros = {0L};
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration) {
                        @Override
                        public MicrosecondClock getMicrosecondClock() {
                            return () -> nowMicros[0];
                        }
                    };
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);

                Assert.assertFalse(state.isRoleChangeCloseDeferred());
                Assert.assertFalse(state.isRoleChangeCloseGraceExpired());

                state.deferRoleChangeClose("replica access is read-only");
                Assert.assertTrue(state.isRoleChangeCloseDeferred());
                Assert.assertFalse(state.isRoleChangeCloseGraceExpired());
                Assert.assertEquals("replica access is read-only", state.getRoleChangeCloseReason().toString());

                // Follow-on gate hits must not extend the deadline or clobber the reason.
                nowMicros[0] = QwpIngressProcessorState.ROLE_CHANGE_CLOSE_UPLOAD_GRACE_MICROS - 1;
                state.deferRoleChangeClose("a different reason");
                Assert.assertEquals("replica access is read-only", state.getRoleChangeCloseReason().toString());
                Assert.assertFalse(state.isRoleChangeCloseGraceExpired());

                // The deferral spans messages: per-message resets must not drop it.
                state.clear();
                state.clearMessageState();
                Assert.assertTrue(state.isRoleChangeCloseDeferred());

                // Grace budget exhausts exactly at the deadline.
                nowMicros[0] = QwpIngressProcessorState.ROLE_CHANGE_CLOSE_UPLOAD_GRACE_MICROS;
                Assert.assertTrue(state.isRoleChangeCloseGraceExpired());

                // Connection recycle resets the deferral.
                state.onDisconnected();
                Assert.assertFalse(state.isRoleChangeCloseDeferred());
                Assert.assertFalse(state.isRoleChangeCloseGraceExpired());
                Assert.assertEquals(0, state.getRoleChangeCloseReason().length());
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testOnFatalCloseBlockedFromResumeCloseClearsDeferredClose() throws Exception {
        // The already-RESUME_CLOSE branch of onFatalCloseBlocked: a previous fatal close was partially
        // flushed (onFatalCloseSendBlocked parked the CLOSE frame bytes and moved sendState to
        // RESUME_CLOSE). A second fatal-close attempt behind it must NOT re-defer a code/reason -- the
        // parked bytes ARE the CLOSE frame; the resume path finishes flushing them and disconnects. So
        // the branch clears the just-stored deferred code/reason and leaves sendState at RESUME_CLOSE.
        // Driven through the public API (no reflection) to pin the real production path.
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);

                // Park a partially-flushed CLOSE frame: sendState -> RESUME_CLOSE, deferred close cleared.
                state.onFatalCloseSendBlocked();
                final int resumeClose = state.getSendState();
                Assert.assertFalse("precondition: must not be READY", state.isSendReady());
                Assert.assertEquals(-1, state.getDeferredCloseCode());

                // A second fatal close arrives while the CLOSE frame is still parked.
                state.onFatalCloseBlocked(1011, "internal error");

                // The branch is idempotent: the parked CLOSE frame stands, the redundant code/reason are
                // discarded, and the state stays RESUME_CLOSE (never re-deferred).
                Assert.assertEquals("sendState must stay RESUME_CLOSE", resumeClose, state.getSendState());
                Assert.assertEquals("deferred close code must be cleared", -1, state.getDeferredCloseCode());
                Assert.assertEquals("deferred close reason must be cleared", 0, state.getDeferredCloseReason().length());

                // Idempotent under repetition (a re-entered deferral must not resurrect a code/reason).
                state.onFatalCloseBlocked(1013, "try again later");
                Assert.assertEquals(resumeClose, state.getSendState());
                Assert.assertEquals(-1, state.getDeferredCloseCode());
                Assert.assertEquals(0, state.getDeferredCloseReason().length());

                // Null reason path through the same branch.
                state.onFatalCloseBlocked(1000, null);
                Assert.assertEquals(resumeClose, state.getSendState());
                Assert.assertEquals(-1, state.getDeferredCloseCode());
                Assert.assertEquals(0, state.getDeferredCloseReason().length());
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testOnFatalCloseBlockedTransitionTableCoversAllSendStates() throws Exception {
        // Exhaustive transition table for onFatalCloseBlocked across every input sendState. Pins the
        // full routing contract, including the RESUME_CLOSE idempotent branch and the ack/durable-ack
        // collapse-to-*_THEN_CLOSE arms that keep the deferred code/reason for the resume path.
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);

                final int READY = sendStateConst("SEND_STATE_READY");
                final int RESUME_ACK = sendStateConst("SEND_STATE_RESUME_ACK");
                final int RESUME_ERROR = sendStateConst("SEND_STATE_RESUME_ERROR");
                final int RESUME_ACK_THEN_ERROR = sendStateConst("SEND_STATE_RESUME_ACK_THEN_ERROR");
                final int RESUME_DURABLE_ACK = sendStateConst("SEND_STATE_RESUME_DURABLE_ACK");
                final int RESUME_DURABLE_ACK_THEN_ERROR = sendStateConst("SEND_STATE_RESUME_DURABLE_ACK_THEN_ERROR");
                final int RESUME_CLOSE = sendStateConst("SEND_STATE_RESUME_CLOSE");
                final int RESUME_ACK_THEN_CLOSE = sendStateConst("SEND_STATE_RESUME_ACK_THEN_CLOSE");
                final int RESUME_DURABLE_ACK_THEN_CLOSE = sendStateConst("SEND_STATE_RESUME_DURABLE_ACK_THEN_CLOSE");
                final int RESUME_PONG = sendStateConst("SEND_STATE_RESUME_PONG");
                final int RESUME_DRAIN_THEN_CLOSE = sendStateConst("SEND_STATE_RESUME_DRAIN_THEN_CLOSE");

                // ACK-family inputs collapse to RESUME_ACK_THEN_CLOSE, RETAINING the deferred code/reason.
                for (int in : new int[]{RESUME_ACK, RESUME_ACK_THEN_ERROR, RESUME_ACK_THEN_CLOSE}) {
                    setSendState(state, in);
                    state.onFatalCloseBlocked(1011, "boom");
                    Assert.assertEquals("input=" + in, RESUME_ACK_THEN_CLOSE, state.getSendState());
                    Assert.assertEquals("input=" + in, 1011, state.getDeferredCloseCode());
                    Assert.assertEquals("input=" + in, "boom", state.getDeferredCloseReason().toString());
                }

                // DURABLE-ACK-family inputs collapse to RESUME_DURABLE_ACK_THEN_CLOSE, RETAINING code/reason.
                for (int in : new int[]{RESUME_DURABLE_ACK, RESUME_DURABLE_ACK_THEN_ERROR, RESUME_DURABLE_ACK_THEN_CLOSE}) {
                    setSendState(state, in);
                    state.onFatalCloseBlocked(1012, "later");
                    Assert.assertEquals("input=" + in, RESUME_DURABLE_ACK_THEN_CLOSE, state.getSendState());
                    Assert.assertEquals("input=" + in, 1012, state.getDeferredCloseCode());
                    Assert.assertEquals("input=" + in, "later", state.getDeferredCloseReason().toString());
                }

                // RESUME_CLOSE stays put and CLEARS the redundant code/reason (parked bytes ARE the CLOSE).
                setSendState(state, RESUME_CLOSE);
                state.onFatalCloseBlocked(1011, "boom");
                Assert.assertEquals(RESUME_CLOSE, state.getSendState());
                Assert.assertEquals(-1, state.getDeferredCloseCode());
                Assert.assertEquals(0, state.getDeferredCloseReason().length());

                // All other inputs park behind a non-ack response: drain-then-close, RETAINING code/reason.
                for (int in : new int[]{READY, RESUME_ERROR, RESUME_PONG, RESUME_DRAIN_THEN_CLOSE}) {
                    setSendState(state, in);
                    state.onFatalCloseBlocked(1001, "going away");
                    Assert.assertEquals("input=" + in, RESUME_DRAIN_THEN_CLOSE, state.getSendState());
                    Assert.assertEquals("input=" + in, 1001, state.getDeferredCloseCode());
                    Assert.assertEquals("input=" + in, "going away", state.getDeferredCloseReason().toString());
                }
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    @Test
    public void testOnFatalCloseBlockedFuzz() throws Exception {
        // Property fuzz over onFatalCloseBlocked: for a random input sendState, random close code and
        // random reason (null / empty / non-empty), the method must never throw, must always leave the
        // connection in a terminal close-bearing state, and must obey the retain-vs-clear contract:
        //   RESUME_CLOSE   -> stays RESUME_CLOSE, deferred code/reason CLEARED
        //   ACK family     -> RESUME_ACK_THEN_CLOSE, code/reason RETAINED
        //   DURABLE family -> RESUME_DURABLE_ACK_THEN_CLOSE, code/reason RETAINED
        //   everything else-> RESUME_DRAIN_THEN_CLOSE, code/reason RETAINED
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpIngressProcessorState state = new QwpIngressProcessorState(1024, 4096, engine, lineConfig);
            try {
                state.of(1, AllowAllSecurityContext.INSTANCE);

                final int READY = sendStateConst("SEND_STATE_READY");
                final int RESUME_ACK = sendStateConst("SEND_STATE_RESUME_ACK");
                final int RESUME_ERROR = sendStateConst("SEND_STATE_RESUME_ERROR");
                final int RESUME_ACK_THEN_ERROR = sendStateConst("SEND_STATE_RESUME_ACK_THEN_ERROR");
                final int RESUME_DURABLE_ACK = sendStateConst("SEND_STATE_RESUME_DURABLE_ACK");
                final int RESUME_DURABLE_ACK_THEN_ERROR = sendStateConst("SEND_STATE_RESUME_DURABLE_ACK_THEN_ERROR");
                final int RESUME_CLOSE = sendStateConst("SEND_STATE_RESUME_CLOSE");
                final int RESUME_ACK_THEN_CLOSE = sendStateConst("SEND_STATE_RESUME_ACK_THEN_CLOSE");
                final int RESUME_DURABLE_ACK_THEN_CLOSE = sendStateConst("SEND_STATE_RESUME_DURABLE_ACK_THEN_CLOSE");
                final int RESUME_PONG = sendStateConst("SEND_STATE_RESUME_PONG");
                final int RESUME_DRAIN_THEN_CLOSE = sendStateConst("SEND_STATE_RESUME_DRAIN_THEN_CLOSE");

                final int[] inputs = {
                        READY, RESUME_ACK, RESUME_ERROR, RESUME_ACK_THEN_ERROR, RESUME_DURABLE_ACK,
                        RESUME_DURABLE_ACK_THEN_ERROR, RESUME_CLOSE, RESUME_ACK_THEN_CLOSE,
                        RESUME_DURABLE_ACK_THEN_CLOSE, RESUME_PONG, RESUME_DRAIN_THEN_CLOSE
                };

                final long seed = System.nanoTime();
                final Rnd rnd = new Rnd(seed, seed ^ 0x9E3779B97F4A7C15L);
                final String msg = "onFatalCloseBlocked fuzz seed=" + seed;
                for (int iter = 0; iter < 50_000; iter++) {
                    final int in = inputs[rnd.nextInt(inputs.length)];
                    final int code = rnd.nextInt();
                    final int reasonKind = rnd.nextInt(3);
                    final String reason = reasonKind == 0 ? null : (reasonKind == 1 ? "" : "r" + rnd.nextInt(1000));

                    setSendState(state, in);
                    state.onFatalCloseBlocked(code, reason);

                    final int out = state.getSendState();
                    if (in == RESUME_ACK || in == RESUME_ACK_THEN_ERROR || in == RESUME_ACK_THEN_CLOSE) {
                        Assert.assertEquals(msg, RESUME_ACK_THEN_CLOSE, out);
                        Assert.assertEquals(msg, code, state.getDeferredCloseCode());
                        assertReason(msg, reason, state.getDeferredCloseReason());
                    } else if (in == RESUME_DURABLE_ACK || in == RESUME_DURABLE_ACK_THEN_ERROR || in == RESUME_DURABLE_ACK_THEN_CLOSE) {
                        Assert.assertEquals(msg, RESUME_DURABLE_ACK_THEN_CLOSE, out);
                        Assert.assertEquals(msg, code, state.getDeferredCloseCode());
                        assertReason(msg, reason, state.getDeferredCloseReason());
                    } else if (in == RESUME_CLOSE) {
                        Assert.assertEquals(msg, RESUME_CLOSE, out);
                        Assert.assertEquals(msg, -1, state.getDeferredCloseCode());
                        Assert.assertEquals(msg, 0, state.getDeferredCloseReason().length());
                    } else {
                        Assert.assertEquals(msg, RESUME_DRAIN_THEN_CLOSE, out);
                        Assert.assertEquals(msg, code, state.getDeferredCloseCode());
                        assertReason(msg, reason, state.getDeferredCloseReason());
                    }
                }
            } finally {
                state.onDisconnected();
                state.close();
            }
        });
    }

    private static void assertReason(String msg, String expected, CharSequence actual) {
        if (expected == null || expected.isEmpty()) {
            Assert.assertEquals(msg, 0, actual.length());
        } else {
            Assert.assertEquals(msg, expected, actual.toString());
        }
    }

    private static int sendStateConst(String name) throws Exception {
        Field f = QwpIngressProcessorState.class.getDeclaredField(name);
        f.setAccessible(true);
        return f.getInt(null);
    }

    private static void setSendState(QwpIngressProcessorState state, int value) throws Exception {
        Field f = QwpIngressProcessorState.class.getDeclaredField("sendState");
        f.setAccessible(true);
        f.setInt(state, value);
    }

    private static FakeConsumerTudCache installFakeTudCache(
            QwpIngressProcessorState state, io.questdb.cairo.CairoEngine engine, LineHttpProcessorConfiguration lineConfig
    ) throws Exception {
        Field f = QwpIngressProcessorState.class.getDeclaredField("tudCache");
        f.setAccessible(true);
        Misc.free((QwpTudCache) f.get(state));
        FakeConsumerTudCache fake = new FakeConsumerTudCache(engine, lineConfig);
        f.set(state, fake);
        return fake;
    }

    private static void addNativeData(QwpIngressProcessorState state, byte[] data) {
        long ptr = Unsafe.malloc(data.length, MemoryTag.NATIVE_HTTP_CONN);
        try {
            for (int i = 0; i < data.length; i++) {
                Unsafe.putByte(ptr + i, data[i]);
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
                (_, method, _) -> switch (method.getName()) {
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
        return wrapQwpPayload(payload, (byte) 0);
    }

    private static byte[] wrapQwpPayload(byte[] payload, byte flags) {
        byte[] message = new byte[12 + payload.length];
        message[0] = 'Q';
        message[1] = 'W';
        message[2] = 'P';
        message[3] = '1';
        message[4] = 1; // version
        message[5] = flags;
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
        private String[] commitDirNames;
        private long[] commitSeqTxns;
        private String[] commitTableNames;
        private Throwable commitThrow;
        private String[] maxRowsCommitDirNames;
        private long[] maxRowsCommitSeqTxns;
        private String[] maxRowsCommitTableNames;
        private Throwable maxRowsCommitThrow;
        private int maxRowsCommitCallCount;

        FakeConsumerTudCache(io.questdb.cairo.CairoEngine engine, LineHttpProcessorConfiguration lineConfig) {
            super(engine, true, true, new DefaultColumnTypes(lineConfig), PartitionBy.DAY);
        }

        @Override
        public void commitAll(CommittedTxnConsumer consumer) throws Throwable {
            if (consumer != null && commitTableNames != null) {
                for (int i = 0; i < commitTableNames.length; i++) {
                    consumer.accept(commitTableNames[i], commitDirNames[i], commitSeqTxns[i]);
                }
            }
            commitTableNames = null;
            commitDirNames = null;
            commitSeqTxns = null;
            if (commitThrow != null) {
                Throwable t = commitThrow;
                commitThrow = null;
                throw t;
            }
        }

        @Override
        public void commitIfMaxUncommittedRowsReached(CommittedTxnConsumer consumer) throws Throwable {
            maxRowsCommitCallCount++;
            if (consumer != null && maxRowsCommitTableNames != null) {
                for (int i = 0; i < maxRowsCommitTableNames.length; i++) {
                    consumer.accept(maxRowsCommitTableNames[i], maxRowsCommitDirNames[i], maxRowsCommitSeqTxns[i]);
                }
            }
            maxRowsCommitTableNames = null;
            maxRowsCommitDirNames = null;
            maxRowsCommitSeqTxns = null;
            if (maxRowsCommitThrow != null) {
                Throwable t = maxRowsCommitThrow;
                maxRowsCommitThrow = null;
                throw t;
            }
        }

        void queueCommit(String[] tableNames, String[] dirNames, long[] seqTxns) {
            this.commitTableNames = tableNames;
            this.commitDirNames = dirNames;
            this.commitSeqTxns = seqTxns;
        }

        void queueCommitThrow(Throwable t) {
            this.commitThrow = t;
        }

        void queueMaxRowsCommit(String[] tableNames, String[] dirNames, long[] seqTxns) {
            this.maxRowsCommitTableNames = tableNames;
            this.maxRowsCommitDirNames = dirNames;
            this.maxRowsCommitSeqTxns = seqTxns;
        }

        void queueMaxRowsCommitThrow(Throwable t) {
            this.maxRowsCommitThrow = t;
        }

        int getMaxRowsCommitCallCount() {
            return maxRowsCommitCallCount;
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
