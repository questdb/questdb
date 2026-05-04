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
import io.questdb.std.CharSequenceLongHashMap;
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
    public void testCollectDurableProgressMultiTable() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpProcessorState state = new QwpProcessorState(1024, 4096, engine, lineConfig);
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
            QwpProcessorState state = new QwpProcessorState(1024, 4096, engine, lineConfig);
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
            QwpProcessorState state = new QwpProcessorState(1024, 4096, engine, lineConfig);
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
            QwpProcessorState state = new QwpProcessorState(1024, 4096, engine, lineConfig);
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
            QwpProcessorState state = new QwpProcessorState(1024, 4096, engine, lineConfig);
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
            QwpProcessorState state = new QwpProcessorState(1024, 4096, engine, lineConfig);
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
            QwpProcessorState state = new QwpProcessorState(1024, 4096, engine, lineConfig);
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
            QwpProcessorState state = new QwpProcessorState(1024, 4096, engine, lineConfig);
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
            QwpProcessorState state = new QwpProcessorState(1024, 4096, engine, lineConfig);
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
            QwpProcessorState state = new QwpProcessorState(1024, 4096, engine, lineConfig);
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
            QwpProcessorState state = new QwpProcessorState(1024, 4096, engine, lineConfig);
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
            QwpProcessorState state = new QwpProcessorState(1024, 4096, engine, lineConfig);
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
            QwpProcessorState state = new QwpProcessorState(1024, 4096, engine, lineConfig);
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
            QwpProcessorState state = new QwpProcessorState(1024, 4096, engine, lineConfig);
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
            QwpProcessorState state = new QwpProcessorState(1024, 4096, engine, lineConfig);
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
    public void testCommitConsumerThrowRejectsState() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpProcessorState state = new QwpProcessorState(1024, 4096, engine, lineConfig);
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
            QwpProcessorState state = new QwpProcessorState(1024, 4096, engine, lineConfig);
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
            QwpProcessorState state = new QwpProcessorState(1024, 4096, engine, lineConfig);
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
            QwpProcessorState state = new QwpProcessorState(1024, 4096, engine, lineConfig);
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
    public void testComputeAckPayloadSizeMatchesWrittenBytes() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            QwpProcessorState state = new QwpProcessorState(1024, 4096, engine, lineConfig);
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
                    int tableBytes = QwpProcessorState.writeTableSeqTxnEntries(ptr + 9, state.getPendingAckSeqTxns());
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
            QwpProcessorState state = new QwpProcessorState(1024, 4096, engine, lineConfig);
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
                    int tableBytes = QwpProcessorState.writeTableSeqTxnEntries(ptr + 1, progress);
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

    @Test
    public void testWriteTableSeqTxnEntriesEmpty() throws Exception {
        assertMemoryLeak(() -> {
            CharSequenceLongHashMap entries = new CharSequenceLongHashMap();
            long ptr = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
            try {
                int written = QwpProcessorState.writeTableSeqTxnEntries(ptr, entries);
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
                int written = QwpProcessorState.writeTableSeqTxnEntries(ptr, entries);
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
                int written = QwpProcessorState.writeTableSeqTxnEntries(ptr, entries);
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
                int written = QwpProcessorState.writeTableSeqTxnEntries(ptr, entries);
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
            QwpProcessorState state = new QwpProcessorState(1024, 4096, engine, lineConfig);
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
            QwpProcessorState state = new QwpProcessorState(1024, 4096, engine, lineConfig);
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
            QwpProcessorState state = new QwpProcessorState(1024, 4096, engine, lineConfig);
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
            QwpProcessorState state = new QwpProcessorState(1024, 4096, engine, lineConfig);
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

    private static int fieldSize(QwpProcessorState state, String fieldName) throws Exception {
        Field f = QwpProcessorState.class.getDeclaredField(fieldName);
        f.setAccessible(true);
        Object map = f.get(state);
        // Both CharSequenceLongHashMap and CharSequenceObjHashMap expose size().
        return (int) map.getClass().getMethod("size").invoke(map);
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

    private static void addNativeData(QwpProcessorState state, byte[] data) {
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
        private String[] commitDirNames;
        private long[] commitSeqTxns;
        private String[] commitTableNames;
        private Throwable commitThrow;

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

        void queueCommit(String[] tableNames, String[] dirNames, long[] seqTxns) {
            this.commitTableNames = tableNames;
            this.commitDirNames = dirNames;
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
