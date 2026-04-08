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
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.processors.LineHttpProcessorConfiguration;
import io.questdb.cutlass.line.tcp.DefaultColumnTypes;
import io.questdb.cutlass.line.tcp.TableUpdateDetails;
import io.questdb.cutlass.line.tcp.WalTableUpdateDetails;
import io.questdb.cutlass.qwp.protocol.QwpColumnDef;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.server.QwpProcessorState;
import io.questdb.cutlass.qwp.server.QwpTudCache;
import io.questdb.std.LowerCaseUtf8SequenceObjHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Proxy;

public class QwpProcessorStateTest extends AbstractCairoTest {

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
                QwpColumnDef[] schema = {
                        new QwpColumnDef("val", QwpConstants.TYPE_INT),
                        new QwpColumnDef("", QwpConstants.TYPE_TIMESTAMP_NANOS)
                };

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
    public void testGetTableUpdateDetailsReturnsNullForInvalidColumnName() throws Exception {
        assertMemoryLeak(() -> {
            LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            DefaultColumnTypes defaultColumnTypes = new DefaultColumnTypes(lineConfig);
            try (QwpTudCache cache = new QwpTudCache(
                    engine, true, true, defaultColumnTypes, PartitionBy.DAY)
            ) {
                QwpColumnDef[] schema = {
                        new QwpColumnDef("inv?lid", QwpConstants.TYPE_INT),
                        new QwpColumnDef("", QwpConstants.TYPE_TIMESTAMP)
                };

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

    @SuppressWarnings("unchecked")
    private static int getCacheSize(QwpTudCache cache) throws Exception {
        Field field = QwpTudCache.class.getDeclaredField("tableUpdateDetails");
        field.setAccessible(true);
        return ((LowerCaseUtf8SequenceObjHashMap<WalTableUpdateDetails>) field.get(cache)).size();
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
}
