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
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.processors.LineHttpProcessorConfiguration;
import io.questdb.cutlass.line.tcp.DefaultColumnTypes;
import io.questdb.cutlass.line.tcp.TableUpdateDetails;
import io.questdb.cutlass.line.tcp.WalTableUpdateDetails;
import io.questdb.cutlass.qwp.server.QwpProcessorState;
import io.questdb.cutlass.qwp.server.QwpTudCache;
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
            QwpTudCache cache = new QwpTudCache(engine, true, true, defaultColumnTypes, PartitionBy.DAY);
            try {
                WalTableUpdateDetails tud = cache.getTableUpdateDetails(
                        AllowAllSecurityContext.INSTANCE,
                        new Utf8String("clear_distress"),
                        null,
                        null
                );
                Assert.assertNotNull(tud);

                // Close the TUD so its writerAPI becomes null.
                // This makes rollback() throw NullPointerException.
                tud.close();

                // clear() should catch the exception, enter the distressed
                // code path, free the TUD, and clear the map.
                cache.clear();
            } finally {
                cache.close();
            }
        });
    }

    @Test
    public void testCloseFreesWalWriterOnConstructorFailure() throws Exception {
        // Reproduces the pattern in QwpTudCache.getTableUpdateDetails() where
        // engine.getWalWriter() is passed inline to the WalTableUpdateDetails
        // constructor. If the constructor throws, the writer leaks.
        // The fix acquires the writer into a local variable and closes it
        // in a catch block.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test_leak (ts TIMESTAMP, val INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            TableToken tableToken = engine.verifyTableName("test_leak");

            TableWriterAPI walWriter = engine.getWalWriter(tableToken);
            try {
                // Passing null engine causes NPE in the TableUpdateDetails
                // constructor before the writer is stored, simulating any
                // constructor failure after the writer has been acquired.
                new WalTableUpdateDetails(
                        null,
                        null,
                        walWriter,
                        null,
                        new Utf8String("test_leak"),
                        null,
                        -1,
                        false,
                        Long.MAX_VALUE
                );
                Assert.fail("should have thrown NullPointerException");
            } catch (Throwable th) {
                Misc.free(walWriter);
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
            QwpTudCache cache = new QwpTudCache(engine, true, true, defaultColumnTypes, PartitionBy.DAY);
            try {
                WalTableUpdateDetails tud = cache.getTableUpdateDetails(
                        AllowAllSecurityContext.INSTANCE,
                        new Utf8String("commit_drop"),
                        null,
                        null
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
                try {
                    cache.commitAll();
                } catch (Exception e) {
                    throw e;
                } catch (Throwable t) {
                    throw new AssertionError("unexpected throwable", t);
                }
            } finally {
                cache.close();
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
            QwpTudCache cache = new QwpTudCache(engine, true, true, defaultColumnTypes, PartitionBy.DAY);
            try {
                WalTableUpdateDetails tud = cache.getTableUpdateDetails(
                        AllowAllSecurityContext.INSTANCE,
                        new Utf8String("commit_fail"),
                        null,
                        null
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
            } finally {
                cache.close();
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

    private static void replaceWriterWithFake(WalTableUpdateDetails tud, boolean isTableDropped) throws Exception {
        TableToken tableToken = tud.getTableToken();
        Field writerField = TableUpdateDetails.class.getDeclaredField("writerAPI");
        writerField.setAccessible(true);

        // Free the real writer to avoid native memory leaks.
        Misc.free((TableWriterAPI) writerField.get(tud));

        writerField.set(tud, Proxy.newProxyInstance(
                TableWriterAPI.class.getClassLoader(),
                new Class[]{TableWriterAPI.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "getUncommittedRowCount":
                            return 1L;
                        case "commit":
                            if (isTableDropped) {
                                throw CairoException.tableDropped(tableToken);
                            }
                            throw CairoException.nonCritical().put("simulated commit failure");
                        case "close":
                        case "rollback":
                            return null;
                        default:
                            throw new UnsupportedOperationException(method.getName());
                    }
                }
        ));
    }
}
